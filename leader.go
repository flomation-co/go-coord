// Package coord is what lets a Flomation service run as more than one copy,
// using only the Postgres database every service already has.
//
//   - Leader: one copy at a time runs a job, through a session advisory lock.
//   - Claim: take one row of work with a conditional UPDATE.
//   - Bus: pass live updates between copies with LISTEN/NOTIFY.
//   - Registry: record which copies are alive and how to reach them.
package coord

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"hash/fnv"
	"sync/atomic"
	"time"
)

// Leader holds a Postgres advisory lock for one named job. Every copy runs
// one; at most one of them holds the lock, and IsLeader reports whether this
// copy does.
//
// The lock is a session lock on one pinned connection. A lock taken through
// the pool could be released on a different connection than took it. If the
// connection or the copy dies, Postgres releases the lock and another copy
// takes it on its next attempt.
//
// Leadership can be lost at any moment, so a job that must not run twice
// should still claim its rows (see Claim). The lock stops the waste of every
// copy doing the work; the claim stops the harm.
type Leader struct {
	db    *sql.DB
	name  string
	key   int64
	retry time.Duration
	check time.Duration

	leading atomic.Bool
}

// NewLeader returns a Leader for the job called name. Different names lead
// independently, so different jobs can lead on different copies.
func NewLeader(db *sql.DB, name string) *Leader {
	return &Leader{
		db:    db,
		name:  name,
		key:   LockKey(name),
		retry: 10 * time.Second,
		check: 5 * time.Second,
	}
}

// LockKey is the advisory lock key for a job name. It is a hash, so names
// must be distinct across every service sharing a database.
func LockKey(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("flomation coord leader " + name))
	return int64(h.Sum64())
}

// Name is the job name this Leader was made for.
func (l *Leader) Name() string { return l.name }

// IsLeader reports whether this copy holds the lock right now.
func (l *Leader) IsLeader() bool { return l.leading.Load() }

// Run tries for the lock until ctx is cancelled, holding it while it can and
// trying again after losing it. onChange, if not nil, is called each time
// leadership is gained or lost. Run releases the lock before it returns, so
// call it in a goroutine and cancel ctx on shutdown to hand over promptly.
func (l *Leader) Run(ctx context.Context, onChange func(leading bool)) {
	for {
		l.hold(ctx, onChange)
		select {
		case <-ctx.Done():
			return
		case <-time.After(l.retry):
		}
	}
}

// hold takes the lock if it can and keeps it until ctx is cancelled or the
// connection fails.
func (l *Leader) hold(ctx context.Context, onChange func(bool)) {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()

	var got bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, l.key).Scan(&got); err != nil || !got {
		return
	}

	// Once the lock is held, the connection is never handed back to the pool:
	// a pooled session still holding the lock would keep every other copy
	// out with nobody leading. Marking it bad makes Close discard it, which
	// ends the session and releases the lock whatever else went wrong.
	defer func() { _ = conn.Raw(func(any) error { return driver.ErrBadConn }) }()

	l.set(true, onChange)
	defer l.set(false, onChange)

	ticker := time.NewTicker(l.check)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Unlock first so another copy can take over at once, before
			// this session has finished closing.
			unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = conn.ExecContext(unlockCtx, `SELECT pg_advisory_unlock($1)`, l.key)
			cancel()
			return
		case <-ticker.C:
			if err := conn.PingContext(ctx); err != nil {
				// The session, and with it the lock, may be gone. Stop
				// leading; another attempt starts on a new connection.
				return
			}
		}
	}
}

func (l *Leader) set(leading bool, onChange func(bool)) {
	if l.leading.Swap(leading) != leading && onChange != nil {
		onChange(leading)
	}
}
