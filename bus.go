package coord

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"
)

// MaxPayload is the largest NOTIFY payload Postgres accepts, less a margin.
// Send ids and let the listener read the row, rather than the data itself.
const MaxPayload = 7900

// Notify sends payload on channel to every listening copy, this one
// included. Inside a transaction it is delivered when the transaction
// commits, and not at all if it rolls back, so it can never announce a row
// that other copies cannot yet read.
func Notify(ctx context.Context, db Execer, channel, payload string) error {
	if len(payload) > MaxPayload {
		return fmt.Errorf("coord: notify payload is %d bytes, over %d", len(payload), MaxPayload)
	}
	_, err := db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, channel, payload)
	return err
}

// Bus listens for notifications and hands each to the handlers subscribed
// to its channel. Each copy runs one, on a connection of its own.
//
// The connection must go straight to Postgres. LISTEN does not work through
// a transaction-pooling proxy such as RDS Proxy or PgBouncer.
//
// Notifications sent while the connection is down are lost. When it comes
// back, every OnReconnect handler runs, so each can recover what it might
// have missed: wake its waiters, or re-read the state it caches.
type Bus struct {
	dsn string
	log func(msg string, err error)

	mu          sync.RWMutex
	handlers    map[string][]func(payload string)
	reconnected []func()
	listener    *pq.Listener
}

// NewBus returns a Bus for the database at dsn (a lib/pq connection string).
// logf, if not nil, receives connection problems.
func NewBus(dsn string, logf func(msg string, err error)) *Bus {
	if logf == nil {
		logf = func(string, error) {}
	}
	return &Bus{dsn: dsn, log: logf, handlers: map[string][]func(string){}}
}

// Subscribe calls fn with the payload of every notification on channel.
// Handlers run one at a time, in the order notifications arrive, so a slow
// handler delays the others: hand slow work to a goroutine.
func (b *Bus) Subscribe(channel string, fn func(payload string)) error {
	b.mu.Lock()
	first := len(b.handlers[channel]) == 0
	b.handlers[channel] = append(b.handlers[channel], fn)
	l := b.listener
	b.mu.Unlock()

	if first && l != nil {
		if err := l.Listen(channel); err != nil && !errors.Is(err, pq.ErrChannelAlreadyOpen) {
			return err
		}
	}
	return nil
}

// OnReconnect calls fn each time the listening connection is re-established
// after being lost.
func (b *Bus) OnReconnect(fn func()) {
	b.mu.Lock()
	b.reconnected = append(b.reconnected, fn)
	b.mu.Unlock()
}

// Run listens until ctx is cancelled. Subscribe may be called before or
// after Run starts.
func (b *Bus) Run(ctx context.Context) error {
	l := pq.NewListener(b.dsn, time.Second, 30*time.Second, func(ev pq.ListenerEventType, err error) {
		switch ev {
		case pq.ListenerEventDisconnected:
			b.log("coord: bus disconnected", err)
		case pq.ListenerEventConnectionAttemptFailed:
			b.log("coord: bus reconnect failed", err)
		}
	})
	defer func() { _ = l.Close() }()

	b.mu.Lock()
	b.listener = l
	channels := make([]string, 0, len(b.handlers))
	for ch := range b.handlers {
		channels = append(channels, ch)
	}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.listener = nil
		b.mu.Unlock()
	}()

	for _, ch := range channels {
		if err := l.Listen(ch); err != nil && !errors.Is(err, pq.ErrChannelAlreadyOpen) {
			return err
		}
	}

	// A quiet connection can die without anyone noticing; ping it so a dead
	// one is found and replaced, and OnReconnect handlers run.
	ping := time.NewTicker(60 * time.Second)
	defer ping.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case n := <-l.Notify:
			if n == nil {
				// lib/pq sends nil after re-establishing the connection.
				b.mu.RLock()
				fns := append([]func(){}, b.reconnected...)
				b.mu.RUnlock()
				for _, fn := range fns {
					fn()
				}
				continue
			}
			b.mu.RLock()
			fns := append([]func(string){}, b.handlers[n.Channel]...)
			b.mu.RUnlock()
			for _, fn := range fns {
				fn(n.Extra)
			}
		case <-ping.C:
			go func() { _ = l.Ping() }()
		}
	}
}
