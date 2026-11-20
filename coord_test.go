package coord

// Tests run against a real PostgreSQL, because every guarantee here is one
// Postgres makes. Skipped unless COORD_TEST_POSTGRES_DSN is set, e.g.:
//
//	docker run -d --rm -p 55433:5432 -e POSTGRES_PASSWORD=pw postgres:17
//	COORD_TEST_POSTGRES_DSN='postgres://postgres:pw@localhost:55433/postgres?sslmode=disable' go test ./...

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func testDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("COORD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set COORD_TEST_POSTGRES_DSN to run coord tests")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("cannot reach PostgreSQL: %v", err)
	}
	return db, dsn
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func fastLeader(db *sql.DB, name string) *Leader {
	l := NewLeader(db, name)
	l.retry = 50 * time.Millisecond
	l.check = 50 * time.Millisecond
	return l
}

func countLeaders(ls []*Leader) int {
	n := 0
	for _, l := range ls {
		if l.IsLeader() {
			n++
		}
	}
	return n
}

func TestLeaderOnlyOneLeads(t *testing.T) {
	db, _ := testDB(t)
	name := fmt.Sprintf("test-one-%d", time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ls []*Leader
	for i := 0; i < 4; i++ {
		l := fastLeader(db, name)
		ls = append(ls, l)
		go l.Run(ctx, nil)
	}

	eventually(t, 3*time.Second, "a leader", func() bool { return countLeaders(ls) == 1 })
	// Stays at one for a while: nobody else takes it while it is held.
	for i := 0; i < 20; i++ {
		if n := countLeaders(ls); n != 1 {
			t.Fatalf("%d leaders", n)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestLeaderHandsOverOnShutdown(t *testing.T) {
	db, _ := testDB(t)
	name := fmt.Sprintf("test-handover-%d", time.Now().UnixNano())

	ctxA, cancelA := context.WithCancel(context.Background())
	a := fastLeader(db, name)
	doneA := make(chan struct{})
	go func() { a.Run(ctxA, nil); close(doneA) }()
	eventually(t, 3*time.Second, "a to lead", a.IsLeader)

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	b := fastLeader(db, name)
	go b.Run(ctxB, nil)
	time.Sleep(200 * time.Millisecond)
	if b.IsLeader() {
		t.Fatal("b leads while a holds the lock")
	}

	cancelA()
	<-doneA
	if a.IsLeader() {
		t.Fatal("a still reports leading after Run returned")
	}
	eventually(t, 3*time.Second, "b to take over", b.IsLeader)
}

// If the leader's session dies (the copy crashes, the network drops),
// Postgres releases the lock and another copy takes over.
func TestLeaderTakeoverWhenSessionDies(t *testing.T) {
	db, _ := testDB(t)
	name := fmt.Sprintf("test-dies-%d", time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, b := fastLeader(db, name), fastLeader(db, name)
	go a.Run(ctx, nil)
	eventually(t, 3*time.Second, "a to lead", a.IsLeader)
	go b.Run(ctx, nil)

	// Kill whichever backend holds the lock.
	var killed bool
	err := db.QueryRow(`
		SELECT pg_terminate_backend(pid) FROM pg_locks
		WHERE locktype = 'advisory' AND granted
		  AND ((classid::bigint << 32) | objid::bigint) = $1`, a.key).Scan(&killed)
	if err != nil || !killed {
		t.Fatalf("could not terminate the leader's session: %v", err)
	}

	eventually(t, 3*time.Second, "exactly one new leader", func() bool {
		return countLeaders([]*Leader{a, b}) == 1
	})
}

// A lock must never be left on a pooled connection after leading ends.
func TestLeaderLeavesNoLockBehind(t *testing.T) {
	db, _ := testDB(t)
	name := fmt.Sprintf("test-nolock-%d", time.Now().UnixNano())
	l := fastLeader(db, name)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { l.Run(ctx, nil); close(done) }()
	eventually(t, 3*time.Second, "leading", l.IsLeader)
	cancel()
	<-done

	eventually(t, 3*time.Second, "lock released", func() bool {
		var n int
		_ = db.QueryRow(`SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory'
			AND ((classid::bigint << 32) | objid::bigint) = $1`, l.key).Scan(&n)
		return n == 0
	})
}

func TestLeaderOnChange(t *testing.T) {
	db, _ := testDB(t)
	l := fastLeader(db, fmt.Sprintf("test-change-%d", time.Now().UnixNano()))
	var mu sync.Mutex
	var seen []bool
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		l.Run(ctx, func(leading bool) { mu.Lock(); seen = append(seen, leading); mu.Unlock() })
		close(done)
	}()
	eventually(t, 3*time.Second, "leading", l.IsLeader)
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || !seen[0] || seen[1] {
		t.Fatalf("onChange calls %v, want [true false]", seen)
	}
}

func TestClaimHasOneWinner(t *testing.T) {
	db, _ := testDB(t)
	table := fmt.Sprintf("claim_test_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE TABLE ` + table + ` (id INT PRIMARY KEY, claimed_at TIMESTAMPTZ)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE ` + table) })
	if _, err := db.Exec(`INSERT INTO ` + table + ` (id) VALUES (1)`); err != nil {
		t.Fatal(err)
	}

	q := `UPDATE ` + table + ` SET claimed_at = NOW() WHERE id = $1 AND claimed_at IS NULL`
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := Claim(context.Background(), db, q, 1)
			if err != nil {
				t.Error(err)
			}
			if ok {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d winners", wins.Load())
	}
}

func TestClaimRefusesAMultiRowQuery(t *testing.T) {
	db, _ := testDB(t)
	table := fmt.Sprintf("claim_multi_%d", time.Now().UnixNano())
	if _, err := db.Exec(`CREATE TABLE ` + table + ` (id INT, claimed BOOL DEFAULT FALSE)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DROP TABLE ` + table) })
	_, _ = db.Exec(`INSERT INTO ` + table + ` (id) VALUES (1), (2)`)
	if _, err := Claim(context.Background(), db, `UPDATE `+table+` SET claimed = TRUE`); err == nil {
		t.Fatal("want an error for a claim that changed two rows")
	}
}

func TestBusDeliversAndReconnects(t *testing.T) {
	db, dsn := testDB(t)
	channel := fmt.Sprintf("coord_test_%d", time.Now().UnixNano())

	bus := NewBus(dsn, nil)
	got := make(chan string, 10)
	if err := bus.Subscribe(channel, func(p string) { got <- p }); err != nil {
		t.Fatal(err)
	}
	var reconnects atomic.Int32
	bus.OnReconnect(func() { reconnects.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = bus.Run(ctx) }()

	// The listener connects asynchronously; send until one arrives.
	eventually(t, 5*time.Second, "first notification", func() bool {
		_ = Notify(ctx, db, channel, "hello")
		select {
		case p := <-got:
			return p == "hello"
		case <-time.After(50 * time.Millisecond):
			return false
		}
	})

	// A notification in a rolled-back transaction is never delivered.
	tx, _ := db.Begin()
	_ = Notify(ctx, tx, channel, "rolled-back")
	_ = tx.Rollback()
	_ = Notify(ctx, db, channel, "after")
	for p := range got {
		if p == "rolled-back" {
			t.Fatal("a rolled-back notification was delivered")
		}
		if p == "after" {
			break
		}
	}

	// Kill the listening session; the bus reconnects and says so.
	if _, err := db.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE query ILIKE 'listen%' AND pid <> pg_backend_pid()`); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "reconnect", func() bool { return reconnects.Load() > 0 })
	eventually(t, 5*time.Second, "delivery after reconnect", func() bool {
		_ = Notify(ctx, db, channel, "again")
		select {
		case p := <-got:
			return p == "again"
		case <-time.After(50 * time.Millisecond):
			return false
		}
	})
}

func TestNotifyRefusesAnOversizedPayload(t *testing.T) {
	db, _ := testDB(t)
	big := make([]byte, MaxPayload+1)
	for i := range big {
		big[i] = 'x'
	}
	if err := Notify(context.Background(), db, "c", string(big)); err == nil {
		t.Fatal("want an error for an oversized payload")
	}
}

func TestRegistry(t *testing.T) {
	db, _ := testDB(t)
	if _, err := db.Exec(RegistrySchema); err != nil {
		t.Fatal(err)
	}
	service := fmt.Sprintf("svc-%d", time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	stamp := time.Now().UnixNano() % 1e12
	a := NewRegistry(db, service, fmt.Sprintf("00000000-0000-0000-000a-%012d", stamp), "10.0.0.1:9000")
	b := NewRegistry(db, service, fmt.Sprintf("00000000-0000-0000-000b-%012d", stamp), "10.0.0.2:9000")
	doneA := make(chan struct{})
	go func() { a.Run(ctx); close(doneA) }()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	go b.Run(ctxB)

	eventually(t, 3*time.Second, "both registered", func() bool {
		live, err := b.Live(context.Background())
		return err == nil && len(live) == 2
	})
	in, err := b.Lookup(context.Background(), a.ID())
	if err != nil || in == nil || in.Address != "10.0.0.1:9000" {
		t.Fatalf("lookup a: %+v, %v", in, err)
	}

	// Shutdown removes the record at once.
	cancel()
	<-doneA
	in, err = b.Lookup(context.Background(), a.ID())
	if err != nil || in != nil {
		t.Fatalf("a still found after shutdown: %+v, %v", in, err)
	}

	// A copy that stopped heartbeating is ignored.
	if _, err := db.Exec(`UPDATE service_instance SET heartbeat_at = NOW() - interval '1 hour' WHERE id = $1`, b.ID()); err != nil {
		t.Fatal(err)
	}
	if in, _ := a.Lookup(context.Background(), b.ID()); in != nil {
		t.Fatal("a stale copy was returned")
	}
}
