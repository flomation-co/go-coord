# coord

Coordination for running a Flomation service as more than one copy, using
only the Postgres database the service already has. No Redis, no queue.

| Piece | Use it for | How |
|---|---|---|
| `Leader` | A job that should run on one copy at a time (retention, backfills, a scheduler tick) | Session advisory lock on a pinned connection |
| `Claim` | Work per row that must happen once (send this email, charge this invoice) | Conditional `UPDATE`; act only if it changed the row |
| `Bus` / `Notify` | Live updates every copy must hear (a run is queued, a log line arrived) | `LISTEN`/`NOTIFY` |
| `Registry` | Finding the copy that holds something in memory (a live voice call) | Heartbeat rows in `service_instance` |

## Leader

```go
leader := coord.NewLeader(db, "billing-scheduler")
go leader.Run(ctx, nil) // cancel ctx on SIGTERM to hand over at once

for range ticker.C {
	if !leader.IsLeader() {
		continue
	}
	runTick()
}
```

If the copy dies, its session ends and Postgres releases the lock; another
copy takes over within about ten seconds. Leadership can be lost mid-tick, so
work that must not happen twice still needs a `Claim`.

Job names are hashed to lock keys and must be distinct across every service
sharing a database. Prefix them with the service name.

## Claim

```go
ok, err := coord.Claim(ctx, db, `
	UPDATE reminder SET sending_at = NOW()
	WHERE id = $1 AND sent_at IS NULL
	  AND (sending_at IS NULL OR sending_at < NOW() - interval '10 minutes')`, id)
if err != nil || !ok {
	return // another copy has it
}
```

The `WHERE` clause must match the row only while it is free, and the `SET`
must stop it matching. A claim that changes more than one row is an error.

## Bus

```go
bus := coord.NewBus(dsn, func(msg string, err error) { log.WithError(err).Warn(msg) })
_ = bus.Subscribe("execution_ready", func(id string) { notifier.Wake(id) })
bus.OnReconnect(notifier.WakeAll) // notifications sent while down are lost
go bus.Run(ctx)

_ = coord.Notify(ctx, tx, "execution_ready", id) // delivered on commit
```

Send ids, not data: payloads are limited to about 8 KB, and the listener can
read the row. The bus needs a direct connection; `LISTEN` does not work
through RDS Proxy or PgBouncer in transaction mode. Count it in the
connection budget: one per copy.

## Registry

Add `coord.RegistrySchema` to one of the service's own migrations; this
package never changes a schema.

```go
reg := coord.NewRegistry(db, "launch", instanceID, privateIP+":9443")
go reg.Run(ctx) // removes the row on shutdown
owner, err := reg.Lookup(ctx, ownerID) // nil once it is gone or stale
```

## Tests

They run against a real Postgres:

```sh
docker run -d --rm -p 55433:5432 -e POSTGRES_PASSWORD=pw postgres:17
COORD_TEST_POSTGRES_DSN='postgres://postgres:pw@localhost:55433/postgres?sslmode=disable' go test -race ./...
```
