package coord

import (
	"context"
	"database/sql"
	"time"
)

// RegistrySchema is the table the Registry uses. A service that uses the
// Registry adds it in one of its own migrations: this package never changes
// a schema.
const RegistrySchema = `
CREATE TABLE IF NOT EXISTS service_instance (
    id           UUID        PRIMARY KEY,
    service      TEXT        NOT NULL,
    address      TEXT        NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_service_instance_live ON service_instance (service, heartbeat_at);
`

// Registry records that this copy of a service is alive and where other
// copies can reach it, so work held in one copy's memory (a live voice
// call) can be forwarded to it.
type Registry struct {
	db       *sql.DB
	id       string
	service  string
	address  string
	interval time.Duration
}

// Instance is one live copy of a service.
type Instance struct {
	ID          string
	Address     string
	HeartbeatAt time.Time
}

// NewRegistry returns a Registry entry for this copy. id must be unique per
// copy, such as a UUID made at start-up; address is how other copies reach
// this one, such as its private IP and internal port.
func NewRegistry(db *sql.DB, service, id, address string) *Registry {
	return &Registry{db: db, id: id, service: service, address: address, interval: 10 * time.Second}
}

// ID is this copy's instance id.
func (r *Registry) ID() string { return r.id }

// Stale is how long without a heartbeat before a copy is treated as gone.
func (r *Registry) Stale() time.Duration { return 3 * r.interval }

// Run records this copy and heartbeats until ctx is cancelled, then removes
// the record so other copies stop forwarding to it at once.
func (r *Registry) Run(ctx context.Context) {
	beat := func() {
		_, _ = r.db.ExecContext(ctx, `
			INSERT INTO service_instance (id, service, address) VALUES ($1, $2, $3)
			ON CONFLICT (id) DO UPDATE SET service = EXCLUDED.service, address = EXCLUDED.address, heartbeat_at = NOW()`,
			r.id, r.service, r.address)
	}
	beat()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = r.db.ExecContext(cleanup, `DELETE FROM service_instance WHERE id = $1`, r.id)
			cancel()
			return
		case <-ticker.C:
			beat()
		}
	}
}

// Lookup returns the live copy with the given id, or nil if it is gone.
func (r *Registry) Lookup(ctx context.Context, id string) (*Instance, error) {
	var in Instance
	err := r.db.QueryRowContext(ctx, `
		SELECT id, address, heartbeat_at FROM service_instance
		WHERE id = $1 AND service = $2 AND heartbeat_at > NOW() - $3::interval`,
		id, r.service, r.Stale().String()).Scan(&in.ID, &in.Address, &in.HeartbeatAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &in, nil
}

// Live returns every live copy of this service, this one included.
func (r *Registry) Live(ctx context.Context) ([]Instance, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, address, heartbeat_at FROM service_instance
		WHERE service = $1 AND heartbeat_at > NOW() - $2::interval
		ORDER BY started_at`,
		r.service, r.Stale().String())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Instance
	for rows.Next() {
		var in Instance
		if err := rows.Scan(&in.ID, &in.Address, &in.HeartbeatAt); err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}
