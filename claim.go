package coord

import (
	"context"
	"database/sql"
	"fmt"
)

// Execer is satisfied by *sql.DB, *sql.Tx, *sql.Conn and the sqlx types.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Claim runs a conditional UPDATE that takes one row of work, and reports
// whether this caller got it. The query's WHERE clause is the claim: it must
// match the row only while nobody else holds it, and the SET must make it
// stop matching. For example:
//
//	UPDATE reminder SET sending_at = NOW()
//	WHERE id = $1 AND sent_at IS NULL
//	  AND (sending_at IS NULL OR sending_at < NOW() - interval '10 minutes')
//
// Every copy can try at once; Postgres lets exactly one UPDATE change the
// row. The same condition fences a copy that stalled past its lease: by the
// time it tries to record the outcome, the row has moved on, and an update
// made with the same condition changes nothing.
//
// A query that changes more than one row is an error, because then the
// condition is not selecting one row of work.
func Claim(ctx context.Context, db Execer, query string, args ...any) (bool, error) {
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n > 1 {
		return false, fmt.Errorf("coord: claim changed %d rows, want at most 1", n)
	}
	return n == 1, nil
}
