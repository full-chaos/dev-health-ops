package restorefence

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrNotFenced reports that the current timeline has no fence record: the
// database was restored or promoted and `auth-migrate restore-fence apply` has
// not run for it.
var ErrNotFenced = errors.New("restorefence: the current timeline has no fence record")

// ErrCheckFailed reports that the fence state could not be read. The check
// fails closed on it.
var ErrCheckFailed = errors.New("restorefence: reading the fence state failed")

// Querier is the one method Check needs; a pool and a transaction satisfy it.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Check is the runtime's readiness probe. It returns nil only when
// restore_fence_state holds a row for the timeline the server is on now. A
// server still in recovery, a missing function and a failed read are all
// "not ready".
func Check(ctx context.Context, q Querier, schema authschema.ValidatedIdentifier) error {
	s := authschema.Quote(schema)
	var fenced bool
	err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+s+`.restore_fence_state WHERE timeline_id = `+s+`.restore_fence_timeline())`).Scan(&fenced)
	if err != nil {
		return fmt.Errorf("%w", ErrCheckFailed)
	}
	if !fenced {
		return ErrNotFenced
	}
	return nil
}

// Timeline returns the timeline id the server is on, or nil while it is in
// recovery.
func Timeline(ctx context.Context, q Querier, schema authschema.ValidatedIdentifier) (*int64, error) {
	var id *int64
	if err := q.QueryRow(ctx, `SELECT `+authschema.Quote(schema)+`.restore_fence_timeline()`).Scan(&id); err != nil {
		return nil, fmt.Errorf("%w", ErrCheckFailed)
	}
	return id, nil
}
