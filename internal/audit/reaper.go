package audit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrReapFailed reports that the reaper could not complete a sweep. A failed
// reap loses nothing: the rows it did not delete are still there.
var ErrReapFailed = errors.New("audit: reaping the outbox failed")

// maxReapLimit bounds one sweep so the DELETE holds locks for a bounded time.
const maxReapLimit = 10_000

// Reap deletes outbox events that were published before cutoff and returns how
// many it removed.
//
// THE INVARIANT: an event that has NOT been published is never deleted, at any
// age. Reaping an unpublished row silently breaks the outbox's only promise,
// and its absence is indistinguishable from its never having existed.
// `published_at IS NOT NULL` is stated even though `published_at < $1` already
// excludes NULL, so the rule survives someone rewriting the window.
//
// FOR UPDATE SKIP LOCKED means a row the publisher holds is stepped over and
// reaped by a later sweep.
func Reap(
	ctx context.Context,
	pool *pgxpool.Pool,
	schema authschema.ValidatedIdentifier,
	cutoff time.Time,
	limit int,
) (int64, error) {
	if pool == nil {
		return 0, fmt.Errorf("%w: nil pool", ErrReapFailed)
	}
	if limit <= 0 || limit > maxReapLimit {
		return 0, fmt.Errorf("%w: limit %d is outside 1..%d", ErrReapFailed, limit, maxReapLimit)
	}
	if cutoff.IsZero() {
		return 0, fmt.Errorf("%w: cutoff is the zero time", ErrReapFailed)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("%w: beginning the sweep: %w", ErrReapFailed, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := armTx(ctx, tx); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrReapFailed, err)
	}

	table := authschema.Quote(schema) + ".auth_outbox_events"
	tag, err := tx.Exec(ctx, reapStatement(table), cutoff.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("%w: deleting published events: %w", ErrReapFailed, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: committing the sweep: %w", ErrReapFailed, err)
	}
	return tag.RowsAffected(), nil
}

// reapSelection picks the oldest published rows past the cutoff. It is split
// out so the plan test explains exactly the text Reap runs.
func reapSelection(table string) string {
	return `SELECT e.id
		    FROM ` + table + ` AS e
		    WHERE e.published_at IS NOT NULL
		      AND e.published_at < $1
		    ORDER BY e.published_at, e.id
		    FOR UPDATE SKIP LOCKED
		    LIMIT $2`
}

func reapStatement(table string) string {
	return `
		WITH reapable AS (
		    ` + reapSelection(table) + `
		)
		DELETE FROM ` + table + ` AS e
		USING reapable
		WHERE e.id = reapable.id`
}
