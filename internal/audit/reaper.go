package audit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// ErrReapFailed reports that the reaper could not complete a sweep. It is
// distinct from ErrMutationFailed because a failed reap loses nothing: the rows
// it did not delete are still there and the next sweep sees them.
var ErrReapFailed = errors.New("audit: reaping the outbox failed")

// maxReapLimit bounds one sweep. An unbounded DELETE on a table the relay is
// concurrently reading is a lock-duration problem, not a correctness one, but
// it is the kind that only appears under production volume.
const maxReapLimit = 10_000

// Reap deletes outbox events that have already been published and are older
// than before, and returns how many it removed.
//
// THE INVARIANT, and the only one that matters here: an event that has NOT been
// published is never deleted, at any age. The outbox exists to guarantee that
// an event committed alongside a state change is eventually delivered; reaping
// an unpublished row silently breaks that guarantee, and nothing downstream can
// detect the loss, because the row's absence is indistinguishable from its
// never having existed.
//
// published_at IS NOT NULL is therefore stated explicitly even though it is
// redundant against `published_at < $1` -- SQL's three-valued logic already
// makes NULL < x untrue, so the row would survive anyway. That is a correct-by-
// accident guard. It holds only while the comparison stays a bare `<`, and it
// requires the reader to reason about NULL semantics to see the invariant at
// all. The redundant predicate makes the rule legible and survives someone
// rewriting the window.
//
// The sweep takes FOR UPDATE SKIP LOCKED so it never blocks the relay: a row
// the relay is mid-publish is locked, and the reaper steps over it rather than
// waiting. That row is reaped on a later sweep, which is the correct outcome --
// there is no deadline on deleting a delivered event.
func Reap(
	ctx context.Context,
	pool *pgxpool.Pool,
	schema authschema.ValidatedIdentifier,
	before time.Time,
	limit int,
) (int64, error) {
	if pool == nil {
		return 0, fmt.Errorf("%w: nil pool", ErrReapFailed)
	}
	if limit <= 0 || limit > maxReapLimit {
		return 0, fmt.Errorf("%w: limit %d is outside 1..%d", ErrReapFailed, limit, maxReapLimit)
	}
	if before.IsZero() {
		// A zero cutoff would format as year 1 and reap nothing, which looks
		// like a working reaper that never reclaims anything. Refuse instead.
		return 0, fmt.Errorf("%w: cutoff is the zero time", ErrReapFailed)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("%w: beginning the sweep: %w", ErrReapFailed, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	table := authschema.Quote(schema) + ".auth_outbox_events"
	command, err := tx.Exec(ctx, `
		WITH reapable AS (
		    SELECT e.id
		    FROM `+table+` AS e
		    WHERE e.published_at IS NOT NULL
		      AND e.published_at < $1
		    ORDER BY e.published_at, e.id
		    FOR UPDATE SKIP LOCKED
		    LIMIT $2
		)
		DELETE FROM `+table+` AS e
		USING reapable
		WHERE e.id = reapable.id`, before.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("%w: deleting published events: %w", ErrReapFailed, err)
	}
	deleted := command.RowsAffected()

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: committing the sweep: %w", ErrReapFailed, err)
	}
	return deleted, nil
}
