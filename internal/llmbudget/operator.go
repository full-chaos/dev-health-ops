package llmbudget

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The operator side of CHAOS-3240 / CHAOS-9451. A `reserved` row means a
// provider call may be in flight or may have billed with its usage lost (a
// crash, a lost connection). Python never freed such a row on its own, and
// neither does Go: it counts at its maximum exposure until an operator who can
// prove the provider did not bill voids it by id.

// StaleReservation is one `reserved` row older than the requested age.
type StaleReservation struct {
	ID               string
	OrgID            string
	WindowStart      time.Time
	Provider         string
	Model            string
	ReservedMicroUSD int64
	CreatedAt        time.Time
}

// Querier is the read side the stale listing needs.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// ListStale lists `reserved` rows created before now-olderThan, oldest first.
// It reads only: nothing is voided, so no budget is ever reopened by age.
func ListStale(ctx context.Context, db Querier, olderThan time.Duration, now time.Time) ([]StaleReservation, error) {
	if olderThan <= 0 {
		return nil, errors.New("the stale age must be positive")
	}
	rows, err := db.Query(ctx, `SELECT id::text, org_id::text, window_start, provider, model, reserved_micro_usd, created_at
FROM byo_llm_budget_reservations WHERE status = 'reserved' AND created_at < $1 ORDER BY created_at, id`, now.Add(-olderThan).UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StaleReservation
	for rows.Next() {
		var row StaleReservation
		if err := rows.Scan(&row.ID, &row.OrgID, &row.WindowStart, &row.Provider, &row.Model, &row.ReservedMicroUSD, &row.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// Executor is the write side the void needs.
type Executor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// ErrNotReserved is returned when the row is missing or already settled.
var ErrNotReserved = errors.New("the reservation does not exist or is no longer reserved")

// VoidReservation voids one `reserved` row: it stops counting (voided rows are
// skipped by Compute). The caller must have proof that the provider did not
// bill; this function cannot check that.
func VoidReservation(ctx context.Context, db Executor, id string, now time.Time) error {
	tag, err := db.Exec(ctx, `UPDATE byo_llm_budget_reservations
SET status = 'voided', input_tokens = 0, output_tokens = 0, cached_input_tokens = 0, actual_micro_usd = 0, reconciled_at = $2
WHERE id::text = $1 AND status = 'reserved'`, id, now.UTC())
	if err != nil {
		return fmt.Errorf("void reservation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrNotReserved
	}
	return nil
}
