package syncdispatchruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// pendingUnitCounts ports _pending_unit_counts verbatim: classify every
// non-terminal unit of a run into "dispatchable now" or "in flight",
// tracking the earliest still-future deferral along the way. Dispatch()
// reads this ONLY when a pass claimed nothing this time, to distinguish
// "more work later" (schedule a redispatch) from "genuinely done" (call
// finalize) -- see scheduleRedispatch/the Dispatch() handler.
type pendingUnitCounts struct {
	dispatchable   int
	inFlight       int
	nextDeferredAt *time.Time
}

func computePendingUnitCounts(ctx context.Context, tx pgx.Tx, syncRunID string, now time.Time) (pendingUnitCounts, error) {
	staleCutoff := staleDispatchCutoff(now)
	rows, err := tx.Query(ctx, `
SELECT status, updated_at, available_at
FROM public.sync_run_units
WHERE sync_run_id = $1::uuid AND status IN ($2, $3, $4, $5)`,
		syncRunID, syncRunUnitStatusPlanned, syncRunUnitStatusDispatching, syncRunUnitStatusRunning, syncRunUnitStatusRetrying)
	if err != nil {
		return pendingUnitCounts{}, fmt.Errorf("load pending unit counts: %w", err)
	}
	defer rows.Close()

	var counts pendingUnitCounts
	for rows.Next() {
		var status string
		var updatedAt time.Time
		var availableAt *time.Time
		if err := rows.Scan(&status, &updatedAt, &availableAt); err != nil {
			return pendingUnitCounts{}, fmt.Errorf("scan pending unit count row: %w", err)
		}
		switch status {
		case syncRunUnitStatusPlanned:
			counts.dispatchable++
		case syncRunUnitStatusDispatching:
			if !updatedAt.After(staleCutoff) {
				counts.dispatchable++
			} else {
				counts.inFlight++
			}
		case syncRunUnitStatusRunning:
			counts.inFlight++
		default: // RETRYING
			if availableAt != nil {
				if !availableAt.After(now) {
					counts.dispatchable++
				} else {
					counts.nextDeferredAt = earlierOf(counts.nextDeferredAt, *availableAt)
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return pendingUnitCounts{}, fmt.Errorf("load pending unit counts: %w", err)
	}
	return counts, nil
}
