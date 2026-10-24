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
	// One aggregate row whatever the unit count: the classification below is the per-row switch this function used to
	// run in Go. A RETRYING unit with no available_at is in neither class.
	var counts pendingUnitCounts
	err := tx.QueryRow(ctx, `
SELECT
  count(*) FILTER (WHERE status = $2
       OR (status = $3 AND updated_at <= $6)
       OR (status = $5 AND available_at IS NOT NULL AND available_at <= $7)),
  count(*) FILTER (WHERE status = $4
       OR (status = $3 AND updated_at > $6)),
  min(available_at) FILTER (WHERE status = $5 AND available_at IS NOT NULL AND available_at > $7)
FROM public.sync_run_units
WHERE sync_run_id = $1::uuid AND status IN ($2, $3, $4, $5)`,
		syncRunID, syncRunUnitStatusPlanned, syncRunUnitStatusDispatching, syncRunUnitStatusRunning,
		syncRunUnitStatusRetrying, staleCutoff, now,
	).Scan(&counts.dispatchable, &counts.inFlight, &counts.nextDeferredAt)
	if err != nil {
		return pendingUnitCounts{}, fmt.Errorf("load pending unit counts: %w", err)
	}
	return counts, nil
}
