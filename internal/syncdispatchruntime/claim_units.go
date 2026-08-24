package syncdispatchruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// execReturningIDs runs an UPDATE ... RETURNING id::text and collects the
// returned ids, closing the rows before returning.
func execReturningIDs(ctx context.Context, tx pgx.Tx, sql string, args ...any) ([]string, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

// claimUnits ports _claim_units verbatim: atomically claim dispatchable
// units for a run via three UPDATE ... RETURNING statements, so two
// concurrent dispatch passes can never both enqueue the same unit.
//
//  1. Fresh PLANNED units.
//  2. Due RETRYING units (available_at <= now) -- available_at is cleared
//     on claim.
//  3. Stale DISPATCHING units are RECLAIMED by age (a worker was enqueued
//     but never picked up, e.g. a broker restart) -- status stays
//     DISPATCHING, only updated_at moves, so a later redispatch re-enqueues
//     it. F2: RUNNING units are NEVER reclaimed here -- re-dispatching a
//     RUNNING unit would run it a second time concurrently and cause
//     duplicate provider writes. Durable dead-worker recovery for RUNNING
//     is reconcile_sync_dispatch's job, not this one's.
//
// Both claim writes (1 and 2) clear first_blocked_at (CHAOS-3412 review
// round 2): a successful claim is the moment a unit stops being blocked, so
// the aggregate blocked clock stops with it. Set on PLANNED too even though
// it is already NULL there, so the claim path has ONE rule rather than two
// that could drift.
//
// cappedUnitIDs (the concurrency guard's deferred set) is excluded from
// ALL THREE statements -- Python builds this as a conditional WHERE clause
// (only appended when non-empty); here it is always included as
// NOT (id = ANY($n::uuid[])), which is semantically identical for an empty
// array (id = ANY('{}') is always false, so NOT(...) is always true) and
// avoids building the query text conditionally.
//
// The stale-reclaim step additionally excludes every id already claimed by
// steps 1-2 (Python: ~SyncRunUnit.id.in_(claimed_ids)) -- a row already
// claimed this call is not also a stale-reclaim candidate.
func claimUnits(ctx context.Context, tx pgx.Tx, syncRunID string, cappedUnitIDs map[string]bool, now time.Time) ([]budgetUnit, error) {
	cappedSlice := mapKeysToSlice(cappedUnitIDs)

	plannedClaimed, err := execReturningIDs(ctx, tx, `
UPDATE public.sync_run_units
SET status = $2, updated_at = $3, first_blocked_at = NULL
WHERE sync_run_id = $1::uuid AND status = $4
  AND NOT (id = ANY($5::uuid[]))
RETURNING id::text`,
		syncRunID, syncRunUnitStatusDispatching, now, syncRunUnitStatusPlanned, cappedSlice)
	if err != nil {
		return nil, fmt.Errorf("claim planned units: %w", err)
	}

	claimedIDs := map[string]bool{}
	for _, id := range plannedClaimed {
		claimedIDs[id] = true
	}

	dueRetryingClaimed, err := execReturningIDs(ctx, tx, `
UPDATE public.sync_run_units
SET status = $2, updated_at = $3, available_at = NULL, first_blocked_at = NULL
WHERE sync_run_id = $1::uuid AND status = $4
  AND available_at IS NOT NULL AND available_at <= $3
  AND NOT (id = ANY($5::uuid[]))
RETURNING id::text`,
		syncRunID, syncRunUnitStatusDispatching, now, syncRunUnitStatusRetrying, cappedSlice)
	if err != nil {
		return nil, fmt.Errorf("claim due retrying units: %w", err)
	}
	for _, id := range dueRetryingClaimed {
		claimedIDs[id] = true
	}

	excludeFromStale := append(mapKeysToSlice(claimedIDs), cappedSlice...)
	staleReclaimed, err := execReturningIDs(ctx, tx, `
UPDATE public.sync_run_units
SET updated_at = $2
WHERE sync_run_id = $1::uuid AND status = $3 AND updated_at <= $4
  AND NOT (id = ANY($5::uuid[]))
RETURNING id::text`,
		syncRunID, now, syncRunUnitStatusDispatching, staleDispatchCutoff(now), excludeFromStale)
	if err != nil {
		return nil, fmt.Errorf("reclaim stale dispatching units: %w", err)
	}
	for _, id := range staleReclaimed {
		claimedIDs[id] = true
	}

	if len(claimedIDs) == 0 {
		return nil, nil
	}

	rows, err := tx.Query(ctx, `
SELECT`+budgetUnitSelectColumns+`
FROM public.sync_run_units
WHERE id = ANY($1::uuid[])
ORDER BY id`,
		mapKeysToSlice(claimedIDs))
	if err != nil {
		return nil, fmt.Errorf("load claimed units: %w", err)
	}
	defer rows.Close()
	var units []budgetUnit
	for rows.Next() {
		unit, err := scanBudgetUnit(rows, syncRunID)
		if err != nil {
			return nil, fmt.Errorf("scan claimed unit: %w", err)
		}
		units = append(units, unit)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load claimed units: %w", err)
	}
	return units, nil
}
