package syncdispatchruntime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
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
// authorizedUnitIDs (CHAOS-4605) is the OPPOSITE polarity and the opposite
// default: it is an ALLOW-list, `id = ANY($n::uuid[])`, so an empty set
// claims NOTHING rather than everything. It carries authorizeRun's
// guardDecision.authorizedUnitIDs plus any unit BudgetGuard's surplus phase
// pulled forward in this same transaction (those are admitted against
// authorizeRun's own slotHeadroom, i.e. against a bucket that WAS locked and
// cap-checked, so they are authorized by construction).
//
// Why an allow-list rather than a re-read, a late lock, or a higher
// isolation level -- the three shapes CHAOS-4605 lists:
//
//   - A re-read immediately before this call is still followed by these
//     statements' own fresh read, so it narrows the window without closing
//     it; closing it that way means re-running the whole per-bucket cap
//     decision under the locks, and the lock set itself is derived from the
//     first snapshot, so a bucket whose only unit was RUNNING is still
//     neither locked nor re-checked.
//   - Taking a bucket lock HERE, for rows these statements are about to
//     claim, breaks the ascending-key acquisition order that makes
//     authorizeRun, LeaseRepair.Step and UnreclaimableSweep.Step
//     deadlock-free against each other (dispatch_guard.go's
//     sortedDispatchBuckets doc comment) -- and it still leaves that
//     bucket's concurrency cap unevaluated for this pass.
//   - REPEATABLE READ/SERIALIZABLE turns the interleaving into a 40001
//     serialization failure. Dispatch() has no retry harness for one, and
//     CHAOS-4550 is on record for why aborting the whole pass over a single
//     unit is the wrong failure mode here.
//
// Pinning the claim to the authorized set fails in the only safe direction:
// a unit that became claimable after the snapshot is left RETRYING/PLANNED
// for the NEXT pass, which snapshots, locks and cap-checks it properly. The
// caller is told which units those were (the second return value) so it can
// arm that next pass rather than strand them.
//
// The stale-reclaim step additionally excludes every id already claimed by
// steps 1-2 (Python: ~SyncRunUnit.id.in_(claimed_ids)) -- a row already
// claimed this call is not also a stale-reclaim candidate.
func claimUnits(
	ctx context.Context, tx pgx.Tx, syncRunID string,
	authorizedUnitIDs, cappedUnitIDs map[string]bool, now time.Time,
) ([]budgetUnit, []string, error) {
	cappedSlice := mapKeysToSlice(cappedUnitIDs)
	authorizedSlice := mapKeysToSlice(authorizedUnitIDs)

	// Every row the three statements below WOULD have claimed on their own
	// fresh read, minus the ones this pass authorized. Non-empty means the
	// snapshot-to-claim window was actually observed, which is the signal
	// CHAOS-4605 exists to make visible -- and the caller must re-arm a
	// dispatch for them.
	deferredOutsideSnapshot, err := claimableOutsideSnapshot(ctx, tx, syncRunID, authorizedSlice, cappedSlice, now)
	if err != nil {
		return nil, nil, err
	}

	plannedClaimed, err := execReturningIDs(ctx, tx, `
UPDATE public.sync_run_units
SET status = $2, updated_at = $3, first_blocked_at = NULL
WHERE sync_run_id = $1::uuid AND status = $4
  AND id = ANY($6::uuid[])
  AND NOT (id = ANY($5::uuid[]))
RETURNING id::text`,
		syncRunID, syncRunUnitStatusDispatching, now, syncRunUnitStatusPlanned, cappedSlice, authorizedSlice)
	if err != nil {
		return nil, nil, fmt.Errorf("claim planned units: %w", err)
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
  AND id = ANY($6::uuid[])
  AND NOT (id = ANY($5::uuid[]))
RETURNING id::text`,
		syncRunID, syncRunUnitStatusDispatching, now, syncRunUnitStatusRetrying, cappedSlice, authorizedSlice)
	if err != nil {
		return nil, nil, fmt.Errorf("claim due retrying units: %w", err)
	}
	for _, id := range dueRetryingClaimed {
		claimedIDs[id] = true
	}

	excludeFromStale := append(mapKeysToSlice(claimedIDs), cappedSlice...)
	staleReclaimed, err := execReturningIDs(ctx, tx, `
UPDATE public.sync_run_units
SET updated_at = $2
WHERE sync_run_id = $1::uuid AND status = $3 AND updated_at <= $4
  AND id = ANY($6::uuid[])
  AND NOT (id = ANY($5::uuid[]))
RETURNING id::text`,
		syncRunID, now, syncRunUnitStatusDispatching, staleDispatchCutoff(now), excludeFromStale, authorizedSlice)
	if err != nil {
		return nil, nil, fmt.Errorf("reclaim stale dispatching units: %w", err)
	}
	for _, id := range staleReclaimed {
		claimedIDs[id] = true
	}

	if len(claimedIDs) == 0 {
		return nil, deferredOutsideSnapshot, nil
	}

	rows, err := tx.Query(ctx, `
SELECT`+budgetUnitSelectColumns+`
FROM public.sync_run_units
WHERE id = ANY($1::uuid[])
ORDER BY id`,
		mapKeysToSlice(claimedIDs))
	if err != nil {
		return nil, nil, fmt.Errorf("load claimed units: %w", err)
	}
	defer rows.Close()
	var units []budgetUnit
	for rows.Next() {
		unit, err := scanBudgetUnit(rows, syncRunID)
		if err != nil {
			return nil, nil, fmt.Errorf("scan claimed unit: %w", err)
		}
		units = append(units, unit)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("load claimed units: %w", err)
	}
	return units, deferredOutsideSnapshot, nil
}

// claimableOutsideSnapshot returns, in id order, every unit of this run that
// the three claim statements' own predicates match RIGHT NOW but that this
// pass never authorized and never deliberately capped -- i.e. exactly the
// rows the snapshot-to-claim window (CHAOS-4605) made claimable after
// authorizeRun had already decided this pass's capacity.
//
// The predicate is the disjunction of the three claim statements' status/
// timing tests, kept in the same file and the same order as the statements
// it mirrors so the two cannot drift silently. capped ids are excluded
// because a capped unit is a DELIBERATE deferral the guard already accounts
// for and already re-arms; only the unauthorized remainder is news.
func claimableOutsideSnapshot(
	ctx context.Context, tx pgx.Tx, syncRunID string, authorizedSlice, cappedSlice []string, now time.Time,
) ([]string, error) {
	ids, err := execReturningIDs(ctx, tx, `
SELECT id::text FROM public.sync_run_units
WHERE sync_run_id = $1::uuid
  AND (
        status = $2
     OR (status = $3 AND available_at IS NOT NULL AND available_at <= $4)
     OR (status = $5 AND updated_at <= $6)
      )
  AND NOT (id = ANY($7::uuid[]))
  AND NOT (id = ANY($8::uuid[]))
ORDER BY id`,
		syncRunID, syncRunUnitStatusPlanned, syncRunUnitStatusRetrying, now,
		syncRunUnitStatusDispatching, staleDispatchCutoff(now), authorizedSlice, cappedSlice)
	if err != nil {
		return nil, fmt.Errorf("load claimable units outside the guard snapshot: %w", err)
	}
	return ids, nil
}

// claimDeferralSampleSize bounds how many unit ids emitClaimSnapshotDeferral
// puts on one log line / span.
const claimDeferralSampleSize = 20

// emitClaimSnapshotDeferral is the telemetry for the branch CHAOS-4605 adds:
// a unit that was claimable at claim time, was never authorized by this
// pass's capacity decision, and is therefore left for the next pass. A
// structured log line plus span attributes/event on the CURRENT span, the
// same shape and same best-effort contract as emitBucketDecision.
//
// A non-empty count is not an error -- it is the concurrency window being
// observed. A count that stays non-empty across consecutive passes for the
// same run IS a symptom: it means something keeps making units claimable
// between the guard's snapshot and the claim.
func emitClaimSnapshotDeferral(ctx context.Context, logger *slog.Logger, syncRunID string, deferredUnitIDs []string) {
	if len(deferredUnitIDs) == 0 {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}
	// The COUNT is the metric; the ids are a sample. A run may hold up to
	// SYNC_RUN_MAX_UNITS (default 1000) units, and neither a log line nor a
	// span attribute is the right place for a thousand uuids.
	sample := deferredUnitIDs
	if len(sample) > claimDeferralSampleSize {
		sample = sample[:claimDeferralSampleSize]
	}
	logger.WarnContext(ctx, "dispatch_sync_run.claim_deferred_outside_guard_snapshot",
		slog.String("sync_run_id", syncRunID),
		slog.Int("claim.deferred_outside_snapshot", len(deferredUnitIDs)),
		slog.Any("claim.deferred_unit_id_sample", sample),
	)
	span := oteltrace.SpanFromContext(ctx)
	if span == nil || !span.IsRecording() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("sync_run_id", syncRunID),
		attribute.Int("claim.deferred_outside_snapshot", len(deferredUnitIDs)),
		attribute.StringSlice("claim.deferred_unit_id_sample", sample),
	}
	span.SetAttributes(attrs...)
	span.AddEvent("dispatch_sync_run.claim_deferred_outside_guard_snapshot", oteltrace.WithAttributes(attrs...))
}
