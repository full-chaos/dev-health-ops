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

// execReturningIDs runs a statement whose result set is a single id::text
// column -- an UPDATE ... RETURNING id::text, or a plain SELECT id::text --
// and collects the ids, closing the rows before returning.
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
//   - Raising the isolation level does not address the no-concurrency shape
//     at ALL: the guard and the claim take two different application
//     timestamps (native_dispatch_sync_run_service.go:244 vs the fresh
//     nowUTC() at the claim), and no isolation level reconciles two
//     timestamps. For the concurrent shape it would convert interleavings
//     into serialization failures, and Dispatch() has no retry harness
//     around its transaction, with CHAOS-4550 on record for why aborting the
//     whole pass over a single unit is the wrong failure mode. Whether EVERY
//     such interleaving raises 40001 is unverified and is not relied on.
//
// SCOPE: this establishes the invariant for the native dispatch pass, not for
// every write of status='dispatching' in the repository. Worker-side lease
// handbacks (internal/providersync/repository_postgres.go's ReleaseForRetry
// and the budget/rate-limit/continuation deferrals) move RUNNING -> DISPATCHING
// through a lease-owner CAS; they hand a unit already in flight back to the
// queue rather than admitting fresh work, and they are outside this
// transaction. Do not read this allow-list as a global admission gate.
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

	// Probed AFTER the three statements, not before (codex sol architecture
	// round, reservation 3). A writer that commits between the probe and the
	// claims -- LeaseRepair.Step is the live example -- makes a row claimable
	// that the allow-list correctly refuses; probing first meant that row was
	// refused SILENTLY and the caller's redispatch arm never fired for it.
	// Reading last makes the probe observe the latest state this transaction
	// can see, so anything the claim statements just declined to touch is
	// reported.
	//
	// Reading last cannot double-count what was just claimed: every claimed
	// row is now DISPATCHING with updated_at = now, which matches none of the
	// three predicates below (a fresh DISPATCHING row is not past the stale
	// cutoff), and every claimed row is in authorizedSlice anyway. It is also
	// excluded by claimedIDs explicitly -- see that parameter's doc for why the
	// ordering is a compile-time data dependency and not a convention.
	//
	// This is a narrower window, not a closed one: a writer can still commit
	// after this read. That residue is not a liveness hole -- the reconciler's
	// dispatch materializer independently finds PLANNED, stale-DISPATCHING and
	// due-RETRYING units and re-arms dispatch (internal/syncreconciler/
	// materializer.go), so a row this probe misses is still picked up. The arm
	// here exists to make the common case prompt, not to be the only backstop.
	deferredOutsideSnapshot, err := claimableOutsideSnapshot(ctx, tx, syncRunID, claimedIDs, authorizedSlice, cappedSlice, now)
	if err != nil {
		return nil, nil, err
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
// pass did not take -- i.e. exactly the rows the snapshot-to-claim window
// (CHAOS-4605) made claimable after authorizeRun had already decided this
// pass's capacity.
//
// The predicate is the disjunction of the three claim statements' status/
// timing tests, kept in the same file and the same order as the statements it
// mirrors so the two cannot drift silently. capped ids are excluded because a
// capped unit is a DELIBERATE deferral the guard already accounts for and
// already re-arms; only the unclaimed remainder is news.
//
// claimedIDs is what makes the ORDERING structural rather than conventional.
// This must run AFTER the three claim statements (codex sol architecture round,
// reservation 3): probing first means a row made claimable between the probe
// and the claims is refused SILENTLY, with no wakeup armed for it. A comment
// saying "call me last" is not enforcement, and the obvious guard -- a test
// asserting the ordering -- does not work: the two positions are
// indistinguishable unless a commit lands exactly between them, which no test
// can arrange without a production seam. (Confirmed the hard way: a mutation
// moving this call back to the head SURVIVED an ordering test built without
// that seam.)
//
// So the ordering is expressed as a data dependency instead. claimedIDs does
// not exist until all three statements have run, so a caller CANNOT invoke this
// early -- it stops compiling. That the exclusion is also semantically the right
// one is what makes this honest rather than a fake argument: "rows this pass did
// not take" is literally `NOT claimed`, and every claimed id is in
// authorizedSlice anyway, so the clause changes no result. It states the
// contract directly instead of through a proxy, and gets the enforcement free.
func claimableOutsideSnapshot(
	ctx context.Context, tx pgx.Tx, syncRunID string, claimedIDs map[string]bool,
	authorizedSlice, cappedSlice []string, now time.Time,
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
  AND NOT (id = ANY($9::uuid[]))
ORDER BY id`,
		syncRunID, syncRunUnitStatusPlanned, syncRunUnitStatusRetrying, now,
		syncRunUnitStatusDispatching, staleDispatchCutoff(now), authorizedSlice, cappedSlice,
		mapKeysToSlice(claimedIDs))
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
