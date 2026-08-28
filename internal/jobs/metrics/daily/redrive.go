package daily

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

const dailyTargetDayLayout = "2006-01-02"

// RunningRunIDs lists daily_metrics_runs still status='running' for orgID
// with target_day in [fromDay, toDay] (inclusive, UTC calendar days). This is
// the scope an operator names when requesting a redrive, resolved BEFORE
// RedriveStrandedPartitions runs -- callers that must repair the Python
// compatibility-bridge ledger first (CHAOS-4304's ambiguous/stuck-executing
// rows; see redrive.go's package doc) need these run ids to do that
// ordering safely: publishing a fresh metrics.daily_partition job for a
// partition whose ledger row is still ambiguous only reproduces
// ambiguous_refused and re-terminalizes it failed_permanent, undoing this
// pass's own reset.
func (store *PostgresStore) RunningRunIDs(
	ctx context.Context, orgID string, fromDay, toDay time.Time,
) ([]string, error) {
	if !store.valid() || !validUUID(orgID) {
		return nil, ErrUnavailable
	}
	if toDay.Before(fromDay) {
		return nil, ErrInvalidState
	}
	rows, err := store.pool.Query(ctx, `
SELECT id::text FROM public.daily_metrics_runs
WHERE org_id = $1::uuid AND target_day BETWEEN $2 AND $3 AND status = 'running'
ORDER BY id`, orgID, fromDay.UTC().Truncate(24*time.Hour), toDay.UTC().Truncate(24*time.Hour))
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	var runIDs []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return nil, ErrUnavailable
		}
		runIDs = append(runIDs, runID)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	return runIDs, nil
}

// RedriveOutcome summarizes one operator-invoked strand-repair pass
// (CHAOS-4358).
type RedriveOutcome struct {
	// PermanentReset counts partitions whose failed_permanent status this
	// pass overrode back to 'failed' (an explicit operator exception to the
	// terminal state CHAOS-4319 introduced).
	PermanentReset int
	// RedispatchedRunIDs lists every run at least one partition job was
	// freshly enqueued for.
	RedispatchedRunIDs []string
	// RedrivenPartitions counts partitions a fresh metrics.daily_partition
	// job was enqueued for.
	RedrivenPartitions int
}

// RedriveStrandedPartitions repairs the CHAOS-4358 stranding class: a
// daily_metrics_run stuck status='running' forever because every
// daily_partition River job for it already failed and was discarded by
// River's attempt budget, and nothing else ever re-enqueues work for that
// run (post_sync/scheduled fanouts only ever create NEW runs, never redrive
// an old one). It does not touch the Python compatibility-bridge ledger
// (metric_compatibility_executions) -- a partition whose ledger row is stuck
// 'ambiguous' still needs the separate operator repair path CHAOS-4304 adds
// before a redriven attempt can do anything but hit ambiguous_refused again;
// this function only makes the Go side reachable, in either case.
//
// This publishes metrics.daily_partition jobs DIRECTLY (via
// PublishRedrivePartitionTx) rather than re-enqueuing metrics.daily_dispatch
// and letting Dispatcher.Work publish partitions itself. That indirection
// was tried first and proven insufficient against the real local stack: a
// fresh dispatch job runs Dispatcher.Work's ORDINARY partition publish
// (PublishPartition), whose dedupe_key is "metrics.daily_partition:" +
// partition.ID -- permanent and keyed on the immutable partition id, so
// re-dispatching a run only unblocked the few partitions that had never been
// published before; every already-dispatched-and-failed partition (the
// dominant stranded case) stayed silently stuck at the SAME outbox wall one
// hop deeper. Publishing partition jobs directly, with a redrive-scoped
// dedupe key, is both simpler and the only path that actually works: this
// function already knows exactly which partitions to redrive, so there is
// nothing for a re-run of Dispatcher.Work's discovery/dispatch step to add.
//
// orgID + [fromDay, toDay] (inclusive, UTC calendar days) scope which runs
// are eligible, matching how an operator names a stranding window from the
// ledger evidence (org + date range), not individual run/partition ids they
// would otherwise have to enumerate by hand. nonce must be unique per
// invocation (a fresh UUID is the expected caller pattern) -- see
// PublishRedrivePartitionTx for why it exists.
func (store *PostgresStore) RedriveStrandedPartitions(
	ctx context.Context,
	publisher *PostgresPublisher,
	orgID string,
	fromDay, toDay time.Time,
	nonce string,
) (RedriveOutcome, error) {
	var outcome RedriveOutcome
	if !store.valid() || !validUUID(orgID) || publisher == nil || nonce == "" {
		return outcome, ErrUnavailable
	}
	if toDay.Before(fromDay) {
		return outcome, ErrInvalidState
	}
	from := fromDay.UTC().Truncate(24 * time.Hour)
	to := toDay.UTC().Truncate(24 * time.Hour)

	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return outcome, ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	now := store.now().UTC()
	// Step 1: an explicit, operator-scoped override of failed_permanent
	// (CHAOS-4319's terminal state). Bounded to runs the caller named by
	// org+day range -- never a blanket resurrection, unlike
	// DispatchablePartitions's own automatic reclaim, which must keep
	// excluding failed_permanent rows it did not decide to override.
	resetCommand, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_partitions AS partition
SET status = 'failed', failure_reason = NULL, updated_at = $1
FROM public.daily_metrics_runs AS run
WHERE partition.run_id = run.id
  AND run.org_id = $2::uuid
  AND run.target_day BETWEEN $3 AND $4
  AND run.status = 'running'
  AND partition.status = 'failed_permanent'`, now, orgID, from, to)
	if err != nil {
		return outcome, ErrUnavailable
	}
	outcome.PermanentReset = int(resetCommand.RowsAffected())

	// Step 2: every dispatchable (pending/failed) partition in scope --
	// including partitions this pass just reset in step 1, and any partition
	// that already sat 'failed' unreachable because nothing had ever
	// re-published a metrics.daily_partition job for it (the core CHAOS-4358
	// gap; plus 'running' with an EXPIRED lease -- codex review round 2: the
	// final River attempt can die after ClaimPartition succeeds but before
	// ReleasePartition ever runs, leaving the durable row 'running' forever
	// with nothing left to reclaim it. ClaimPartition already treats this
	// exact shape as reclaimable (classifyLease's leaseReclaimable branch)
	// regardless of caller, so publishing a fresh job for it is exactly as
	// safe as for a 'failed' one -- a live lease (not yet expired) stays
	// excluded, matching ClaimPartition's own leaseHeld snooze). One row per
	// partition, carrying its run's identity alongside it, so each can be
	// published directly without a second round trip.
	partitionRows, err := tx.Query(ctx, `
SELECT partition.id::text, partition.run_id::text, partition.repo_ids::text,
       run.org_id::text, run.generation, run.status, run.target_day::text
FROM public.daily_metrics_partitions AS partition
JOIN public.daily_metrics_runs AS run ON run.id = partition.run_id
WHERE run.org_id = $1::uuid
  AND run.target_day BETWEEN $2 AND $3
  AND run.status = 'running'
  AND (
    partition.status IN ('pending', 'failed')
    OR (partition.status = 'running' AND partition.lease_expires_at < $4)
  )
ORDER BY partition.run_id, partition.ordinal`, orgID, from, to, now)
	if err != nil {
		return outcome, ErrUnavailable
	}
	type redrivePartition struct {
		partition Partition
		run       Run
	}
	var targets []redrivePartition
	for partitionRows.Next() {
		var target redrivePartition
		var repoIDs, targetDay string
		if err := partitionRows.Scan(
			&target.partition.ID, &target.partition.RunID, &repoIDs,
			&target.run.OrganizationID, &target.run.Generation, &target.run.Status, &targetDay,
		); err != nil {
			partitionRows.Close()
			return outcome, ErrUnavailable
		}
		target.run.ID = target.partition.RunID
		if target.partition.RepoIDs, err = parsePartitionRepoIDs(repoIDs); err != nil {
			partitionRows.Close()
			return outcome, ErrInvalidState
		}
		if target.run.TargetDay, err = time.Parse(dailyTargetDayLayout, targetDay); err != nil {
			partitionRows.Close()
			return outcome, ErrInvalidState
		}
		targets = append(targets, target)
	}
	rowsErr := partitionRows.Err()
	partitionRows.Close()
	if rowsErr != nil {
		return outcome, ErrUnavailable
	}

	redispatchedRuns := map[string]struct{}{}
	for _, target := range targets {
		if err := publisher.PublishRedrivePartitionTx(ctx, tx, target.run, target.partition, nonce); err != nil {
			return outcome, err
		}
		outcome.RedrivenPartitions++
		redispatchedRuns[target.run.ID] = struct{}{}
	}
	for runID := range redispatchedRuns {
		outcome.RedispatchedRunIDs = append(outcome.RedispatchedRunIDs, runID)
	}
	sort.Strings(outcome.RedispatchedRunIDs)

	if err := tx.Commit(ctx); err != nil {
		return outcome, ErrUnavailable
	}
	// codex review (round 1): today's only caller (dev-health-workerctl) is a
	// one-shot CLI process with no Prometheus scrape endpoint and no
	// observer wired, so these calls are a no-op in practice -- this is
	// forward-wiring for a future long-lived caller (e.g. an automatic
	// strand-repair reconciler sweep), not a claim that a manual redrive is
	// visible on this counter today. The durable, queryable record of a
	// manual redrive is the worker_job_outbox rows this pass just committed
	// under the "metrics.daily_partition:redrive:<nonce>" dedupe-key prefix
	// (see docs/reference/cli/index.md's `metrics daily-redrive` entry).
	store.observeRedrive("failed_permanent_reset", outcome.PermanentReset)
	store.observeRedrive("dispatch_redriven", outcome.RedrivenPartitions)
	return outcome, nil
}

// defaultFinalizeSweepLimit bounds how many stranded-finalize candidates one
// FindStrandedFinalizeRuns pass returns, so an operator/reconciler sweep
// never locks an unbounded number of rows in one pass.
const defaultFinalizeSweepLimit = 500

// FindStrandedFinalizeRuns lists daily_metrics_runs ids in the CHAOS-4389
// stranding class: status='running', every partition succeeded, but
// finalization never reached a terminal state and nothing is currently
// working it (finalization_status is 'pending'/'failed', or 'running' with
// an expired lease). This mirrors CHAOS-4358's DispatchablePartitions gap
// one layer later: CompletePartition enqueues metrics.daily_finalize exactly
// once per run, under the fixed idempotency key "metrics.daily_finalize:"+
// run.ID, permanently deduped by the outbox -- so if that one job is ever
// discarded by River before CompleteFinalize runs, nothing else ever
// re-enqueues it. Scoped across every organization (unlike
// RedriveStrandedPartitions's org+day-range scope) because an operator
// naming this stranding shape does not know in advance which orgs/days it
// hit -- that is exactly what this scan answers. limit <= 0 uses
// defaultFinalizeSweepLimit.
//
// This is a DETECTION query only -- it reports every run in this shape,
// including 'failed'/expired-'running' ones a caller should NOT bulk-redrive
// blind (see RedriveStrandedFinalize's allowPriorAttempt parameter). An
// operator sweep still needs to know about those to decide what to do with
// them by hand; only the redrive step itself narrows further.
func (store *PostgresStore) FindStrandedFinalizeRuns(ctx context.Context, limit int) ([]string, error) {
	if !store.valid() {
		return nil, ErrUnavailable
	}
	if limit <= 0 {
		limit = defaultFinalizeSweepLimit
	}
	now := store.now().UTC()
	rows, err := store.pool.Query(ctx, `
SELECT run.id::text
FROM public.daily_metrics_runs AS run
WHERE run.status = 'running'
  AND (
    run.finalization_status IN ('pending', 'failed')
    OR (run.finalization_status = 'running' AND run.finalization_lease_expires_at < $1)
  )
  -- codex review (P1): EXISTS, not just the sibling NOT EXISTS below, is
  -- load-bearing. A run between ClaimDispatch (status='running') and
  -- MaterializeScheduledFanout (which inserts the first partition rows) has
  -- ZERO partitions -- the NOT EXISTS alone is vacuously true for it, which
  -- would let this scan treat "hasn't started" identically to "100%
  -- succeeded" and finalize a run with no computed work at all.
  AND EXISTS (
      SELECT 1 FROM public.daily_metrics_partitions AS partition
      WHERE partition.run_id = run.id
  )
  AND NOT EXISTS (
      SELECT 1 FROM public.daily_metrics_partitions AS partition
      WHERE partition.run_id = run.id AND partition.status <> 'succeeded'
  )
-- codex review (P2, round 2): a bare ORDER BY updated_at lets a page of
-- older 'failed'/expired-'running' rows -- exactly the ones --all-complete's
-- bulk sweep will skip via allowPriorAttempt -- starve a genuinely
-- never-attempted 'pending' row out of the LIMIT window, so a default sweep
-- can keep returning the same unactionable candidates and never reach the
-- one row it could actually redrive. Sort 'pending' first so it is never
-- pushed past the limit by older rows this pass cannot act on anyway.
ORDER BY (run.finalization_status <> 'pending'), run.updated_at
LIMIT $2`, now, limit)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	var runIDs []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return nil, ErrUnavailable
		}
		runIDs = append(runIDs, runID)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	store.observeFinalizeSweep("detected", len(runIDs))
	return runIDs, nil
}

// FinalizeRedriveOutcome summarizes one CHAOS-4389 stranded-finalize redrive
// pass.
type FinalizeRedriveOutcome struct {
	// FinalizedRunIDs lists every run a fresh metrics.daily_finalize job was
	// freshly enqueued for.
	FinalizedRunIDs []string
}

// RedriveStrandedFinalize enqueues a fresh metrics.daily_finalize job
// (CHAOS-4389) for each run in runIDs, re-verifying eligibility under a row
// lock immediately before publishing -- the same defense-in-depth
// RedriveStrandedPartitions uses -- so a run that settled (its original
// finalize completed, or another caller already redrove it) between the
// caller naming it and this call runs is silently skipped rather than
// double-published or erroring the whole batch. Safe to call repeatedly:
// each pass only ever touches runs still eligible at the instant of its own
// row lock. nonce must be unique per invocation (a fresh UUID is the
// expected caller pattern), matching the sibling redrive publishers'
// contract -- reused across every runID in this call, since each run's
// dedupe key already embeds its own run.ID and this call is one coherent
// operator/sweep action.
//
// allowPriorAttempt gates whether a run whose finalize was already CLAIMED
// at least once (finalization_status 'failed', or 'running' with an expired
// lease) is eligible, versus only a run whose finalize was never attempted
// at all ('pending'). codex review (P1): 'failed' does not mean "nothing
// happened" -- FinalizeHandler.Work sets it both when the compatibility call
// itself failed AND when the call SUCCEEDED (writing real, durable
// user_metrics_daily/compounding_risk_daily/team_cognitive_load_daily rows)
// but the subsequent CompleteFinalize bookkeeping write failed. Blindly
// redriving that shape re-runs Finalize() and appends a second full set of
// rows to tables that are plain MergeTree, not ReplacingMergeTree -- safe
// for a consumer that dedupes by argMax(computed_at) (the established
// convention in this codebase), a real risk for one that does not. Mirrors
// `daily-redrive`'s own established split for the identical class of risk:
// a bulk pass only ever authorizes the provably-safe subset (there,
// retry_safe; here, never-attempted); the riskier subset needs a human
// naming ONE specific run they have actually reviewed, matching
// `daily-redrive`'s confirm_succeeded needing the single-execution endpoint,
// never the bulk one.
func (store *PostgresStore) RedriveStrandedFinalize(
	ctx context.Context,
	publisher *PostgresPublisher,
	runIDs []string,
	nonce string,
	allowPriorAttempt bool,
) (FinalizeRedriveOutcome, error) {
	var outcome FinalizeRedriveOutcome
	if !store.valid() || publisher == nil || nonce == "" {
		return outcome, ErrUnavailable
	}
	now := store.now().UTC()
	for _, runID := range runIDs {
		if !validUUID(runID) {
			return outcome, ErrInvalidState
		}
		finalized, err := store.redriveOneStrandedFinalize(ctx, publisher, runID, nonce, now, allowPriorAttempt)
		if err != nil {
			return outcome, err
		}
		if finalized {
			outcome.FinalizedRunIDs = append(outcome.FinalizedRunIDs, runID)
		}
	}
	sort.Strings(outcome.FinalizedRunIDs)
	store.observeFinalizeSweep("finalized", len(outcome.FinalizedRunIDs))
	return outcome, nil
}

// redriveOneStrandedFinalize re-verifies and, if still eligible, redrives
// exactly one run's finalize inside its own transaction, so one run's
// dedupe/policy rejection cannot roll back an otherwise-successful batch.
func (store *PostgresStore) redriveOneStrandedFinalize(
	ctx context.Context,
	publisher *PostgresPublisher,
	runID, nonce string,
	now time.Time,
	allowPriorAttempt bool,
) (bool, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return false, ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	var run Run
	var targetDay, finalizationStatus string
	var leaseExpiresAt *time.Time
	var hasPartitions, allPartitionsSucceeded bool
	err = tx.QueryRow(ctx, `
SELECT run.id::text, run.org_id::text, run.generation, run.status, run.target_day::text,
  run.finalization_status, run.finalization_lease_expires_at,
  EXISTS (
      SELECT 1 FROM public.daily_metrics_partitions AS partition
      WHERE partition.run_id = run.id
  ),
  NOT EXISTS (
      SELECT 1 FROM public.daily_metrics_partitions AS partition
      WHERE partition.run_id = run.id AND partition.status <> 'succeeded'
  )
FROM public.daily_metrics_runs AS run
WHERE run.id = $1::uuid
FOR UPDATE OF run`, runID).Scan(
		&run.ID, &run.OrganizationID, &run.Generation, &run.Status, &targetDay,
		&finalizationStatus, &leaseExpiresAt, &hasPartitions, &allPartitionsSucceeded,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, ErrUnavailable
	}
	// codex review (P1): hasPartitions is load-bearing on its own, not just
	// the boolean AND below -- a run with zero partitions (still between
	// ClaimDispatch and MaterializeScheduledFanout) makes
	// allPartitionsSucceeded's NOT EXISTS vacuously true, which would
	// otherwise let this function finalize a run that has not computed
	// anything at all.
	if run.Status != "running" || !hasPartitions || !allPartitionsSucceeded {
		return false, nil
	}
	priorAttempt := finalizationStatus == "failed" ||
		(finalizationStatus == "running" && (leaseExpiresAt == nil || leaseExpiresAt.Before(now)))
	eligible := finalizationStatus == "pending" || (allowPriorAttempt && priorAttempt)
	if !eligible {
		return false, nil
	}
	if run.TargetDay, err = time.Parse(dailyTargetDayLayout, targetDay); err != nil {
		return false, ErrInvalidState
	}
	if err := publisher.PublishRedriveFinalizeTx(ctx, tx, run, nonce); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, ErrUnavailable
	}
	return true, nil
}
