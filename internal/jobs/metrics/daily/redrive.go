package daily

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
//
// CHAOS-4405 (team-lead's approval condition (2)/(3) on that ticket's
// finalize-redrive verb, escalated 2026-08-28): a run with an 'open' row in
// daily_metrics_finalize_redrive_events is excluded here while that
// specific redrive's claim is plausibly still in flight -- an unattended
// `daily-finalize --all-complete` sweep running concurrently must never
// double-dispatch it. This is deliberately NOT permanent: the first
// version of this exclusion covered any row at all, whether the redriven
// finalize eventually succeeded or failed -- a silent-failure shape, since
// a failed redrive would then leave the run running-with-failed-finalize
// forever invisible to this sweep (and every other recovery tool). Now
// CompleteFinalize/ReleaseFinalize (postgres.go) close the run's own open
// row the instant that SPECIFIC claim resolves -- 'closed_succeeded' (moot,
// the run's status='succeeded' already drops it out of the WHERE clause
// above) or 'closed_failed' (the interesting case: the run reappears here
// immediately, exactly like any other ordinary CHAOS-4389 discard, since
// nothing but 'open' rows are excluded). This does not weaken detection of
// the ordinary CHAOS-4389 discard shape: that class of run was never
// touched by finalize-redrive, so its provenance table has no row for it
// and this EXISTS check changes nothing.
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
  AND NOT EXISTS (
      SELECT 1 FROM public.daily_metrics_finalize_redrive_events AS redrive_event
      WHERE redrive_event.run_id = run.id AND redrive_event.status = 'open'
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

// FinalizeRangeRedriveDay is one calendar day's outcome from
// RedriveFinalizeForRange.
type FinalizeRangeRedriveDay struct {
	Day time.Time
	// RunID is the daily_metrics_runs id this day's finalize was (or would
	// have been) redriven through. Empty when no eligible run exists at all
	// for this day.
	RunID string
	// Outcome is "redriven" or "skipped_ineligible" for a real pass, or
	// "would_redrive"/"would_skip_ineligible" under DryRun -- prefixed
	// distinctly so a caller can never mistake a preview result for a
	// completed write.
	Outcome string
	// ResetFromSucceeded is true when this day's run was (or, under DryRun,
	// would have been) already status='succeeded' and needed the
	// terminal-state reset to become reachable again -- team-lead's
	// approval condition (5): telemetry and the per-day result must
	// distinguish this from an ordinary never-attempted/failed/
	// expired-lease redrive.
	ResetFromSucceeded bool
}

// FinalizeRangeRedriveOutcome summarizes one CHAOS-4405 historical
// finalize-redrive pass, one entry per calendar day in the requested range.
type FinalizeRangeRedriveOutcome struct {
	Days []FinalizeRangeRedriveDay
}

// RedriveFinalizeForRange (CHAOS-4405) re-runs metrics.daily_finalize for
// orgID across [fromDay, toDay], one calendar day at a time, for the
// historical team-aggregation backfill: run_daily_metrics_finalize
// (job_daily.py) now writes compounding_risk_daily(scope='team') and ALL of
// team_cognitive_load_daily exactly once per org/day (CHAOS-4399's fix,
// #1963) -- any day finalized BEFORE that fix landed has zero rows in
// either table and needs finalize re-run purely to backfill them, even
// though the run itself already reached a terminal 'succeeded' state.
//
// Unlike RedriveStrandedFinalize (CHAOS-4389, which only ever touches a run
// still stuck non-terminal), this function's whole point is to re-execute a
// run's finalize AFTER it already completed. Both Go's ClaimFinalize and the
// Python compat bridge's own claim-row query (worker_metrics.py's
// _load_daily_execution) hard-require status='running' before Finalize()
// can run at all -- publishing a fresh metrics.daily_finalize job for an
// already-'succeeded' row through the ordinary pipeline is a guaranteed
// silent no-op (ClaimFinalize's own `if status != "running" { return nil,
// nil }` refuses it before any HTTP call is even made). So when
// includeSucceeded is true and a day's run is 'succeeded', this function
// transactionally resets it back to status='running',
// finalization_status='pending' (clearing any stale claim/lease) in the
// SAME transaction as the publish below -- it either fully lands (reset +
// fresh outbox row) or fully rolls back, never a reset with no way to reach
// FinalizeHandler.Work again. This is a deliberate, explicit,
// operator-invoked re-execution of a day the operator has already reviewed;
// run_daily_metrics_finalize's outputs are read back via
// argMax(computed_at) dedup, the same convention every other daily-metrics
// reader in this schema already uses, so a second full write for the same
// day changes nothing a correctly-written reader observes.
//
// includeSucceeded=false restricts this to the same safe subset
// RedriveStrandedFinalize's bulk path uses (never-attempted/failed/expired-
// lease), scoped to this org+day-range instead of globally -- useful for a
// dry run of the eligibility scan before authorizing the
// state-mutating 'succeeded' case.
//
// reason is the operator's own --review-evidence text, recorded verbatim in
// daily_metrics_finalize_redrive_events for every day actually reset (team-
// lead's approval condition (1)) -- never persisted when dryRun is true,
// since a dry run makes no durable writes at all (condition (4)).
//
// dryRun computes and reports exactly what a real pass would do -- the same
// eligibility scan, the same per-day row lock -- without executing the
// reset, the provenance insert, or the publish; every transaction this
// function opens under dryRun is rolled back, never committed.
//
// nonce must be unique per invocation, matching the sibling redrive
// functions' contract. Ignored (and may be empty) when dryRun is true.
func (store *PostgresStore) RedriveFinalizeForRange(
	ctx context.Context,
	publisher *PostgresPublisher,
	orgID string,
	fromDay, toDay time.Time,
	nonce string,
	includeSucceeded bool,
	reason string,
	dryRun bool,
) (FinalizeRangeRedriveOutcome, error) {
	var outcome FinalizeRangeRedriveOutcome
	if !store.valid() || !validUUID(orgID) || publisher == nil || (!dryRun && nonce == "") {
		return outcome, ErrUnavailable
	}
	if !dryRun && strings.TrimSpace(reason) == "" {
		return outcome, ErrUnavailable
	}
	if toDay.Before(fromDay) {
		return outcome, ErrInvalidState
	}
	from := fromDay.UTC().Truncate(24 * time.Hour)
	to := toDay.UTC().Truncate(24 * time.Hour)
	now := store.now().UTC()

	// One candidate run per calendar day: whichever run for that day has
	// every partition succeeded, preferring the most recently updated when
	// more than one generation qualifies (team-scope aggregation re-reads
	// ClickHouse for the whole org/day, not a specific run's own partition
	// rows, so any one qualifying run for that day is equally valid to
	// redrive through).
	rows, err := store.pool.Query(ctx, `
SELECT DISTINCT ON (run.target_day) run.target_day::text, run.id::text
FROM public.daily_metrics_runs AS run
WHERE run.org_id = $1::uuid AND run.target_day BETWEEN $2 AND $3
  AND EXISTS (
      SELECT 1 FROM public.daily_metrics_partitions AS partition
      WHERE partition.run_id = run.id
  )
  AND NOT EXISTS (
      SELECT 1 FROM public.daily_metrics_partitions AS partition
      WHERE partition.run_id = run.id AND partition.status <> 'succeeded'
  )
ORDER BY run.target_day, run.updated_at DESC`, orgID, from, to)
	if err != nil {
		return outcome, ErrUnavailable
	}
	candidateRunByDay := make(map[string]string)
	for rows.Next() {
		var day, runID string
		if err := rows.Scan(&day, &runID); err != nil {
			rows.Close()
			return outcome, ErrUnavailable
		}
		candidateRunByDay[day] = runID
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return outcome, ErrUnavailable
	}

	redrivenPrefix, skippedOutcome := "redriven", "skipped_ineligible"
	if dryRun {
		redrivenPrefix, skippedOutcome = "would_redrive", "would_skip_ineligible"
	}
	var redrivenDays, redrivenFromSucceededDays, skippedDays int
	for day := from; !day.After(to); day = day.AddDate(0, 0, 1) {
		dayKey := day.Format(dailyTargetDayLayout)
		runID, ok := candidateRunByDay[dayKey]
		if !ok {
			outcome.Days = append(outcome.Days, FinalizeRangeRedriveDay{Day: day, Outcome: skippedOutcome})
			skippedDays++
			continue
		}
		matched, resetFromSucceeded, err := store.redriveOneFinalizeForRange(
			ctx, publisher, runID, nonce, now, includeSucceeded, reason, dryRun,
		)
		if err != nil {
			return outcome, err
		}
		if matched {
			dayOutcome := redrivenPrefix
			if resetFromSucceeded {
				dayOutcome = redrivenPrefix + "_reset_from_succeeded"
			}
			outcome.Days = append(outcome.Days, FinalizeRangeRedriveDay{
				Day: day, RunID: runID, Outcome: dayOutcome, ResetFromSucceeded: resetFromSucceeded,
			})
			redrivenDays++
			if resetFromSucceeded {
				redrivenFromSucceededDays++
			}
		} else {
			outcome.Days = append(outcome.Days, FinalizeRangeRedriveDay{Day: day, RunID: runID, Outcome: skippedOutcome})
			skippedDays++
		}
	}
	if !dryRun {
		// Team-lead's approval condition (5): the redriven-from-succeeded
		// subset gets its own telemetry outcome label, distinct from an
		// ordinary never-attempted/failed/expired-lease redrive -- a dry run
		// makes no durable state change at all, so it must never move a
		// counter meant to describe real work.
		store.observeFinalizeRedrive("redriven", redrivenDays-redrivenFromSucceededDays)
		store.observeFinalizeRedrive("redriven_reset_from_succeeded", redrivenFromSucceededDays)
		store.observeFinalizeRedrive("skipped_ineligible", skippedDays)
	}
	return outcome, nil
}

// redriveOneFinalizeForRange re-verifies and, if still eligible, redrives
// exactly one day's finalize inside its own transaction, so one day's
// dedupe/policy rejection cannot roll back an otherwise-successful range.
// Returns whether it matched (would redrive / redrove) and whether that
// required resetting a terminal 'succeeded' state. dryRun performs the
// identical read and eligibility check under the identical row lock, but
// never executes the reset, the provenance insert, or the publish -- the
// transaction is always rolled back, never committed, so a dry run is
// provably free of durable side effects (team-lead's approval condition
// (4)).
func (store *PostgresStore) redriveOneFinalizeForRange(
	ctx context.Context,
	publisher *PostgresPublisher,
	runID, nonce string,
	now time.Time,
	includeSucceeded bool,
	reason string,
	dryRun bool,
) (matched, resetFromSucceeded bool, err error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return false, false, ErrUnavailable
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	var run Run
	var targetDay, status, finalizationStatus string
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
		&run.ID, &run.OrganizationID, &run.Generation, &status, &targetDay,
		&finalizationStatus, &leaseExpiresAt, &hasPartitions, &allPartitionsSucceeded,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, ErrUnavailable
	}
	if !hasPartitions || !allPartitionsSucceeded {
		return false, false, nil
	}
	run.Status = status
	priorStatus, priorFinalizationStatus := status, finalizationStatus

	needsReset := false
	switch status {
	case "running":
		// Same eligibility RedriveStrandedFinalize uses -- a run still
		// legitimately in flight for another reason (a live finalize lease)
		// must not be touched.
		priorAttempt := finalizationStatus == "failed" ||
			(finalizationStatus == "running" && (leaseExpiresAt == nil || leaseExpiresAt.Before(now)))
		if finalizationStatus != "pending" && !priorAttempt {
			return false, false, nil
		}
	case "succeeded":
		if !includeSucceeded {
			return false, false, nil
		}
		needsReset = true
	default:
		// pending/failed/canceled/no_repositories: never this function's
		// concern -- CHAOS-4389's own sweep (or nothing at all) owns those.
		return false, false, nil
	}

	if dryRun {
		// Eligibility is fully determined -- report it and roll back
		// without touching anything durable.
		return true, needsReset, nil
	}

	if needsReset {
		// Provenance FIRST (approval condition (1)): a reset is a state
		// write on a succeeded run and must be traceable, in the SAME
		// transaction as the reset and the publish below -- either the
		// whole triple (provenance + reset + publish) lands, or none of it
		// does. Written before the reset itself so the row this transaction
		// is about to overwrite is captured exactly as it read.
		if _, err := tx.Exec(ctx, `
INSERT INTO public.daily_metrics_finalize_redrive_events
    (id, run_id, org_id, target_day, prior_status, prior_finalization_status, actor, reason, nonce)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4::date, $5, $6, 'finalize-redrive', $7, $8)`,
			uuid.New(), runID, run.OrganizationID, targetDay, priorStatus, priorFinalizationStatus, reason, nonce,
		); err != nil {
			return false, false, ErrUnavailable
		}
		// Reset the terminal state back to claimable in the SAME
		// transaction as the provenance row above and the publish below --
		// either this fully lands (provenance + reset + fresh outbox row)
		// or fully rolls back, never a reset with no way to reach
		// FinalizeHandler.Work again.
		//
		// generation is ALSO reset here, to a fresh value derived from
		// nonce (CHAOS-4405 finding, posted on #1971): the Python
		// compatibility bridge's execution-ledger identity is
		// uuid5(run_id, family, generation, scope_digest) -- unchanged
		// generation means the SAME identity, and that identity already
		// reached 'succeeded' the first time this run finalized.
		// _reserve_execution's existing-row check would find it, return
		// {"status": "skipped"}, and never call run_daily_metrics_finalize
		// again -- a bare status reset through this endpoint is a
		// guaranteed silent no-op, not a retry (see
		// test_finalize_execution_is_skipped_not_reexecuted_for_the_same_identity_after_reclaim,
		// CHAOS-4409's own contract test pinning exactly this). A fresh
		// generation gives the redriven attempt a genuinely new identity,
		// so run_daily_metrics_finalize actually executes again. Bounded
		// to the column's 64-byte limit: "redrive:" (8) + a UUID nonce
		// (36) = 44 bytes, well inside it, and independent of how long the
		// run's ORIGINAL generation string was (e.g. a "post-sync:<uuid>"
		// prefix) -- this run's partitions are all already 'succeeded' and
		// will never be redispatched under any generation, so nothing else
		// ever reads this run's stored generation value for partition
		// identity again.
		newGeneration := "redrive:" + nonce
		command, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'running', finalization_status = 'pending',
    finalization_claim_token = NULL, finalization_lease_expires_at = NULL,
    generation = $3, updated_at = $1
WHERE id = $2::uuid AND status = 'succeeded' AND finalization_status = 'succeeded'`,
			now, runID, newGeneration)
		if err != nil {
			return false, false, ErrUnavailable
		}
		if command.RowsAffected() != 1 {
			// Settled differently under us since the row lock above (e.g. a
			// concurrent caller already reset it) -- skip, don't force it.
			return false, false, nil
		}
		run.Status = "running"
		run.Generation = newGeneration
	}

	if run.TargetDay, err = time.Parse(dailyTargetDayLayout, targetDay); err != nil {
		return false, false, ErrInvalidState
	}
	if err := publisher.PublishRedriveFinalizeTx(ctx, tx, run, nonce); err != nil {
		return false, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, false, ErrUnavailable
	}
	return true, needsReset, nil
}

// riverSchemaPattern validates a Postgres schema name before it is
// interpolated into SQL identifier position -- pgx has no bind-param form
// for identifiers. Duplicated from (unexported)
// internal/joboutbox/terminal_delivery_repair.go's riverSchemaPattern: this
// package cannot import that one, and a duplicate is intentional/precedented
// (see that file's own comment on the same constant).
var riverSchemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// ReconcileOrphanedFinalizeRedriveRuns closes the CHAOS-4405 residual gap
// (team-lead escalation, 2026-08-28, same day as the redriven_failed
// escalation): a finalize-redrive event stays 'open' forever if the River
// job it published is discarded/cancelled, or never reaches River at all,
// BEFORE ClaimFinalize is ever called for it. Neither
// CompleteFinalize/ReleaseFinalize (postgres.go, which only ever fire once a
// claim resolves) nor transitionFinalize's own "redriven_failed" close-out
// can ever see this shape, because no claim happens at all -- the run stays
// excluded from FindStrandedFinalizeRuns by its own 'open' row forever,
// invisible to every recovery tool including an unattended
// `daily-finalize --all-complete` sweep.
//
// Two independent orphan shapes, both closed the same way:
//   - The redrive's outbox row never reached River at all: relay-level
//     status='dead' (internal/joboutbox/repository.go exhausts its own
//     delivery attempts and sets this), or the row itself is simply absent
//     (defensive -- the publish that should have created it never landed).
//     Neither needs the queue-control pool or River at all.
//   - The outbox row WAS delivered into River (status='delivered',
//     river_job_id set -- worker_job_outbox's own delivery-state check
//     constraint guarantees the two travel together), but River itself
//     discarded or cancelled that job before a worker ever reached
//     ClaimFinalize. This needs a SEPARATE read against queueControlPool:
//     the domain pool (store.pool) has no grants on the river schema,
//     matching every other caller of river_job in this codebase (e.g.
//     internal/joboutbox/strand_repair.go's NewStrandRepair).
//
// For each event found orphaned, this closes it 'closed_orphaned' with its
// own small UPDATE ... WHERE status = 'open' on the domain pool -- one exec
// per row, not one shared transaction, since the two reads above already
// come from different pools and cannot share one anyway. Re-checking
// status = 'open' on the UPDATE guards the race against
// CompleteFinalize/ReleaseFinalize closing the SAME row concurrently: if
// that already happened between this function's read and its write, the
// UPDATE affects zero rows and this function silently does not double-count
// or overwrite a real outcome with 'closed_orphaned'.
//
// riverSchema must pass riverSchemaPattern -- it is interpolated into SQL
// identifier position, not bound as a parameter. queueControlPool may be nil
// (and riverSchema may be anything) as long as no candidate actually needs
// the River-side check; a candidate that does need it with no pool
// configured is a hard ErrUnavailable, never a silent skip, since silently
// leaving it 'open' is exactly the invisible-forever failure this function
// exists to close.
func (store *PostgresStore) ReconcileOrphanedFinalizeRedriveRuns(
	ctx context.Context,
	queueControlPool *pgxpool.Pool,
	riverSchema string,
) (int, error) {
	if !store.valid() {
		return 0, ErrUnavailable
	}

	rows, err := store.pool.Query(ctx, `
SELECT event.id::text, outbox.status, outbox.river_job_id
FROM public.daily_metrics_finalize_redrive_events AS event
LEFT JOIN public.worker_job_outbox AS outbox
  ON outbox.dedupe_key = 'metrics.daily_finalize:redrive:' || event.run_id::text || ':' || event.nonce
WHERE event.status = 'open'`)
	if err != nil {
		return 0, ErrUnavailable
	}
	type pendingRiverCheck struct {
		eventID    string
		riverJobID int64
	}
	var orphanedEventIDs []string
	var riverCandidates []pendingRiverCheck
	for rows.Next() {
		var eventID string
		var outboxStatus *string
		var riverJobID *int64
		if err := rows.Scan(&eventID, &outboxStatus, &riverJobID); err != nil {
			rows.Close()
			return 0, ErrUnavailable
		}
		switch {
		case outboxStatus == nil:
			// No outbox row at all -- defensive: the publish that should
			// have created it alongside this event, in the same
			// transaction, never landed (or it was already cleaned up).
			// Either way, nothing will ever complete this claim.
			orphanedEventIDs = append(orphanedEventIDs, eventID)
		case *outboxStatus == "dead":
			// Relay-level: exhausted its own delivery attempts before this
			// job ever reached River (internal/joboutbox/repository.go).
			// Nothing River-side to check.
			orphanedEventIDs = append(orphanedEventIDs, eventID)
		case *outboxStatus == "delivered" && riverJobID != nil:
			riverCandidates = append(riverCandidates, pendingRiverCheck{eventID: eventID, riverJobID: *riverJobID})
		default:
			// 'pending'/'claimed' -- the relay itself is plausibly still
			// working this job; leave it 'open'. ('delivered' with a NULL
			// river_job_id cannot happen: worker_job_outbox's own
			// ck_worker_job_outbox_delivery_state check constraint forbids
			// it.)
		}
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return 0, ErrUnavailable
	}

	if len(riverCandidates) > 0 {
		if queueControlPool == nil || !riverSchemaPattern.MatchString(riverSchema) {
			return 0, ErrUnavailable
		}
		jobTable := pgx.Identifier{riverSchema, "river_job"}.Sanitize()
		riverJobIDs := make([]int64, len(riverCandidates))
		for i, candidate := range riverCandidates {
			riverJobIDs[i] = candidate.riverJobID
		}
		riverRows, err := queueControlPool.Query(ctx,
			"SELECT id FROM "+jobTable+" WHERE id = ANY($1) AND state::text IN ('discarded', 'cancelled')",
			riverJobIDs)
		if err != nil {
			return 0, ErrUnavailable
		}
		discardedOrCancelled := make(map[int64]struct{}, len(riverJobIDs))
		for riverRows.Next() {
			var riverJobID int64
			if err := riverRows.Scan(&riverJobID); err != nil {
				riverRows.Close()
				return 0, ErrUnavailable
			}
			discardedOrCancelled[riverJobID] = struct{}{}
		}
		riverRowsErr := riverRows.Err()
		riverRows.Close()
		if riverRowsErr != nil {
			return 0, ErrUnavailable
		}
		for _, candidate := range riverCandidates {
			if _, ok := discardedOrCancelled[candidate.riverJobID]; ok {
				orphanedEventIDs = append(orphanedEventIDs, candidate.eventID)
			}
		}
	}

	now := store.now().UTC()
	closed := 0
	for _, eventID := range orphanedEventIDs {
		command, err := store.pool.Exec(ctx, `
UPDATE public.daily_metrics_finalize_redrive_events
SET status = 'closed_orphaned', closed_at = $1
WHERE id = $2::uuid AND status = 'open'`, now, eventID)
		if err != nil {
			return closed, ErrUnavailable
		}
		closed += int(command.RowsAffected())
	}
	store.observeFinalizeRedrive("redriven_orphaned", closed)
	return closed, nil
}
