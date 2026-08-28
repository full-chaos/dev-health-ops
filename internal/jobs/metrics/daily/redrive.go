package daily

import (
	"context"
	"sort"
	"time"
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
	// gap). One row per partition, carrying its run's identity alongside it,
	// so each can be published directly without a second round trip.
	partitionRows, err := tx.Query(ctx, `
SELECT partition.id::text, partition.run_id::text, partition.repo_ids::text,
       run.org_id::text, run.generation, run.status, run.target_day::text
FROM public.daily_metrics_partitions AS partition
JOIN public.daily_metrics_runs AS run ON run.id = partition.run_id
WHERE run.org_id = $1::uuid
  AND run.target_day BETWEEN $2 AND $3
  AND run.status = 'running'
  AND partition.status IN ('pending', 'failed')
ORDER BY partition.run_id, partition.ordinal`, orgID, from, to)
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
