package daily

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// SupportedPartitionRecomputeFamilies lists the `metrics partition-recompute`
// families this verb accepts (CHAOS-4459). A metrics.daily partition always
// computes every family in ONE HTTP compatibility-bridge call plus whichever
// native executors have cut over (job_daily.py's run_daily_metrics_job,
// compute.py:149's compute_daily_metrics) -- there is no way to recompute
// JUST repo_user_commit without recomputing the partition's other families
// alongside it. --family therefore scopes intent/audit (which gap this
// invocation is repairing, recorded in the provenance row below), not the
// actual blast radius: every family in the partition is recomputed. This is
// safe -- every other native/bridge family's writer is append-only with
// computed_at-keyed reader dedup (see repouser/clickhouse.go's Writer doc
// comment on the shared fail-open contract) -- but it is NOT free, so this
// list stays closed rather than accepting an arbitrary string.
var SupportedPartitionRecomputeFamilies = []string{"repo_user_commit"}

func isSupportedPartitionRecomputeFamily(family string) bool {
	for _, supported := range SupportedPartitionRecomputeFamilies {
		if supported == family {
			return true
		}
	}
	return false
}

// PartitionRangeRedriveDay is one calendar day's outcome from
// RedrivePartitionsForRange.
type PartitionRangeRedriveDay struct {
	Day     time.Time
	RunID   string
	Outcome string
}

// PartitionRangeRedriveOutcome summarizes one partition-recompute pass.
type PartitionRangeRedriveOutcome struct {
	Days []PartitionRangeRedriveDay
}

// RedrivePartitionsForRange repairs the CHAOS-4459 class: a
// daily_metrics_run whose partitions are ALL status='succeeded' (the ledger
// reports the day complete) but whose family output was computed under a
// writer that is now known to have been wrong for at least one of the
// partition's families (CHAOS-4341: the native repo_user_commit executor
// wrote org_id="" on repo_metrics_daily/user_metrics_daily/commit_metrics
// before PR #1960). Unlike RedriveStrandedPartitions (CHAOS-4358), which
// only ever touches a run still status='running' with dispatchable
// (pending/failed/expired-lease) partitions, a 'succeeded' run's partitions
// are never dispatchable again through any existing path -- daily-redrive's
// own eligibility query excludes them by construction, exactly the gap
// CHAOS-4459 names.
//
// Mirrors RedriveFinalizeForRange's (CHAOS-4405) shape: one candidate run
// per calendar day, its own transaction per day so one day's ineligibility
// cannot roll back an otherwise-successful range, a fresh generation minted
// from nonce so the redriven attempt gets a genuinely new
// compatibility-bridge execution-ledger identity
// (uuid5(run_id, family, generation, scope_digest) -- an unchanged
// generation would make _reserve_execution find the SAME identity already
// 'succeeded' and skip re-executing it), and a durable provenance row
// (daily_metrics_partition_recompute_events) written in the SAME
// transaction as the reset and the fresh publish, before either.
//
// family must be one of SupportedPartitionRecomputeFamilies -- recorded on
// the provenance row for audit, but see that var's doc comment for why it
// does not narrow which families are actually recomputed. nonce must be
// unique per invocation (ignored, and may be empty, under dryRun).
// dryRun computes and reports the identical eligibility scan a real pass
// would, under the identical row lock, without resetting anything, writing
// the provenance row, or publishing -- every transaction this function opens
// under dryRun is rolled back, never committed.
func (store *PostgresStore) RedrivePartitionsForRange(
	ctx context.Context,
	publisher *PostgresPublisher,
	orgID string,
	fromDay, toDay time.Time,
	nonce string,
	family string,
	reason string,
	dryRun bool,
) (PartitionRangeRedriveOutcome, error) {
	var outcome PartitionRangeRedriveOutcome
	if !store.valid() || !validUUID(orgID) || publisher == nil || (!dryRun && nonce == "") {
		return outcome, ErrUnavailable
	}
	if !isSupportedPartitionRecomputeFamily(family) {
		return outcome, ErrInvalidState
	}
	if !dryRun && strings.TrimSpace(reason) == "" {
		return outcome, ErrUnavailable
	}
	if toDay.Before(fromDay) {
		return outcome, ErrInvalidState
	}
	from := fromDay.UTC().Truncate(24 * time.Hour)
	to := toDay.UTC().Truncate(24 * time.Hour)

	// One candidate run per calendar day: whichever run for that day has
	// every partition succeeded, preferring the most recently updated when
	// more than one generation qualifies -- same shape as
	// RedriveFinalizeForRange's own candidate scan.
	rows, err := store.pool.Query(ctx, `
SELECT DISTINCT ON (run.target_day) run.target_day::text, run.id::text
FROM public.daily_metrics_runs AS run
WHERE run.org_id = $1::uuid AND run.target_day BETWEEN $2 AND $3
  AND run.status = 'succeeded'
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

	redrivenOutcome, skippedOutcome := "redriven", "skipped_ineligible"
	if dryRun {
		redrivenOutcome, skippedOutcome = "would_redrive", "would_skip_ineligible"
	}
	var redrivenDays, skippedDays int
	for day := from; !day.After(to); day = day.AddDate(0, 0, 1) {
		dayKey := day.Format(dailyTargetDayLayout)
		runID, ok := candidateRunByDay[dayKey]
		if !ok {
			outcome.Days = append(outcome.Days, PartitionRangeRedriveDay{Day: day, Outcome: skippedOutcome})
			skippedDays++
			continue
		}
		matched, err := store.redriveOnePartitionsForRange(ctx, publisher, runID, nonce, family, reason, dryRun)
		if err != nil {
			return outcome, err
		}
		if matched {
			outcome.Days = append(outcome.Days, PartitionRangeRedriveDay{Day: day, RunID: runID, Outcome: redrivenOutcome})
			redrivenDays++
		} else {
			outcome.Days = append(outcome.Days, PartitionRangeRedriveDay{Day: day, RunID: runID, Outcome: skippedOutcome})
			skippedDays++
		}
	}
	if !dryRun {
		store.observePartitionRecompute(family, "redriven", redrivenDays)
		store.observePartitionRecompute(family, "skipped_ineligible", skippedDays)
	}
	return outcome, nil
}

// redriveOnePartitionsForRange re-verifies and, if still eligible, resets
// and redrives exactly one day's partitions inside its own transaction.
// Returns whether it matched (would redrive / redrove). dryRun performs the
// identical read and eligibility check under the identical row lock but
// never executes the reset, the provenance insert, or the publish -- the
// transaction is always rolled back, never committed.
func (store *PostgresStore) redriveOnePartitionsForRange(
	ctx context.Context,
	publisher *PostgresPublisher,
	runID, nonce, family, reason string,
	dryRun bool,
) (matched bool, err error) {
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
	var targetDay, status, priorGeneration string
	var hasPartitions, allPartitionsSucceeded bool
	err = tx.QueryRow(ctx, `
SELECT run.id::text, run.org_id::text, run.generation, run.status, run.target_day::text,
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
		&run.ID, &run.OrganizationID, &priorGeneration, &status, &targetDay,
		&hasPartitions, &allPartitionsSucceeded,
	)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, ErrUnavailable
	}
	// Settled differently under us since the candidate scan above (e.g. a
	// concurrent redrive already reset it, or it never fully succeeded) --
	// skip, don't force it. Only a terminal 'succeeded' run with every
	// partition succeeded is this verb's concern; 'running' is
	// daily-redrive's/the automatic path's territory.
	if status != "succeeded" || !hasPartitions || !allPartitionsSucceeded {
		return false, nil
	}

	if dryRun {
		return true, nil
	}

	if run.TargetDay, err = time.Parse(dailyTargetDayLayout, targetDay); err != nil {
		return false, ErrInvalidState
	}

	// Fresh generation (CHAOS-4405 precedent): the compatibility bridge's
	// execution-ledger identity is uuid5(run_id, family, generation,
	// scope_digest) -- the ORIGINAL identity already reached 'succeeded',
	// so an unchanged generation would make a redriven attempt land on the
	// SAME identity and get silently skipped rather than actually
	// recomputing. run_id itself is unchanged: only the generation VALUE
	// this run is stored under moves.
	newGeneration := "recompute:" + nonce
	run.Generation = newGeneration

	// Provenance FIRST (CHAOS-4405 precedent, team-lead's approval
	// condition (1)): a reset of a terminal row is itself a state write and
	// must be traceable, in the SAME transaction as the reset and the
	// publish below -- either the whole triple lands, or none of it does.
	if _, err := tx.Exec(ctx, `
INSERT INTO public.daily_metrics_partition_recompute_events
    (id, run_id, org_id, target_day, family, prior_status, prior_generation, actor, reason, nonce)
VALUES ($1::uuid, $2::uuid, $3::uuid, $4::date, $5, $6, $7, 'partition-recompute', $8, $9)`,
		uuid.New(), runID, run.OrganizationID, targetDay, family, status, priorGeneration, reason, nonce,
	); err != nil {
		return false, ErrUnavailable
	}

	now := store.now().UTC()
	command, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_runs
SET status = 'running', generation = $3,
    finalization_status = 'pending', finalization_claim_token = NULL,
    finalization_lease_expires_at = NULL, finalized_at = NULL, updated_at = $1
WHERE id = $2::uuid AND status = 'succeeded'`,
		now, runID, newGeneration)
	if err != nil {
		return false, ErrUnavailable
	}
	if command.RowsAffected() != 1 {
		// Re-verify raced us between the row lock above and this write --
		// skip rather than force it.
		return false, nil
	}

	partitionRows, err := tx.Query(ctx, `
SELECT id::text, repo_ids::text
FROM public.daily_metrics_partitions
WHERE run_id = $1::uuid
ORDER BY ordinal`, runID)
	if err != nil {
		return false, ErrUnavailable
	}
	type target struct {
		partition Partition
	}
	var targets []target
	for partitionRows.Next() {
		var repoIDs string
		var one target
		one.partition.RunID = runID
		if err := partitionRows.Scan(&one.partition.ID, &repoIDs); err != nil {
			partitionRows.Close()
			return false, ErrUnavailable
		}
		if one.partition.RepoIDs, err = parsePartitionRepoIDs(repoIDs); err != nil {
			partitionRows.Close()
			return false, ErrInvalidState
		}
		targets = append(targets, one)
	}
	partitionRowsErr := partitionRows.Err()
	partitionRows.Close()
	if partitionRowsErr != nil {
		return false, ErrUnavailable
	}

	if _, err := tx.Exec(ctx, `
UPDATE public.daily_metrics_partitions
SET status = 'pending', claim_token = NULL, lease_expires_at = NULL,
    completed_at = NULL, attempt_count = 0, failure_reason = NULL, updated_at = $1
WHERE run_id = $2::uuid`, now, runID); err != nil {
		return false, ErrUnavailable
	}

	for _, one := range targets {
		if err := publisher.PublishRedrivePartitionTx(ctx, tx, run, one.partition, nonce); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, ErrUnavailable
	}
	return true, nil
}
