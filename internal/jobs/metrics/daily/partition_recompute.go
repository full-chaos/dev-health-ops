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
// actual blast radius: every family in the partition is recomputed.
//
// CORRECTION (codex review, P1): this is NOT universally safe. Most
// native/bridge family writers are append-only with computed_at-keyed
// reader dedup, but NOT ALL -- file_metrics_daily's hotspot/churn readers
// (worker_metrics.py:2469-2474) sum raw rows with no argMax dedup, so a
// second generation for the same day double-counts them. An operator using
// this command is therefore choosing to also rewrite hotspot/churn numbers
// for the recomputed org+day; the CLI help text and docs call this out. A
// real per-family-scoped recompute (rather than whole-partition) needs
// either a skip_families-on-publish mechanism (today skipFamilies is
// computed dynamically at claim time from ledger state, not settable by
// the publisher -- see daily.go's computeNativeFamilies) or fixing every
// non-dedup-safe reader; tracked as follow-up, not attempted here.
var SupportedPartitionRecomputeFamilies = []string{"repo_user_commit"}

// recomputeGenerationMarker is APPENDED (never prepended -- see below) to a
// daily_metrics_runs.generation value to mark a CHAOS-4459
// partition-recompute reset, so CompletePartition can detect it and publish
// finalize under a fresh redrive-scoped key instead of the ordinary
// permanently-deduped one (codex review, P1 -- see CompletePartition's own
// doc comment on why the ordinary publish is a silent no-op here).
//
// Appended, not a replacement prefix (codex review, P2): the compatibility
// bridge's execution-ledger identity is uuid5(run_id, family, generation,
// scope_digest), so generation must genuinely change for the recompute to
// actually re-execute -- but Python's _LATEST_DAILY_METRICS_RUN_SQL
// (workers/recommendations_tasks.py) finds a day's AUTHORITATIVE run via
// `generation LIKE 'fixed-schedule:daily_metrics_fanout:%'`, a
// classification prefix that must keep matching. Replacing the whole value
// (this ticket's first version) made a run mid-recompute -- or stuck,
// indefinitely, if the recompute fails -- invisible to that readiness
// check entirely, not just "not yet ready". Appending preserves the
// producer classification while still yielding a distinct string (and
// therefore a fresh uuid5 identity). Requires widening
// daily_metrics_runs.generation from varchar(64) to text (migration
// 0117): the widest classified value (the fan-out prefix, ~57 bytes) plus
// this suffix (11 bytes + a 36-byte UUID) does not fit 64.
const recomputeGenerationMarker = "#recompute:"

// recomputeNonce extracts the nonce RedrivePartitionsForRange appended to
// generation, if generation belongs to a partition-recompute reset.
func recomputeNonce(generation string) (string, bool) {
	_, nonce, found := strings.Cut(generation, recomputeGenerationMarker)
	if !found {
		return "", false
	}
	return nonce, true
}

// redriveFinalizePublisher is the optional capability CompletePartition
// uses to publish a fresh-key finalize job for a run RedrivePartitionsForRange
// reset (CHAOS-4459). Deliberately a narrow type assertion against the
// Publisher CompletePartition already receives, not a Publisher interface
// change -- every existing Publisher fake/implementer used by other tests
// is unaffected; only *PostgresPublisher (which already has this method,
// publisher.go) satisfies it in production.
type redriveFinalizePublisher interface {
	PublishRedriveFinalizeTx(ctx context.Context, tx pgx.Tx, run Run, nonce string) error
}

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

	// One candidate run per calendar day, restricted to the SCHEDULED
	// FAN-OUT generation specifically (codex review, P1): unlike
	// RedriveFinalizeForRange (which re-reads the whole org/day from
	// ClickHouse regardless of which run's partition rows triggered it),
	// this verb resets a run's OWN partitions, and those partitions carry a
	// specific repo_ids scope. A post-sync run's scope is whatever repos
	// that sync touched -- often a narrow subset of the org -- while the
	// nightly fan-out's scope is the full org repo set
	// (MaterializeScheduledFanout). Picking "most recently updated" could
	// silently select a narrow post-sync run, report the whole day
	// "redriven", and leave every repo outside that sync's scope still
	// wrong. Restricting to the fan-out generation makes the selected run's
	// scope unambiguous and always org-wide; a day with no succeeded
	// fan-out run is reported skipped_ineligible rather than guessed at.
	rows, err := store.pool.Query(ctx, `
SELECT DISTINCT ON (run.target_day) run.target_day::text, run.id::text
FROM public.daily_metrics_runs AS run
WHERE run.org_id = $1::uuid AND run.target_day BETWEEN $2 AND $3
  AND run.status = 'succeeded'
  AND run.generation LIKE $4
  AND EXISTS (
      SELECT 1 FROM public.daily_metrics_partitions AS partition
      WHERE partition.run_id = run.id
  )
  AND NOT EXISTS (
      SELECT 1 FROM public.daily_metrics_partitions AS partition
      WHERE partition.run_id = run.id AND partition.status <> 'succeeded'
  )
ORDER BY run.target_day, run.updated_at DESC`, orgID, from, to, scheduledFanoutGenerationPrefix+"%")
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
	// daily-redrive's/the automatic path's territory. Re-verified under the
	// row lock, matching the candidate scan's own fan-out-only restriction
	// (codex review, P1) -- a non-fan-out generation must never reach the
	// reset below even if something upstream let it through.
	if status != "succeeded" || !hasPartitions || !allPartitionsSucceeded ||
		!isScheduledFanoutGeneration(priorGeneration) {
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
	// this run is stored under moves. Appended (recomputeGenerationMarker's
	// own doc comment), not replaced: preserves the fixed-schedule
	// classification prefix _LATEST_DAILY_METRICS_RUN_SQL keys off.
	newGeneration := priorGeneration + recomputeGenerationMarker + nonce
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
