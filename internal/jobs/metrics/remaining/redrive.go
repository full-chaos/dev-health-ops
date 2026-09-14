package remaining

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// StrandedPartition is one 'failed' partition of a StrandedRun, as reported
// by StrandedRuns.
type StrandedPartition struct {
	ID      string
	Ordinal int
}

// StrandedRun is one remaining_metric_runs row still status='running' that
// StrandedRuns found carrying at least one 'failed' partition -- a run
// PartitionHandler.Work's own last-partition terminalize logic
// (PostgresStore.ReleasePartitionTerminally) has not yet closed out, because
// at least one other partition of it is still pending/running (still
// legitimately in flight), or because it dates from before that logic
// existed at all.
type StrandedRun struct {
	RunID            string
	Family           string
	Generation       string
	ScopeKey         string
	FailedPartitions []StrandedPartition
	SucceededCount   int
	PendingOrRunning int
}

// StrandedRuns lists every remaining_metric_runs row in orgID (optionally
// narrowed to one family and/or one run) that is status='running' with at
// least one 'failed' partition -- the read-only table `metrics remaining
// redrive` prints before doing anything else, in both --dry-run and real
// invocations. family/runID empty means unfiltered; runID, if given, must
// already be a canonical UUID string.
func (store *PostgresStore) StrandedRuns(
	ctx context.Context, orgID, family, runID string,
) ([]StrandedRun, error) {
	if !store.valid() || !validUUID(orgID) {
		return nil, ErrUnavailable
	}
	if runID != "" && !validUUID(runID) {
		return nil, ErrInvalidState
	}
	rows, err := store.pool.Query(ctx, `
SELECT run.id::text, run.family, run.generation, run.scope_key,
    coalesce(array_agg(partition.id::text ORDER BY partition.ordinal)
        FILTER (WHERE partition.status = 'failed'), '{}'),
    coalesce(array_agg(partition.ordinal ORDER BY partition.ordinal)
        FILTER (WHERE partition.status = 'failed'), '{}'),
    count(*) FILTER (WHERE partition.status = 'succeeded'),
    count(*) FILTER (WHERE partition.status IN ('pending', 'running'))
FROM public.remaining_metric_runs AS run
JOIN public.remaining_metric_partitions AS partition ON partition.run_id = run.id
WHERE run.org_id = $1::uuid AND run.status = 'running'
  AND ($2 = '' OR run.family = $2)
  AND ($3 = '' OR run.id = $3::uuid)
GROUP BY run.id
HAVING count(*) FILTER (WHERE partition.status = 'failed') > 0
ORDER BY run.id`, orgID, family, runID)
	if err != nil {
		return nil, store.wrapUnavailable(ctx, "query stranded runs", err, "family", family)
	}
	defer rows.Close()
	var result []StrandedRun
	for rows.Next() {
		var run StrandedRun
		var partitionIDs []string
		var partitionOrdinals []int
		if err := rows.Scan(
			&run.RunID, &run.Family, &run.Generation, &run.ScopeKey,
			&partitionIDs, &partitionOrdinals, &run.SucceededCount, &run.PendingOrRunning,
		); err != nil {
			return nil, store.wrapUnavailable(ctx, "scan stranded run", err, "family", family)
		}
		for index, id := range partitionIDs {
			ordinal := 0
			if index < len(partitionOrdinals) {
				ordinal = partitionOrdinals[index]
			}
			run.FailedPartitions = append(run.FailedPartitions, StrandedPartition{ID: id, Ordinal: ordinal})
		}
		result = append(result, run)
	}
	if err := rows.Err(); err != nil {
		return nil, store.wrapUnavailable(ctx, "read stranded runs", err, "family", family)
	}
	return result, nil
}

// RemainingRedriveOutcome summarizes one `metrics remaining redrive`
// invocation: exactly one of the two halves below is populated, matching
// whether terminalize was requested.
type RemainingRedriveOutcome struct {
	Terminalize bool
	// RedrivenRunIDs / RedrivenPartitions describe a redrive
	// (Terminalize == false): the failed partitions a fresh job was
	// published for, grouped by run.
	RedrivenRunIDs     []string
	RedrivenPartitions int
	// TerminalizedRunIDs describes a terminalize (Terminalize == true): the
	// runs this pass moved from 'running' to 'failed'.
	TerminalizedRunIDs []string
}

// Redrive is `metrics remaining redrive`'s single entry point (CHAOS gap:
// remaining_metric_runs had a schema-legal 'failed' status nothing ever
// wrote, and no verb ever re-enqueued a run's failed partitions the way
// `metrics daily-redrive` does for the daily family). Scoped to orgID,
// optionally narrowed to one family and/or one run.
//
// terminalize gates which of the two mutually exclusive actions this call
// performs, and reason is REQUIRED whenever terminalize is true (refused
// with ErrInvalidState otherwise, before any write) -- terminalizing a run
// is a one-way trip out of 'running' with no automatic path back, so it
// needs the same "state what you verified" bar every other operator-
// authorized terminal action in this CLI requires. When terminalize is
// false, reason is ignored: an ordinary redrive re-enqueues work through
// the exact same executor path the original dispatch used and is not, by
// itself, a decision that needs justifying.
func (store *PostgresStore) Redrive(
	ctx context.Context,
	publisher *PostgresPublisher,
	orgID, family, runID string,
	nonce string,
	terminalize bool,
	reason string,
) (RemainingRedriveOutcome, error) {
	outcome := RemainingRedriveOutcome{Terminalize: terminalize}
	if terminalize {
		terminalized, err := store.terminalizeStrandedRuns(ctx, orgID, family, runID, reason)
		outcome.TerminalizedRunIDs = terminalized
		return outcome, err
	}
	redrivenRuns, redrivenPartitions, err := store.redriveFailedPartitions(ctx, publisher, orgID, family, runID, nonce)
	outcome.RedrivenRunIDs = redrivenRuns
	outcome.RedrivenPartitions = redrivenPartitions
	return outcome, err
}

// redriveFailedPartitions re-enqueues every currently-'failed' partition of
// every run StrandedRuns finds in scope, one run per transaction (so one
// run's publish failure cannot roll back an otherwise-successful batch),
// re-reading each run's failed partitions under a row lock immediately
// before publishing -- the same defense-in-depth daily's redrive functions
// use -- so a run that settled between the initial scan and this call
// (its last failed partition already terminalized the run, or a concurrent
// redrive already re-enqueued it) is silently skipped rather than
// double-published.
func (store *PostgresStore) redriveFailedPartitions(
	ctx context.Context, publisher *PostgresPublisher, orgID, family, runID, nonce string,
) ([]string, int, error) {
	if !store.valid() || !validUUID(orgID) || publisher == nil || nonce == "" {
		return nil, 0, ErrUnavailable
	}
	candidates, err := store.StrandedRuns(ctx, orgID, family, runID)
	if err != nil {
		return nil, 0, err
	}
	var redrivenRuns []string
	redrivenPartitions := 0
	for _, candidate := range candidates {
		redriven, err := store.redriveOneRun(ctx, publisher, candidate.RunID, nonce)
		if err != nil {
			return redrivenRuns, redrivenPartitions, err
		}
		if redriven > 0 {
			redrivenRuns = append(redrivenRuns, candidate.RunID)
			redrivenPartitions += redriven
		}
	}
	sort.Strings(redrivenRuns)
	return redrivenRuns, redrivenPartitions, nil
}

// redriveOneRun re-verifies and redrives exactly one run's currently-'failed'
// partitions inside its own transaction. Returns how many partitions it
// published a fresh job for.
func (store *PostgresStore) redriveOneRun(
	ctx context.Context, publisher *PostgresPublisher, runID, nonce string,
) (int, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return 0, store.wrapUnavailable(ctx, "begin redrive tx", err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()

	var run Run
	err = tx.QueryRow(ctx, `
SELECT id::text, org_id::text, family, generation, scope_key, status, generation_seed
FROM public.remaining_metric_runs WHERE id = $1::uuid FOR UPDATE`, runID).Scan(
		&run.ID, &run.OrganizationID, &run.Family, &run.Generation, &run.ScopeKey, &run.Status, &run.Seed,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, store.wrapUnavailable(ctx, "load redrive run", err)
	}
	if run.Status != "running" {
		// Settled since the initial scan (terminalized, canceled, or
		// completed) -- nothing left to redrive.
		return 0, nil
	}

	rows, err := tx.Query(ctx, `
SELECT id::text, ordinal, scope
FROM public.remaining_metric_partitions
WHERE run_id = $1::uuid AND status = 'failed'
ORDER BY ordinal`, runID)
	if err != nil {
		return 0, store.wrapUnavailable(ctx, "query redrive partitions", err)
	}
	var partitions []Partition
	for rows.Next() {
		var partition Partition
		if err := rows.Scan(&partition.ID, &partition.Ordinal, &partition.Scope); err != nil {
			rows.Close()
			return 0, store.wrapUnavailable(ctx, "scan redrive partition", err)
		}
		partition.RunID = runID
		partitions = append(partitions, partition)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return 0, store.wrapUnavailable(ctx, "read redrive partitions", rowsErr)
	}
	if len(partitions) == 0 {
		return 0, nil
	}
	for _, partition := range partitions {
		if err := publisher.PublishRedrivePartitionTx(ctx, tx, run, partition, nonce); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, store.wrapUnavailable(ctx, "commit redrive tx", err)
	}
	return len(partitions), nil
}

// terminalizeStrandedRuns moves every run StrandedRuns finds in scope from
// 'running' to 'failed', provided it is genuinely safe to: no partition of
// it may still be 'pending'/'running' (the identical guard
// ReleasePartitionTerminally applies automatically for the last-partition
// case) -- a run with other partitions still legitimately in flight is
// skipped, never force-terminalized out from under still-live work. reason
// must be non-empty; refused with ErrInvalidState otherwise, before any
// candidate is even scanned.
func (store *PostgresStore) terminalizeStrandedRuns(
	ctx context.Context, orgID, family, runID, reason string,
) ([]string, error) {
	if !store.valid() || !validUUID(orgID) {
		return nil, ErrUnavailable
	}
	if strings.TrimSpace(reason) == "" {
		return nil, ErrInvalidState
	}
	candidates, err := store.StrandedRuns(ctx, orgID, family, runID)
	if err != nil {
		return nil, err
	}
	var terminalized []string
	now := store.now().UTC()
	for _, candidate := range candidates {
		command, err := store.pool.Exec(ctx, `
UPDATE public.remaining_metric_runs AS run
SET status = 'failed', updated_at = $1
WHERE run.id = $2::uuid AND run.status = 'running'
  AND EXISTS (
      SELECT 1 FROM public.remaining_metric_partitions AS partition
      WHERE partition.run_id = run.id AND partition.status = 'failed'
  )
  AND NOT EXISTS (
      SELECT 1 FROM public.remaining_metric_partitions AS partition
      WHERE partition.run_id = run.id AND partition.status IN ('pending', 'running')
  )`, now, candidate.RunID)
		if err != nil {
			return terminalized, store.wrapUnavailable(ctx, "terminalize stranded run", err)
		}
		if command.RowsAffected() == 1 {
			terminalized = append(terminalized, candidate.RunID)
		}
	}
	sort.Strings(terminalized)
	return terminalized, nil
}
