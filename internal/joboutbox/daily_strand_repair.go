package joboutbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

const (
	strandRecoveryCode   = "daily_strand_rearmed"
	strandRecoveryDetail = "terminal delivery rearmed while domain work was unfinished"

	dispositionRearm       = "rearm"
	dispositionSkipJobLive = "skip_job_live"
)

// StrandRepairResult counts one pass. Skips are counted, not merely omitted:
// this repair deliberately refuses to touch a job River may still rescue, and
// that refusal is only safe if it is visible. A SkippedJobLive count that climbs
// while nothing is ever rearmed is the signature of a rescuer that has stopped
// running, which would otherwise look identical to "no strands exist".
type StrandRepairResult struct {
	Rearmed        int
	SkippedJobLive int
}

// StrandRepair rearms a daily-metrics outbox row whose River delivery is
// terminal while the authoritative domain row proves the work never finished.
//
// It exists because a job that reports success is deleted by River, and the
// reclaim path that would have recovered the domain lease is only ever reached
// by a job (CHAOS-3991). Nothing else re-drives that work: the operator retry
// verb is refused unconditionally by a fail-closed Phase 1 domain guard
// (CHAOS-4030), and republishing is a no-op because the outbox row still holds
// its dedupe key. Rearming the row and deleting the dead delivery in one
// transaction is the only path that produces a fresh, executable job.
type StrandRepair struct {
	begin  func(context.Context) (pgx.Tx, error)
	client interface {
		JobDeleteTx(context.Context, pgx.Tx, int64) (*rivertype.JobRow, error)
	}
	partitionQuery string
	finalizeQuery  string
}

func NewStrandRepair(queueControlPool *pgxpool.Pool, riverSchema string) (*StrandRepair, error) {
	if queueControlPool == nil || !riverSchemaPattern.MatchString(riverSchema) {
		return nil, ErrInvalidConfiguration
	}
	jobTable := pgx.Identifier{riverSchema, "river_job"}.Sanitize()
	client, err := river.NewClient(riverpgxv5.New(queueControlPool), &river.Config{Schema: riverSchema})
	if err != nil {
		return nil, ErrInvalidConfiguration
	}
	return &StrandRepair{
		begin:          queueControlPool.Begin,
		client:         riverDeleteAdapter{client: client},
		partitionQuery: fmt.Sprintf(repairStrandedPartitionSQL, jobTable),
		finalizeQuery:  fmt.Sprintf(repairStrandedFinalizeSQL, jobTable),
	}, nil
}

// Step runs one bounded pass over both daily shapes.
func (repair *StrandRepair) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (StrandRepairResult, error) {
	if repair == nil || repair.begin == nil || repair.client == nil || ctx == nil || now.IsZero() ||
		limit < minReconcilerLimit || limit > maxReconcilerLimit {
		return StrandRepairResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return StrandRepairResult{}, err
	}
	result := StrandRepairResult{}
	for _, query := range []string{repair.partitionQuery, repair.finalizeQuery} {
		shape, err := repair.stepShape(ctx, query, now, limit)
		if err != nil {
			return StrandRepairResult{}, err
		}
		result.Rearmed += shape.Rearmed
		result.SkippedJobLive += shape.SkippedJobLive
	}
	return result, nil
}

func (repair *StrandRepair) stepShape(
	ctx context.Context,
	query string,
	now time.Time,
	limit int,
) (StrandRepairResult, error) {
	tx, err := repair.begin(ctx)
	if err != nil || tx == nil {
		return StrandRepairResult{}, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, query, now.UTC(), limit)
	if err != nil || rows == nil {
		return StrandRepairResult{}, ErrUnavailable
	}
	defer rows.Close()
	type candidate struct {
		outboxID    string
		riverJobID  int64
		disposition string
	}
	candidates := make([]candidate, 0, limit)
	for rows.Next() {
		var found candidate
		if err := rows.Scan(&found.outboxID, &found.riverJobID, &found.disposition); err != nil ||
			!uuidPattern.MatchString(found.outboxID) || found.riverJobID <= 0 {
			return StrandRepairResult{}, ErrUnavailable
		}
		candidates = append(candidates, found)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(candidates) > limit {
		return StrandRepairResult{}, ErrUnavailable
	}
	result := StrandRepairResult{}
	for _, found := range candidates {
		switch found.disposition {
		case dispositionSkipJobLive:
			result.SkippedJobLive++
			continue
		case dispositionRearm:
		default:
			return StrandRepairResult{}, ErrUnavailable
		}
		deleted, err := repair.client.JobDeleteTx(ctx, tx, found.riverJobID)
		if err != nil || deleted == nil || deleted.ID != found.riverJobID {
			return StrandRepairResult{}, ErrUnavailable
		}
		// The delete must have removed a terminal row. Re-checking the returned
		// state closes the window between the predicate and the delete: a job
		// that became runnable in between must not be removed.
		if !terminalRiverState(deleted.State) {
			return StrandRepairResult{}, ErrUnavailable
		}
		command, err := tx.Exec(ctx, `
			UPDATE public.worker_job_outbox
			SET status = 'pending', next_attempt_at = $2,
				last_error_code = $3, last_error_detail = $4, last_error_at = $2,
				river_job_id = NULL, delivered_at = NULL,
				claim_token = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = $2
			WHERE id = $1 AND status = 'delivered'`,
			found.outboxID, now.UTC(), strandRecoveryCode, strandRecoveryDetail)
		if err != nil || command.RowsAffected() != 1 {
			return StrandRepairResult{}, ErrUnavailable
		}
		result.Rearmed++
	}
	if err := tx.Commit(ctx); err != nil {
		return StrandRepairResult{}, ErrUnavailable
	}
	return result, nil
}

func terminalRiverState(state rivertype.JobState) bool {
	return state == rivertype.JobStateCompleted ||
		state == rivertype.JobStateDiscarded ||
		state == rivertype.JobStateCancelled
}

// Both queries select on DOMAIN state and report a disposition rather than
// filtering, so a refusal is counted instead of vanishing.
//
// Safety here comes from the domain row, not from the job state. That is why
// these accept a `completed` delivery where the provider-unit repair accepts
// only `discarded`: CHAOS-3991 strands work by making a job report SUCCESS, so
// requiring a failed-looking job would miss every row this repair exists for.
// The authoritative partition or run row is what proves the work never
// finished, and no rearm happens without it.
//
// Only TERMINAL River deliveries are rearmed. A job River may still rescue is
// non-terminal by definition, so the stale-but-not-yet-rescuable window is
// excluded structurally rather than by recomputing River's rescue horizon --
// which would couple this SQL to RescueStuckJobsAfter and to each kind's
// timeout, and would silently re-break when either moved.
//
// A live idempotency lease must also never be rearmed past: that job would be
// ACKed as a duplicate success without reaching its handler, manufacturing a
// fresh strand instead of clearing one. This is enforced WITHOUT reading
// worker_job_runs, because the queue-control role must never be granted access
// to execution state or external-effect evidence.
//
// The domain lease is a sound proxy. Both leases are ten minutes, both renew
// every lease/3, and both are renewed by the same worker process -- the
// idempotency claim is taken BEFORE the handler acquires the domain lease, so
// it expires no later. A dead worker therefore leaves both expired together,
// and an expired domain lease implies an expired idempotency lease.
//
// The converse is covered too: if a replacement claimant holds a live
// idempotency lease, it has also taken the domain lease, and the
// `lease_expires_at <= $1` predicate below already excludes that row.
const repairStrandedPartitionSQL = `
	SELECT outbox.id::text, job.id,
		CASE
			WHEN job.state::text NOT IN ('completed', 'discarded', 'cancelled') THEN 'skip_job_live'
			ELSE 'rearm'
		END AS disposition
	FROM public.worker_job_outbox AS outbox
	JOIN public.daily_metrics_partitions AS partition
		ON partition.id::text = outbox.args #>> '{domain,id}'
	JOIN public.daily_metrics_runs AS run
		ON run.id = partition.run_id
	JOIN %s AS job
		ON job.id = outbox.river_job_id
	WHERE outbox.job_kind = 'metrics.daily_partition'
		AND outbox.status = 'delivered'
		AND outbox.river_job_id IS NOT NULL
		AND outbox.args #>> '{domain,type}' = 'daily_metrics_partition'
		AND partition.status = 'running'
		AND partition.lease_expires_at IS NOT NULL
		AND partition.lease_expires_at <= $1
		AND run.status = 'running'
	ORDER BY outbox.delivered_at, outbox.id
	FOR UPDATE OF outbox, job SKIP LOCKED
	LIMIT $2::int
`

// The finalize shape additionally preserves ClaimFinalize's own eligibility
// guard: a run whose partitions are not all succeeded is still owned by the
// partition layer, and rearming its finalizer would produce a job that can only
// no-op.
const repairStrandedFinalizeSQL = `
	SELECT outbox.id::text, job.id,
		CASE
			WHEN job.state::text NOT IN ('completed', 'discarded', 'cancelled') THEN 'skip_job_live'
			ELSE 'rearm'
		END AS disposition
	FROM public.worker_job_outbox AS outbox
	JOIN public.daily_metrics_runs AS run
		ON run.id::text = outbox.args #>> '{domain,id}'
	JOIN %s AS job
		ON job.id = outbox.river_job_id
	WHERE outbox.job_kind = 'metrics.daily_finalize'
		AND outbox.status = 'delivered'
		AND outbox.river_job_id IS NOT NULL
		AND outbox.args #>> '{domain,type}' = 'daily_metrics_run'
		AND run.status = 'running'
		AND run.finalization_status IN ('pending', 'running', 'failed')
		AND (
			run.finalization_lease_expires_at IS NULL
			OR run.finalization_lease_expires_at <= $1
		)
		AND NOT EXISTS (
			SELECT 1 FROM public.daily_metrics_partitions AS sibling
			WHERE sibling.run_id = run.id AND sibling.status <> 'succeeded'
		)
	ORDER BY outbox.delivered_at, outbox.id
	FOR UPDATE OF outbox, job SKIP LOCKED
	LIMIT $2::int
`
