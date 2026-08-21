package joboutbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	dispositionSkipGrace   = "skip_idempotency_grace"

	// insufficientPrivilege is PostgreSQL's SQLSTATE for a denied statement.
	// This repair is the first queue-role path to read the daily-metrics and
	// work-graph tables, so a missing grant is a live deployment risk rather
	// than a theoretical one: the grants ship in the ops runtime image
	// (scripts/worker/provision_river_roles.sql) while the posture assertion
	// ships in the Go binaries, and a split rollout leaves one without the
	// other. Returning ErrNotAuthorized rather than ErrUnavailable keeps that
	// failure legible; the sibling sync package's single opaque error made a
	// 42501 read as "database unavailable" for a component that was not even
	// involved.
	insufficientPrivilege = "42501"

	// strandIdempotencyLease mirrors defaultIdempotencyLease at
	// internal/jobruntime/idempotency_postgres.go:28. It cannot be imported:
	// that constant is unexported, and this package must not depend on the
	// execution-state package it deliberately cannot read. The duplication is
	// pinned by TestStrandRepairLeaseProxyPremise, which parses both files and
	// fails if they diverge.
	strandIdempotencyLease = 10 * time.Minute
)

// ErrNotAuthorized reports a statement the queue-control role is not granted.
// It is separate from ErrUnavailable because the two demand different
// operator actions: one is a database outage, the other is a missing grant
// that no amount of retrying will fix.
var ErrNotAuthorized = errors.New("worker outbox role is not authorized")

// StrandRepairResult counts one pass. Skips are counted, not merely omitted:
// this repair deliberately refuses to touch a job River may still rescue, and
// that refusal is only safe if it is visible. A SkippedJobLive count that climbs
// while nothing is ever rearmed is the signature of a rescuer that has stopped
// running, which would otherwise look identical to "no strands exist".
type StrandRepairResult struct {
	Rearmed        int
	SkippedJobLive int
	// SkippedIdempotencyGrace counts deliveries that are terminal but have not
	// been terminal for a full idempotency lease yet. They are reported rather
	// than filtered for the same reason as SkippedJobLive: this guard sits
	// between a strand and a duplicate success, and a guard whose refusals are
	// invisible cannot be distinguished from one that never fires. A count
	// that stays pinned at zero while strands persist means the grace is
	// mis-derived, not that the window never opens.
	SkippedIdempotencyGrace int
}

// StrandRepair rearms a daily-metrics or work-graph outbox row whose River
// delivery is terminal while the authoritative domain row proves the work
// never finished.
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
	workGraphQuery string
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
		workGraphQuery: fmt.Sprintf(repairStrandedWorkGraphSQL, jobTable),
	}, nil
}

// Step runs one bounded pass over every shape.
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
	for _, query := range []string{repair.partitionQuery, repair.finalizeQuery, repair.workGraphQuery} {
		shape, err := repair.stepShape(ctx, query, now, limit)
		if err != nil {
			return StrandRepairResult{}, err
		}
		result.Rearmed += shape.Rearmed
		result.SkippedJobLive += shape.SkippedJobLive
		result.SkippedIdempotencyGrace += shape.SkippedIdempotencyGrace
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
		return StrandRepairResult{}, classifyStrandError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// finalizedBefore is the instant a terminal River job stops being able to
	// carry a live idempotency claim. See the premise note above the queries.
	finalizedBefore := now.UTC().Add(-strandIdempotencyLease)
	rows, err := tx.Query(ctx, query, now.UTC(), limit, finalizedBefore)
	if err != nil || rows == nil {
		return StrandRepairResult{}, classifyStrandError(err)
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
		return StrandRepairResult{}, classifyStrandError(rows.Err())
	}
	result := StrandRepairResult{}
	for _, found := range candidates {
		switch found.disposition {
		case dispositionSkipJobLive:
			result.SkippedJobLive++
			continue
		case dispositionSkipGrace:
			result.SkippedIdempotencyGrace++
			continue
		case dispositionRearm:
		default:
			return StrandRepairResult{}, ErrUnavailable
		}
		deleted, err := repair.client.JobDeleteTx(ctx, tx, found.riverJobID)
		if err != nil || deleted == nil || deleted.ID != found.riverJobID {
			return StrandRepairResult{}, classifyStrandError(err)
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
			return StrandRepairResult{}, classifyStrandError(err)
		}
		result.Rearmed++
	}
	if err := tx.Commit(ctx); err != nil {
		return StrandRepairResult{}, classifyStrandError(err)
	}
	return result, nil
}

func terminalRiverState(state rivertype.JobState) bool {
	return state == rivertype.JobStateCompleted ||
		state == rivertype.JobStateDiscarded ||
		state == rivertype.JobStateCancelled
}

// classifyStrandError separates a denied statement from an unavailable
// database. Everything else stays ErrUnavailable, including a nil error on a
// path that should not have reached here.
func classifyStrandError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == insufficientPrivilege {
		return ErrNotAuthorized
	}
	return ErrUnavailable
}

// Every query selects on DOMAIN state and reports a disposition rather than
// filtering, so a refusal is counted instead of vanishing.
//
// Safety here comes from the domain row, not from the job state. That is why
// these accept a `completed` delivery where the provider-unit repair accepts
// only `discarded`: CHAOS-3991 strands work by making a job report SUCCESS, so
// requiring a failed-looking job would miss every row this repair exists for.
// The authoritative domain row is what proves the work never finished, and no
// rearm happens without it.
//
// Only TERMINAL River deliveries are rearmed. A job River may still rescue is
// non-terminal by definition, so the stale-but-not-yet-rescuable window is
// excluded structurally rather than by recomputing River's rescue horizon --
// which would couple this SQL to RescueStuckJobsAfter and to each kind's
// timeout, and would silently re-break when either moved.
//
// # The live-idempotency-lease refusal, and why it reads no worker_job_runs
//
// A live idempotency lease must never be rearmed past: that job would be ACKed
// as a duplicate success without reaching its handler, manufacturing a fresh
// strand instead of clearing one (CHAOS-3998). This is enforced WITHOUT reading
// worker_job_runs, because the queue-control role must never be granted access
// to execution state or external-effect evidence
// (internal/jobruntime/idempotency_postgres.go:32).
//
// Two independent arguments carry that refusal, and BOTH are needed, because
// the first one says nothing at all about a row that was never claimed.
//
//  1. For a row with a lease: the domain lease is a sound proxy. The
//     idempotency lease and each domain lease are all ten minutes
//     (internal/jobruntime/idempotency_postgres.go:28,
//     internal/jobs/metrics/daily/postgres.go:19,
//     internal/jobs/workgraph/postgres.go:17); all renew every lease/3
//     (idempotency_postgres.go:187, internal/jobs/metrics/daily/daily.go:348,
//     internal/jobs/workgraph/handler.go:85); all are renewed by the same
//     worker process; and the idempotency claim is taken at
//     internal/jobruntime/adapter.go:386, strictly BEFORE the handler that
//     acquires the domain lease at adapter.go:432. So the idempotency lease
//     expires no later than the domain lease, and an expired domain lease
//     implies an expired idempotency lease. The converse is covered too: a
//     replacement claimant holding a live idempotency lease has necessarily
//     taken the domain lease, which the `lease_expires_at <= $1` predicates
//     below already exclude.
//
//     None of those constants is importable from here -- they are unexported,
//     in three separate packages -- so this argument is anchored by file:line
//     and pinned by TestStrandRepairLeaseProxyPremise, which parses all three
//     files and fails if the durations, the renewal divisors, or the claim
//     ordering diverge. If that test is ever deleted, this repair loses its
//     only guard against re-arming into a guaranteed duplicate-success no-op.
//
//  2. For a row that was NEVER claimed there is no lease to test, and argument
//     1 is vacuous. Both the finalize shape (a run whose finalize lease is
//     NULL) and the work-graph shape (a request in state 'pending', which the
//     table's CHECK constraint forces to hold a NULL claim_token and lease)
//     admit such rows. Because the claim at adapter.go:386 precedes the
//     handler, a live claim beside an unclaimed domain row is a real state.
//     The guard for those rows is `job.finalized_at <= $3`, i.e. the delivery
//     has been terminal for at least one idempotency lease. A River job is
//     finalized only after the adapter returns, and the adapter finishes its
//     claim on that path (adapter.go:442); the surviving case is a process
//     that died mid-handler, whose claim then stops renewing and expires
//     within one lease. Either way, one lease after finalization no claim tied
//     to that job can still be live. This derives from the idempotency lease
//     itself, not from River's rescue horizon, so it does not reintroduce the
//     coupling that the terminal-only rule exists to avoid.
//
// # Why the domain row is read but not locked
//
// The domain tables are joined without FOR UPDATE. That is not an oversight
// and cannot be changed: PostgreSQL requires UPDATE privilege to lock a row,
// and the queue-control role holds SELECT only -- locking it would demand
// exactly the write authority this split exists to withhold.
//
// It is also unnecessary. The outbox row's dedupe key is unique per (kind,
// domain id), so the delivery bound to it is the only River job that can exist
// for that pair; there is no second live job to race. Concurrent repair
// replicas are serialized by `FOR UPDATE OF outbox, job SKIP LOCKED`, which
// takes the outbox row first, matching lockCurrentDelivery's order. What
// remains is a claimant appearing between the snapshot and the commit, which
// requires a runnable job that the terminal-only predicate has already
// excluded.
//
// # Why rearming is safe for every kind covered
//
// A rearmed row produces a job that is indistinguishable from a River retry of
// the same job: same kind, same args, same dedupe key, same domain row. Every
// kind here already runs with max_attempts > 1, so each handler is required to
// be re-enterable, and each domain layer fences re-entry with its own claim
// CAS. This repair adds no re-execution shape the system does not already
// support.
const repairStrandedPartitionSQL = `
	SELECT outbox.id::text, job.id,
		CASE
			WHEN job.state::text NOT IN ('completed', 'discarded', 'cancelled') THEN 'skip_job_live'
			WHEN job.finalized_at IS NULL OR job.finalized_at > $3 THEN 'skip_idempotency_grace'
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
			WHEN job.finalized_at IS NULL OR job.finalized_at > $3 THEN 'skip_idempotency_grace'
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

// The work-graph shape carries one table for five kinds, so the request is
// bound by `kind` as well as by id -- the same pair PostgresStore.Claim keys
// on. The accepted states are exactly the two Claim will reclaim: 'pending',
// and 'running' with an expired lease
// (internal/jobs/workgraph/postgres.go:69-73). Every other state is excluded
// structurally: 'succeeded', 'failed' and 'canceled' are terminal and
// immutable by trigger, and 'ambiguous' is refused by Claim, so rearming any
// of them could only mint a job that no-ops. 'ambiguous' is also the state
// CHAOS-3999's abandonment contract owns, and this sweep must not pre-empt it.
const repairStrandedWorkGraphSQL = `
	SELECT outbox.id::text, job.id,
		CASE
			WHEN job.state::text NOT IN ('completed', 'discarded', 'cancelled') THEN 'skip_job_live'
			WHEN job.finalized_at IS NULL OR job.finalized_at > $3 THEN 'skip_idempotency_grace'
			ELSE 'rearm'
		END AS disposition
	FROM public.worker_job_outbox AS outbox
	JOIN public.work_graph_execution_requests AS request
		ON request.id::text = outbox.args #>> '{domain,id}'
		AND request.kind = outbox.job_kind
	JOIN %s AS job
		ON job.id = outbox.river_job_id
	WHERE outbox.job_kind IN (
			'workgraph.build', 'investment.materialize', 'investment.dispatch',
			'investment.chunk', 'investment.finalize'
		)
		AND outbox.status = 'delivered'
		AND outbox.river_job_id IS NOT NULL
		AND (
			(
				request.state = 'running'
				AND request.lease_expires_at IS NOT NULL
				AND request.lease_expires_at <= $1
			)
			OR (
				request.state = 'pending'
				AND request.claim_token IS NULL
				AND request.lease_expires_at IS NULL
			)
		)
	ORDER BY outbox.delivered_at, outbox.id
	FOR UPDATE OF outbox, job SKIP LOCKED
	LIMIT $2::int
`
