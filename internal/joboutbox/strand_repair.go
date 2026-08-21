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

	// insufficientPrivilege is PostgreSQL's SQLSTATE for a denied statement.
	// This repair is the first queue-role path to read the daily-metrics and
	// work-graph tables, so a missing grant is a live deployment risk rather
	// than a theoretical one: the grants ship in the ops runtime image
	// (scripts/worker/provision_river_roles.sql and, authoritatively,
	// internal/storage/river/migrate.go) while the posture assertion ships in
	// the Go binaries. Returning ErrNotAuthorized rather than ErrUnavailable
	// keeps that failure legible; the sibling sync package's single opaque
	// error made a 42501 read as "database unavailable" for a component that
	// was not even involved.
	insufficientPrivilege = "42501"
)

// ErrNotAuthorized reports a statement a runtime role is not granted. It is
// separate from ErrUnavailable because the two demand different operator
// actions: one is a database outage, the other is a missing grant that no
// amount of retrying will fix.
var ErrNotAuthorized = errors.New("worker outbox role is not authorized")

// StrandRepairResult counts one pass. Every refusal is counted rather than
// merely omitted: each one sits between a stranded run and a manufactured
// duplicate success, and a guard whose refusals are invisible cannot be told
// apart from one that never fires.
//
//   - SkippedJobLive climbing while nothing is rearmed is the signature of a
//     River rescuer that has stopped running, which would otherwise look
//     identical to "no strands exist".
//   - SkippedClaimLive is the execution-state refusal. This design could not
//     implement it at all until the domain-pool read replaced the lease proxy.
//   - SkippedClaimSettled is a case a lease-based proxy could never have seen:
//     an idempotency row already marked succeeded or terminal, where a rearmed
//     job is ACKed as a duplicate without running. Those rows need a different
//     remedy, and counting them is how anyone finds out they exist.
type StrandRepairResult struct {
	Rearmed             int
	SkippedJobLive      int
	SkippedClaimLive    int
	SkippedClaimSettled int
	// SkippedRaceLost counts candidates that survived the survey and the claim
	// check but no longer matched under the phase-3 lock -- another replica
	// rearmed the row, or its delivery stopped being terminal, in between.
	// Without this the loser of a two-replica race returns a successful zero
	// and the contention is invisible, breaking the same "counted, not
	// filtered" rule the other refusals follow.
	SkippedRaceLost int
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
//
// # Two pools, on purpose
//
// Selection and rearm run on the QUEUE-CONTROL pool, which owns the outbox and
// the River schema. The execution-state check runs on the DOMAIN pool, before
// the queue transaction opens. That split is not incidental: the queue-control
// role must never be granted access to execution state or external-effect
// evidence (internal/jobruntime/idempotency_postgres.go), and the domain role
// already holds SELECT on worker_job_runs
// (internal/storage/river/migrate.go), so the read needs no new privilege
// anywhere.
//
// An earlier design inferred the execution state from the domain lease instead,
// to avoid reading worker_job_runs at all. That inference was unsound on the
// exact path this repair exists for -- see the note above the queries.
type StrandRepair struct {
	beginQueue  func(context.Context) (pgx.Tx, error)
	queryQueue  func(context.Context, string, ...any) (pgx.Rows, error)
	queryDomain func(context.Context, string, ...any) (pgx.Rows, error)
	client      interface {
		JobDeleteTx(context.Context, pgx.Tx, int64) (*rivertype.JobRow, error)
	}
	shapes []strandShape
}

// strandShape is one domain's predicate in both the forms a pass needs: an
// unlocked survey that gathers candidates for the execution-state check, and a
// locked re-read that re-proves the same predicate before anything is mutated.
// Both are generated from ONE template so the two can never drift into
// disagreeing about what is eligible.
type strandShape struct {
	name   string
	survey string
	lock   string
}

func newStrandShape(name, template, jobTable string) strandShape {
	return strandShape{
		name:   name,
		survey: fmt.Sprintf(template, jobTable, "", ""),
		// Phase 3 matches the surveyed (outbox id, river_job_id) PAIR, not the
		// id alone. Matching on the id would be an ABA hole: between the survey
		// and the lock another replica can rearm the row and the relay can mint
		// a REPLACEMENT delivery in the same pass, so an id-only match would
		// re-read whatever job is current and delete a delivery that was never
		// surveyed. The pair is the delivery generation, and requiring it makes
		// phase 3 a true CAS.
		lock: fmt.Sprintf(template, jobTable,
			`AND EXISTS (
				SELECT 1 FROM unnest($3::uuid[], $4::bigint[]) AS approved(outbox_id, river_job_id)
				WHERE approved.outbox_id = outbox.id
					AND approved.river_job_id = outbox.river_job_id
			)`, "FOR UPDATE OF outbox, job SKIP LOCKED"),
	}
}

func NewStrandRepair(
	queueControlPool *pgxpool.Pool,
	domainPool *pgxpool.Pool,
	riverSchema string,
) (*StrandRepair, error) {
	if queueControlPool == nil || domainPool == nil || !riverSchemaPattern.MatchString(riverSchema) {
		return nil, ErrInvalidConfiguration
	}
	jobTable := pgx.Identifier{riverSchema, "river_job"}.Sanitize()
	client, err := river.NewClient(riverpgxv5.New(queueControlPool), &river.Config{Schema: riverSchema})
	if err != nil {
		return nil, ErrInvalidConfiguration
	}
	return &StrandRepair{
		beginQueue:  queueControlPool.Begin,
		queryQueue:  queueControlPool.Query,
		queryDomain: domainPool.Query,
		client:      riverDeleteAdapter{client: client},
		shapes: []strandShape{
			newStrandShape("partition", repairStrandedPartitionSQL, jobTable),
			newStrandShape("finalize", repairStrandedFinalizeSQL, jobTable),
			newStrandShape("workgraph", repairStrandedWorkGraphSQL, jobTable),
		},
	}, nil
}

// strandCandidate is one outbox row under consideration, carrying the identity
// the execution-state check needs. jobKind and dedupeKey are exactly the pair
// worker_job_runs is unique on (job_kind, idempotency_key), because the outbox
// dedupe_key IS the envelope's idempotency key.
type strandCandidate struct {
	outboxID    string
	riverJobID  int64
	jobKind     string
	dedupeKey   string
	disposition string
}

// Step runs one bounded pass over every shape.
func (repair *StrandRepair) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (StrandRepairResult, error) {
	if repair == nil || repair.beginQueue == nil || repair.queryQueue == nil ||
		repair.queryDomain == nil || repair.client == nil || ctx == nil || now.IsZero() ||
		limit < minReconcilerLimit || limit > maxReconcilerLimit {
		return StrandRepairResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return StrandRepairResult{}, err
	}
	result := StrandRepairResult{}
	for _, shape := range repair.shapes {
		shapeResult, err := repair.stepShape(ctx, shape, now, limit)
		if err != nil {
			return StrandRepairResult{}, err
		}
		result.Rearmed += shapeResult.Rearmed
		result.SkippedJobLive += shapeResult.SkippedJobLive
		result.SkippedClaimLive += shapeResult.SkippedClaimLive
		result.SkippedClaimSettled += shapeResult.SkippedClaimSettled
		result.SkippedRaceLost += shapeResult.SkippedRaceLost
	}
	return result, nil
}

func (repair *StrandRepair) stepShape(
	ctx context.Context,
	shape strandShape,
	now time.Time,
	limit int,
) (StrandRepairResult, error) {
	result := StrandRepairResult{}

	// Phase 1 -- survey on the queue pool, WITHOUT locking. Locking here would
	// hold outbox rows across the domain round-trip for no benefit: nothing is
	// mutated until phase 3 re-proves the predicate under a lock anyway.
	candidates, err := repair.survey(ctx, shape.survey, now, limit)
	if err != nil {
		return StrandRepairResult{}, err
	}
	eligible := make([]strandCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		switch candidate.disposition {
		case dispositionSkipJobLive:
			result.SkippedJobLive++
		case dispositionRearm:
			eligible = append(eligible, candidate)
		default:
			return StrandRepairResult{}, ErrUnavailable
		}
	}
	if len(eligible) == 0 {
		return result, nil
	}

	// Phase 2 -- execution state, on the DOMAIN pool, before any queue
	// transaction is open.
	approved, live, settled, err := repair.filterByClaim(ctx, eligible, now)
	if err != nil {
		return StrandRepairResult{}, err
	}
	result.SkippedClaimLive += live
	result.SkippedClaimSettled += settled
	if len(approved) == 0 {
		return result, nil
	}

	// Phase 3 -- lock, re-prove, and rearm on the queue pool.
	rearmed, lost, err := repair.rearm(ctx, shape.lock, now, limit, approved)
	if err != nil {
		return StrandRepairResult{}, err
	}
	result.Rearmed += rearmed
	result.SkippedRaceLost += lost
	return result, nil
}

func (repair *StrandRepair) survey(
	ctx context.Context,
	query string,
	now time.Time,
	limit int,
) ([]strandCandidate, error) {
	rows, err := repair.queryQueue(ctx, query, now.UTC(), limit)
	if err != nil || rows == nil {
		return nil, classifyStrandError(err)
	}
	defer rows.Close()
	candidates := make([]strandCandidate, 0, limit)
	for rows.Next() {
		var found strandCandidate
		if err := rows.Scan(&found.outboxID, &found.riverJobID, &found.jobKind,
			&found.dedupeKey, &found.disposition); err != nil ||
			!uuidPattern.MatchString(found.outboxID) || found.riverJobID <= 0 ||
			found.jobKind == "" || found.dedupeKey == "" {
			return nil, ErrUnavailable
		}
		candidates = append(candidates, found)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(candidates) > limit {
		return nil, classifyStrandError(rows.Err())
	}
	return candidates, nil
}

// filterByClaim is the execution-state check the lease proxy used to stand in
// for. It reads worker_job_runs on the DOMAIN pool and refuses any candidate
// whose logical job either still holds a live claim or has already settled.
func (repair *StrandRepair) filterByClaim(
	ctx context.Context,
	candidates []strandCandidate,
	now time.Time,
) ([]strandCandidate, int, int, error) {
	kinds := make([]string, 0, len(candidates))
	keys := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		kinds = append(kinds, candidate.jobKind)
		keys = append(keys, candidate.dedupeKey)
	}
	rows, err := repair.queryDomain(ctx, claimStateSQL, now.UTC(), kinds, keys)
	if err != nil || rows == nil {
		return nil, 0, 0, classifyStrandError(err)
	}
	defer rows.Close()
	type claimKey struct{ kind, key string }
	claims := make(map[claimKey]string, len(candidates))
	for rows.Next() {
		var kind, key, disposition string
		if err := rows.Scan(&kind, &key, &disposition); err != nil {
			return nil, 0, 0, ErrUnavailable
		}
		claims[claimKey{kind, key}] = disposition
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, 0, classifyStrandError(rows.Err())
	}
	approved := make([]strandCandidate, 0, len(candidates))
	liveCount, settledCount := 0, 0
	for _, candidate := range candidates {
		switch claims[claimKey{candidate.jobKind, candidate.dedupeKey}] {
		case claimLive:
			liveCount++
		case claimSettled:
			settledCount++
		case "", claimReclaimable:
			// No row at all, or a row whose claim is reclaimable. Both mean a
			// fresh delivery reaches the handler rather than being ACKed as a
			// duplicate.
			approved = append(approved, candidate)
		default:
			return nil, 0, 0, ErrUnavailable
		}
	}
	return approved, liveCount, settledCount, nil
}

func (repair *StrandRepair) rearm(
	ctx context.Context,
	query string,
	now time.Time,
	limit int,
	approved []strandCandidate,
) (int, int, error) {
	ids := make([]string, 0, len(approved))
	jobIDs := make([]int64, 0, len(approved))
	for _, candidate := range approved {
		ids = append(ids, candidate.outboxID)
		jobIDs = append(jobIDs, candidate.riverJobID)
	}
	tx, err := repair.beginQueue(ctx)
	if err != nil || tx == nil {
		return 0, 0, classifyStrandError(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, query, now.UTC(), limit, ids, jobIDs)
	if err != nil || rows == nil {
		return 0, 0, classifyStrandError(err)
	}
	defer rows.Close()
	locked := make([]strandCandidate, 0, len(approved))
	for rows.Next() {
		var found strandCandidate
		if err := rows.Scan(&found.outboxID, &found.riverJobID, &found.jobKind,
			&found.dedupeKey, &found.disposition); err != nil ||
			!uuidPattern.MatchString(found.outboxID) || found.riverJobID <= 0 {
			return 0, 0, ErrUnavailable
		}
		// The locked re-read must still agree the delivery is terminal. A row
		// that turned live between the survey and the lock is dropped -- and
		// counted below rather than vanishing.
		if found.disposition == dispositionRearm {
			locked = append(locked, found)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, classifyStrandError(rows.Err())
	}
	// Anything approved that did not come back rearmable lost a race: another
	// replica took the row, or the delivery stopped being terminal.
	lost := len(approved) - len(locked)
	rearmed := 0
	for _, found := range locked {
		deleted, err := repair.client.JobDeleteTx(ctx, tx, found.riverJobID)
		if err != nil || deleted == nil || deleted.ID != found.riverJobID {
			return 0, 0, classifyStrandError(err)
		}
		// The delete must have removed a terminal row. Re-checking the returned
		// state closes the window between the predicate and the delete: a job
		// that became runnable in between must not be removed.
		if !terminalRiverState(deleted.State) {
			return 0, 0, ErrUnavailable
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
			return 0, 0, classifyStrandError(err)
		}
		rearmed++
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, classifyStrandError(err)
	}
	return rearmed, lost, nil
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

const (
	claimLive        = "claim_live"
	claimSettled     = "claim_settled"
	claimReclaimable = "claim_reclaimable"
)

// claimStateSQL answers, for each candidate, whether a fresh delivery would
// actually reach its handler. It runs on the DOMAIN pool.
//
// worker_job_runs is unique on (job_kind, idempotency_key), and the outbox
// dedupe_key IS the envelope's idempotency key, so the pair identifies the
// logical job exactly. This is a direct read, not an inference.
//
// The three outcomes mirror PostgresIdempotency.Begin:
//
//   - claim_live: status 'running' with an unexpired lease. Begin returns
//     ClaimAlreadyComplete, so the rearmed job would be ACKed WITHOUT running
//     and the domain row would stay unfinished -- a strand manufactured by the
//     repair (CHAOS-3998). Refuse.
//   - claim_settled: status 'succeeded' or 'terminal'. Begin returns
//     ClaimAlreadyComplete or ClaimTerminal for these too, so a rearm is
//     equally futile. A lease-based proxy could never see this case at all,
//     because a settled row has no lease to reason about.
//   - claim_reclaimable: anything else -- 'retryable', or 'running' with an
//     expired lease. Begin takes the row over and the handler runs. Safe.
//
// A candidate with NO row is also safe: the first delivery creates it.
const claimStateSQL = `
	SELECT runs.job_kind, runs.idempotency_key,
		CASE
			WHEN runs.status = 'running'
				AND runs.lease_expires_at IS NOT NULL
				AND runs.lease_expires_at > $1
				THEN 'claim_live'
			WHEN runs.status IN ('succeeded', 'terminal') THEN 'claim_settled'
			ELSE 'claim_reclaimable'
		END AS disposition
	FROM public.worker_job_runs AS runs
	JOIN unnest($2::text[], $3::text[]) AS pair(job_kind, idempotency_key)
		ON pair.job_kind = runs.job_kind
		AND pair.idempotency_key = runs.idempotency_key
`

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
// # Why the idempotency claim is READ rather than inferred
//
// A live idempotency claim must never be rearmed past: that job would be ACKed
// as a duplicate success without reaching its handler, manufacturing a fresh
// strand instead of clearing one (CHAOS-3998).
//
// This was originally enforced WITHOUT reading worker_job_runs, by treating the
// domain lease as a proxy: same duration, same renewal divisor, same process,
// and the claim taken before the domain lease, so an expired domain lease was
// argued to imply an expired claim. Adversarial review broke that argument on
// the very path this repair exists for:
//
//   - the claim renewer runs on context.Background()
//     (internal/jobruntime/idempotency_postgres.go), so it keeps renewing for
//     as long as its row is 'running', while the DOMAIN renewer cancels the
//     work context and returns when a renewal fails
//     (internal/jobs/metrics/daily/daily.go). A domain lease can therefore
//     lapse while the claim is still being renewed.
//   - River's RESCUER stamps finalized_at on a job whose worker is still
//     alive. Any argument of the form "terminal for long enough implies the
//     claim is gone" assumed River finalizes only after the adapter returns,
//     which is exactly false on the rescuer path.
//
// The claim is now read directly on the domain pool (claimStateSQL above),
// which needs no new privilege and keeps execution state away from the
// queue-control role.
//
// # The residual window, and why it is strictly smaller
//
// The claim is read before the queue transaction opens, so a claim created
// between that read and the commit is not seen. That window is bounded three
// ways:
//
//  1. Only a running job can create a claim, and the only job that can exist
//     for this (kind, domain id) is the terminal delivery about to be deleted
//     -- there is nothing runnable in the window to create one.
//  2. Phase 3 re-reads the same predicate under FOR UPDATE ... SKIP LOCKED and
//     drops any row that stopped being terminal, so the mutation is a CAS on
//     the state the survey saw.
//  3. If a claim nevertheless became live, the rearmed job is ACKed as a
//     duplicate and the row simply remains stranded -- the same state as
//     before the pass, not a worse one.
//
// The proxy's hole, by contrast, was unbounded in time: any worker stalled with
// a live renewer qualified, for as long as it stalled.
//
// # Why the domain row is read but not locked
//
// The domain tables are joined without FOR UPDATE, and that cannot change:
// PostgreSQL requires UPDATE privilege to lock a row, and the queue-control
// role holds SELECT only.
//
// It is also unnecessary, though for a narrower reason than "the schema
// guarantees it". The database enforces uniqueness on dedupe_key alone, NOT on
// (kind, domain id). What makes the bound delivery the only job for that pair
// is how the keys are built, and that differs by domain:
//
//   - daily: the key is DERIVED from the domain id --
//     "metrics.daily_partition:"+partition.ID and
//     "metrics.daily_finalize:"+run.ID (internal/jobs/metrics/daily/publisher.go).
//   - work graph: the key is caller-supplied
//     (internal/jobs/workgraph/publisher.go passes request.IdempotencyKey
//     through), and holds only because
//     work_graph_execution_requests.idempotency_key is itself UNIQUE and one
//     per request row.
//
// So the property is real but rests on the publishers, not on a constraint,
// which is why the org and kind predicates below bind the row tightly rather
// than trusting the key alone. For the daily shapes the org lives ONLY on
// daily_metrics_runs -- daily_metrics_partitions carries no org_id column, a
// partition's org is reachable solely through its run_id foreign key -- so
// both shapes bind the envelope's organization_id against run.org_id, the
// partition shape via its run join and the finalize shape directly.
//
// # Why rearming is safe for every kind covered
//
// A rearmed row produces a job indistinguishable from a River retry of the same
// job: same kind, same args, same dedupe key, same domain row. Every kind here
// already runs with max_attempts > 1, so each handler is required to be
// re-enterable, and each domain layer fences re-entry with its own claim CAS.
const repairStrandedPartitionSQL = `
	SELECT outbox.id::text, job.id, outbox.job_kind, outbox.dedupe_key,
		CASE
			WHEN job.state::text NOT IN ('completed', 'discarded', 'cancelled') THEN 'skip_job_live'
			ELSE 'rearm'
		END AS disposition
	FROM public.worker_job_outbox AS outbox
	JOIN public.daily_metrics_partitions AS partition
		ON partition.id::text = outbox.args #>> '{domain,id}'
	JOIN public.daily_metrics_runs AS run
		ON run.id = partition.run_id
		AND run.org_id::text = outbox.args ->> 'organization_id'
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
		%s
	ORDER BY outbox.delivered_at, outbox.id
	%s
	LIMIT $2::int
`

// The finalize shape additionally preserves ClaimFinalize's own eligibility
// rule, mirrored exactly rather than approximated. classifyLease reaches its
// reclaimable branch only when the finalize lease is non-NULL; with a NULL
// lease a 'running' finalization falls through to claimable, which excludes
// 'running', and settles -- so rearming that row would mint a finalizer
// ClaimFinalize then refuses. The pending/failed branch stays deliberately
// stricter than ClaimFinalize, which would claim those even under a live
// lease: being MORE permissive than the claimer is the dangerous direction,
// while being less permissive costs one pass of latency.
const repairStrandedFinalizeSQL = `
	SELECT outbox.id::text, job.id, outbox.job_kind, outbox.dedupe_key,
		CASE
			WHEN job.state::text NOT IN ('completed', 'discarded', 'cancelled') THEN 'skip_job_live'
			ELSE 'rearm'
		END AS disposition
	FROM public.worker_job_outbox AS outbox
	JOIN public.daily_metrics_runs AS run
		ON run.id::text = outbox.args #>> '{domain,id}'
		AND run.org_id::text = outbox.args ->> 'organization_id'
	JOIN %s AS job
		ON job.id = outbox.river_job_id
	WHERE outbox.job_kind = 'metrics.daily_finalize'
		AND outbox.status = 'delivered'
		AND outbox.river_job_id IS NOT NULL
		AND outbox.args #>> '{domain,type}' = 'daily_metrics_run'
		AND run.status = 'running'
		AND (
			(
				run.finalization_status = 'running'
				AND run.finalization_lease_expires_at IS NOT NULL
				AND run.finalization_lease_expires_at <= $1
			)
			OR (
				run.finalization_status IN ('pending', 'failed')
				AND (
					run.finalization_lease_expires_at IS NULL
					OR run.finalization_lease_expires_at <= $1
				)
			)
		)
		AND NOT EXISTS (
			SELECT 1 FROM public.daily_metrics_partitions AS sibling
			WHERE sibling.run_id = run.id
				AND sibling.status <> 'succeeded'
		)
		%s
	ORDER BY outbox.delivered_at, outbox.id
	%s
	LIMIT $2::int
`

// The work-graph shape carries one table for five kinds, so the request is
// bound by kind as well as by id -- the same pair PostgresStore.Claim keys on.
// The accepted states are exactly the two Claim will reclaim: 'pending', and
// 'running' with an expired lease. Every other state is excluded by
// CONSTRUCTION rather than by an exclusion list: listing only the reclaimable
// states means a state added to the table later is refused by default instead
// of silently inheriting eligibility. That also keeps 'ambiguous' -- which
// Claim refuses, and which CHAOS-3999's abandonment contract owns -- out of
// this sweep without naming it.
const repairStrandedWorkGraphSQL = `
	SELECT outbox.id::text, job.id, outbox.job_kind, outbox.dedupe_key,
		CASE
			WHEN job.state::text NOT IN ('completed', 'discarded', 'cancelled') THEN 'skip_job_live'
			ELSE 'rearm'
		END AS disposition
	FROM public.worker_job_outbox AS outbox
	JOIN public.work_graph_execution_requests AS request
		ON request.id::text = outbox.args #>> '{domain,id}'
		AND request.kind = outbox.job_kind
		AND request.org_id::text = outbox.args ->> 'organization_id'
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
		%s
	ORDER BY outbox.delivered_at, outbox.id
	%s
	LIMIT $2::int
`
