package syncreconciler

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	riverUnhandledRescueError    = "Stuck job rescued by JobRescuer"
	riverUnhandledRescueEvidence = "river_unhandled_rescue"
	// riverDeliveryExhaustedEvidence is stamped only when River retired a
	// coordinator delivery by spending its whole attempt budget. It is
	// deliberately distinct from the rescue evidence: the two branches recover
	// the same row into the same status, so a shared code would make a
	// reclaim-loop indistinguishable from an ordinary rescue in the durable
	// record an operator reads.
	riverDeliveryExhaustedEvidence = "river_delivery_exhausted"
)

var riverSchemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// TerminalDeliveryRepairResult counts recovered deliveries. ExhaustedRecovered
// is the subset that spent River's whole attempt budget and is reported
// separately because it, unlike a maintenance rescue, can repeat for the same
// row indefinitely when the underlying failure is deterministic.
type TerminalDeliveryRepairResult struct {
	Recovered          int
	ExhaustedRecovered int
}

// TerminalDeliveryRepair restores a durable dispatch intent when River itself
// proves it retired the delivery without the authoritative domain work having
// been re-armed. Two proofs qualify, and only two: the global JobRescuer
// discarded a still-retryable job as unhandled, or the job spent its entire
// attempt budget. Cancelled, completed, and still-live jobs stay excluded, as
// does a discard that happened with attempts still on the clock for any other
// reason -- that is a worker declaring the work permanently failed, and
// reclaiming it would relitigate a decision the domain already made.
//
// The exhausted case is included for the coordinator kinds specifically
// (CHAOS-3951). The original exclusion was written so that "domain retry
// budgets remain authoritative", which is correct wherever a second budget
// exists -- a provider unit carries its own retries, so River giving up merely
// ends one delivery of work the domain will re-plan. The four sync-dispatch
// coordinator kinds have no such budget: River's MaxAttempts IS the only one.
// Excluding them there did not defer to a domain budget, it deferred to
// nothing, and the SyncRun stayed non-terminal with no error recorded and no
// path back. Python bounded these kinds at max_retries=0 precisely because
// redelivery was owned elsewhere and unbounded; this restores that guarantee at
// the transport instead of the task.
//
// Reclaiming is safe here without any additional predicate because the existing
// ones already prove the delivery is the live one: an outbox row still
// 'dispatched' whose transport_job_id is this exact job, at the current route
// generation, cannot have been re-armed by a task body that ran, and cannot be
// a superseded delivery -- either of those breaks the linkage. So this can
// never double-deliver work the domain has already moved past.
type TerminalDeliveryRepair struct {
	begin beginFunc
	query string
}

func NewTerminalDeliveryRepair(
	queueControlPool *pgxpool.Pool,
	riverSchema string,
) (*TerminalDeliveryRepair, error) {
	if queueControlPool == nil || !riverSchemaPattern.MatchString(riverSchema) {
		return nil, ErrInvalidConfiguration
	}
	jobTable := pgx.Identifier{riverSchema, "river_job"}.Sanitize()
	return &TerminalDeliveryRepair{
		begin: queueControlPool.Begin,
		query: fmt.Sprintf(repairTerminalRiverDeliverySQL, jobTable),
	}, nil
}

func (repair *TerminalDeliveryRepair) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (TerminalDeliveryRepairResult, error) {
	if repair == nil || repair.begin == nil || ctx == nil || now.IsZero() ||
		limit < minimumStepLimit || limit > maximumStepLimit {
		return TerminalDeliveryRepairResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return TerminalDeliveryRepairResult{}, err
	}
	tx, err := repair.begin(ctx)
	if err != nil || tx == nil {
		return TerminalDeliveryRepairResult{}, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(
		ctx,
		repair.query,
		now.UTC(),
		limit,
		riverUnhandledRescueError,
		riverUnhandledRescueEvidence,
		riverDeliveryExhaustedEvidence,
	)
	if err != nil || rows == nil {
		return TerminalDeliveryRepairResult{}, ErrUnavailable
	}
	defer rows.Close()
	result := TerminalDeliveryRepairResult{}
	for rows.Next() {
		var id, evidence string
		if err := rows.Scan(&id, &evidence); err != nil || !uuidPattern.MatchString(id) {
			return TerminalDeliveryRepairResult{}, ErrUnavailable
		}
		switch evidence {
		case riverUnhandledRescueEvidence:
		case riverDeliveryExhaustedEvidence:
			result.ExhaustedRecovered++
		default:
			// The evidence code is chosen by this statement, so an unknown one
			// means the row was recovered under a branch this code does not
			// know it has. Fail the whole step rather than report a count that
			// describes something else.
			return TerminalDeliveryRepairResult{}, ErrUnavailable
		}
		result.Recovered++
	}
	if err := rows.Err(); err != nil || result.Recovered > limit {
		return TerminalDeliveryRepairResult{}, ErrUnavailable
	}
	if err := tx.Commit(ctx); err != nil {
		return TerminalDeliveryRepairResult{}, ErrUnavailable
	}
	return result, nil
}

// The queue role owns the River schema and UPDATE on sync_dispatch_outbox.
// The route generation fence prevents recovery after an operator has changed
// transport ownership. FOR UPDATE SKIP LOCKED makes replicas converge on at
// most one replacement delivery.
//
// Two disjoint recovery branches share every other predicate, and each stamps
// its own durable evidence code:
//
//   - remaining attempts AND the exact latest-error text identifies River
//     v0.40's unhandled-kind rescue branch; an ordinary terminal worker failure
//     never satisfies both clauses.
//   - a spent attempt budget identifies exhaustion, whatever the last error
//     said. The error text is deliberately NOT matched here: the whole point is
//     that a coordinator bridge failure carries arbitrary transport text.
//
// The branches are written as an explicit disjunction rather than by relaxing
// the attempt comparison, so that a discard with attempts remaining still
// requires the rescue sentinel. Republishing does not collide with the retired
// job's unique key: the publisher includes the outbox attempt counter in its
// args and the claim increments it, so each redelivery has distinct args.
//
// CHAOS-4092: the job join is on job.id, River's bigint primary key, never on
// job.id::text. Casting the PK to text (the original shape) is not sargable
// against river_job_pkey, so Postgres fell back to river_job_kind and filtered
// every row of that kind per candidate -- O(candidates x jobs-of-kind), which
// is what turned a 106k-row river_job into a 9.5h crash loop. The comparison
// instead casts outbox.transport_job_id the other way, to bigint, restoring an
// index-friendly job.id = <value> predicate that a nested-loop Index Scan on
// river_job_pkey can use per candidate: O(candidates) regardless of table
// size.
//
// The cast is guarded by a CASE, not a separate `transport_job_id ~ regex`
// AND-ed alongside it: Postgres's `~` and `::bigint` are both immutable, so
// the planner is free to reorder or fold two independent boolean quals and
// could evaluate the cast before the regex guards it, and a non-numeric
// transport_job_id would fault the whole step with a driver error (a worse
// failure than the join it replaces -- a poison row instead of a slow one).
// A CASE's THEN branch is evaluated only when its WHEN is true by the SQL
// standard's short-circuit semantics, which SQL does guarantee regardless of
// planning, so the cast never runs against a non-numeric value. A NULL or
// non-numeric transport_job_id makes the CASE evaluate to NULL, and
// `job.id = NULL` is never true, so those rows are excluded exactly as they
// were under the old join (a NULL transport_job_id was never a text match
// either). transport_job_id is always either NULL or
// strconv.FormatInt(riverJobID, 10) -- see syncdispatchruntime.Publisher.Publish
// and markRiverDispatchedSQL's NULLIF of an empty string into $5 -- so the
// guard is defensive against any row this schema does not currently produce,
// not a documented alternate shape. The digit
// count is capped at 18 so the guarded value can never overflow int64 (max
// 9223372036854775807, 19 digits) and fault the cast that way either.
const repairTerminalRiverDeliverySQL = `
WITH candidates AS (
	SELECT outbox.id,
		CASE
			WHEN job.attempt >= job.max_attempts THEN $5::text
			ELSE $4::text
		END AS recovery_code
	FROM public.sync_dispatch_outbox AS outbox
	JOIN public.sync_runs AS run
		ON run.id = outbox.sync_run_id
		AND run.status IN ('planned', 'dispatching', 'running')
	JOIN public.sync_dispatch_transport_routes AS route
		ON route.kind = outbox.kind
	JOIN %s AS job
		ON job.id = CASE
				WHEN outbox.transport_job_id ~ '^[0-9]{1,18}$'
				THEN outbox.transport_job_id::bigint
			END
		AND job.kind = outbox.kind
	WHERE outbox.status = 'dispatched'
		AND outbox.dispatched_transport = 'river'
		AND outbox.dispatched_route_generation = route.generation
		AND route.transport = 'river'
		AND route.paused = FALSE
		AND job.state::text = 'discarded'
		AND job.finalized_at IS NOT NULL
		AND cardinality(job.errors) > 0
		AND (
			job.attempt >= job.max_attempts
			OR (job.errors[cardinality(job.errors)]->>'error') = $3
		)
	ORDER BY outbox.dispatched_at, outbox.id
	FOR UPDATE OF outbox, job SKIP LOCKED
	LIMIT $2::int
)
UPDATE public.sync_dispatch_outbox AS outbox
SET status = 'pending',
	available_at = $1,
	last_error = candidates.recovery_code,
	dispatched_at = NULL,
	dispatched_transport = NULL,
	dispatched_route_generation = NULL,
	transport_job_id = NULL,
	claim_token = NULL,
	claim_expires_at = NULL,
	claim_transport = NULL,
	claim_route_generation = NULL,
	updated_at = $1
FROM candidates
WHERE outbox.id = candidates.id
RETURNING outbox.id::text, candidates.recovery_code
`
