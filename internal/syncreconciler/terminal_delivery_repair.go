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
	// riverRescueOnlyCancelError is the ONE cancellation this repair accepts,
	// and the constant is written out rather than imported so this package
	// keeps no dependency on internal/jobrescue. The string is pinned against
	// its source in TestRescueOnlyCancelSentinelMatchesJobRescue, so it cannot
	// drift silently -- the drift would be invisible in production, because a
	// sentinel that no longer matches simply stops recovering anything.
	//
	// The "JobCancelError: " prefix is River's, not ours: rivertype's
	// JobCancelError.Error() prepends it, and the executor persists
	// res.ErrorStr() -- that wrapped string -- into river_job.errors. Matching
	// the inner message alone would never fire.
	riverRescueOnlyCancelError = "JobCancelError: rescue-only River worker reached execution"
	// riverRescueOnlyCancelEvidence is its own durable code for the reason the
	// other two are: the three branches recover the same row into the same
	// status, and a shared code would make a wrong-queue insert
	// indistinguishable from a rescue or an exhausted budget in the record an
	// operator reads.
	riverRescueOnlyCancelEvidence = "river_rescue_only_cancel"
)

var riverSchemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// TerminalDeliveryRepairResult counts recovered deliveries. ExhaustedRecovered
// is the subset that spent River's whole attempt budget and is reported
// separately because it, unlike a maintenance rescue, can repeat for the same
// row indefinitely when the underlying failure is deterministic.
type TerminalDeliveryRepairResult struct {
	Recovered          int
	ExhaustedRecovered int
	// RescueOnlyCancelsRecovered is the subset whose River job was CANCELLED
	// by a rescue-only worker (CHAOS-4097). It is counted separately because
	// it means something an operator must act on that the other two do not:
	// a coordinator job was inserted onto a queue whose client does not
	// execute that kind. That is a registry or routing fault, and it repeats
	// deterministically until someone fixes it.
	RescueOnlyCancelsRecovered int
}

// TerminalDeliveryRepair restores a durable dispatch intent when River itself
// proves it retired the delivery without the authoritative domain work having
// been re-armed. Three proofs qualify, and only three: the global JobRescuer
// discarded a still-retryable job as unhandled, the job spent its entire
// attempt budget, or a rescue-only worker cancelled it. Completed and
// still-live jobs stay excluded, as does a discard that happened with attempts
// still on the clock for any other reason -- that is a worker declaring the
// work permanently failed, and reclaiming it would relitigate a decision the
// domain already made.
//
// # Why 'cancelled' is admitted, and why only this one class of it
//
// CHAOS-4097 asked for cancelled terminal jobs. Simply adding the state would
// have been wrong: 'cancelled' is not one outcome, it is several, and they do
// not share recovery semantics. The four coordinator kinds enumerate cleanly,
// because syncdispatchruntime's workers are plain river.WorkerDefaults that
// never return river.JobCancel themselves -- they return whatever the bridge
// returned. So exactly two things can cancel one of these jobs:
//
//  1. internal/jobrescue's rescueOnlyWorker. A kind was inserted onto a queue
//     whose client registers it for maintenance type information only, so the
//     worker cancels rather than executing a domain effect in the wrong worker
//     family. NOTHING ran. This is the same failure the unhandled-rescue
//     branch above recovers -- a registry/queue mismatch -- differing only in
//     whether River's rescuer reached the job first or a producer did. It gets
//     the same remedy for the same reason.
//  2. internal/joboperator's Cancel verb: a person deliberately stopping this
//     job. Republishing it would silently revert an operator decision, which
//     is the one thing a repair must never do.
//
// The two are separated STRUCTURALLY, not by string alone. River's own
// JobCancel query stamps metadata.cancel_attempted_at and appends nothing to
// errors; a worker-returned cancel appends an errors entry and never sets that
// key. Requiring the sentinel AND the absence of cancel_attempted_at means an
// operator cancel cannot be mistaken for a rescue-only one even if a future
// caller starts passing a matching message.
//
// A cancelled job is also, always, at attempt 1 with its budget unspent, so it
// can never satisfy the exhausted branch: this had to be its own disjunct.
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
		riverRescueOnlyCancelError,
		riverRescueOnlyCancelEvidence,
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
		case riverRescueOnlyCancelEvidence:
			result.RescueOnlyCancelsRecovered++
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
// Three disjoint recovery branches share every other predicate, and each stamps
// its own durable evidence code:
//
//   - remaining attempts AND the exact latest-error text identifies River
//     v0.40's unhandled-kind rescue branch; an ordinary terminal worker failure
//     never satisfies both clauses.
//   - a spent attempt budget identifies exhaustion, whatever the last error
//     said. The error text is deliberately NOT matched here: the whole point is
//     that a coordinator bridge failure carries arbitrary transport text.
//   - a CANCELLED job whose last error is the rescue-only sentinel AND whose
//     metadata carries no cancel_attempted_at (CHAOS-4097). Both halves are
//     required: the sentinel says a worker cancelled it, and the absent key
//     says River's own cancel query did not, which is what keeps an operator's
//     deliberate cancellation out of this branch. See the type doc above for
//     the full enumeration of what can cancel a coordinator job.
//
// The state comparison moved OUT of the shared predicates and into the
// branches. Leaving `state = 'discarded'` above them and adding 'cancelled'
// alongside it would have let a cancelled job satisfy the exhausted branch by
// its attempt count, which is exactly the operator-cancel case this must
// refuse -- and it would have done so silently.
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
			WHEN job.state::text = 'cancelled' THEN $7::text
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
		AND job.finalized_at IS NOT NULL
		AND cardinality(job.errors) > 0
		AND (
			(
				job.state::text = 'discarded'
				AND (
					job.attempt >= job.max_attempts
					OR (job.errors[cardinality(job.errors)]->>'error') = $3
				)
			)
			OR (
				job.state::text = 'cancelled'
				AND job.metadata ->> 'cancel_attempted_at' IS NULL
				AND (job.errors[cardinality(job.errors)]->>'error') = $6
			)
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
