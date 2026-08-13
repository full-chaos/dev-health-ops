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
)

var riverSchemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

type TerminalDeliveryRepairResult struct {
	Recovered int
}

// TerminalDeliveryRepair restores a durable dispatch intent only when River
// itself proves that its global JobRescuer discarded a still-retryable job as
// unhandled. It deliberately excludes exhausted, cancelled, completed, and
// ordinary worker failures, so domain retry budgets remain authoritative.
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
		query: fmt.Sprintf(repairUnhandledRiverDeliverySQL, jobTable),
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
	)
	if err != nil || rows == nil {
		return TerminalDeliveryRepairResult{}, ErrUnavailable
	}
	defer rows.Close()
	result := TerminalDeliveryRepairResult{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil || !uuidPattern.MatchString(id) {
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
// most one replacement delivery. The exact latest-error plus remaining-attempt
// predicate identifies River v0.40's unhandled-kind rescue branch; an ordinary
// terminal worker failure never satisfies both clauses.
const repairUnhandledRiverDeliverySQL = `
WITH candidates AS (
	SELECT outbox.id
	FROM public.sync_dispatch_outbox AS outbox
	JOIN public.sync_runs AS run
		ON run.id = outbox.sync_run_id
		AND run.status IN ('planned', 'dispatching', 'running')
	JOIN public.sync_dispatch_transport_routes AS route
		ON route.kind = outbox.kind
	JOIN %s AS job
		ON job.id::text = outbox.transport_job_id
		AND job.kind = outbox.kind
	WHERE outbox.status = 'dispatched'
		AND outbox.dispatched_transport = 'river'
		AND outbox.dispatched_route_generation = route.generation
		AND route.transport = 'river'
		AND route.paused = FALSE
		AND job.state::text = 'discarded'
		AND job.finalized_at IS NOT NULL
		AND job.attempt < job.max_attempts
		AND cardinality(job.errors) > 0
		AND (job.errors[cardinality(job.errors)]->>'error') = $3
	ORDER BY outbox.dispatched_at, outbox.id
	FOR UPDATE OF outbox, job SKIP LOCKED
	LIMIT $2::int
)
UPDATE public.sync_dispatch_outbox AS outbox
SET status = 'pending',
	available_at = $1,
	last_error = $4,
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
RETURNING outbox.id::text
`
