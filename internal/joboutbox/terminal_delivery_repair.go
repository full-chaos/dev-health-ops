package joboutbox

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	riverUnhandledRescueError      = "Stuck job rescued by JobRescuer"
	providerTerminalRecoveryCode   = "river_unhandled_rescue"
	providerTerminalRecoveryDetail = "terminal River delivery recovered"
)

var riverSchemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

type TerminalDeliveryRepairResult struct {
	Recovered int
}

// TerminalDeliveryRepair rearms a provider-unit outbox row only when its
// exact River delivery was discarded by River's unhandled-kind rescue path
// while the authoritative run and unit remain active and due. The outbox and
// River rows are locked together so replicas converge on one replacement.
type TerminalDeliveryRepair struct {
	begin func(context.Context) (pgx.Tx, error)
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
		query: fmt.Sprintf(repairProviderUnitTerminalDeliverySQL, jobTable),
	}, nil
}

func (repair *TerminalDeliveryRepair) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (TerminalDeliveryRepairResult, error) {
	if repair == nil || repair.begin == nil || ctx == nil || now.IsZero() ||
		limit < minReconcilerLimit || limit > maxReconcilerLimit {
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
		providerTerminalRecoveryCode,
		providerTerminalRecoveryDetail,
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

// The queue-control role has read-only access to sync_runs/sync_run_units and
// mutation authority over the generic outbox and River schema. The identity
// predicates bind the immutable outbox envelope, authoritative domain row,
// current outbox delivery, and River metadata together before any row is
// rearmed. A paused integration is intentionally absent: pausing prevents new
// planning, but does not cancel already-planned work in an active run.
const repairProviderUnitTerminalDeliverySQL = `
WITH candidates AS (
	SELECT outbox.id
	FROM public.worker_job_outbox AS outbox
	JOIN public.sync_run_units AS unit
		ON unit.id = CASE
			WHEN (outbox.args #>> '{payload,unit_id}') ~
				'^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
			THEN (outbox.args #>> '{payload,unit_id}')::uuid
			ELSE NULL
		END
		AND unit.id::text = outbox.args #>> '{domain,id}'
		AND unit.org_id = outbox.args ->> 'organization_id'
	JOIN public.sync_runs AS run
		ON run.id = unit.sync_run_id
		AND run.org_id = unit.org_id
	JOIN %s AS job
		ON job.id = outbox.river_job_id
		AND job.kind = outbox.job_kind
		AND job.args = outbox.args::jsonb
		AND job.metadata ->> 'worker_outbox_id' = outbox.id::text
		AND job.metadata ->> 'payload_hash' = outbox.payload_hash
		AND job.metadata ->> 'contract_version' = outbox.contract_version::text
	WHERE outbox.status = 'delivered'
		AND outbox.job_kind = 'sync.provider_unit'
		AND outbox.dedupe_key = 'sync.provider_unit:' || unit.id::text
		AND outbox.args #>> '{domain,type}' = 'sync_run_unit'
		AND unit.status = 'dispatching'
		AND (unit.available_at IS NULL OR unit.available_at <= $1)
		AND unit.lease_owner IS NULL
		AND unit.lease_expires_at IS NULL
		AND run.status IN ('planned', 'dispatching', 'running')
		AND job.state::text = 'discarded'
		AND job.finalized_at IS NOT NULL
		AND job.attempt < job.max_attempts
		AND cardinality(job.errors) > 0
		AND (job.errors[cardinality(job.errors)]->>'error') = $3
	ORDER BY outbox.delivered_at, outbox.id
	FOR UPDATE OF outbox, job SKIP LOCKED
	LIMIT $2::int
)
UPDATE public.worker_job_outbox AS outbox
SET status = 'pending',
	next_attempt_at = $1,
	last_error_code = $4,
	last_error_detail = $5,
	last_error_at = $1,
	river_job_id = NULL,
	delivered_at = NULL,
	claim_token = NULL,
	claimed_at = NULL,
	claim_expires_at = NULL,
	updated_at = $1
FROM candidates
WHERE outbox.id = candidates.id
RETURNING outbox.id::text
`
