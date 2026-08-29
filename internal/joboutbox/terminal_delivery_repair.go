package joboutbox

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

const (
	riverUnhandledRescueError                = "Stuck job rescued by JobRescuer"
	providerTerminalRecoveryCode             = "river_unhandled_rescue"
	providerTerminalRecoveryDetail           = "terminal River delivery recovered"
	providerPostRepairContractRecoveryCode   = "post_repair_contract_rejection_recovered"
	providerPostRepairContractRecoveryDetail = "post-repair contract rejection recovered"
	providerContractRejectedCode             = "contract_rejected"
	providerContractRejectedDetail           = "stored job contract was rejected"
)

var riverSchemaPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

type TerminalDeliveryRepairResult struct {
	Recovered                             int
	PostRepairContractRejectionsRecovered int
}

// TerminalDeliveryRepair rearms a provider-unit outbox row only when its
// exact River delivery was discarded by River's unhandled-kind rescue path
// while the authoritative run and unit remain active and due. The outbox and
// River rows are locked together so replicas converge on one replacement.
type TerminalDeliveryRepair struct {
	begin  func(context.Context) (pgx.Tx, error)
	client interface {
		JobDeleteTx(context.Context, pgx.Tx, int64) (*rivertype.JobRow, error)
	}
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
	stateInMask := pgx.Identifier{riverSchema, "river_job_state_in_bitmask"}.Sanitize()
	client, err := river.NewClient(riverpgxv5.New(queueControlPool), &river.Config{Schema: riverSchema})
	if err != nil {
		return nil, ErrInvalidConfiguration
	}
	return &TerminalDeliveryRepair{
		begin:  queueControlPool.Begin,
		client: riverDeleteAdapter{client: client},
		query:  fmt.Sprintf(repairProviderUnitTerminalDeliverySQL, jobTable, stateInMask),
	}, nil
}

type riverDeleteAdapter struct{ client *river.Client[pgx.Tx] }

func (adapter riverDeleteAdapter) JobDeleteTx(ctx context.Context, tx pgx.Tx, id int64) (*rivertype.JobRow, error) {
	return adapter.client.JobDeleteTx(ctx, tx, id)
}

func (repair *TerminalDeliveryRepair) Step(
	ctx context.Context,
	now time.Time,
	limit int,
) (TerminalDeliveryRepairResult, error) {
	if repair == nil || repair.begin == nil || repair.client == nil || ctx == nil || now.IsZero() ||
		limit < minReconcilerLimit || limit > maxReconcilerLimit {
		return TerminalDeliveryRepairResult{}, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return TerminalDeliveryRepairResult{}, err
	}
	tx, err := repair.begin(ctx)
	if err != nil || tx == nil {
		return TerminalDeliveryRepairResult{}, classifyStrandError(err)
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
		providerPostRepairContractRecoveryCode,
		providerPostRepairContractRecoveryDetail,
		providerContractRejectedCode,
		providerContractRejectedDetail,
	)
	if err != nil || rows == nil {
		return TerminalDeliveryRepairResult{}, classifyStrandError(err)
	}
	defer rows.Close()
	type candidate struct {
		outboxID, recoveryCode, recoveryDetail string
		riverJobID                             int64
	}
	candidates := make([]candidate, 0, limit)
	for rows.Next() {
		var candidate candidate
		if err := rows.Scan(&candidate.outboxID, &candidate.riverJobID, &candidate.recoveryCode, &candidate.recoveryDetail); err != nil ||
			!uuidPattern.MatchString(candidate.outboxID) || candidate.riverJobID <= 0 {
			return TerminalDeliveryRepairResult{}, ErrUnavailable
		}
		candidates = append(candidates, candidate)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(candidates) > limit {
		return TerminalDeliveryRepairResult{}, classifyStrandError(err)
	}
	result := TerminalDeliveryRepairResult{}
	for _, candidate := range candidates {
		deleted, err := repair.client.JobDeleteTx(ctx, tx, candidate.riverJobID)
		if err != nil || deleted == nil || deleted.ID != candidate.riverJobID || deleted.State != rivertype.JobStateDiscarded {
			return TerminalDeliveryRepairResult{}, classifyStrandError(err)
		}
		command, err := tx.Exec(ctx, `
			UPDATE public.worker_job_outbox
			SET status = 'pending', next_attempt_at = $2,
				last_error_code = $3, last_error_detail = $4, last_error_at = $2,
				river_job_id = NULL, delivered_at = NULL,
				claim_token = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = $2
			WHERE id = $1`, candidate.outboxID, now.UTC(), candidate.recoveryCode, candidate.recoveryDetail)
		if err != nil || command.RowsAffected() != 1 {
			return TerminalDeliveryRepairResult{}, classifyStrandError(err)
		}
		result.Recovered++
		if candidate.recoveryCode == providerPostRepairContractRecoveryCode {
			result.PostRepairContractRejectionsRecovered++
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return TerminalDeliveryRepairResult{}, classifyStrandError(err)
	}
	return result, nil
}

// The queue-control role has read-only access to sync_runs/sync_run_units and
// mutation authority over the generic outbox and River schema. The identity
// predicates bind the immutable outbox envelope, authoritative domain row,
// current outbox delivery, and River metadata together before any row is
// rearmed. The supported River delete runs in this same transaction after the
// exact terminal row is locked; a new relay step then creates fresh strict
// metadata. A paused integration is intentionally absent: pausing prevents
// new planning, but does not cancel already-planned work in an active run.
const repairProviderUnitTerminalDeliverySQL = `
	SELECT outbox.id::text, job.id,
		CASE
			WHEN outbox.status = 'delivered' THEN $4::text
			ELSE $6::text
		END AS recovery_code,
		CASE
			WHEN outbox.status = 'delivered' THEN $5::text
			ELSE $7::text
		END AS recovery_detail
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
	JOIN LATERAL (
		SELECT candidate_job.*
		FROM %s AS candidate_job
		WHERE candidate_job.kind = outbox.job_kind
			AND candidate_job.args = outbox.args::jsonb
			AND candidate_job.metadata ->> 'worker_outbox_id' = outbox.id::text
			AND candidate_job.metadata ->> 'payload_hash' = outbox.payload_hash
			AND candidate_job.metadata ->> 'contract_version' = outbox.contract_version::text
			AND candidate_job.state::text = 'discarded'
			AND candidate_job.finalized_at IS NOT NULL
			AND candidate_job.attempt < candidate_job.max_attempts
			AND candidate_job.unique_key IS NOT NULL
			AND %s(candidate_job.unique_states, candidate_job.state)
			AND cardinality(candidate_job.errors) > 0
			AND (candidate_job.errors[cardinality(candidate_job.errors)]->>'error') = $3
		ORDER BY candidate_job.id DESC
		LIMIT 1
	) AS job ON TRUE
	WHERE outbox.job_kind = 'sync.provider_unit'
		AND outbox.dedupe_key = 'sync.provider_unit:' || unit.id::text
		AND outbox.args #>> '{domain,type}' = 'sync_run_unit'
		AND unit.status = 'dispatching'
		AND (unit.available_at IS NULL OR unit.available_at <= $1)
		AND unit.lease_owner IS NULL
		AND unit.lease_expires_at IS NULL
		AND run.status IN ('planned', 'dispatching', 'running')
		AND (
			(
				outbox.status = 'delivered'
				AND outbox.river_job_id = job.id
			)
			OR (
				outbox.status = 'dead'
				AND outbox.river_job_id IS NULL
				AND outbox.delivered_at IS NULL
				AND outbox.contract_version = 1
				AND outbox.queue = 'sync_provider'
				AND outbox.priority = 2
				AND outbox.max_attempts = 5
				AND outbox.last_error_code = $8
				AND outbox.last_error_detail = $9
			)
		)
	ORDER BY outbox.delivered_at, outbox.id
	FOR UPDATE OF outbox, job SKIP LOCKED
	LIMIT $2::int
`
