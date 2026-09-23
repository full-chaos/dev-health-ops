package joboperator

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrAuditUnavailable = errors.New("worker operator audit unavailable")

// AuditedActions is every Action a worker_operator_audits row can carry:
// the service's own audited mutations (Service.mutate) and the direct-write
// verbs (Service.Audited). The table's action check
// (ck_worker_operator_audits_action, alembic 0138) must allow exactly these.
// TestEveryAuditedMutationIsInAuditedActions pins the service's own list,
// workersctl's TestEveryDirectWriteVerbIsAuditedBeforeItWrites pins
// DirectWriteActions to the verbs, and
// TestAuditActionMigrationMatchesAuditedActions pins the migration. Two
// Actions share the string job_routes.apply_checked_in (the sync-route and
// job-route apply), so the list holds strings, not Action names.
var AuditedActions = append(append([]Action{}, serviceAuditedActions...), DirectWriteActions...)

// serviceAuditedActions are the Actions of the service's own audited
// mutations: jobs, queues, workers, sync routes and job routes.
var serviceAuditedActions = []Action{
	ActionCancel,
	ActionRetry,
	ActionPauseQueue,
	ActionResumeQueue,
	ActionDrain,
	ActionUndrain,
	ActionApplyJobRoute,
	ActionRollbackJobRoute,
	ActionPauseRoute,
	ActionDrainRoute,
	ActionResumeRoute,
}

// DirectWriteActions are the Actions of the `dho workers` verbs whose write
// does not go through the service's own backends; each runs its write
// through Service.Audited.
var DirectWriteActions = []Action{
	ActionProvidersyncCleanup,
	ActionProvidersyncOwnershipCleanup,
	ActionSyncDispatchOutboxClose,
	ActionWorkgraphTrigger,
	ActionInvestmentTrigger,
	ActionLedgerRepair,
	ActionMetricsDailyStart,
	ActionMetricsDailyRedrive,
	ActionMetricsDailyFinalize,
	ActionMetricsFinalizeRedrive,
	ActionMetricsPartitionRecompute,
	ActionMetricsRemainingStart,
	ActionMetricsRemainingTriggerBackstop,
	ActionMetricsRemainingRedrive,
	ActionExternalRecomputeReplay,
}

// PostgresAuditor stores bounded mutation intent in the semantic database.
// Begin uses its own committed statement, so no queue mutation can happen
// without a durable intent record.
type PostgresAuditor struct {
	pool *pgxpool.Pool
}

func NewPostgresAuditor(pool *pgxpool.Pool) (*PostgresAuditor, error) {
	if pool == nil {
		return nil, ErrAuditUnavailable
	}
	return &PostgresAuditor{pool: pool}, nil
}

func (auditor *PostgresAuditor) Begin(ctx context.Context, event AuditEvent) (AuditHandle, error) {
	if auditor == nil || auditor.pool == nil || event.Principal != OperatorPrincipal || event.CreatedAt.IsZero() {
		return nil, ErrAuditUnavailable
	}
	var auditID int64
	err := auditor.pool.QueryRow(ctx, `
		INSERT INTO public.worker_operator_audits (
			credential_id, principal_type, principal_id, action, resource_type,
			resource_id, reason_code, correlation_id, status, created_at
		) VALUES (NULL, $1, $2, $3, $4, $5, $6, $7, 'started', $8)
		RETURNING id`,
		event.Principal.Type,
		event.Principal.ID,
		string(event.Action),
		event.ResourceType,
		event.ResourceID,
		event.ReasonCode,
		event.CorrelationID,
		event.CreatedAt,
	).Scan(&auditID)
	if err != nil || auditID < 1 {
		return nil, ErrAuditUnavailable
	}
	return &postgresAuditHandle{pool: auditor.pool, id: auditID}, nil
}

type postgresAuditHandle struct {
	pool *pgxpool.Pool
	id   int64
}

func (handle *postgresAuditHandle) Complete(ctx context.Context, status AuditStatus) error {
	if handle == nil || handle.pool == nil || handle.id < 1 ||
		(status != AuditSucceeded && status != AuditFailed && status != AuditOutcomeUnknown) {
		return ErrAuditUnavailable
	}
	result, err := handle.pool.Exec(ctx, `
		UPDATE public.worker_operator_audits
		SET status = $2, completed_at = statement_timestamp()
		WHERE id = $1 AND status = 'started'`,
		handle.id,
		string(status),
	)
	if err != nil || result.RowsAffected() != 1 {
		return ErrAuditUnavailable
	}
	return nil
}
