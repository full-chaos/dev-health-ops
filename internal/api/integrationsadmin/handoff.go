package integrationsadmin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

// The sync and backfill triggers do not plan a run. Python's routes ran
// plan_sync_run in the request; the Go scheduler is the one planner, so these
// routes write the hand-off rows the scheduler reads (synchandoff) and wait a
// bounded time for the plan.
const (
	defaultHandoffWait = 30 * time.Second
	defaultHandoffPoll = 250 * time.Millisecond
)

func (h handlers) handoffWait() time.Duration {
	if h.HandoffWait > 0 {
		return h.HandoffWait
	}
	return defaultHandoffWait
}

func (h handlers) handoffPoll() time.Duration {
	if h.HandoffPoll > 0 {
		return h.HandoffPoll
	}
	return defaultHandoffPoll
}

// handoffScope is what a trigger is handed to the scheduler under: the sync
// configuration, and the integration's enabled sources and datasets.
type handoffScope struct {
	config   *synchandoff.Config
	sources  []string
	datasets []string
}

// errNoRunnableConfig is the refusal of an integration the scheduler could not
// run: Python planned any integration, so this is a named divergence.
var errNoRunnableConfig = refuse(http.StatusConflict, "Integration has no sync configuration the scheduler can run")

// handoffTarget resolves the configuration the scheduler will run the trigger
// under. Python planned from the integration alone; the hand-off needs a
// configuration, so it is the integration's canonical parent
// (canonical_sync_config_for_sync_run: no parent_id, oldest first), the rule
// `dho backfill run` also holds. A configuration the scheduler would refuse is
// refused here, with nothing written: it would otherwise be retried and
// quarantined out of the caller's sight.
//
// It also returns the integration's enabled sources and enabled datasets: an
// omitted selector means all of them in Python's planner, and the scheduler
// would otherwise narrow the run to the configuration's tagged sources and
// sync_targets, so the hand-off names them.
func handoffTarget(ctx context.Context, tx pgx.Tx, orgID string, integrationID uuid.UUID, integrationActive bool) (handoffScope, error) {
	var configID string
	err := tx.QueryRow(ctx, `
SELECT id::text FROM public.sync_configurations
WHERE org_id = $1 AND integration_id = $2::uuid AND parent_id IS NULL
ORDER BY created_at ASC, id ASC LIMIT 1`, orgID, integrationID).Scan(&configID)
	if errors.Is(err, pgx.ErrNoRows) {
		return handoffScope{}, errNoRunnableConfig
	}
	if err != nil {
		return handoffScope{}, fmt.Errorf("resolve the canonical configuration: %w", err)
	}
	config, err := synchandoff.LoadConfig(ctx, tx, configID)
	if err != nil {
		return handoffScope{}, err
	}
	if config == nil || !integrationActive || !config.IsActive || (!config.PlannerManaged && config.SourceID == nil) {
		return handoffScope{}, errNoRunnableConfig
	}
	scope := handoffScope{config: config}
	if scope.sources, err = textColumn(ctx, tx, `
SELECT id::text FROM public.integration_sources
WHERE org_id = $1 AND integration_id = $2::uuid AND is_enabled
ORDER BY full_name, id`, orgID, integrationID); err != nil {
		return handoffScope{}, fmt.Errorf("list enabled sources: %w", err)
	}
	if scope.datasets, err = textColumn(ctx, tx, `
SELECT dataset_key FROM public.integration_datasets
WHERE org_id = $1 AND integration_id = $2::uuid AND is_enabled
ORDER BY dataset_key`, orgID, integrationID); err != nil {
		return handoffScope{}, fmt.Errorf("list enabled datasets: %w", err)
	}
	return scope, nil
}

// textColumn reads one text column into a non-nil slice.
func textColumn(ctx context.Context, tx pgx.Tx, sql string, args ...any) ([]string, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, rows.Err()
}

// handoffResponse waits for the scheduler's plan of the occurrence and writes
// Python's SyncTriggerResponse. A plan the scheduler quarantined answers 400
// (Python's planner refusals were 400); a plan not made within the wait is a
// 202 naming the occurrence.
func (h handlers) handoffResponse(w http.ResponseWriter, r *http.Request, rawIntegrationID string, trigger synchandoff.Trigger) {
	outcome, err := synchandoff.Wait(r.Context(), h.Pool, trigger.OccurrenceID, h.handoffWait(), h.handoffPoll())
	if err != nil {
		h.fail(w, r, "wait for the scheduler", err)
		return
	}
	body := pyjson.NewObject()
	switch outcome.State {
	case synchandoff.StateMaterialized, synchandoff.StateTerminal:
		status := "accepted"
		if outcome.State == synchandoff.StateTerminal {
			status = "disabled"
		}
		body.Set("status", status)
		body.Set("integration_id", rawIntegrationID)
		body.Set("sync_run_id", outcome.SyncRunID)
		body.Set("total_units", int64(outcome.TotalUnits))
	case synchandoff.StateQuarantined:
		h.Logger.WarnContext(r.Context(), "sync hand-off quarantined by the scheduler",
			slog.String("occurrence_id", trigger.OccurrenceID), slog.String("code", outcome.ErrorCode))
		policy.WriteDetail(w, http.StatusBadRequest, "Sync plan rejected: "+outcome.ErrorCode, nil)
		return
	default:
		h.Logger.InfoContext(r.Context(), "sync hand-off not planned within the wait",
			slog.String("occurrence_id", trigger.OccurrenceID), slog.Duration("wait", h.handoffWait()))
		body.Set("status", "pending")
		body.Set("integration_id", rawIntegrationID)
		body.Set("occurrence_id", trigger.OccurrenceID)
	}
	policy.WriteModel(w, http.StatusAccepted, body, nil)
}
