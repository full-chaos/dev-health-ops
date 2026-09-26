package syncadmin

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/synccoverage"
	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

// The sync configuration's "Sync now" and backfill POSTs do not plan a run.
// Python's create_sync_execution_trigger hands a planner-managed parent, or a
// child config pinned to one source, to the Go scheduler (a
// scheduled_sync_occurrences row and its sync_manual_triggers payload, the same
// hand-off a cron tick uses) and waits a bounded time for the plan; these
// routes write exactly those rows (synchandoff) and wait the same way.
//
// Named divergence (the same one the integration sync and backfill routes hold,
// integrationsadmin/handoff.go): a configuration that is neither planner-managed
// nor pinned to one source, which Python planned in process (plan_sync_run),
// is not one the scheduler will run; it is refused here, with nothing written,
// instead of being retried and quarantined out of the caller's sight.
const (
	defaultManualTriggerAwait = 10 * time.Second
	manualTriggerPoll         = 250 * time.Millisecond

	// plannerTagKey is trigger_routing._PLANNER_TAG_KEY: the integration source
	// metadata that ties a source to the planner-managed parent that selected it.
	plannerTagKey = "planner_managed_sync_config_id"
)

var errConfigNotRunnable = refuse(http.StatusConflict,
	"Sync configuration is not one the scheduler can run: it is neither planner-managed nor pinned to one source")

// manualTriggerAwait is _manual_trigger_await_seconds: a positive finite float
// from SYNC_MANUAL_TRIGGER_AWAIT_SECONDS, else the 10 second default.
func (h *handlers) manualTriggerAwait() time.Duration {
	if raw, present := h.lookupEnv("SYNC_MANUAL_TRIGGER_AWAIT_SECONDS"); present {
		if value, ok := pythonparity.ParseFloat(raw); ok && value > 0 && !math.IsInf(value, 0) && !math.IsNaN(value) {
			seconds := value * float64(time.Second)
			if seconds >= float64(math.MaxInt64) {
				return time.Duration(math.MaxInt64)
			}
			return time.Duration(seconds)
		}
	}
	return defaultManualTriggerAwait
}

// checkWorkItemsLimit is the trigger route's tier check: when the config syncs
// work items and the tier caps max_work_items, the org's stored work items must
// be below it (403). It reads ClickHouse and, as Python does, lets the sync
// proceed when ClickHouse is not configured or cannot be read.
func (h *handlers) checkWorkItemsLimit(ctx context.Context, org string, targets []pyjson.Value) error {
	if !containsTarget(targets, "work-items") {
		return nil
	}
	orgUUID, err := pythonparity.ParseUUID(org)
	if err != nil {
		return fmt.Errorf("org id is not a UUID: %w", err)
	}
	inputs, err := licensing.LoadTierLimitInputs(ctx, h.pool, orgUUID)
	if err != nil {
		return err
	}
	limit, err := licensing.GetLimitFrom(inputs, "max_work_items")
	if err != nil {
		return err
	}
	if limit == nil {
		return nil
	}
	maximum, err := pyIntOf(limit)
	if err != nil {
		return err
	}
	if h.clickhouse == nil {
		return nil
	}
	current, readErr := h.workItemCount(ctx, org)
	if readErr != nil {
		h.logger.WarnContext(ctx, "sync admin: work items count unavailable, allowing the sync",
			slog.String("error", readErr.Error()))
		return nil
	}
	if current >= maximum {
		return refuse(http.StatusForbidden, fmt.Sprintf(
			"Work items limit exceeded: %d/%d. Upgrade your tier to sync more work items.", current, maximum))
	}
	return nil
}

func (h *handlers) workItemCount(ctx context.Context, org string) (int64, error) {
	var count uint64
	row := h.clickhouse.QueryRow(ctx, `SELECT count() AS cnt FROM work_items WHERE org_id = {org_id:String}`,
		clickhouse.Named("org_id", org))
	if err := row.Scan(&count); err != nil {
		return 0, err
	}
	if count > math.MaxInt64 {
		return math.MaxInt64, nil
	}
	return int64(count), nil
}

func containsTarget(targets []pyjson.Value, want string) bool {
	for _, target := range targets {
		if text, ok := target.(string); ok && text == want {
			return true
		}
	}
	return false
}

// pyIntOf is int(value) of a tier limit value.
func pyIntOf(value pyjson.Value) (int64, error) {
	switch typed := value.(type) {
	case bool:
		if typed {
			return 1, nil
		}
		return 0, nil
	case pyjson.Int:
		if typed.Int.IsInt64() {
			return typed.Int.Int64(), nil
		}
	case pyjson.Float:
		if !math.IsInf(float64(typed), 0) && !math.IsNaN(float64(typed)) && math.Abs(float64(typed)) < 9e18 {
			return int64(float64(typed)), nil
		}
	case string:
		if parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64); err == nil {
			return parsed, nil
		}
	}
	return 0, fmt.Errorf("int() of tier limit %T value", value)
}

// preflightPlannerCredential is _preflight_planner_credential: a PagerDuty
// config syncs the operational target only and needs an org-scoped credential;
// any other config's linked integration credential, when it has one, must exist
// for the org and provider, be active, and not have failed its last test.
func (h *handlers) preflightPlannerCredential(ctx context.Context, org string, config *syncConfig, targets []pyjson.Value) error {
	provider := pythonparity.Lower(config.Provider)
	if provider == "pagerduty" {
		operationalOnly := len(targets) > 0
		seen := map[string]bool{}
		for _, target := range targets {
			text, isString := target.(string)
			if !isString {
				operationalOnly = false
				continue
			}
			seen[text] = true
		}
		if !operationalOnly || len(seen) != 1 || !seen["operational"] {
			return refuse(http.StatusConflict, "PagerDuty sync target must be operational")
		}
	}
	credentialID, err := h.credentialForConfig(ctx, org, config)
	if err != nil {
		return err
	}
	if credentialID == nil {
		if provider == "pagerduty" {
			return refuse(http.StatusConflict, "PagerDuty sync requires an active organization-scoped credential")
		}
		return nil
	}
	var active bool
	var lastSuccess *bool
	var lastError *string
	err = h.pool.QueryRow(ctx, `
SELECT is_active, last_test_success, last_test_error FROM integration_credentials
WHERE id = $1 AND org_id = $2 AND provider = $3`, *credentialID, org, provider).Scan(&active, &lastSuccess, &lastError)
	if err == pgx.ErrNoRows {
		return refuse(http.StatusBadRequest, "Credential not found")
	}
	if err != nil {
		return fmt.Errorf("read the credential: %w", err)
	}
	if !active {
		return refuse(http.StatusConflict, "Credential is inactive")
	}
	if lastSuccess != nil && !*lastSuccess {
		detail := "Credential preflight failed"
		if lastError != nil && *lastError != "" {
			detail = *lastError
		}
		return refuse(http.StatusConflict, detail)
	}
	return nil
}

// manualSpec is what one manual trigger asks the scheduler for.
type manualSpec struct {
	// mode is the requested mode: "incremental" (promoted to full_resync by the
	// config's sync_options) or "backfill".
	mode, triggeredBy string
	// window is the backfill's window and scope (nil for a plain trigger).
	window *backfillWindow
}

// handOffManual is create_sync_execution_trigger's Go hand-off: build the
// planner request the config routes to, and write the occurrence and manual
// trigger in one transaction, with the coverage projection invalidated.
func (h *handlers) handOffManual(ctx context.Context, org string, configID uuid.UUID, spec manualSpec) (synchandoff.Trigger, error) {
	var trigger synchandoff.Trigger
	err := pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		config, err := synchandoff.LoadConfig(ctx, tx, configID.String())
		if err != nil {
			return err
		}
		if config == nil || config.OrgID != org {
			return refuse(http.StatusNotFound, "Sync configuration not found")
		}
		// planner_request_for_config_if_routed is None without a linked
		// integration; the route answers 400.
		if config.IntegrationID == nil {
			return refuse(http.StatusBadRequest, "Sync configuration has no linked integration")
		}
		if !config.PlannerManaged && config.SourceID == nil {
			return errConfigNotRunnable
		}
		input := synchandoff.MintInput{Mode: spec.mode, TriggeredBy: spec.triggeredBy}
		sourceIDs, datasetKeys, err := h.plannerScope(ctx, tx, config)
		if err != nil {
			return err
		}
		input.SourceIDs, input.DatasetKeys = sourceIDs, datasetKeys
		if input.Mode == "incremental" && synchandoff.Truthy(config.SyncOptions["full_resync"]) {
			input.Mode = "full_resync"
		}
		if window := spec.window; window != nil {
			since, before := window.Since, window.Before
			input.Since, input.Before = &since, &before
			if window.Structured {
				// A structured selector replaces the request's own scope.
				input.SourceIDs, input.DatasetKeys = nil, nil
				if window.SourceIDsSet {
					input.SourceIDs = append([]string{}, window.SourceIDs...)
				}
				if window.DatasetKeysSet {
					input.DatasetKeys = append([]string{}, window.DatasetKeys...)
				}
			}
		}
		trigger, err = synchandoff.Mint(ctx, tx, config, input, h.now())
		if err != nil {
			return err
		}
		if trigger.Existing {
			return fmt.Errorf("occurrence %s already exists", trigger.OccurrenceID)
		}
		return synccoverage.InvalidateForConfig(ctx, tx, org, config.ID)
	})
	return trigger, err
}

// plannerScope is plan_request_for_config's source and dataset scope, plus
// planner_request_for_config_if_routed's tag scoping. A child pinned to one
// source runs that source, and the datasets its targets name (none: all
// enabled). A planner-managed parent runs the enabled sources tagged for it (an
// empty list runs nothing; PagerDuty is account-scoped and leaves it unset).
func (h *handlers) plannerScope(ctx context.Context, tx pgx.Tx, config *synchandoff.Config) (sourceIDs, datasetKeys []string, err error) {
	if config.SourceID != nil {
		sourceIDs = []string{*config.SourceID}
		if keys := providersync.DatasetKeysForTargets(config.Provider, config.SyncTargets); len(keys) > 0 {
			datasetKeys = keys
		}
		return sourceIDs, datasetKeys, nil
	}
	if config.PlannerManaged && pythonparity.Lower(config.Provider) != "pagerduty" {
		rows, err := tx.Query(ctx, `
SELECT id::text FROM public.integration_sources
WHERE org_id = $1 AND integration_id = $2::uuid AND is_enabled AND metadata->>$3 = $4
ORDER BY full_name, id`, config.OrgID, *config.IntegrationID, plannerTagKey, config.ID)
		if err != nil {
			return nil, nil, fmt.Errorf("list the planner-tagged sources: %w", err)
		}
		defer rows.Close()
		sourceIDs = []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, nil, err
			}
			sourceIDs = append(sourceIDs, id)
		}
		if err := rows.Err(); err != nil {
			return nil, nil, err
		}
	}
	return sourceIDs, nil, nil
}

// manualOutcome waits for the scheduler's plan of the occurrence and answers the
// outcomes both routes share (failed, pending, disabled). answered is false for
// a plan made, which each route answers itself.
func (h *handlers) manualOutcome(w http.ResponseWriter, r *http.Request, configID string, trigger synchandoff.Trigger) (synchandoff.Outcome, bool, error) {
	outcome, err := synchandoff.Wait(r.Context(), h.pool, trigger.OccurrenceID, h.manualTriggerAwait(), manualTriggerPoll)
	if err != nil {
		return outcome, false, err
	}
	body := pyjson.NewObject()
	switch outcome.State {
	case synchandoff.StateQuarantined:
		body.Set("status", "failed")
		body.Set("config_id", configID)
		body.Set("occurrence_id", trigger.OccurrenceID)
		body.Set("reason", "scheduled sync occurrence quarantined: "+outcome.ErrorCode)
	case synchandoff.StatePending:
		body.Set("status", "pending")
		body.Set("config_id", configID)
		body.Set("occurrence_id", trigger.OccurrenceID)
	case synchandoff.StateTerminal:
		body.Set("status", "disabled")
		body.Set("config_id", configID)
		body.Set("run_id", outcome.JobRunID)
		body.Set("total_units", int64(outcome.TotalUnits))
	default:
		return outcome, false, nil
	}
	policy.WriteModel(w, http.StatusAccepted, body, nil)
	return outcome, true, nil
}

// triggerSyncConfig is sync.py's trigger_sync_config.
func (h *handlers) triggerSyncConfig(w http.ResponseWriter, r *http.Request) {
	config, ok := h.configFromPath(w, r)
	if !ok {
		return
	}
	ctx, org := r.Context(), orgID(r)
	targets, err := storedTargets(config)
	if err != nil {
		h.fail(w, r, "sync_targets", err)
		return
	}
	if err := h.requireCanonicalIncident(ctx, org, targets); err != nil {
		h.answerOrFail(w, r, "canonical_incident_feature", err)
		return
	}
	if !config.IsActive {
		policy.WriteDetail(w, http.StatusConflict, "Sync configuration is paused and cannot be triggered", nil)
		return
	}
	if err := h.checkWorkItemsLimit(ctx, org, targets); err != nil {
		h.answerOrFail(w, r, "work_items_limit", err)
		return
	}
	if err := h.preflightPlannerCredential(ctx, org, config, targets); err != nil {
		h.answerOrFail(w, r, "preflight_credential", err)
		return
	}
	trigger, err := h.handOffManual(ctx, org, config.ID, manualSpec{mode: "incremental", triggeredBy: "manual"})
	if err != nil {
		h.answerOrFail(w, r, "hand_off_trigger", err)
		return
	}
	outcome, answered, err := h.manualOutcome(w, r, config.ID.String(), trigger)
	if err != nil {
		h.fail(w, r, "wait for the scheduler", err)
		return
	}
	if answered {
		return
	}
	body := pyjson.NewObject()
	body.Set("status", "triggered")
	body.Set("config_id", config.ID.String())
	body.Set("sync_run_id", outcome.SyncRunID)
	body.Set("run_id", outcome.JobRunID)
	body.Set("total_units", int64(outcome.TotalUnits))
	policy.WriteModel(w, http.StatusAccepted, body, nil)
}
