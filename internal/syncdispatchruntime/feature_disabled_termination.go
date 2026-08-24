package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	scheduledsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrFeatureDisabledPlanNotTerminal ports terminalize_feature_disabled_plan's
// RuntimeError. reference_discovery runs before dispatch, so every unit
// should still be unclaimed (planned/retrying/dispatching) or RUNNING at
// this point -- terminalizeFeatureDisabledRun folds both into a terminal
// state (success/failed) unconditionally. A non-terminal result here means
// some OTHER mechanism already moved a unit somewhere terminalizeFeatureDisabledRun
// doesn't know how to close out, an invariant violation this port surfaces
// the same way Python does (loudly), not by silently proceeding to graph
// termination against a run that isn't actually done.
var ErrFeatureDisabledPlanNotTerminal = errors.New("feature-disabled planned run retained nonterminal units")

// canonicalIncidentFeatureDisabledMessage ports
// CanonicalIncidentFeatureDisabledError.__str__ verbatim:
// f"{FEATURE_DISABLED_ERROR_CATEGORY}: canonical incident ingestion is
// disabled ({reason.value})". The reason comes from
// scheduler/sync.CanonicalIncidentDecisionForUpdate -- the SAME decision
// path CanonicalIncidentAllowedForUpdate uses, extended to also surface the
// FeatureDecisionReason it was already computing internally. Losing this
// text (an earlier draft used a fixed "unspecified_reason" placeholder) was
// ruled a cause-erasure regression of the same class CHAOS-4159/#1881
// exists to prevent: a generic label overwriting a specific diagnostic
// cause, just one layer up (the native gate's own denial reason, not a
// planner result's).
func canonicalIncidentFeatureDisabledMessage(reason scheduledsync.FeatureDecisionReason) string {
	return fmt.Sprintf("%s: canonical incident ingestion is disabled (%s)", featureDisabledErrorCategory, reason)
}

// FeatureDisabledRunTransition mirrors feature_denial.py's dataclass of the
// same name.
type FeatureDisabledRunTransition struct {
	FailedUnits  int
	RunningUnits int
	RunTerminal  bool
}

// syncRunRequiresCanonicalIncidentFeature ports
// canonical_incident_gate.py::sync_run_requires_canonical_incident_feature:
// unit-level dataset scopes take precedence when any units exist (the
// common case once planning has run), falling back to the integration's
// enabled dataset scopes only for a run with no units yet. Provider strings
// are passed through UNMODIFIED, matching Python's `str(provider)` -- any
// normalization is providersync.Capability's job, not this caller's.
func syncRunRequiresCanonicalIncidentFeature(ctx context.Context, tx pgx.Tx, runID, integrationID string) (bool, error) {
	rows, err := tx.Query(ctx, `SELECT provider, dataset_key FROM public.sync_run_units WHERE sync_run_id = $1::uuid`, runID)
	if err != nil {
		return false, ErrReferenceDiscoveryUnavailable
	}
	var unitScopes [][2]string
	for rows.Next() {
		var provider, dataset string
		if err := rows.Scan(&provider, &dataset); err != nil {
			rows.Close()
			return false, ErrReferenceDiscoveryUnavailable
		}
		unitScopes = append(unitScopes, [2]string{provider, dataset})
	}
	if rows.Err() != nil {
		rows.Close()
		return false, ErrReferenceDiscoveryUnavailable
	}
	rows.Close()

	if len(unitScopes) > 0 {
		for _, scope := range unitScopes {
			if datasetScopeRequiresCanonicalIncident(scope[0], scope[1]) {
				return true, nil
			}
		}
		return false, nil
	}

	if integrationID == "" {
		return false, nil
	}
	integrationRows, err := tx.Query(ctx, `
SELECT integrations.provider, integration_datasets.dataset_key
FROM public.integration_datasets
JOIN public.integrations ON integrations.id = integration_datasets.integration_id
WHERE integration_datasets.integration_id = $1::uuid
  AND integration_datasets.is_enabled = true`, integrationID)
	if err != nil {
		return false, ErrReferenceDiscoveryUnavailable
	}
	defer integrationRows.Close()
	for integrationRows.Next() {
		var provider, dataset string
		if err := integrationRows.Scan(&provider, &dataset); err != nil {
			return false, ErrReferenceDiscoveryUnavailable
		}
		if datasetScopeRequiresCanonicalIncident(provider, dataset) {
			return true, nil
		}
	}
	return false, integrationRows.Err()
}

func datasetScopeRequiresCanonicalIncident(provider, dataset string) bool {
	targets, ok := providersyncCapabilityLegacyTargets(provider, dataset)
	if !ok {
		return false
	}
	return scheduledsync.SyncTargetsRequireCanonicalIncident(targets)
}

// terminalizeFeatureDisabledRun ports feature_denial.py::terminalize_feature_disabled_run
// verbatim, including its two-phase unit termination: a bulk update for
// units with no active lease (planned/retrying/dispatching), then a
// per-unit, lease-owner-matched update for units that were RUNNING at the
// moment this function READ them -- re-checked against the CURRENT
// lease_owner (including the NULL case, via IS NOT DISTINCT FROM) so a unit
// that finished or was reclaimed in between is never clobbered. Mutates run
// in place (completedUnits/failedUnits/errorText/result, and status/
// completedAt when terminal) the same way Python's ORM-attached `run`
// object is mutated by the SQLAlchemy session -- callers downstream (the
// graph termination) read those fields off the same run value afterward.
func terminalizeFeatureDisabledRun(
	ctx context.Context, tx pgx.Tx, run *finalizeSyncRun, errorText string, now time.Time,
) (FeatureDisabledRunTransition, error) {
	resultPayload := map[string]any{"error_category": featureDisabledErrorCategory}
	resultJSON, err := json.Marshal(resultPayload)
	if err != nil {
		return FeatureDisabledRunTransition{}, ErrReferenceDiscoveryUnavailable
	}

	type runningLease struct {
		unitID string
		owner  *string
	}
	rows, err := tx.Query(ctx, `
SELECT id::text, lease_owner FROM public.sync_run_units
WHERE sync_run_id = $1::uuid AND status = 'running'`, run.id)
	if err != nil {
		return FeatureDisabledRunTransition{}, ErrReferenceDiscoveryUnavailable
	}
	var runningLeases []runningLease
	for rows.Next() {
		var lease runningLease
		if err := rows.Scan(&lease.unitID, &lease.owner); err != nil {
			rows.Close()
			return FeatureDisabledRunTransition{}, ErrReferenceDiscoveryUnavailable
		}
		runningLeases = append(runningLeases, lease)
	}
	if rows.Err() != nil {
		rows.Close()
		return FeatureDisabledRunTransition{}, ErrReferenceDiscoveryUnavailable
	}
	rows.Close()

	if _, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = $2, available_at = NULL, error = $3, result = $4::json,
    lease_owner = NULL, lease_expires_at = NULL, updated_at = $5
WHERE sync_run_id = $1::uuid AND status IN ('planned', 'retrying', 'dispatching')`,
		run.id, syncRunUnitStatusFailed, errorText, resultJSON, now); err != nil {
		return FeatureDisabledRunTransition{}, ErrReferenceDiscoveryUnavailable
	}

	for _, lease := range runningLeases {
		if _, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = $2, available_at = NULL, error = $3, result = $4::json,
    lease_owner = NULL, lease_expires_at = NULL, updated_at = $5
WHERE id = $1::uuid AND sync_run_id = $6::uuid AND status = 'running'
  AND lease_owner IS NOT DISTINCT FROM $7`,
			lease.unitID, syncRunUnitStatusFailed, errorText, resultJSON, now,
			run.id, lease.owner); err != nil {
			return FeatureDisabledRunTransition{}, ErrReferenceDiscoveryUnavailable
		}
	}

	statusRows, err := tx.Query(ctx, `SELECT status FROM public.sync_run_units WHERE sync_run_id = $1::uuid`, run.id)
	if err != nil {
		return FeatureDisabledRunTransition{}, ErrReferenceDiscoveryUnavailable
	}
	var failedUnits, runningUnits, successUnits, totalUnits int
	for statusRows.Next() {
		var status string
		if err := statusRows.Scan(&status); err != nil {
			statusRows.Close()
			return FeatureDisabledRunTransition{}, ErrReferenceDiscoveryUnavailable
		}
		totalUnits++
		switch status {
		case syncRunUnitStatusFailed:
			failedUnits++
		case "running":
			runningUnits++
		case syncRunUnitStatusSuccess:
			successUnits++
		}
	}
	if statusRows.Err() != nil {
		statusRows.Close()
		return FeatureDisabledRunTransition{}, ErrReferenceDiscoveryUnavailable
	}
	statusRows.Close()
	runTerminal := failedUnits+successUnits == totalUnits

	run.failedUnits = failedUnits
	run.completedUnits = successUnits
	run.errorText = &errorText
	run.result = resultPayload

	if runTerminal {
		completedAt := now
		if run.completedAt != nil {
			completedAt = *run.completedAt
		}
		if _, err := tx.Exec(ctx, `
UPDATE public.sync_runs
SET failed_units = $2, completed_units = $3, error = $4, result = $5::json,
    status = $6, completed_at = $7
WHERE id = $1::uuid`,
			run.id, failedUnits, successUnits, errorText, resultJSON, syncRunStatusFailed, completedAt); err != nil {
			return FeatureDisabledRunTransition{}, ErrReferenceDiscoveryUnavailable
		}
		run.status = syncRunStatusFailed
		run.completedAt = &completedAt
	} else {
		if _, err := tx.Exec(ctx, `
UPDATE public.sync_runs
SET failed_units = $2, completed_units = $3, error = $4, result = $5::json
WHERE id = $1::uuid`,
			run.id, failedUnits, successUnits, errorText, resultJSON); err != nil {
			return FeatureDisabledRunTransition{}, ErrReferenceDiscoveryUnavailable
		}
	}

	return FeatureDisabledRunTransition{
		FailedUnits:  failedUnits,
		RunningUnits: runningUnits,
		RunTerminal:  runTerminal,
	}, nil
}

// terminalizeFeatureDisabledGraph ports _terminalize_feature_disabled_graph:
// terminalizes the reference-discovery ledger, marks every pending outbox
// row for this run "dispatched" with the feature_disabled sentinel
// (preserved verbatim by every native upsert's terminal-denial CASE branch
// -- see upsertDiscoveryOutboxWakeup/upsertPostSyncOutboxWakeup), ensures a
// terminally-dispatched finalize_sync_run outbox row exists via the same
// ON CONFLICT (sync_run_id, kind) upsert those functions already use, and
// reuses observeTerminalSyncRun (built for finalize_sync_run, CHAOS-4175
// family 1 -- same package, no export needed) for the BackfillJob/JobRun
// observer updates Python drives through sync_observers_for_terminal_sync_run.
//
// completedAt must be run.completedAt as left by terminalizeFeatureDisabledRun
// (Python: `run.completed_at or datetime.now(...)`, evaluated AFTER that
// call already coalesced it) -- callers must invoke terminalizeFeatureDisabledRun
// first and pass its mutated run through unchanged.
func terminalizeFeatureDisabledGraph(
	ctx context.Context, tx pgx.Tx, run *finalizeSyncRun, errorText string, completedAt time.Time,
) error {
	sanitized := sanitizeErrorText(errorText)
	resultJSON, err := json.Marshal(map[string]any{"error_category": featureDisabledErrorCategory})
	if err != nil {
		return ErrReferenceDiscoveryUnavailable
	}

	if _, err := tx.Exec(ctx, `
UPDATE public.sync_run_reference_discoveries
SET status = $2, lease_owner = NULL, lease_expires_at = NULL, last_heartbeat_at = $3,
    completed_at = $3, error = $4, result = $5::json, updated_at = $3
WHERE sync_run_id = $1::uuid AND status IN ($6, $7, $8)`,
		run.id, discoveryStatusFailed, completedAt, sanitized, resultJSON,
		discoveryStatusPlanned, discoveryStatusRetrying, discoveryStatusRunning); err != nil {
		return ErrReferenceDiscoveryUnavailable
	}

	if _, err := tx.Exec(ctx, `
UPDATE public.sync_dispatch_outbox
SET status = 'dispatched', dispatched_at = $2, last_error = $3, claim_token = NULL,
    claim_expires_at = NULL, claim_transport = NULL, claim_route_generation = NULL,
    dispatched_transport = NULL, dispatched_route_generation = NULL, transport_job_id = NULL,
    updated_at = $2
WHERE sync_run_id = $1::uuid AND status = 'pending'`,
		run.id, completedAt, featureDisabledErrorCategory); err != nil {
		return ErrReferenceDiscoveryUnavailable
	}

	newID := uuid.New().String()
	if _, err := tx.Exec(ctx, `
INSERT INTO public.sync_dispatch_outbox (
    id, org_id, sync_run_id, kind, status, available_at, attempts,
    dispatched_at, last_error, created_at, updated_at
) VALUES ($1::uuid, $2, $3::uuid, $4, 'dispatched', $5, 0, $5, $6, $5, $5)
ON CONFLICT (sync_run_id, kind) DO UPDATE
SET status = 'dispatched',
    last_error = EXCLUDED.last_error,
    dispatched_at = EXCLUDED.dispatched_at,
    claim_token = NULL,
    claim_expires_at = NULL,
    claim_transport = NULL,
    claim_route_generation = NULL,
    dispatched_transport = NULL,
    dispatched_route_generation = NULL,
    transport_job_id = NULL,
    updated_at = EXCLUDED.updated_at`,
		newID, run.orgID, run.id, outboxKindFinalizeSyncRun, completedAt, featureDisabledErrorCategory); err != nil {
		return ErrReferenceDiscoveryUnavailable
	}

	return observeTerminalSyncRun(ctx, tx, run, completedAt, syncRunStatusFailed, run.completedUnits, run.failedUnits,
		map[string]any{"error_category": featureDisabledErrorCategory}, &sanitized)
}

// terminalizeFeatureDisabledPlan ports terminalize_feature_disabled_plan:
// the reference_discovery caller's variant, which REQUIRES the run to have
// gone fully terminal (discovery runs before dispatch, so every unit is
// still a fresh plan with no in-flight dispatched work) and always
// terminalizes the graph immediately after. reason is the
// FeatureDecisionReason CanonicalIncidentDecisionForUpdate produced for the
// denial that triggered this call -- callers must not reuse a message
// computed for a different decision.
func terminalizeFeatureDisabledPlan(
	ctx context.Context, tx pgx.Tx, run *finalizeSyncRun, reason scheduledsync.FeatureDecisionReason, now time.Time,
) (FeatureDisabledRunTransition, error) {
	message := canonicalIncidentFeatureDisabledMessage(reason)
	transition, err := terminalizeFeatureDisabledRun(ctx, tx, run, message, now)
	if err != nil {
		return FeatureDisabledRunTransition{}, err
	}
	if !transition.RunTerminal {
		return transition, ErrFeatureDisabledPlanNotTerminal
	}
	// run.completedAt is guaranteed non-nil here: terminalizeFeatureDisabledRun
	// sets it whenever it returns RunTerminal true.
	if err := terminalizeFeatureDisabledGraph(ctx, tx, run, message, *run.completedAt); err != nil {
		return transition, err
	}
	return transition, nil
}

// armFeatureDisabledFinalize ports _arm_feature_disabled_finalize: a
// best-effort insert of a PENDING finalize_sync_run outbox row for the
// dispatch_sync_run caller's NON-terminal case (some units still running,
// so that run isn't ready to finalize inline the way reference_discovery's
// always-terminal case is). ON CONFLICT (sync_run_id, kind) DO NOTHING
// reproduces Python's begin_nested/IntegrityError/rollback race guard
// without needing to classify the driver error by SQLSTATE: true only when
// this call actually created the row, matching _arm_feature_disabled_finalize's
// bool return exactly. Not yet called from this family -- landing now,
// alongside its terminal sibling, per the "port once, share" instruction,
// ready for dispatch_sync_run to use when that family lands.
func armFeatureDisabledFinalize(ctx context.Context, tx pgx.Tx, orgID, runID string, availableAt time.Time) (bool, error) {
	newID := uuid.New().String()
	tag, err := tx.Exec(ctx, `
INSERT INTO public.sync_dispatch_outbox (id, org_id, sync_run_id, kind, status, available_at, attempts, created_at, updated_at)
VALUES ($1::uuid, $2, $3::uuid, $4, 'pending', $5, 0, $5, $5)
ON CONFLICT (sync_run_id, kind) DO NOTHING`,
		newID, orgID, runID, outboxKindFinalizeSyncRun, availableAt)
	if err != nil {
		return false, ErrReferenceDiscoveryUnavailable
	}
	return tag.RowsAffected() > 0, nil
}
