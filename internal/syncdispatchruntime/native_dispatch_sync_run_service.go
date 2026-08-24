package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	scheduledsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// ErrDispatchSyncRunUnavailable ports the bare execution-failure class every
// sibling native service already has (ErrFinalizeSyncRunUnavailable,
// ErrReferenceDiscoveryUnavailable) -- a query/scan failure this deep in
// Dispatch() is indistinguishable from any other Postgres blip mid-pass,
// classified retryable via River's own retry policy on the job, not a
// permanent verdict about the run.
var ErrDispatchSyncRunUnavailable = errors.New("native dispatch_sync_run is unavailable")

// errDispatchNotYetImplemented marks the deliberate, visible stopping
// point of this slice (the "decision.allowed == true" continuation --
// concurrency partial-cap, BudgetGuard, claim/route/enqueue, and the
// pending-unit-counts/redispatch/finalize tail -- lands in a follow-up
// commit). Kept distinct from ErrDispatchSyncRunUnavailable so a test
// reaching this point can assert PRECISELY "everything before here is
// correct," not just "some error happened." Not exported: an internal-
// development-only marker, not a caller-facing sentinel.
var errDispatchNotYetImplemented = errors.New("dispatch_sync_run: allowed-pass continuation not yet implemented (CHAOS-4175 family 3)")

// NativeDispatchSyncRunService is the native equivalent of bridge.Dispatch /
// Python's dispatch_sync_run task -- CHAOS-4175 family 3, the last of the
// three sync-dispatch coordinator families to move off the HTTP
// compatibility bridge. Mirrors NativeFinalizeSyncRunService/
// NativeReferenceDiscoveryService's exact composition shape: a pool this
// service owns no lifecycle over, opening its own transaction(s) per call
// rather than accepting an ambient one, matching Python's own multiple
// separate `with get_postgres_session_sync() as session:` blocks inside
// ONE dispatch_sync_run call (the main claim/route/enqueue pass, then a
// second session just for _pending_unit_counts, then a third
// _schedule_redispatch opens for itself) rather than one transaction end
// to end.
type NativeDispatchSyncRunService struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
	bridge budgetEstimator
	now    func() time.Time
}

// NewNativeDispatchSyncRunService constructs the native dispatch_sync_run
// executor. bridge is the estimate-only bridge client (CHAOS-4175 ruling):
// the credential-bound half of budget admission (SyncTaskBootstrap.load,
// the six per-provider estimator classes) stays Python-side behind
// /dispatch-budget-estimate; everything else in this service is native.
func NewNativeDispatchSyncRunService(
	pool *pgxpool.Pool,
	logger *slog.Logger,
	bridge budgetEstimator,
) (*NativeDispatchSyncRunService, error) {
	if pool == nil || bridge == nil {
		return nil, ErrDispatchSyncRunUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &NativeDispatchSyncRunService{pool: pool, logger: logger, bridge: bridge, now: time.Now}, nil
}

func (service *NativeDispatchSyncRunService) nowUTC() time.Time {
	if service.now == nil {
		return time.Now().UTC()
	}
	return service.now().UTC()
}

// Dispatch is the native equivalent of bridge.Dispatch / Python's
// dispatch_sync_run(sync_run_id). Ports the function's FIRST session block
// verbatim through the total-cap hard-deny branch (workers/sync_units.py:
// 772-935 roughly) -- authorize, feature-gate, and either deny the whole
// pass or continue into budget admission and the claim/route/enqueue loop
// (that continuation lands in a follow-up commit; see the TODO marker
// below, which is a deliberate, visible stopping point, not a silent gap).
//
// Idempotent / redispatchable, same as Python: nothing here assumes this
// is the run's only or first dispatch attempt.
func (service *NativeDispatchSyncRunService) Dispatch(ctx context.Context, args DispatchSyncRunArgs) error {
	if service == nil || service.pool == nil || ctx == nil || args.valid() != nil {
		return ErrDispatchSyncRunUnavailable
	}
	tx, err := service.pool.Begin(ctx)
	if err != nil {
		return ErrDispatchSyncRunUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The outbox-relay route-generation fence every native service already
	// starts with (finalizeWorker/referenceDiscoveryWorker's own first
	// step) -- Go-runtime plumbing with no Python equivalent to port, since
	// Python never had competing route generations to fence against in the
	// same process.
	current, err := currentTransportReference(ctx, tx, args, outboxKindDispatchSyncRun)
	if err != nil {
		return err
	}
	if !current {
		return nil
	}

	run, err := loadFinalizeRun(ctx, tx, args.OrganizationID(), args.SyncRunID())
	if err != nil {
		return err
	}
	if run == nil {
		// Python: `if run is None: ... return {"status": "missing", ...}`.
		service.logger.WarnContext(ctx, "dispatch_sync_run.missing", slog.String("sync_run_id", args.SyncRunID()))
		return tx.Commit(ctx)
	}

	now := service.nowUTC()

	// --- Feature gate (canonical incident ingestion) ---
	// Python: `requires_canonical_feature = sync_run_requires_canonical_incident_feature(session, run)`
	// then `require_canonical_incident_feature_for_update_sync` (which
	// raises on denial). The Go port of the underlying decision
	// (scheduledsync.CanonicalIncidentDecisionForUpdate) already returns
	// (allowed, reason) directly -- no exception to catch, just an if.
	requiresFeature, err := syncRunRequiresCanonicalIncidentFeature(ctx, tx, run.id, run.integrationID)
	if err != nil {
		return err
	}
	if requiresFeature {
		allowed, reason, decisionErr := scheduledsync.CanonicalIncidentDecisionForUpdate(ctx, tx, run.orgID, now)
		if decisionErr != nil {
			return ErrDispatchSyncRunUnavailable
		}
		if !allowed {
			return service.terminalizeFeatureDisabled(ctx, tx, run, reason, now)
		}
	}

	// --- Reference-discovery gate ---
	// Python: `if not reference_discovery_succeeded(session, run_uuid): ...`.
	// dispatch_sync_run gates ALL unit dispatch on this -- reference
	// discovery must complete before any unit is claimed.
	succeeded, err := referenceDiscoverySucceeded(ctx, tx, run.id)
	if err != nil {
		return err
	}
	if !succeeded {
		if err := ensureReferenceDiscoveryWakeup(ctx, tx, run.orgID, run.id, now); err != nil {
			return err
		}
		service.logger.InfoContext(ctx, "dispatch_sync_run.blocked_on_reference_discovery", slog.String("sync_run_id", run.id))
		return tx.Commit(ctx)
	}

	// --- DispatchGuard: authorize this pass ---
	decision, err := authorizeRun(ctx, tx, service.logger, run.orgID, run.id, now)
	if err != nil {
		return err
	}
	if !decision.allowed {
		return service.denyRun(ctx, tx, run, decision, now)
	}
	// Python has a SECOND `if not decision.allowed:` check immediately
	// after this point (a "continuing_after_denial_for_active_units" log
	// line). It is unreachable: the block above returns unconditionally on
	// BOTH of its own branches, so nothing after it can ever observe
	// decision.allowed == false. Confirmed dead code, not ported -- writing
	// an unreachable branch in Go would not be a behavior change either
	// way, only a Go-side accumulation of dead code Python already has.

	// TODO(CHAOS-4175 family 3, next commit): concurrency partial-cap,
	// BudgetGuard.observe_run/enforce_run/reconfirm_cooldowns, provider-
	// family routing + claim + enqueue, and the tail (pending-unit-counts /
	// redispatch / finalize branches). Everything above this point is
	// tested and committed; this return is the explicit, visible stopping
	// point for this slice -- not a silent gap.
	return errDispatchNotYetImplemented
}

// terminalizeFeatureDisabled ports the feature-gate denial branch verbatim:
// terminalizeFeatureDisabledRun decides whether the run went fully
// terminal (transition.RunTerminal), and Dispatch() must pick between the
// SAME two siblings Python picks between -- terminalizeFeatureDisabledGraph
// when it did, armFeatureDisabledFinalize (arm a later finalize check)
// when some units are still genuinely in flight (a normal state at
// dispatch time, unlike reference-discovery's own always-terminal case).
func (service *NativeDispatchSyncRunService) terminalizeFeatureDisabled(
	ctx context.Context, tx pgx.Tx, run *finalizeSyncRun, reason scheduledsync.FeatureDecisionReason, now time.Time,
) error {
	message := canonicalIncidentFeatureDisabledMessage(reason)
	transition, err := terminalizeFeatureDisabledRun(ctx, tx, run, message, now)
	if err != nil {
		return err
	}
	if transition.RunTerminal {
		// run.completedAt is guaranteed non-nil here: terminalizeFeatureDisabledRun
		// sets it whenever it returns RunTerminal true.
		if err := terminalizeFeatureDisabledGraph(ctx, tx, run, message, *run.completedAt); err != nil {
			return err
		}
	} else {
		if _, err := armFeatureDisabledFinalize(ctx, tx, run.orgID, run.id, now); err != nil {
			return err
		}
	}
	service.logger.WarnContext(ctx, "dispatch_sync_run.feature_disabled",
		slog.String("sync_run_id", run.id), slog.String("org_id", run.orgID),
		slog.String("error_category", featureDisabledErrorCategory),
		slog.Int("running_units", transition.RunningUnits))
	return tx.Commit(ctx)
}

// denyRun ports the total-cap hard-deny branch verbatim: two different
// failure shapes depending on whether the run has active (DISPATCHING/
// RUNNING) units. Active units present means the run cannot be
// terminalized outright (something is still in flight), so only the
// never-going-to-dispatch-again units (PLANNED/RETRYING/stale DISPATCHING)
// are failed and a later finalize check is armed. No active units means
// every remaining unit can never legally dispatch again either (the guard
// re-denies every redispatch), so the WHOLE run is failed now -- leaving
// it stranded under a terminal run is invisible to the reconciler (it
// skips terminal runs) and pollutes coverage as a permanent requested-but-
// uncovered window.
func (service *NativeDispatchSyncRunService) denyRun(
	ctx context.Context, tx pgx.Tx, run *finalizeSyncRun, decision guardDecision, now time.Time,
) error {
	errorText := decision.reason
	if errorText == "" {
		errorText = "sync dispatch denied"
	}

	hasActive, err := runHasDispatchingOrRunningUnits(ctx, tx, run.id)
	if err != nil {
		return err
	}
	if hasActive {
		failedPlanned, err := failPlannedUnits(ctx, tx, run.id, errorText, now)
		if err != nil {
			return err
		}
		failedStale, err := failStaleDispatchingUnits(ctx, tx, run.id, errorText, now)
		if err != nil {
			return err
		}
		if err := armDeniedActiveFinalize(ctx, tx, run.id, now); err != nil {
			return err
		}
		service.logger.WarnContext(ctx, "dispatch_sync_run.denied_with_active_units",
			slog.String("sync_run_id", run.id), slog.String("reason", errorText),
			slog.Int("failed_planned_units", failedPlanned), slog.Int("failed_stale_dispatching_units", failedStale))
		return tx.Commit(ctx)
	}

	// No active units: fail the whole run now.
	failedPlanned, err := failPlannedUnits(ctx, tx, run.id, errorText, now)
	if err != nil {
		return err
	}
	totalFailedUnits := run.failedUnits + failedPlanned
	cappedResultJSON, err := json.Marshal(map[string]any{"capped_unit_ids": decision.cappedUnitIDs})
	if err != nil {
		return ErrDispatchSyncRunUnavailable
	}
	if _, err := tx.Exec(ctx, `
UPDATE public.sync_runs
SET status = $2, completed_at = $3, error = $4, failed_units = $5, result = $6::json
WHERE id = $1::uuid`,
		run.id, syncRunStatusFailed, now, errorText, totalFailedUnits, cappedResultJSON); err != nil {
		return ErrDispatchSyncRunUnavailable
	}
	if err := observeTerminalSyncRun(ctx, tx, run, now, syncRunStatusFailed, run.completedUnits, totalFailedUnits,
		map[string]any{}, &errorText); err != nil {
		return err
	}
	service.logger.WarnContext(ctx, "dispatch_sync_run.denied",
		slog.String("sync_run_id", run.id), slog.String("reason", errorText), slog.Int("failed_planned_units", failedPlanned))
	return tx.Commit(ctx)
}
