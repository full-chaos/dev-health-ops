package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/providerfamilycontract"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	scheduledsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// ErrDispatchSyncRunUnavailable ports the bare execution-failure class every
// sibling native service already has (ErrFinalizeSyncRunUnavailable,
// ErrReferenceDiscoveryUnavailable) -- a query/scan failure this deep in
// Dispatch() is indistinguishable from any other Postgres blip mid-pass,
// classified retryable via River's own retry policy on the job, not a
// permanent verdict about the run.
var ErrDispatchSyncRunUnavailable = errors.New("native dispatch_sync_run is unavailable")

// ErrDispatchProviderUnitRoute ports WorkerJobRouteError for THIS
// producer's two raise sites verbatim: the sync.provider_unit job-kind
// transport is not currently River-owned (resolve_worker_job_route /
// PROVIDER_UNIT_OUTBOX_ROUTES), or a claimed unit fails
// validate_provider_family_claim (a malformed persisted atomic-family
// claim). Python uses the SAME exception class for both -- a route-store
// failure and a malformed claim are both "this producer cannot safely
// stage this job" -- so this stays one sentinel, not two.
var ErrDispatchProviderUnitRoute = errors.New("provider-unit route is unavailable")

// providerUnitOutboxRoutes ports PROVIDER_UNIT_OUTBOX_ROUTES verbatim: the
// durable sync.provider_unit routes under which River owns provider units.
// Deliberately narrower than jobruntime.Descriptor.Executable() (which also
// accepts "shadow") -- Python's own set does not include shadow, and
// jobroute.Controller.ResolveInTx's underlying worker_job_routes table can
// return any of celery/shadow/river_canary/river for THIS kind (unlike the
// four sync-dispatch coordinator kinds, which the checked-in route artifact
// constrains to celery/river only). Using .Executable() here would silently
// admit a shadow-routed claim Python would refuse -- confirmed by reading
// jobroute.allowed()/readState() before writing this, not assumed.
var providerUnitOutboxRoutes = map[string]bool{"river_canary": true, "river": true}

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
	pool            *pgxpool.Pool
	logger          *slog.Logger
	bridge          budgetEstimator
	producer        *joboutbox.Producer
	registry        joboutbox.PolicyRegistry
	routeController *jobroute.Controller
	now             func() time.Time
}

// NewNativeDispatchSyncRunService constructs the native dispatch_sync_run
// executor. bridge is the estimate-only bridge client (CHAOS-4175 ruling):
// the credential-bound half of budget admission (SyncTaskBootstrap.load,
// the six per-provider estimator classes) stays Python-side behind
// /dispatch-budget-estimate; everything else in this service is native.
// producer/registry/routeController are the SAME dependency shapes
// teamAutoimportPostSyncWriter already uses for its own Publish/PublishDeferred
// call, plus the route-fencing Controller every native producer that
// enqueues into River must hold (jobroute.Controller.ResolveInTx's own doc
// comment: "Every native coordinator this ticket ports... must call this").
func NewNativeDispatchSyncRunService(
	pool *pgxpool.Pool,
	logger *slog.Logger,
	bridge budgetEstimator,
	producer *joboutbox.Producer,
	registry joboutbox.PolicyRegistry,
	routeController *jobroute.Controller,
) (*NativeDispatchSyncRunService, error) {
	if pool == nil || bridge == nil || producer == nil || registry == nil || routeController == nil {
		return nil, ErrDispatchSyncRunUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &NativeDispatchSyncRunService{
		pool: pool, logger: logger, bridge: bridge,
		producer: producer, registry: registry, routeController: routeController,
		now: time.Now,
	}, nil
}

func (service *NativeDispatchSyncRunService) nowUTC() time.Time {
	if service.now == nil {
		return time.Now().UTC()
	}
	return service.now().UTC()
}

// Dispatch is the native equivalent of bridge.Dispatch / Python's
// dispatch_sync_run(sync_run_id), ported verbatim end to end
// (workers/sync_units.py:759-1158): the outbox-relay route fence, the
// feature/reference-discovery/DispatchGuard gate chain and total-cap
// denial (in the FIRST transaction), concurrency partial-cap, the full
// BudgetGuard sequence, the sync.provider_unit route fence, the atomic
// claim and per-unit validate+route+enqueue loop, the DISPATCHING status
// flip (all still the SAME first transaction -- team-lead-confirmed
// against origin/main: the claim/route/enqueue/status-flip is one
// Postgres transaction in Python too, closing the producer kill window
// its own comment names), and the tail (a SECOND transaction for
// pending-unit-counts, deciding between a countdown/deferred redispatch
// and arming finalize; scheduleRedispatch and armFinalizeSyncRunWakeup
// each open their own transaction beyond that, matching Python's own
// separate sessions for _schedule_redispatch calls).
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

	// --- Concurrency partial-cap: defer overflow units, proceed with rest ---
	cappedIDs := map[string]bool{}
	if decision.concurrencyCapped && len(decision.cappedUnitIDs) > 0 {
		for _, id := range decision.cappedUnitIDs {
			cappedIDs[id] = true
		}
		service.logger.InfoContext(ctx, "dispatch_sync_run.concurrency_capped",
			slog.String("sync_run_id", run.id), slog.Int("capped_count", len(cappedIDs)), slog.String("reason", decision.reason))
	}

	// --- BudgetGuard dry-run telemetry (side effect only; return discarded,
	// matching Python's own bare `BudgetGuard.observe_run(...)` call) ---
	// Each BudgetGuard call below gets its OWN fresh timestamp, matching
	// Python exactly: none of observe_run/enforce_run/reconfirm_cooldowns
	// receives an explicit now= from Dispatch() in Python, so each defaults
	// to its own datetime.now(timezone.utc) at its own call time rather
	// than sharing one value captured earlier in this function.
	if _, err := observeRun(ctx, tx, service.bridge, service.logger, run.orgID, run.id, cappedIDs, service.nowUTC()); err != nil {
		return err
	}

	enforcedAt := service.nowUTC()
	budgetResult, err := enforceRun(ctx, tx, service.bridge, service.logger, run.orgID, run.id, cappedIDs, decision.slotHeadroom, enforcedAt)
	if err != nil {
		return err
	}
	for id := range budgetResult.deferredUnitIDs {
		cappedIDs[id] = true
	}

	// CHAOS-2760 TOCTOU closure: reconfirm_cooldowns' surplusPromotedAt MUST
	// be enforceRun's OWN `enforcedAt` (not this call's own fresh now) --
	// that is the timestamp _admit_unit_from_surplus actually wrote as
	// available_at for any surplus-admitted unit in budgetResult.candidateUnits.
	reconfirmResult, err := reconfirmCooldowns(ctx, tx, service.logger, run.id, budgetResult.candidateUnits, budgetResult.estimatesByUnit,
		cappedIDs, budgetResult.jitterSeconds, budgetResult.surplusPriorAvailableAt, enforcedAt, service.nowUTC())
	if err != nil {
		return err
	}
	for id := range reconfirmResult.excludedUnitIDs {
		cappedIDs[id] = true
	}

	nextDeferredAt := budgetResult.nextDeferredAt
	if reconfirmResult.nextDeferredAt != nil && (nextDeferredAt == nil || reconfirmResult.nextDeferredAt.Before(*nextDeferredAt)) {
		nextDeferredAt = reconfirmResult.nextDeferredAt
	}

	// --- sync.provider_unit route resolution, fenced in THIS transaction so
	// a rollback racing this commit is always observed (jobroute.Controller.
	// ResolveInTx's own doc comment) ---
	route, err := service.routeController.ResolveInTx(ctx, tx, jobcontract.KindSyncProviderUnit)
	if err != nil {
		return ErrDispatchProviderUnitRoute
	}
	if !providerUnitOutboxRoutes[route] {
		// CHAOS-4054 step 4 deleted the Celery dispatch plane: there is no
		// second runtime left to fall through to, so a non-river route here
		// is a fail-closed route fault, never a Celery dispatch.
		return ErrDispatchProviderUnitRoute
	}

	// --- Atomic claim: fresh PLANNED, due RETRYING, reclaimed stale
	// DISPATCHING, excluding every id capped/deferred/excluded above ---
	claimedUnits, err := claimUnits(ctx, tx, run.id, cappedIDs, service.nowUTC())
	if err != nil {
		return err
	}

	riverQueued := 0
	var unroutableUnits []budgetUnit
	for _, unit := range claimedUnits {
		// Atomic provider families are admitted before transport selection,
		// so a malformed claim can reach neither River nor a stranded state.
		if err := providerfamilycontract.ValidateClaim(unit.provider, unit.datasetKey, unit.processorFlags, true); err != nil {
			return ErrDispatchProviderUnitRoute
		}
		// Routability is decided per pair, not per run, and the capability
		// matrix is its only source: a pair is executable when the matrix
		// marks it route-ready AND plannable (the canonical writer identity
		// of its family). Ported as Python has it (routes_to_river checks
		// both), even though for every atomic family currently declared,
		// RouteReady-true-but-Plannable-false is unreachable HERE: those are
		// exactly the non-canonical family aliases ValidateClaim above
		// already refuses first. Kept for fidelity and as a real guard
		// against the day a dataset is RouteReady in providersync without a
		// matching providerfamilycontract policy entry -- a gap between the
		// two registries this check alone would catch.
		descriptor, ok := providersync.Descriptor(unit.provider, unit.datasetKey)
		if ok && descriptor.RouteReady && descriptor.Plannable {
			organizationID := unit.orgID
			envelope := jobcontract.Envelope{
				ContractVersion: jobcontract.ContractVersionV1,
				OrganizationID:  &organizationID,
				CorrelationID:   "sync-run:" + run.id,
				IdempotencyKey:  jobcontract.KindSyncProviderUnit + ":" + unit.id,
				Domain:          jobcontract.DomainLink{Type: "sync_run_unit", ID: unit.id},
				Payload:         jobcontract.ProviderUnitPayload{UnitID: unit.id},
			}
			if err := service.producer.Publish(ctx, tx, jobcontract.KindSyncProviderUnit, envelope); err != nil {
				return err
			}
			riverQueued++
			continue
		}
		// The matrix does not route this pair and there is no fallback
		// runtime to publish it to. Terminalize instead of leaving it
		// wedged (CHAOS-3990).
		unroutableUnits = append(unroutableUnits, unit)
	}

	if len(unroutableUnits) > 0 {
		terminalizeNow := service.nowUTC()
		terminalized, err := terminalizeUnroutableUnits(ctx, tx, unroutableUnits, terminalizeNow)
		if err != nil {
			return err
		}
		service.logger.WarnContext(ctx, "dispatch_sync_run.unroutable_units_terminalized",
			slog.String("sync_run_id", run.id), slog.Int("unroutable_units", terminalized),
			slog.String("error_category", featureDisabledErrorCategory))
	}

	if riverQueued > 0 {
		writeNow := service.nowUTC()
		if _, err := tx.Exec(ctx, `
UPDATE public.sync_runs
SET status = $2, started_at = COALESCE(started_at, $3)
WHERE id = $1::uuid`,
			run.id, syncRunStatusDispatching, writeNow); err != nil {
			return ErrDispatchSyncRunUnavailable
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return ErrDispatchSyncRunUnavailable
	}

	if riverQueued > 0 {
		if nextDeferredAt != nil {
			scheduleRedispatch(ctx, service.pool, service.logger, run.id, nextDeferredAt, service.nowUTC())
		} else if len(cappedIDs) > 0 {
			scheduleRedispatch(ctx, service.pool, service.logger, run.id, nil, service.nowUTC())
		}
		service.logger.InfoContext(ctx, "dispatch_sync_run.dispatched",
			slog.String("sync_run_id", run.id), slog.Int("queued_units", riverQueued))
		return nil
	}

	// --- Tail: river_queued == 0 -- nothing was claimable this pass.
	// SECOND, separate transaction (Python's own second
	// `with get_postgres_session_sync() as session:` block, opened only
	// after the first one above already committed).
	tailTx, err := service.pool.Begin(ctx)
	if err != nil {
		return ErrDispatchSyncRunUnavailable
	}
	counts, err := computePendingUnitCounts(ctx, tailTx, run.id, service.nowUTC())
	if err != nil {
		_ = tailTx.Rollback(ctx)
		return err
	}
	if err := tailTx.Commit(ctx); err != nil {
		return ErrDispatchSyncRunUnavailable
	}

	// a) Deferred work remains dispatchable now (a PLANNED unit, or a due
	// RETRYING/stale-DISPATCHING one) -> countdown redispatch so it drains
	// once slots free up. scheduleRedispatch's own exception-swallowing
	// means the try/except Python wraps THIS call in can never actually
	// fire (matching the dead-code precedent already found and NOT ported
	// elsewhere in this function) -- nothing to replicate beyond the call
	// itself.
	if counts.dispatchable > 0 {
		scheduleRedispatch(ctx, service.pool, service.logger, run.id, nil, service.nowUTC())
		service.logger.InfoContext(ctx, "dispatch_sync_run.noop",
			slog.String("sync_run_id", run.id), slog.Int("queued_units", 0), slog.Int("pending_units", counts.dispatchable))
		return nil
	}
	// b) Nothing dispatchable, but something is genuinely in flight
	// (DISPATCHING not yet stale, or RUNNING) -- wait for it; no wakeup to
	// arm, this pass's job here is done.
	if counts.inFlight > 0 {
		service.logger.InfoContext(ctx, "dispatch_sync_run.waiting_inflight",
			slog.String("sync_run_id", run.id), slog.Int("queued_units", 0), slog.Int("in_flight_units", counts.inFlight))
		return nil
	}
	// c) Nothing dispatchable or in flight, but a RETRYING unit has a
	// future available_at -- arm a redispatch for exactly that time rather
	// than the default countdown, so a long backoff is honored instead of
	// being polled early for nothing.
	if counts.nextDeferredAt != nil {
		scheduleRedispatch(ctx, service.pool, service.logger, run.id, counts.nextDeferredAt, service.nowUTC())
		service.logger.InfoContext(ctx, "dispatch_sync_run.deferred",
			slog.String("sync_run_id", run.id), slog.Int("queued_units", 0), slog.Time("next_deferred_at", *counts.nextDeferredAt))
		return nil
	}
	// d) No pending work of any kind (a zero-unit run, or every unit
	// already terminal) -- arm finalize (idempotent; handles both the
	// zero-unit and already-finalized cases the same way
	// finalize_sync_run's own entry logic already does). A redispatch here
	// would loop forever with nothing left to ever claim.
	service.logger.InfoContext(ctx, "dispatch_sync_run.noop_finalize", slog.String("sync_run_id", run.id), slog.Int("queued_units", 0))
	finalizeTx, err := service.pool.Begin(ctx)
	if err != nil {
		return ErrDispatchSyncRunUnavailable
	}
	defer func() { _ = finalizeTx.Rollback(ctx) }()
	if err := armFinalizeSyncRunWakeup(ctx, finalizeTx, run.id, service.nowUTC()); err != nil {
		return err
	}
	if err := finalizeTx.Commit(ctx); err != nil {
		return ErrDispatchSyncRunUnavailable
	}
	return nil
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
		if err := armFinalizeSyncRunWakeup(ctx, tx, run.id, now); err != nil {
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
