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
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
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

// ErrDispatchProviderUnitRoute is RETIRED (CHAOS-4550, 2026-08-29). It used
// to port Python's WorkerJobRouteError for a claimed unit that fails
// validate_provider_family_claim (a malformed persisted atomic-family
// claim): Dispatch `return`ed this sentinel straight out of the claim loop,
// which -- because claimUnits' own claim runs in the SAME transaction --
// aborted the entire pass on the deferred tx.Rollback and reclaimed the
// identical unit next redispatch, forever (live discard storm: one stale
// pre-CHAOS-4054 work-items claim, 548+ delivery attempts / 17 days).
// Dispatch no longer returns it: a ValidateClaim failure now terminalizes
// just the offending unit (invalidClaimUnit / terminalizeInvalidClaimUnits,
// dispatch_denial.go), the same CHAOS-3990 idiom unroutableUnits already
// uses, instead of failing the whole pass. Nothing in this package returns
// this sentinel anymore; kept only as a comment marker, not a var, so a
// stray external `errors.Is` match fails loudly instead of silently
// matching an error path that no longer exists.

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
	pool     *pgxpool.Pool
	logger   *slog.Logger
	bridge   budgetEstimator
	producer *joboutbox.Producer
	registry joboutbox.PolicyRegistry
	observer jobruntime.BudgetEstimateFailureObserver
	now      func() time.Time
}

// NewNativeDispatchSyncRunService constructs the native dispatch_sync_run
// executor. bridge is the estimate-only bridge client (CHAOS-4175 ruling):
// the credential-bound half of budget admission (SyncTaskBootstrap.load,
// the six per-provider estimator classes) stays Python-side behind
// /dispatch-budget-estimate; everything else in this service is native.
// producer/registry are the SAME dependency shapes teamAutoimportPostSyncWriter
// already uses for its own Publish/PublishDeferred call. There is
// deliberately no jobroute.Controller here (CHAOS-4175 ruling, see Dispatch's
// doc comment): this service's transaction runs on the domain role, which
// has no grant on worker_job_routes.
// observers follows NewNativeFinalizeSyncRunService's own variadic-observer
// convention: at most the first is used, and it is optional -- a caller
// that doesn't care about budget-estimate-failure telemetry (a unit test,
// say) can omit it entirely rather than threading a stub through.
func NewNativeDispatchSyncRunService(
	pool *pgxpool.Pool,
	logger *slog.Logger,
	bridge budgetEstimator,
	producer *joboutbox.Producer,
	registry joboutbox.PolicyRegistry,
	observers ...jobruntime.BudgetEstimateFailureObserver,
) (*NativeDispatchSyncRunService, error) {
	if pool == nil || bridge == nil || producer == nil || registry == nil {
		return nil, ErrDispatchSyncRunUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	var observer jobruntime.BudgetEstimateFailureObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &NativeDispatchSyncRunService{
		pool: pool, logger: logger, bridge: bridge,
		producer: producer, registry: registry, observer: observer,
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
// BudgetGuard sequence, the atomic claim and per-unit validate+enqueue
// loop, the DISPATCHING status flip (all still the SAME first transaction
// -- team-lead-confirmed against origin/main: the claim/enqueue/
// status-flip is one Postgres transaction in Python too, closing the
// producer kill window its own comment names), and the tail (a SECOND
// transaction for pending-unit-counts, deciding between a countdown/
// deferred redispatch and arming finalize; scheduleRedispatch and
// armFinalizeSyncRunWakeup each open their own transaction beyond that,
// matching Python's own separate sessions for _schedule_redispatch calls).
//
// RULING REVERSAL (CHAOS-4175, team-lead, superseding an earlier ruling in
// this same ticket): Dispatch does NOT resolve sync.provider_unit's route
// via jobroute.Controller.ResolveInTx before enqueueing, even though that
// is what Python's resolve_worker_job_route does and even though an
// earlier ruling on this branch said every native producer must call
// ResolveInTx in its own write transaction. Reversed because the domain
// role this transaction runs under has NO grant on worker_job_routes at
// all (internal/storage/postgres/domain_authorization.go's domainPosture:
// "worker_job_routes ... moved OUT of this posture entirely ... attributes
// [it] exclusively to the coordinator role") and dev-health-worker (the
// binary that hosts this service) never opts into a coordinator pool
// (cmd/dev-health-worker/dependencies.go's openWorkerDatabase never calls
// RuntimeConfig.WithCoordinator). A tx-scoped route read here would 42501
// in production; it only looked safe in this package's own integration
// tests because those fixtures run one ungated role against one pool.
//
// The route-pause/rollback race ResolveInTx exists to close is closed by a
// DIFFERENT mechanism on the Go side, with the same guarantee moved to a
// different lock: service.producer.Publish below stages into
// worker_job_outbox under an INSERT (domain has that grant -- it is a
// three-role table), and jobroute.Controller.Rollback takes `LOCK TABLE
// public.worker_job_outbox IN SHARE ROW EXCLUSIVE MODE` (control.go:197).
// An uncommitted Publish holds ROW EXCLUSIVE on that same table, which
// blocks a concurrent Rollback's SHARE ROW EXCLUSIVE until this
// transaction commits or aborts -- Python fenced via a lock on the route
// row itself; Go fences via a lock on the outbox table the enqueue itself
// touches. Route resolution for sync.provider_unit lives ONLY at drain,
// in the relay (internal/joboutbox's relay step calls routes.Resolve per
// claim, via a jobroute.Controller constructed on the COORDINATOR pool --
// cmd/dev-health-reconciler/dependencies.go's jobroute.NewController(
// coordinatorPool, ...) -- not the queue-control pool the relay's own
// outbox claim/delivery runs on; codex round 3 caught this file's own
// doc comment stating the wrong role): a celery-routed claim is deferred back,
// a paused route stalls the relay step. No native producer on origin/main
// calls the route controller either -- this mirrors
// teamAutoimportPostSyncWriter's existing Publish/PublishDeferred
// precedent, which this service already otherwise follows.
//
// Classification-table divergence this ruling introduces: Python refuses
// eagerly at the producer when sync.provider_unit's route is celery or
// paused (WorkerJobRouteError, sync_units.py:987-1022, raised before any
// enqueue). Go stages the unit into the outbox unconditionally and lets
// the relay decide at drain time -- a celery route defers the claim back
// (redispatchable, not lost), a paused route stalls the relay step (no
// unit visibly stuck in a Go-only error state; the run's units sit
// DISPATCHING until the pause lifts or rolls back). This is accepted, not
// a gap: production has no Celery consumer left for this kind, and the
// relay is the sole route authority by construction (on the coordinator
// pool, for the route read specifically -- see above).
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
	// raises on denial, and LOCKS feature_flags/org_feature_overrides --
	// sync_units.py:804). Go deliberately calls the NON-locking
	// scheduledsync.CanonicalIncidentDecision here instead (CHAOS-4209
	// ruling): this transaction runs on the domain role, which has SELECT
	// only on those two tables (they are coordinator-exclusive), so a FOR
	// UPDATE lock 42501s -- the same class CHAOS-4209 found in family 2's
	// claim(). This is a recorded, deliberate divergence from Python's own
	// locking call at this exact site, not an oversight: a stale
	// non-locked read here is caught by feature_disabled_termination's own
	// re-check at materialization time (CanonicalIncidentAllowedForUpdate/
	// CanonicalIncidentDecisionForUpdate, which DO lock, on the coordinator
	// role), so the entitlement decision this gate makes is never the last
	// word either way.
	requiresFeature, err := syncRunRequiresCanonicalIncidentFeature(ctx, tx, run.id, run.integrationID)
	if err != nil {
		return err
	}
	if requiresFeature {
		allowed, reason, decisionErr := scheduledsync.CanonicalIncidentDecision(ctx, tx, run.orgID, now)
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
	budgetResult, err := enforceRun(ctx, tx, service.bridge, service.logger, run.orgID, run.id, cappedIDs, decision.slotHeadroom, enforcedAt, service.observer)
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

	// --- Atomic claim: fresh PLANNED, due RETRYING, reclaimed stale
	// DISPATCHING, excluding every id capped/deferred/excluded above ---
	claimedUnits, err := claimUnits(ctx, tx, run.id, cappedIDs, service.nowUTC())
	if err != nil {
		return err
	}

	riverQueued := 0
	var unroutableUnits []budgetUnit
	var invalidClaimUnits []invalidClaimUnit
	for _, unit := range claimedUnits {
		// Atomic provider families are admitted before transport selection,
		// so a malformed claim can reach neither River nor a stranded state.
		//
		// CHAOS-4550 (route-unavailable discard storm, live 2026-08-29):
		// this must NEVER abort the whole pass. claimUnits' own claim runs
		// in THIS SAME transaction, so returning an error here rolls
		// EVERYTHING back (the deferred tx.Rollback undoes the claim too) --
		// the identical poisoned unit is reclaimed and refused again on the
		// very next redispatch, forever, taking every OTHER unit in the run
		// down with it each time (observed live: sync_run
		// f02b38f3-4018-4029-8fed-8aa8f0a0264d, 548+ delivery attempts / 17
		// days, one stale pre-CHAOS-4054 work-items claim missing its
		// sibling family_dataset_* flags). Terminalize just this unit
		// instead -- same CHAOS-3990 idiom the unroutableUnits branch below
		// already uses, just with its own error_category so an operator can
		// tell "claim was corrupt/stale" apart from "no route exists".
		if err := providerfamilycontract.ValidateClaim(unit.provider, unit.datasetKey, unit.processorFlags, true); err != nil {
			invalidClaimUnits = append(invalidClaimUnits, invalidClaimUnit{
				unit:   unit,
				reason: invalidClaimReason(unit.provider, unit.datasetKey, err),
			})
			continue
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

	if len(invalidClaimUnits) > 0 {
		terminalizeNow := service.nowUTC()
		terminalizedInvalid, err := terminalizeInvalidClaimUnits(ctx, tx, invalidClaimUnits, terminalizeNow)
		if err != nil {
			return err
		}
		service.logger.WarnContext(ctx, "dispatch_sync_run.invalid_claim_units_terminalized",
			slog.String("sync_run_id", run.id), slog.Int("invalid_claim_units", terminalizedInvalid),
			slog.String("error_category", invalidProviderFamilyClaimErrorCategory))
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
