package syncdispatchruntime

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var ErrWorkerRegistration = errors.New("sync dispatch worker registration failed")

// coordinatorTracerName scopes this package's spans.
//
// otel.Tracer is looked up fresh on every call rather than cached in a
// package var: see internal/jobruntime.jobTracerName for why a cached handle
// silently stays bound to the first TracerProvider it ever saw.
const coordinatorTracerName = "github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"

// spanForCoordinatorJob opens the span for one coordinator dispatch, parented
// from traceParent when the run planner captured one (CHAOS-3996) -- resolved
// from sync_runs by the same database lookup that resolves the domain
// reference itself, since TransportArgs carries only typed fields and has no
// arbitrary JSON payload to decode a parent out of the way
// internal/jobruntime.startJobSpan does for the outbox path. An empty
// traceParent (a run planned before CHAOS-3996, or with tracing disabled)
// produces a root span, same as before this ticket.
func spanForCoordinatorJob(ctx context.Context, kind, syncRunID, traceParent string) (context.Context, oteltrace.Span) {
	if traceParent != "" {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": traceParent})
	}
	return otel.Tracer(coordinatorTracerName).Start(ctx, "dev_health.sync_dispatch."+kind, oteltrace.WithAttributes(
		attribute.String("dev_health.job.kind", kind),
		attribute.String("dev_health.sync_run_id", syncRunID),
	))
}

func finishCoordinatorSpan(span oteltrace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

// RegisterWorkers adds all four guarded at-least-once coordinator consumers.
// Each worker carries only a durable domain reference and delegates execution
// through the authenticated compatibility boundary.
// RegisterWorkers registers the sync-dispatch coordinator kinds. These are
// syncdispatchcontract kinds outside the bounded job registry, so they carry no
// registry descriptor and are not subject to registry startup validation;
// CUT-10 brings them under registered kinds.
//
// sync.team_autoimport is deliberately NOT registered here. It is a bounded
// registry kind, and a registry kind may only be consumed when its durable
// route permits River execution. Registering it unconditionally alongside these
// coordinators gave the sync worker a live consumer for a Celery-routed kind
// that no readiness check could observe, because this client is private to the
// coordinator and never reported its handlers. Use
// RegisterTeamAutoimportWorker so the caller owns both the route decision and
// reporting the constructed spec through the canonical capability channel.
func RegisterWorkers(
	workers *river.Workers,
	bridge CoordinatorBridge,
	postSync *NativePostSyncService,
	finalizeSyncRun *NativeFinalizeSyncRunService,
	referenceDiscovery *NativeReferenceDiscoveryService,
) error {
	if workers == nil || bridge == nil || postSync == nil || finalizeSyncRun == nil || referenceDiscovery == nil {
		return ErrWorkerRegistration
	}
	if river.AddWorkerSafely(workers, &dispatchWorker{bridge: bridge}) != nil ||
		river.AddWorkerSafely(workers, &finalizeWorker{service: finalizeSyncRun}) != nil ||
		river.AddWorkerSafely(workers, &postSyncWorker{service: postSync}) != nil ||
		river.AddWorkerSafely(workers, &referenceDiscoveryWorker{service: referenceDiscovery}) != nil {
		return ErrWorkerRegistration
	}
	return nil
}

// RegisterTeamAutoimportWorker registers the one bounded-registry kind this
// runtime hosts. The caller must first prove the kind is executable and must
// report the constructed handler spec to startup validation, so capability is
// observable no matter which River client hosts the worker.
func RegisterTeamAutoimportWorker(workers *river.Workers, bridge CoordinatorBridge) error {
	if workers == nil || bridge == nil {
		return ErrWorkerRegistration
	}
	if river.AddWorkerSafely(workers, &teamAutoimportWorker{bridge: bridge}) != nil {
		return ErrWorkerRegistration
	}
	return nil
}

// RouteCapabilities is the exact River surface registered by this runtime.
func RouteCapabilities() []syncroute.Capability {
	return []syncroute.Capability{
		{Kind: syncdispatchcontract.KindDispatchSyncRun, Transport: syncdispatchcontract.RouteRiver},
		{Kind: syncdispatchcontract.KindFinalizeSyncRun, Transport: syncdispatchcontract.RouteRiver},
		{Kind: syncdispatchcontract.KindPostSync, Transport: syncdispatchcontract.RouteRiver},
		{Kind: syncdispatchcontract.KindReferenceDiscovery, Transport: syncdispatchcontract.RouteRiver},
	}
}

type dispatchWorker struct {
	river.WorkerDefaults[DispatchSyncRunArgs]
	bridge CoordinatorBridge
}

func (worker *dispatchWorker) Work(ctx context.Context, job *river.Job[DispatchSyncRunArgs]) (err error) {
	if worker == nil || worker.bridge == nil || job == nil {
		return ErrWorkerRegistration
	}
	ctx, span := spanForCoordinatorJob(ctx, job.Args.Kind(), job.Args.SyncRunID(), job.Args.TraceParent)
	// A deferred finish (not a direct call after Dispatch returns) so a panic
	// from worker.bridge still ends and exports the span before the panic
	// continues propagating to River's own panic-to-failure handling; this
	// does not recover the panic, only observes it.
	defer func() { finishCoordinatorSpan(span, err) }()
	err = worker.bridge.Dispatch(ctx, job.Args)
	return err
}

type finalizeWorker struct {
	river.WorkerDefaults[FinalizeSyncRunArgs]
	service *NativeFinalizeSyncRunService
}

func (worker *finalizeWorker) Work(ctx context.Context, job *river.Job[FinalizeSyncRunArgs]) (err error) {
	if worker == nil || worker.service == nil || job == nil {
		return ErrWorkerRegistration
	}
	ctx, span := spanForCoordinatorJob(ctx, job.Args.Kind(), job.Args.SyncRunID(), job.Args.TraceParent)
	defer func() { finishCoordinatorSpan(span, err) }()
	err = worker.service.Finalize(ctx, job.Args)
	return err
}

type postSyncWorker struct {
	river.WorkerDefaults[PostSyncArgs]
	service *NativePostSyncService
}

func (worker *postSyncWorker) Work(ctx context.Context, job *river.Job[PostSyncArgs]) (err error) {
	if worker == nil || worker.service == nil || job == nil {
		return ErrWorkerRegistration
	}
	ctx, span := spanForCoordinatorJob(ctx, job.Args.Kind(), job.Args.SyncRunID(), job.Args.TraceParent)
	defer func() { finishCoordinatorSpan(span, err) }()
	err = worker.service.Fanout(ctx, job.Args)
	return err
}

type referenceDiscoveryWorker struct {
	river.WorkerDefaults[ReferenceDiscoveryArgs]
	service *NativeReferenceDiscoveryService
}

type teamAutoimportWorker struct {
	river.WorkerDefaults[TeamAutoimportJobArgs]
	bridge CoordinatorBridge
}

func (worker *teamAutoimportWorker) Work(ctx context.Context, job *river.Job[TeamAutoimportJobArgs]) (err error) {
	if worker == nil || worker.bridge == nil || job == nil {
		return ErrWorkerRegistration
	}
	if err := job.Args.valid(); err != nil {
		return err
	}
	// TeamAutoimportJobArgs does not embed TransportArgs (it is the generic
	// worker-outbox envelope, not a coordinator TransportArgs kind), so there
	// is no TraceParent field to propagate here -- out of this ticket's scope.
	ctx, span := spanForCoordinatorJob(ctx, job.Args.Kind(), job.Args.Payload.SyncRunID, "")
	defer func() { finishCoordinatorSpan(span, err) }()
	err = worker.bridge.TeamAutoImport(ctx, DomainReference{
		OrganizationID: job.Args.OrgID,
		SyncRunID:      job.Args.Payload.SyncRunID,
	})
	return err
}

func (worker *referenceDiscoveryWorker) Work(ctx context.Context, job *river.Job[ReferenceDiscoveryArgs]) (err error) {
	if worker == nil || worker.service == nil || job == nil {
		return ErrWorkerRegistration
	}
	ctx, span := spanForCoordinatorJob(ctx, job.Args.Kind(), job.Args.SyncRunID(), job.Args.TraceParent)
	defer func() { finishCoordinatorSpan(span, err) }()
	err = worker.service.Discover(ctx, job.Args)
	return err
}
