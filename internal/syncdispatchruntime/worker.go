package syncdispatchruntime

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
	"github.com/riverqueue/river"
)

var ErrWorkerRegistration = errors.New("sync dispatch worker registration failed")

// RegisterWorkers adds all four guarded at-least-once coordinator consumers.
// Each worker carries only a durable domain reference and delegates execution
// through the authenticated compatibility boundary.
func RegisterWorkers(workers *river.Workers, bridge CoordinatorBridge) error {
	if workers == nil || bridge == nil {
		return ErrWorkerRegistration
	}
	if river.AddWorkerSafely(workers, &dispatchWorker{bridge: bridge}) != nil ||
		river.AddWorkerSafely(workers, &finalizeWorker{bridge: bridge}) != nil ||
		river.AddWorkerSafely(workers, &postSyncWorker{bridge: bridge}) != nil ||
		river.AddWorkerSafely(workers, &referenceDiscoveryWorker{bridge: bridge}) != nil {
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

func (worker *dispatchWorker) Work(ctx context.Context, job *river.Job[DispatchSyncRunArgs]) error {
	if worker == nil || worker.bridge == nil || job == nil {
		return ErrWorkerRegistration
	}
	return worker.bridge.Dispatch(ctx, job.Args)
}

type finalizeWorker struct {
	river.WorkerDefaults[FinalizeSyncRunArgs]
	bridge CoordinatorBridge
}

func (worker *finalizeWorker) Work(ctx context.Context, job *river.Job[FinalizeSyncRunArgs]) error {
	if worker == nil || worker.bridge == nil || job == nil {
		return ErrWorkerRegistration
	}
	return worker.bridge.Finalize(ctx, job.Args)
}

type postSyncWorker struct {
	river.WorkerDefaults[PostSyncArgs]
	bridge CoordinatorBridge
}

func (worker *postSyncWorker) Work(ctx context.Context, job *river.Job[PostSyncArgs]) error {
	if worker == nil || worker.bridge == nil || job == nil {
		return ErrWorkerRegistration
	}
	return worker.bridge.PostSync(ctx, job.Args)
}

type referenceDiscoveryWorker struct {
	river.WorkerDefaults[ReferenceDiscoveryArgs]
	bridge CoordinatorBridge
}

func (worker *referenceDiscoveryWorker) Work(ctx context.Context, job *river.Job[ReferenceDiscoveryArgs]) error {
	if worker == nil || worker.bridge == nil || job == nil {
		return ErrWorkerRegistration
	}
	return worker.bridge.Discover(ctx, job.Args)
}
