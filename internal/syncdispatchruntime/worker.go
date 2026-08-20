package syncdispatchruntime

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
	"github.com/riverqueue/river"
)

var ErrWorkerRegistration = errors.New("sync dispatch worker registration failed")

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
func RegisterWorkers(workers *river.Workers, bridge CoordinatorBridge, postSync *NativePostSyncService) error {
	if workers == nil || bridge == nil || postSync == nil {
		return ErrWorkerRegistration
	}
	if river.AddWorkerSafely(workers, &dispatchWorker{bridge: bridge}) != nil ||
		river.AddWorkerSafely(workers, &finalizeWorker{bridge: bridge}) != nil ||
		river.AddWorkerSafely(workers, &postSyncWorker{service: postSync}) != nil ||
		river.AddWorkerSafely(workers, &referenceDiscoveryWorker{bridge: bridge}) != nil {
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

// PostSyncFanout is the seam postSyncWorker consumes. NativePostSyncService is
// the only production implementation; the interface exists so the worker's
// error classification is observable without a database.
type PostSyncFanout interface {
	Fanout(context.Context, PostSyncArgs) error
}

type postSyncWorker struct {
	river.WorkerDefaults[PostSyncArgs]
	service PostSyncFanout
}

func (worker *postSyncWorker) Work(ctx context.Context, job *river.Job[PostSyncArgs]) error {
	if worker == nil || worker.service == nil || job == nil {
		return ErrWorkerRegistration
	}
	err := worker.service.Fanout(ctx, job.Args)
	if err != nil && deterministicFanoutRejection(err) {
		return river.JobCancel(err)
	}
	return err
}

// deterministicFanoutRejection reports whether the fanout failed on a verdict
// the checked-in job contract already fixes. Both outbox sentinels are decided
// from contracts/jobs/v1, which jobruntime.Load reads once at process start:
// the descriptor's route, its contract version and the envelope shape are
// identical on every attempt of the same job, so a later attempt reaches the
// same rejection.
//
// This is the coordinator-side equivalent of
// providerunit.deterministicTerminalCategory. Without it a single mis-routed
// publish burned all five attempts, each one rolling back the entire post-sync
// generation, before River discarded the job (CHAOS-3946). Availability and
// transport failures are deliberately NOT listed: those a retry can clear.
func deterministicFanoutRejection(err error) bool {
	return errors.Is(err, joboutbox.ErrPolicyRejected) ||
		errors.Is(err, joboutbox.ErrContractRejected)
}

type referenceDiscoveryWorker struct {
	river.WorkerDefaults[ReferenceDiscoveryArgs]
	bridge CoordinatorBridge
}

type teamAutoimportWorker struct {
	river.WorkerDefaults[TeamAutoimportJobArgs]
	bridge CoordinatorBridge
}

func (worker *teamAutoimportWorker) Work(ctx context.Context, job *river.Job[TeamAutoimportJobArgs]) error {
	if worker == nil || worker.bridge == nil || job == nil {
		return ErrWorkerRegistration
	}
	if err := job.Args.valid(); err != nil {
		return err
	}
	return worker.bridge.TeamAutoImport(ctx, DomainReference{
		OrganizationID: job.Args.OrgID,
		SyncRunID:      job.Args.Payload.SyncRunID,
	})
}

func (worker *referenceDiscoveryWorker) Work(ctx context.Context, job *river.Job[ReferenceDiscoveryArgs]) error {
	if worker == nil || worker.bridge == nil || job == nil {
		return ErrWorkerRegistration
	}
	return worker.bridge.Discover(ctx, job.Args)
}
