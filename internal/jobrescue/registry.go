// Package jobrescue makes River's process-global maintenance election safe for
// a fleet whose execution clients intentionally consume disjoint queues.
package jobrescue

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/riverqueue/river"
)

var (
	ErrInvalidConfiguration     = errors.New("River rescue registry configuration is invalid")
	ErrRescueOnlyWorkerExecuted = errors.New("rescue-only River worker reached execution")
)

type rescueOnlyWorker[T river.JobArgs] struct {
	river.WorkerDefaults[T]
	descriptor jobruntime.Descriptor
}

func (worker rescueOnlyWorker[T]) Timeout(*river.Job[T]) time.Duration {
	return worker.descriptor.Timeout
}

func (worker rescueOnlyWorker[T]) NextRetry(job *river.Job[T]) time.Time {
	if job == nil {
		return time.Time{}
	}
	return jobruntime.NextRetryAt(worker.descriptor, job.JobRow)
}

func (rescueOnlyWorker[T]) Work(context.Context, *river.Job[T]) error {
	// A rescue-only worker is registered for maintenance type information, not
	// execution. Queue separation prevents this path in a valid deployment. If
	// a kind is inserted onto the wrong queue, cancel it rather than executing a
	// domain effect in the wrong worker family.
	return river.JobCancel(ErrRescueOnlyWorkerExecuted)
}

// RegisterMissingWorkers adds type-only workers for every kind that this
// client does not execute. River elects one maintenance leader across all
// clients sharing a schema, and its JobRescuer consults only that leader's
// worker registry. A partial registry therefore turns another queue's stuck
// job into an "unhandled" discard. Complete rescue coverage lets the elected
// client apply the real kind's timeout and retry policy without consuming the
// other queue.
//
// ownedKinds must be the kinds already registered with workers. The returned
// sorted coverage is suitable for startup tests and mutation guards.
func RegisterMissingWorkers(
	workers *river.Workers,
	registry *jobruntime.Registry,
	ownedKinds []string,
) ([]string, error) {
	if workers == nil || registry == nil {
		return nil, ErrInvalidConfiguration
	}
	descriptors := registry.Descriptors()
	descriptorByKind := make(map[string]jobruntime.Descriptor, len(descriptors))
	for _, descriptor := range descriptors {
		descriptorByKind[descriptor.Kind] = descriptor
	}
	required := make(map[string]struct{}, len(descriptors)+4)
	for _, descriptor := range descriptors {
		required[descriptor.Kind] = struct{}{}
	}
	for _, kind := range coordinatorKinds() {
		required[kind] = struct{}{}
	}
	owned := make(map[string]struct{}, len(ownedKinds))
	for _, kind := range ownedKinds {
		if _, known := required[kind]; !known {
			return nil, fmt.Errorf("%w: unknown owned kind %q", ErrInvalidConfiguration, kind)
		}
		if _, duplicate := owned[kind]; duplicate {
			return nil, fmt.Errorf("%w: duplicate owned kind %q", ErrInvalidConfiguration, kind)
		}
		owned[kind] = struct{}{}
	}

	coverage := make([]string, 0, len(required))
	for kind := range required {
		coverage = append(coverage, kind)
		if _, isOwned := owned[kind]; isOwned {
			continue
		}
		descriptor := jobruntime.Descriptor{Kind: kind}
		if configured, ok := descriptorByKind[kind]; ok {
			descriptor = configured
		}
		if err := registerKind(workers, kind, descriptor); err != nil {
			return nil, fmt.Errorf("%w: register %s: %v", ErrInvalidConfiguration, kind, err)
		}
	}
	sort.Strings(coverage)
	return coverage, nil
}

func coordinatorKinds() []string {
	return []string{
		syncdispatchcontract.KindDispatchSyncRun,
		syncdispatchcontract.KindFinalizeSyncRun,
		syncdispatchcontract.KindPostSync,
		syncdispatchcontract.KindReferenceDiscovery,
	}
}

func registerKind(workers *river.Workers, kind string, descriptor jobruntime.Descriptor) error {
	switch kind {
	case jobcontract.KindInvestmentChunk:
		return add[jobruntime.InvestmentChunkArgs](workers, descriptor)
	case jobcontract.KindInvestmentDispatch:
		return add[jobruntime.InvestmentDispatchArgs](workers, descriptor)
	case jobcontract.KindInvestmentFinalize:
		return add[jobruntime.InvestmentFinalizeArgs](workers, descriptor)
	case jobcontract.KindInvestmentMaterialize:
		return add[jobruntime.InvestmentMaterializeArgs](workers, descriptor)
	case jobcontract.KindDailyMetricsDispatch:
		return add[jobruntime.DailyMetricsDispatchArgs](workers, descriptor)
	case jobcontract.KindDailyMetricsFinalize:
		return add[jobruntime.DailyMetricsFinalizeArgs](workers, descriptor)
	case jobcontract.KindDailyMetricsPartition:
		return add[jobruntime.DailyMetricsPartitionArgs](workers, descriptor)
	case jobcontract.KindRemainingCapacity:
		return add[jobruntime.RemainingCapacityArgs](workers, descriptor)
	case jobcontract.KindRemainingComplexity:
		return add[jobruntime.RemainingComplexityArgs](workers, descriptor)
	case jobcontract.KindRemainingDORA:
		return add[jobruntime.RemainingDORAArgs](workers, descriptor)
	case jobcontract.KindRemainingMembership:
		return add[jobruntime.RemainingMembershipArgs](workers, descriptor)
	case jobcontract.KindRemainingRecommendations:
		return add[jobruntime.RemainingRecommendationsArgs](workers, descriptor)
	case jobcontract.KindRemainingReleaseImpact:
		return add[jobruntime.RemainingReleaseImpactArgs](workers, descriptor)
	case jobcontract.KindBillingNotification:
		return add[jobruntime.BillingNotificationArgs](workers, descriptor)
	case jobcontract.KindWebhookDelivery:
		return add[jobruntime.WebhookDeliveryArgs](workers, descriptor)
	case jobcontract.KindReportExecuteOnDemand:
		return add[jobruntime.OnDemandReportExecutionArgs](workers, descriptor)
	case jobcontract.KindReportExecuteScheduled:
		return add[jobruntime.ScheduledReportExecutionArgs](workers, descriptor)
	case jobcontract.KindSyncProviderUnit:
		return add[jobruntime.ProviderUnitArgs](workers, descriptor)
	case jobcontract.KindTeamAutoimport:
		return add[syncdispatchruntime.TeamAutoimportJobArgs](workers, descriptor)
	case jobcontract.KindTeamRepoOwnershipDerivation:
		return add[syncdispatchruntime.TeamRepoOwnershipDerivationJobArgs](workers, descriptor)
	case jobcontract.KindHeartbeat:
		return add[jobruntime.HeartbeatArgs](workers, descriptor)
	case jobcontract.KindRetentionCleanup:
		return add[jobruntime.RetentionCleanupArgs](workers, descriptor)
	case jobcontract.KindSyncCoverageRefresh:
		return add[jobruntime.SyncCoverageRefreshArgs](workers, descriptor)
	case jobcontract.KindWorkGraphBuild:
		return add[jobruntime.WorkGraphBuildArgs](workers, descriptor)
	case syncdispatchcontract.KindDispatchSyncRun:
		return add[syncdispatchruntime.DispatchSyncRunArgs](workers, descriptor)
	case syncdispatchcontract.KindFinalizeSyncRun:
		return add[syncdispatchruntime.FinalizeSyncRunArgs](workers, descriptor)
	case syncdispatchcontract.KindPostSync:
		return add[syncdispatchruntime.PostSyncArgs](workers, descriptor)
	case syncdispatchcontract.KindReferenceDiscovery:
		return add[syncdispatchruntime.ReferenceDiscoveryArgs](workers, descriptor)
	default:
		return fmt.Errorf("unsupported kind %q", kind)
	}
}

func add[T river.JobArgs](workers *river.Workers, descriptor jobruntime.Descriptor) error {
	return river.AddWorkerSafely(workers, rescueOnlyWorker[T]{descriptor: descriptor})
}
