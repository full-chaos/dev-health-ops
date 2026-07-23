// Package daily owns the dormant, ID-only River boundary for daily metrics.
//
// The compatibility executor is deliberately narrow: it receives a durable
// run/partition identity after this package has reloaded and fenced it from
// PostgreSQL. It cannot receive a command, metric rows, SQL, credentials, or
// caller-selected Python module.
package daily

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

var (
	ErrInvalidState = errors.New("daily metrics durable state is invalid")
	ErrUnavailable  = errors.New("daily metrics dependency is unavailable")
)

type Run struct {
	ID             string
	OrganizationID string
	Generation     string
}

type Partition struct {
	ID    string
	RunID string
}

type PartitionClaim struct {
	Partition Partition
	Token     string
}

type FinalizeClaim struct {
	Run   Run
	Token string
}

// Store is the authoritative execution-state boundary. Implementations must
// use bounded leases and fence all completion transitions with their token.
type Store interface {
	LoadRun(context.Context, string) (Run, error)
	DispatchablePartitions(context.Context, string) ([]Partition, error)
	ClaimPartition(context.Context, string) (*PartitionClaim, error)
	CompletePartition(context.Context, PartitionClaim) error
	ReleasePartition(context.Context, PartitionClaim) error
	ClaimFinalize(context.Context, string) (*FinalizeClaim, error)
	CompleteFinalize(context.Context, FinalizeClaim) error
	ReleaseFinalize(context.Context, FinalizeClaim) error
}

// Publisher persists a child handoff. Its production implementation must use
// the checked-in outbox contract rather than inserting a River job directly.
type Publisher interface {
	PublishPartition(context.Context, Run, Partition) error
	PublishFinalize(context.Context, Run) error
}

// CompatibilityExecutor is the only temporary Python seam. Both identities
// are loaded from Store before it is called, so it cannot expand the scope.
type CompatibilityExecutor interface {
	ComputePartition(context.Context, Run, Partition) error
	Finalize(context.Context, Run) error
}

type Dispatcher struct {
	store     Store
	publisher Publisher
}

func NewDispatcher(store Store, publisher Publisher) (*Dispatcher, error) {
	if store == nil || publisher == nil {
		return nil, ErrUnavailable
	}
	return &Dispatcher{store: store, publisher: publisher}, nil
}

func (handler *Dispatcher) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]) error {
	if handler == nil || handler.store == nil || handler.publisher == nil || execution == nil {
		return jobruntime.Permanent(ErrUnavailable)
	}
	runID := execution.Args.Payload.RunID
	if execution.Envelope.Domain.ID != runID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	run, err := handler.store.LoadRun(ctx, runID)
	if err != nil || run.ID != runID || execution.OrganizationID == nil || run.OrganizationID != *execution.OrganizationID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	partitions, err := handler.store.DispatchablePartitions(ctx, runID)
	if err != nil {
		return jobruntime.Retryable(err)
	}
	for _, partition := range partitions {
		if partition.ID == "" || partition.RunID != runID {
			return jobruntime.Permanent(ErrInvalidState)
		}
		if err := handler.publisher.PublishPartition(ctx, run, partition); err != nil {
			return jobruntime.Retryable(err)
		}
	}
	return nil
}

type PartitionHandler struct {
	store         Store
	compatibility CompatibilityExecutor
}

func NewPartitionHandler(store Store, compatibility CompatibilityExecutor) (*PartitionHandler, error) {
	if store == nil || compatibility == nil {
		return nil, ErrUnavailable
	}
	return &PartitionHandler{store: store, compatibility: compatibility}, nil
}

func (handler *PartitionHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.DailyMetricsPartitionArgs]) error {
	if handler == nil || handler.store == nil || handler.compatibility == nil || execution == nil {
		return jobruntime.Permanent(ErrUnavailable)
	}
	partitionID := execution.Args.Payload.PartitionID
	if execution.Envelope.Domain.ID != partitionID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	claim, err := handler.store.ClaimPartition(ctx, partitionID)
	if err != nil {
		return jobruntime.Retryable(err)
	}
	if claim == nil {
		return nil
	}
	run, err := handler.store.LoadRun(ctx, claim.Partition.RunID)
	if err != nil || claim.Partition.ID != partitionID || execution.OrganizationID == nil || run.OrganizationID != *execution.OrganizationID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	if err := handler.compatibility.ComputePartition(ctx, run, claim.Partition); err != nil {
		_ = handler.store.ReleasePartition(ctx, *claim)
		return jobruntime.Retryable(err)
	}
	if err := handler.store.CompletePartition(ctx, *claim); err != nil {
		return jobruntime.Retryable(err)
	}
	return nil
}

type FinalizeHandler struct {
	store         Store
	publisher     Publisher
	compatibility CompatibilityExecutor
}

func NewFinalizeHandler(store Store, publisher Publisher, compatibility CompatibilityExecutor) (*FinalizeHandler, error) {
	if store == nil || publisher == nil || compatibility == nil {
		return nil, ErrUnavailable
	}
	return &FinalizeHandler{store: store, publisher: publisher, compatibility: compatibility}, nil
}

func (handler *FinalizeHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs]) error {
	if handler == nil || handler.store == nil || handler.publisher == nil || handler.compatibility == nil || execution == nil {
		return jobruntime.Permanent(ErrUnavailable)
	}
	runID := execution.Args.Payload.RunID
	if execution.Envelope.Domain.ID != runID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	claim, err := handler.store.ClaimFinalize(ctx, runID)
	if err != nil {
		return jobruntime.Retryable(err)
	}
	if claim == nil {
		return nil
	}
	if execution.OrganizationID == nil || claim.Run.ID != runID || claim.Run.OrganizationID != *execution.OrganizationID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	if err := handler.compatibility.Finalize(ctx, claim.Run); err != nil {
		_ = handler.store.ReleaseFinalize(ctx, *claim)
		return jobruntime.Retryable(err)
	}
	if err := handler.store.CompleteFinalize(ctx, *claim); err != nil {
		return jobruntime.Retryable(err)
	}
	return nil
}
