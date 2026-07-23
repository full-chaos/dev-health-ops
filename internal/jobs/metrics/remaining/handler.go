package remaining

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

type Store interface {
	LoadRun(context.Context, string) (Run, error)
	ClaimPartition(context.Context, string) (*Claim, error)
	RenewPartition(context.Context, Claim) error
	CompletePartition(context.Context, Claim, string) error
	ReleasePartition(context.Context, Claim) error
}

type CompatibilityExecutor interface {
	ComputePartition(context.Context, Run, Partition) error
}

type PartitionHandler[T jobruntime.ContractArgs] struct {
	store          Store
	compatibility  CompatibilityExecutor
	expectedFamily string
}

func NewPartitionHandler[T jobruntime.ContractArgs](
	store Store,
	compatibility CompatibilityExecutor,
	expectedFamily string,
) (*PartitionHandler[T], error) {
	var args T
	kind, ok := JobKindForFamily(expectedFamily)
	if store == nil || compatibility == nil || !ok || args.Kind() != kind {
		return nil, ErrUnavailable
	}
	return &PartitionHandler[T]{
		store: store, compatibility: compatibility, expectedFamily: expectedFamily,
	}, nil
}

func (handler *PartitionHandler[T]) Work(
	ctx context.Context,
	execution *jobruntime.Execution[T],
) error {
	if handler == nil || handler.store == nil || handler.compatibility == nil || execution == nil {
		return jobruntime.Permanent(ErrUnavailable)
	}
	payload, ok := execution.Args.ContractEnvelope().Payload.(jobcontract.RemainingMetricsPartitionPayload)
	if !ok || payload.PartitionID == "" ||
		execution.Envelope.Domain.Type != "remaining_metric_partition" ||
		execution.Envelope.Domain.ID != payload.PartitionID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	claim, err := handler.store.ClaimPartition(ctx, payload.PartitionID)
	if err != nil {
		return jobruntime.Retryable(err)
	}
	if claim == nil {
		return nil
	}
	run, err := handler.store.LoadRun(ctx, claim.Partition.RunID)
	if err != nil {
		releaseClaim(handler.store, ctx, *claim)
		if errors.Is(err, ErrInvalidState) {
			return jobruntime.Permanent(err)
		}
		return jobruntime.Retryable(err)
	}
	if claim.Partition.ID != payload.PartitionID ||
		run.ID != claim.Partition.RunID || run.Status != "running" ||
		run.Family != handler.expectedFamily ||
		execution.OrganizationID == nil || run.OrganizationID != *execution.OrganizationID {
		releaseClaim(handler.store, ctx, *claim)
		return jobruntime.Permanent(ErrInvalidState)
	}
	if err := runWithLeaseRenewal(
		ctx,
		claim.LeaseDuration,
		func(renewCtx context.Context) error {
			return handler.store.RenewPartition(renewCtx, *claim)
		},
		func(workCtx context.Context) error {
			return handler.compatibility.ComputePartition(workCtx, run, claim.Partition)
		},
	); err != nil {
		releaseClaim(handler.store, ctx, *claim)
		return jobruntime.Retryable(err)
	}
	if err := handler.store.CompletePartition(
		ctx,
		*claim,
		"compatibility_execution:"+claim.Partition.ID,
	); err != nil {
		return jobruntime.Retryable(err)
	}
	return nil
}

func runWithLeaseRenewal(
	ctx context.Context,
	leaseDuration time.Duration,
	renew func(context.Context) error,
	work func(context.Context) error,
) error {
	if ctx == nil || leaseDuration < 3*time.Millisecond || renew == nil || work == nil {
		return ErrInvalidState
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	stop := make(chan struct{})
	renewalResult := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(leaseDuration / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				renewalResult <- nil
				return
			case <-ctx.Done():
				cancelWork()
				renewalResult <- ctx.Err()
				return
			case <-ticker.C:
				if err := renew(ctx); err != nil {
					cancelWork()
					renewalResult <- err
					return
				}
			}
		}
	}()
	workErr := work(workCtx)
	close(stop)
	renewalErr := <-renewalResult
	if renewalErr != nil {
		return renewalErr
	}
	return workErr
}

func releaseClaim(store Store, ctx context.Context, claim Claim) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = store.ReleasePartition(releaseCtx, claim)
}
