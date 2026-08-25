package remaining

import (
	"context"
	"errors"
	"fmt"
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
	ComputePartition(context.Context, Run, Partition) (CompatibilityOutcome, error)
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
		// Park until the lease expires rather than burning an attempt on it: a
		// snooze does not consume one, so the reclaim stays reachable however
		// long the current holder takes to die.
		var active *LeaseActiveError
		if errors.As(err, &active) {
			return jobruntime.RetryableAfter(err, active.RetryAfter)
		}
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
	var outcome CompatibilityOutcome
	if err := runWithLeaseRenewal(
		ctx,
		claim.LeaseDuration,
		func(renewCtx context.Context) error {
			return handler.store.RenewPartition(renewCtx, *claim)
		},
		func(workCtx context.Context) error {
			var workErr error
			outcome, workErr = handler.compatibility.ComputePartition(workCtx, run, claim.Partition)
			return workErr
		},
	); err != nil {
		releaseClaim(handler.store, ctx, *claim)
		// CHAOS-4242: a ComputePartition failure wrapping ErrInvalidState is
		// a deterministic precondition failure (malformed/empty scope, no
		// organization, an unparseable day, capacity's missing seed) -- the
		// SAME failure on retry 1, 2, and 3, exactly like the LoadRun branch
		// above. Marking it Retryable (as this branch did before) burns the
		// job's whole attempt budget on three identical failures before
		// discarding -- work that accomplishes nothing, on the fast path
		// that is the actual native-executor precondition bug this ticket
		// is about. Anything else here (a ClickHouse/Postgres query error,
		// a compatibility-bridge HTTP failure) is genuinely transient and
		// stays Retryable.
		if errors.Is(err, ErrInvalidState) {
			return jobruntime.WithReason(jobruntime.Permanent(err), jobruntime.ReasonInvalidState)
		}
		return jobruntime.Retryable(err)
	}
	if err := handler.store.CompletePartition(
		ctx,
		*claim,
		compatibilityCompletionResult(claim.Partition.ID, outcome),
	); err != nil {
		return jobruntime.Retryable(err)
	}
	return nil
}

// compatibilityCompletionResult builds the durable output_evidence string.
// A reported rows_written (including an explicit zero) is embedded so a
// zero-row completion is never stored identically to a real write --
// CHAOS-4243: "the job must report rows_written=0 distinctly, never plain
// success." RowsWritten == nil (not applicable for this family) keeps the
// original unqualified format.
func compatibilityCompletionResult(partitionID string, outcome CompatibilityOutcome) string {
	if outcome.RowsWritten == nil {
		return "compatibility_execution:" + partitionID
	}
	return fmt.Sprintf("compatibility_execution:%s:rows_written=%d", partitionID, *outcome.RowsWritten)
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
