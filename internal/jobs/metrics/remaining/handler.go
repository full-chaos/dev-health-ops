package remaining

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining/stepcause"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

type Store interface {
	LoadRun(context.Context, string) (Run, error)
	ClaimPartition(context.Context, string) (*Claim, error)
	RenewPartition(context.Context, Claim) error
	CompletePartition(context.Context, Claim, string) error
	ReleasePartition(context.Context, Claim) error
	// ReleasePartitionTerminally is ReleasePartition's twin for a failure
	// nothing will ever retry: a Permanent one, or a retryable one on the job's
	// last attempt (see PostgresStore.ReleasePartitionTerminally):
	// it stands the partition down to 'failed' AND, in the same transaction,
	// terminalizes the parent run 'failed' if this was its last outstanding
	// partition -- otherwise a run behind a permanently-discarded job lingers
	// status='running' forever.
	ReleasePartitionTerminally(context.Context, Claim) error
	// ExhaustPartition is the no-claim twin: the job's last attempt could not even claim the partition, so no
	// attempt follows. It marks a non-terminal partition exhausted and terminalizes the run if nothing else can
	// progress (CHAOS-8024, invariant row 5).
	ExhaustPartition(context.Context, string) error
}

type PartitionExecutor interface {
	ComputePartition(context.Context, Run, Partition) (CompatibilityOutcome, error)
}

// CompatibilityOutcome reports one partition's actual write result, distinct
// from the plain nil-error "success" ComputePartition used to return. A
// caller that only checks the error can no longer tell a real write apart
// from a reported-zero write; CompletePartition's stored result string is
// built from this so the two are never conflated durably. Named for the
// original HTTP compatibility-bridge executor (now deleted, CHAOS-4291 --
// every remaining-metrics kind computes natively): every native executor
// still returns this same shape, so the name outlived the bridge it was
// coined for.
type CompatibilityOutcome struct {
	RowsWritten *int
}

type PartitionHandler[T jobruntime.ContractArgs] struct {
	store          Store
	executor       PartitionExecutor
	expectedFamily string
}

func NewPartitionHandler[T jobruntime.ContractArgs](
	store Store,
	executor PartitionExecutor,
	expectedFamily string,
) (*PartitionHandler[T], error) {
	var args T
	kind, ok := JobKindForFamily(expectedFamily)
	if store == nil || executor == nil || !ok || args.Kind() != kind {
		return nil, ErrUnavailable
	}
	return &PartitionHandler[T]{
		store: store, executor: executor, expectedFamily: expectedFamily,
	}, nil
}

func (handler *PartitionHandler[T]) Work(
	ctx context.Context,
	execution *jobruntime.Execution[T],
) error {
	if handler == nil || handler.store == nil || handler.executor == nil || execution == nil {
		return jobruntime.Permanent(ErrUnavailable)
	}
	payload, ok := execution.Args.ContractEnvelope().Payload.(jobcontract.RemainingMetricsPartitionPayload)
	if !ok || payload.PartitionID == "" ||
		execution.Envelope.Domain.Type != "remaining_metric_partition" ||
		execution.Envelope.Domain.ID != payload.PartitionID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	var held *Claim
	// A panic past the claim (River recovers it and counts an attempt) must not strand the partition: stand the
	// claim down the same way a returned failure would, then let River see the panic (CHAOS-8024, invariant row 6).
	defer func() {
		if recovered := recover(); recovered != nil {
			if held != nil {
				releaseFailedAttempt(handler.store, ctx, *held, execution)
			}
			panic(recovered)
		}
	}()
	claim, err := handler.store.ClaimPartition(ctx, payload.PartitionID)
	held = claim
	if err != nil {
		// Park until the lease expires rather than burning an attempt on it: a
		// snooze does not consume one, so the reclaim stays reachable however
		// long the current holder takes to die.
		var active *LeaseActiveError
		if errors.As(err, &active) {
			return jobruntime.RetryableAfter(err, active.RetryAfter)
		}
		if finalAttempt(execution) {
			exhaustPartition(handler.store, ctx, payload.PartitionID)
		}
		return jobruntime.Retryable(stepcause.Failure(stepcause.ClaimPartition, err))
	}
	if claim == nil {
		return nil
	}
	run, err := handler.store.LoadRun(ctx, claim.Partition.RunID)
	if err != nil {
		if errors.Is(err, ErrInvalidState) {
			releaseClaim(handler.store, ctx, *claim)
			return jobruntime.Permanent(err)
		}
		releaseFailedAttempt(handler.store, ctx, *claim, execution)
		return jobruntime.Retryable(stepcause.Failure(stepcause.LoadRun, err))
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
			outcome, workErr = handler.executor.ComputePartition(workCtx, run, claim.Partition)
			return workErr
		},
	); err != nil {
		// CHAOS-4242: a ComputePartition failure wrapping ErrInvalidState is
		// a deterministic precondition failure (malformed/empty scope, no
		// organization, an unparseable day, capacity's missing seed) -- the
		// SAME failure on retry 1, 2, and 3, exactly like the LoadRun branch
		// above. Marking it Retryable (as this branch did before) burns the
		// job's whole attempt budget on three identical failures before
		// discarding -- work that accomplishes nothing, on the fast path
		// that is the actual native-executor precondition bug this ticket
		// is about. Anything else here (a ClickHouse/Postgres query error)
		// is genuinely transient and stays Retryable.
		//
		// This branch must use the terminal release: River discards a
		// Permanent job outright, so if this was the run's last outstanding
		// partition, nothing will ever come back to move the run out of
		// 'running' unless the release itself does it (see
		// ReleasePartitionTerminally). A RETRYABLE failure on the job's last
		// attempt needs the same (releaseFailedAttempt below, CHAOS-8024).
		if errors.Is(err, ErrInvalidState) {
			releaseClaimTerminally(handler.store, ctx, *claim)
			return jobruntime.WithReason(jobruntime.Permanent(err), jobruntime.ReasonInvalidState)
		}
		releaseFailedAttempt(handler.store, ctx, *claim, execution)
		// An error the executor already tagged with its exact step keeps that
		// cause: a second, generic compute_partition step on top would repeat
		// the codes and bury the specific step. Only an untagged error gets the
		// handler's own step.
		if _, tagged := jobruntime.SafeCause(err); tagged {
			return jobruntime.Retryable(err)
		}
		return jobruntime.Retryable(stepcause.Failure(stepcause.ComputePartition, err))
	}
	if err := handler.store.CompletePartition(
		ctx,
		*claim,
		compatibilityCompletionResult(claim.Partition.ID, outcome),
	); err != nil {
		// The complete itself failed (the claim is still held): on the job's last attempt stand the partition down
		// terminally so the run cannot be left running behind a discarded job (CHAOS-8024). Earlier attempts keep
		// the claim; its lease expiring is the retry's reclaim.
		if finalAttempt(execution) {
			releaseClaimTerminally(handler.store, ctx, *claim)
		}
		return jobruntime.Retryable(stepcause.Failure(stepcause.CompletePartition, err))
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
	workErr := func() error {
		// A panicking work func must still stop the renewal goroutine, or it keeps renewing the lease of a claim the
		// panic path is about to release.
		defer close(stop)
		return work(workCtx)
	}()
	renewalErr := <-renewalResult
	if renewalErr != nil {
		return renewalErr
	}
	return workErr
}

// finalAttempt reports whether a failure of this execution is the job's LAST attempt: the adapter discards a
// retryable failure once the attempt counter reaches the descriptor's budget (jobruntime/errors.go retryDecision:
// attempt >= maxAttempts), so no later attempt exists to move the run out of 'running'. An execution without a
// declared budget is never final.
func finalAttempt[T jobruntime.ContractArgs](execution *jobruntime.Execution[T]) bool {
	maxAttempts := execution.Definition.MaxAttempts
	return maxAttempts > 0 && execution.Attempt >= maxAttempts
}

// releaseFailedAttempt stands a failed attempt's claim down. A retryable failure keeps the ordinary release (the
// next attempt reclaims the partition); the LAST attempt uses the terminal release, which in the same transaction
// terminalizes the run 'failed' when no other partition is still pending or running. Without it a partition that
// exhausts River's retries left its run 'running' forever behind a discarded job (CHAOS-8024).
func releaseFailedAttempt[T jobruntime.ContractArgs](store Store, ctx context.Context, claim Claim, execution *jobruntime.Execution[T]) {
	if finalAttempt(execution) {
		releaseClaimTerminally(store, ctx, claim)
		return
	}
	releaseClaim(store, ctx, claim)
}

func releaseClaim(store Store, ctx context.Context, claim Claim) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = store.ReleasePartition(releaseCtx, claim)
}

func releaseClaimTerminally(store Store, ctx context.Context, claim Claim) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	err := store.ReleasePartitionTerminally(releaseCtx, claim)
	if err == nil {
		return
	}
	// The terminal release is fenced on a LIVE lease: a claimant whose lease expired (a slow complete, a lost renewal)
	// cannot take it, and no later attempt exists to move the run out of 'running' (CHAOS-8177). Say so, then mark the
	// partition exhausted without the lease predicate (an expired lease is taken, a live one is left alone).
	// Fixed text plus the bounded error class (logging.ErrorArgs, D4317): never the error text.
	slog.WarnContext(ctx, "remaining metrics terminal release failed; exhausting the partition",
		append([]any{"partition_id", claim.Partition.ID}, logging.ErrorArgs(err)...)...)
	exhaustPartition(store, ctx, claim.Partition.ID)
}

func exhaustPartition(store Store, ctx context.Context, partitionID string) {
	exhaustCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := store.ExhaustPartition(exhaustCtx, partitionID); err != nil {
		// Fixed text, no error text: the run may now stay running behind a discarded job (invariant row 5), so the
		// failure must be visible without leaking a driver message.
		slog.WarnContext(ctx, "remaining metrics could not exhaust a last-attempt partition", "partition_id", partitionID)
	}
}
