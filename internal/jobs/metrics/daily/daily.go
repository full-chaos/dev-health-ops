// Package daily owns the dormant, ID-only River boundary for daily metrics.
//
// The compatibility executor is deliberately narrow: it receives a durable
// run/partition identity after this package has reloaded and fenced it from
// PostgreSQL. It cannot receive a command, metric rows, SQL, credentials, or
// caller-selected Python module.
//
// All three kinds deliberately use the registered metrics queue. Celery's
// current all-org fanout is lightweight and uses default, but this dispatcher
// owns durable run/partition publication and must share the same bounded
// ClickHouse-facing queue as its partitions and finalizer. The checked-in route
// remains Celery until this topology and its compatibility executor are fully
// audited.
package daily

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidState = errors.New("daily metrics durable state is invalid")
	ErrLeaseLost    = errors.New("daily metrics execution lease was lost")
	ErrLeaseActive  = errors.New("daily metrics execution lease is still active")
	ErrUnavailable  = errors.New("daily metrics dependency is unavailable")
)

// LeaseActiveError reports that the claim target is held by a lease that has
// not expired yet, and carries how long is left on it.
//
// It exists because "I could not take this lease" and "there is nothing to do"
// are not the same answer, even though one conditional UPDATE reports both as
// zero matched rows. A claimant that reports a live lease as success lets the
// job runtime retire the job -- and that job is the only thing that would have
// come back to reclaim the lease once it expired. The lease is bounded but the
// retry budget that could outlive it is not: it is spent in tens of seconds
// against a ten-minute lease, so an orphaned lease strands its run forever, and
// with it every handoff fenced on that run's completion key.
type LeaseActiveError struct {
	RetryAfter time.Duration
}

func (err *LeaseActiveError) Error() string { return ErrLeaseActive.Error() }
func (err *LeaseActiveError) Unwrap() error { return ErrLeaseActive }

// retryClaim maps a failed claim onto the job runtime. A live lease becomes a
// snooze that wakes when the lease expires, which does not consume an attempt,
// so the reclaim path stays reachable however long the holder takes to die.
func retryClaim(err error) error {
	var active *LeaseActiveError
	if errors.As(err, &active) {
		return jobruntime.RetryableAfter(err, active.RetryAfter)
	}
	return jobruntime.Retryable(err)
}

// retryCompatibilityError marks err Retryable and, when it is one of the
// compatibility bridge's classified sentinels (CHAOS-4264), attaches the
// matching bounded Reason so the River attempt log explains a signaled or
// resource-exhausted runner without anyone having to read Sentry/dmesg.
// Anything else (including the pre-existing ErrUnavailable) is unaffected --
// Retryable with no reason, exactly as before this ticket.
func retryCompatibilityError(err error) error {
	marked := jobruntime.Retryable(err)
	switch {
	case errors.Is(err, ErrCompatibilityProcessSignaled):
		return jobruntime.WithReason(marked, jobruntime.ReasonProcessSignaled)
	case errors.Is(err, ErrCompatibilityResourceExhausted):
		return jobruntime.WithReason(marked, jobruntime.ReasonResourceExhausted)
	case errors.Is(err, ErrCompatibilityAmbiguousRefused):
		return jobruntime.WithReason(marked, jobruntime.ReasonAmbiguousRefused)
	default:
		return marked
	}
}

type Run struct {
	ID             string
	OrganizationID string
	Generation     string
	Status         string
	// RepositoryDiscoveryRequired is true only for the fixed daily fan-out
	// generation while it has no durable partitions. A metrics-queue worker owns the
	// ClickHouse read and resolves this state before it can publish a partition.
	RepositoryDiscoveryRequired bool
}

// StartRunRequest is the immutable post-sync input for one daily generation.
// Repository IDs are server-owned durable references and are partitioned in
// deterministic order by PostgresStore.
type StartRunRequest struct {
	OrganizationID            string
	TargetDay                 time.Time
	Generation                string
	RepositoryIDs             []string
	PrerequisiteCompletionKey string
}

// ScheduledFanoutRequest creates the durable state for the nightly all-org
// fan-out. Repository discovery intentionally happens later in the heavy
// worker: the scheduler owns only the coordinator Postgres transaction and
// must never make a remote ClickHouse read while holding it.
type ScheduledFanoutRequest struct {
	OrganizationID string
	TargetDay      time.Time
	Generation     string
}

type Partition struct {
	ID    string
	RunID string
}

type PartitionClaim struct {
	Partition     Partition
	Token         string
	LeaseDuration time.Duration
}

type FinalizeClaim struct {
	Run           Run
	Token         string
	LeaseDuration time.Duration
}

// Store is the authoritative execution-state boundary. Implementations must
// use bounded leases and fence renew, release, and completion transitions with
// both the current token and a live lease. An expired claimant has lost all
// mutation authority even when no replacement has claimed yet.
type Store interface {
	LoadRun(context.Context, string) (Run, error)
	ClaimDispatch(context.Context, string) (*Run, error)
	DispatchablePartitions(context.Context, string) ([]Partition, error)
	MaterializeScheduledFanout(context.Context, Run, []string) (bool, error)
	ClaimPartition(context.Context, string) (*PartitionClaim, error)
	RenewPartition(context.Context, PartitionClaim) error
	CompletePartition(context.Context, PartitionClaim, Publisher) error
	ReleasePartition(context.Context, PartitionClaim) error
	ClaimFinalize(context.Context, string) (*FinalizeClaim, error)
	RenewFinalize(context.Context, FinalizeClaim) error
	CompleteFinalize(context.Context, FinalizeClaim) error
	ReleaseFinalize(context.Context, FinalizeClaim) error
}

// RepositoryDiscoverer reads the authoritative repository IDs for one
// organization. It is deliberately called only by the heavy worker after the
// scheduler transaction has committed the daily run and dispatch handoff.
type RepositoryDiscoverer interface {
	RepositoryIDs(context.Context, string) ([]string, error)
}

// Publisher persists a child handoff. Its production implementation must use
// the checked-in outbox contract rather than inserting a River job directly.
type Publisher interface {
	PublishPartition(context.Context, Run, Partition) error
	PublishFinalizeTx(context.Context, pgx.Tx, Run) error
}

type RunPublisher interface {
	PublishDispatchTx(context.Context, pgx.Tx, Run, string) error
}

// CompatibilityExecutor is the only temporary Python seam. Both identities
// are loaded from Store before it is called, so it cannot expand the scope.
type CompatibilityExecutor interface {
	ComputePartition(context.Context, Run, Partition) error
	Finalize(context.Context, Run) error
}

type Dispatcher struct {
	store      Store
	publisher  Publisher
	discoverer RepositoryDiscoverer
}

func NewDispatcher(store Store, publisher Publisher, discoverer RepositoryDiscoverer) (*Dispatcher, error) {
	if store == nil || publisher == nil || discoverer == nil {
		return nil, ErrUnavailable
	}
	return &Dispatcher{store: store, publisher: publisher, discoverer: discoverer}, nil
}

func (handler *Dispatcher) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]) error {
	if handler == nil || handler.store == nil || handler.publisher == nil || handler.discoverer == nil || execution == nil {
		return jobruntime.Permanent(ErrUnavailable)
	}
	runID := execution.Args.Payload.RunID
	if execution.Envelope.Domain.ID != runID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	run, err := handler.store.ClaimDispatch(ctx, runID)
	if err != nil {
		if errors.Is(err, ErrInvalidState) {
			return jobruntime.Permanent(err)
		}
		return jobruntime.Retryable(err)
	}
	if run == nil {
		return nil
	}
	if run.ID != runID || run.Status != "running" || execution.OrganizationID == nil || run.OrganizationID != *execution.OrganizationID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	if run.RepositoryDiscoveryRequired {
		repositoryIDs, err := handler.discoverer.RepositoryIDs(ctx, run.OrganizationID)
		if err != nil {
			return jobruntime.Retryable(err)
		}
		if _, err := handler.store.MaterializeScheduledFanout(ctx, *run, repositoryIDs); err != nil {
			if errors.Is(err, ErrInvalidState) {
				return jobruntime.Permanent(err)
			}
			return jobruntime.Retryable(err)
		}
	}
	partitions, err := handler.store.DispatchablePartitions(ctx, runID)
	if err != nil {
		return jobruntime.Retryable(err)
	}
	for _, partition := range partitions {
		if partition.ID == "" || partition.RunID != runID {
			return jobruntime.Permanent(ErrInvalidState)
		}
		if err := handler.publisher.PublishPartition(ctx, *run, partition); err != nil {
			return jobruntime.Retryable(err)
		}
	}
	return nil
}

type PartitionHandler struct {
	store         Store
	publisher     Publisher
	compatibility CompatibilityExecutor
}

func NewPartitionHandler(store Store, publisher Publisher, compatibility CompatibilityExecutor) (*PartitionHandler, error) {
	if store == nil || publisher == nil || compatibility == nil {
		return nil, ErrUnavailable
	}
	return &PartitionHandler{store: store, publisher: publisher, compatibility: compatibility}, nil
}

func (handler *PartitionHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.DailyMetricsPartitionArgs]) error {
	if handler == nil || handler.store == nil || handler.publisher == nil || handler.compatibility == nil || execution == nil {
		return jobruntime.Permanent(ErrUnavailable)
	}
	partitionID := execution.Args.Payload.PartitionID
	if execution.Envelope.Domain.ID != partitionID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	claim, err := handler.store.ClaimPartition(ctx, partitionID)
	if err != nil {
		return retryClaim(err)
	}
	if claim == nil {
		return nil
	}
	run, err := handler.store.LoadRun(ctx, claim.Partition.RunID)
	if err != nil {
		_ = handler.store.ReleasePartition(ctx, *claim)
		if errors.Is(err, ErrInvalidState) {
			return jobruntime.Permanent(err)
		}
		return jobruntime.Retryable(err)
	}
	if claim.Partition.ID != partitionID || run.Status != "running" || execution.OrganizationID == nil || run.OrganizationID != *execution.OrganizationID {
		_ = handler.store.ReleasePartition(ctx, *claim)
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
		releasePartition(handler.store, ctx, *claim)
		return retryCompatibilityError(err)
	}
	if err := handler.store.CompletePartition(ctx, *claim, handler.publisher); err != nil {
		// The one post-claim exit that used to return without releasing. If the
		// completion failed while the lease was still ours, releasing makes the
		// partition immediately re-claimable instead of parking the retry for the
		// rest of the lease; if the lease was already lost the release is a
		// no-op, and the store records that rather than dropping it.
		releasePartition(handler.store, ctx, *claim)
		return jobruntime.Retryable(err)
	}
	return nil
}

type FinalizeHandler struct {
	store         Store
	compatibility CompatibilityExecutor
}

func NewFinalizeHandler(store Store, compatibility CompatibilityExecutor) (*FinalizeHandler, error) {
	if store == nil || compatibility == nil {
		return nil, ErrUnavailable
	}
	return &FinalizeHandler{store: store, compatibility: compatibility}, nil
}

func (handler *FinalizeHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.DailyMetricsFinalizeArgs]) error {
	if handler == nil || handler.store == nil || handler.compatibility == nil || execution == nil {
		return jobruntime.Permanent(ErrUnavailable)
	}
	runID := execution.Args.Payload.RunID
	if execution.Envelope.Domain.ID != runID {
		return jobruntime.Permanent(ErrInvalidState)
	}
	claim, err := handler.store.ClaimFinalize(ctx, runID)
	if err != nil {
		return retryClaim(err)
	}
	if claim == nil {
		return nil
	}
	if execution.OrganizationID == nil || claim.Run.ID != runID || claim.Run.Status != "running" || claim.Run.OrganizationID != *execution.OrganizationID {
		_ = handler.store.ReleaseFinalize(ctx, *claim)
		return jobruntime.Permanent(ErrInvalidState)
	}
	if err := runWithLeaseRenewal(
		ctx,
		claim.LeaseDuration,
		func(renewCtx context.Context) error {
			return handler.store.RenewFinalize(renewCtx, *claim)
		},
		func(workCtx context.Context) error {
			return handler.compatibility.Finalize(workCtx, claim.Run)
		},
	); err != nil {
		releaseFinalize(handler.store, ctx, *claim)
		return retryCompatibilityError(err)
	}
	if err := handler.store.CompleteFinalize(ctx, *claim); err != nil {
		// Symmetric with the partition layer: this exit claimed and returned
		// retryable without releasing, which is the most likely way the lease
		// behind CHAOS-3991 was orphaned in the first place.
		releaseFinalize(handler.store, ctx, *claim)
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

func releasePartition(store Store, ctx context.Context, claim PartitionClaim) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = store.ReleasePartition(releaseCtx, claim)
}

func releaseFinalize(store Store, ctx context.Context, claim FinalizeClaim) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = store.ReleaseFinalize(releaseCtx, claim)
}
