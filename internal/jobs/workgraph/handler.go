package workgraph

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

type handler struct {
	store         Store
	compatibility CompatibilityExecutor
	// preSteps run before the bridge, in order, inside the same lease
	// renewal. Only KindBuild carries any today; see NativePreStep.
	preSteps []NativePreStep
	// postSteps run AFTER the bridge, in order, inside the same lease renewal.
	// A step belongs here rather than in preSteps when the Python stage would
	// OVERWRITE what it writes; see NativePostStep.
	postSteps []NativePostStep
}

func newHandler(
	store Store, compatibility CompatibilityExecutor,
	preSteps []NativePreStep, postSteps []NativePostStep,
) (*handler, error) {
	if store == nil || compatibility == nil {
		return nil, ErrUnavailable
	}
	for _, step := range preSteps {
		// A nil step would be a wiring bug that silently skips ported compute,
		// which is exactly the failure this seam exists to prevent.
		if step == nil {
			return nil, ErrUnavailable
		}
	}
	for _, step := range postSteps {
		// Same reasoning, slightly sharper: a skipped POST-step leaves the
		// bridge's own values in place, so the build succeeds with rows that
		// look right and carry the pre-port policy.
		if step == nil {
			return nil, ErrUnavailable
		}
	}
	return &handler{
		store: store, compatibility: compatibility,
		preSteps: preSteps, postSteps: postSteps,
	}, nil
}

func (handler *handler) work(ctx context.Context, requestID string, kind Kind, organizationID *string, domain jobcontract.DomainLink) error {
	if handler == nil || handler.store == nil || handler.compatibility == nil || !validUUID(requestID) ||
		organizationID == nil || domain.ID != requestID || domain.Type != domainFor(kind) {
		return jobruntime.Permanent(ErrInvalidState)
	}
	claim, err := handler.store.Claim(ctx, requestID, kind)
	if err != nil {
		if errors.Is(err, ErrInvalidState) {
			return jobruntime.Permanent(err)
		}
		// Park until the lease expires instead of burning an attempt on it. A
		// snooze does not consume one, so the reclaim stays reachable however
		// long the current holder takes to die.
		var active *LeaseActiveError
		if errors.As(err, &active) {
			return jobruntime.RetryableAfter(err, active.RetryAfter)
		}
		return jobruntime.Retryable(err)
	}
	if claim == nil { // a completed request is an idempotent success.
		return nil
	}
	if claim.Request.OrganizationID != *organizationID || claim.Request.ID != requestID || claim.Request.Kind != kind {
		_ = releaseAmbiguous(handler.store, ctx, *claim, "claimed request no longer matches River envelope")
		return jobruntime.Permanent(ErrInvalidState)
	}
	// The pre-steps and the bridge share ONE lease renewal: the lease has to
	// cover the whole execution, and a step that ran under an expired lease
	// would be writing outside its fence.
	evidence, err := runWithLeaseRenewal(ctx, claim.LeaseDuration,
		func(renewCtx context.Context) error { return handler.store.Renew(renewCtx, *claim) },
		func(workCtx context.Context) ([]byte, error) {
			fragments, preStepErr := runPreSteps(workCtx, handler.preSteps, *claim)
			if preStepErr != nil {
				return nil, preStepErr
			}
			executed, executeErr := handler.compatibility.Execute(workCtx, *claim)
			if executeErr != nil {
				return nil, executeErr
			}
			// Post-steps run AFTER the bridge because the bridge OVERWRITES
			// what they write; see NativePostStep. A failure here fails the
			// build: the rows exist but carry Python's values, which is a wrong
			// answer that looks healthy rather than a missing one.
			postFragments, postStepErr := runPostSteps(workCtx, handler.postSteps, *claim)
			if postStepErr != nil {
				return nil, postStepErr
			}
			return mergePreStepEvidence(executed, mergeStepFragments(fragments, postFragments)), nil
		},
	)
	if err != nil {
		if errors.Is(err, ErrLeaseLost) {
			return jobruntime.Retryable(err)
		}
		_ = releaseAmbiguous(handler.store, ctx, *claim, "compatibility execution outcome is unknown")
		return jobruntime.Permanent(err)
	}
	if err := handler.store.Complete(ctx, *claim, evidence); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			return jobruntime.Retryable(err)
		}
		return jobruntime.Retryable(err)
	}
	return nil
}

func releaseAmbiguous(store Store, ctx context.Context, claim Claim, detail string) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return store.Ambiguous(releaseCtx, claim, detail)
}

func runWithLeaseRenewal(ctx context.Context, lease time.Duration, renew func(context.Context) error, work func(context.Context) ([]byte, error)) ([]byte, error) {
	if ctx == nil || lease < 3*time.Millisecond || renew == nil || work == nil {
		return nil, ErrInvalidState
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan struct{})
	renewed := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				renewed <- nil
				return
			case <-ctx.Done():
				cancel()
				renewed <- ctx.Err()
				return
			case <-ticker.C:
				if err := renew(ctx); err != nil {
					cancel()
					renewed <- err
					return
				}
			}
		}
	}()
	evidence, workErr := work(workCtx)
	close(stop)
	if renewalErr := <-renewed; renewalErr != nil {
		return nil, renewalErr
	}
	return evidence, workErr
}

type BuildHandler struct{ *handler }
type MaterializeHandler struct{ *handler }
type DispatchHandler struct{ *handler }
type ChunkHandler struct{ *handler }
type FinalizeHandler struct{ *handler }

// NewBuildHandler builds the workgraph.build handler. preSteps are native Go
// producers that run before the Python bridge, in the order given; see
// NativePreStep for why they live inside this execution rather than beside it.
func NewBuildHandler(
	store Store, executor CompatibilityExecutor,
	preSteps []NativePreStep, postSteps []NativePostStep,
) (*BuildHandler, error) {
	h, err := newHandler(store, executor, preSteps, postSteps)
	return &BuildHandler{h}, err
}
func NewMaterializeHandler(store Store, executor CompatibilityExecutor) (*MaterializeHandler, error) {
	h, err := newHandler(store, executor, nil, nil)
	return &MaterializeHandler{h}, err
}
func NewDispatchHandler(store Store, executor CompatibilityExecutor) (*DispatchHandler, error) {
	h, err := newHandler(store, executor, nil, nil)
	return &DispatchHandler{h}, err
}
func NewChunkHandler(store Store, executor CompatibilityExecutor) (*ChunkHandler, error) {
	h, err := newHandler(store, executor, nil, nil)
	return &ChunkHandler{h}, err
}
func NewFinalizeHandler(store Store, executor CompatibilityExecutor) (*FinalizeHandler, error) {
	h, err := newHandler(store, executor, nil, nil)
	return &FinalizeHandler{h}, err
}

func (h *BuildHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.WorkGraphBuildArgs]) error {
	if execution == nil {
		return jobruntime.Permanent(ErrInvalidState)
	}
	return h.work(ctx, execution.Args.Payload.RequestID, KindBuild, execution.OrganizationID, execution.Envelope.Domain)
}
func (h *MaterializeHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.InvestmentMaterializeArgs]) error {
	if execution == nil {
		return jobruntime.Permanent(ErrInvalidState)
	}
	return h.work(ctx, execution.Args.Payload.RequestID, KindMaterialize, execution.OrganizationID, execution.Envelope.Domain)
}
func (h *DispatchHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.InvestmentDispatchArgs]) error {
	if execution == nil {
		return jobruntime.Permanent(ErrInvalidState)
	}
	return h.work(ctx, execution.Args.Payload.RequestID, KindDispatch, execution.OrganizationID, execution.Envelope.Domain)
}
func (h *ChunkHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.InvestmentChunkArgs]) error {
	if execution == nil {
		return jobruntime.Permanent(ErrInvalidState)
	}
	return h.work(ctx, execution.Args.Payload.ChunkID, KindChunk, execution.OrganizationID, execution.Envelope.Domain)
}
func (h *FinalizeHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.InvestmentFinalizeArgs]) error {
	if execution == nil {
		return jobruntime.Permanent(ErrInvalidState)
	}
	return h.work(ctx, execution.Args.Payload.RunID, KindFinalize, execution.OrganizationID, execution.Envelope.Domain)
}
