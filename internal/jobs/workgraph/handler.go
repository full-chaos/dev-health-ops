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
}

func newHandler(store Store, compatibility CompatibilityExecutor) (*handler, error) {
	if store == nil || compatibility == nil {
		return nil, ErrUnavailable
	}
	return &handler{store: store, compatibility: compatibility}, nil
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
		return jobruntime.Retryable(err)
	}
	if claim == nil { // a completed request is an idempotent success.
		return nil
	}
	if claim.Request.OrganizationID != *organizationID || claim.Request.ID != requestID || claim.Request.Kind != kind {
		_ = releaseAmbiguous(handler.store, ctx, *claim, "claimed request no longer matches River envelope")
		return jobruntime.Permanent(ErrInvalidState)
	}
	evidence, err := runWithLeaseRenewal(ctx, claim.LeaseDuration,
		func(renewCtx context.Context) error { return handler.store.Renew(renewCtx, *claim) },
		func(workCtx context.Context) ([]byte, error) { return handler.compatibility.Execute(workCtx, *claim) },
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

func NewBuildHandler(store Store, executor CompatibilityExecutor) (*BuildHandler, error) {
	h, err := newHandler(store, executor)
	return &BuildHandler{h}, err
}
func NewMaterializeHandler(store Store, executor CompatibilityExecutor) (*MaterializeHandler, error) {
	h, err := newHandler(store, executor)
	return &MaterializeHandler{h}, err
}
func NewDispatchHandler(store Store, executor CompatibilityExecutor) (*DispatchHandler, error) {
	h, err := newHandler(store, executor)
	return &DispatchHandler{h}, err
}
func NewChunkHandler(store Store, executor CompatibilityExecutor) (*ChunkHandler, error) {
	h, err := newHandler(store, executor)
	return &ChunkHandler{h}, err
}
func NewFinalizeHandler(store Store, executor CompatibilityExecutor) (*FinalizeHandler, error) {
	h, err := newHandler(store, executor)
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
