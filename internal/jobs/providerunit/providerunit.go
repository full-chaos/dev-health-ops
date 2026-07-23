// Package providerunit adapts the authoritative SyncRunUnit lease to the
// generic River runtime. River arguments carry only the unit identifier.
package providerunit

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/google/uuid"
)

var ErrUnavailable = errors.New("provider unit runtime is unavailable")

type ExecutorFactory func(
	*providersync.LeaseSession,
) (providersync.CompleteRouteExecutor, error)

type UnitRepository interface {
	providersync.LeaseRepository
	Complete(
		context.Context,
		providersync.Claim,
		map[string]any,
		*time.Time,
		time.Time,
		time.Time,
	) error
	ReleaseForRetry(context.Context, providersync.Claim, time.Time) error
	Fail(
		context.Context,
		providersync.Claim,
		string,
		time.Time,
		time.Time,
	) error
}

type Handler struct {
	Repository    UnitRepository
	Switches      providersync.CompleteRouteSwitches
	BuildExecutor ExecutorFactory
	LeaseDuration time.Duration
	Heartbeat     time.Duration
	Now           func() time.Time
}

func (handler *Handler) now() time.Time {
	if handler.Now != nil {
		return handler.Now().UTC()
	}
	return time.Now().UTC()
}

func (handler *Handler) Work(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.ProviderUnitArgs],
) error {
	if handler == nil || handler.Repository == nil || handler.BuildExecutor == nil ||
		handler.LeaseDuration < time.Second || handler.LeaseDuration > 15*time.Minute ||
		handler.Heartbeat <= 0 || handler.Heartbeat > handler.LeaseDuration/2 ||
		execution == nil || execution.OrganizationID == nil ||
		execution.Envelope.Domain.Type != "sync_run_unit" ||
		execution.Args.Payload.UnitID == "" ||
		execution.Args.Payload.UnitID != execution.Envelope.Domain.ID {
		return jobruntime.DomainMismatch(ErrUnavailable)
	}
	startedAt := handler.now()
	claim, err := handler.Repository.Claim(ctx, providersync.ClaimRequest{
		UnitID:               execution.Args.Payload.UnitID,
		OrgID:                *execution.OrganizationID,
		Owner:                uuid.NewString(),
		Now:                  startedAt,
		LeaseDuration:        handler.LeaseDuration,
		AllowExpiredRecovery: true,
	})
	if err != nil {
		if errors.Is(err, providersync.ErrUnitNotClaimable) {
			return jobruntime.Retryable(err)
		}
		return jobruntime.Permanent(err)
	}
	descriptor, ok := handler.Switches.Descriptor(claim.Provider, claim.Dataset)
	if !ok || !descriptor.RouteReady || !descriptor.RouteEnabled {
		_ = handler.Repository.Fail(
			context.WithoutCancel(ctx), claim, "route_disabled",
			startedAt, handler.now(),
		)
		return jobruntime.Permanent(ErrUnavailable)
	}
	session := &providersync.LeaseSession{
		Repository: handler.Repository,
		Claim:      claim, LeaseDuration: handler.LeaseDuration,
		Deadline: execution.Deadline, Now: handler.Now,
	}
	executor, err := handler.BuildExecutor(session)
	if err == nil {
		var result providersync.CompleteRouteExecutionResult
		result, err = executor.Execute(ctx, session, descriptor)
		if err == nil {
			payload := cloneResult(result.Result)
			payload["go_provider_route"] = map[string]any{
				"effects_written": result.Effects.Written,
				"effects_skipped": result.Effects.Skipped,
				"records":         result.Comparison.NativeRecords,
			}
			if completeErr := handler.Repository.Complete(
				context.WithoutCancel(ctx), session.Claim, payload,
				result.Watermark, startedAt, handler.now(),
			); completeErr != nil {
				err = completeErr
			} else {
				return nil
			}
		}
	}
	completedAt := handler.now()
	if execution.Attempt >= execution.Definition.MaxAttempts {
		_ = handler.Repository.Fail(
			context.WithoutCancel(ctx), session.Claim, "provider_unit_exhausted",
			startedAt, completedAt,
		)
		return jobruntime.Retryable(err)
	}
	if releaseErr := handler.Repository.ReleaseForRetry(
		context.WithoutCancel(ctx), session.Claim, completedAt,
	); releaseErr != nil {
		return jobruntime.Retryable(releaseErr)
	}
	return jobruntime.Retryable(err)
}

func cloneResult(input map[string]any) map[string]any {
	result := make(map[string]any, len(input)+1)
	for key, value := range input {
		result[key] = value
	}
	return result
}

// AuthoritativeIdempotency delegates duplicate/retry decisions to the
// SyncRunUnit CAS and effect ledger instead of introducing a second lease.
type AuthoritativeIdempotency struct{}

func (AuthoritativeIdempotency) Supports(policy string) bool {
	return policy == "sync_run_unit"
}

func (AuthoritativeIdempotency) Begin(
	_ context.Context,
	request jobruntime.ClaimRequest,
) (jobruntime.IdempotencyClaim, error) {
	if request.Kind != jobcontract.KindSyncProviderUnit ||
		request.Policy != "sync_run_unit" ||
		request.Domain.Type != "sync_run_unit" ||
		request.Domain.ID == "" || request.OrganizationID == nil {
		return nil, ErrUnavailable
	}
	return authoritativeClaim{}, nil
}

type authoritativeClaim struct{}

func (authoritativeClaim) State() jobruntime.ClaimState {
	return jobruntime.ClaimProceed
}

func (authoritativeClaim) Finish(context.Context, jobruntime.Completion) error {
	return nil
}

var _ jobruntime.Handler[jobruntime.ProviderUnitArgs] = (*Handler)(nil)
var _ jobruntime.Idempotency = AuthoritativeIdempotency{}
