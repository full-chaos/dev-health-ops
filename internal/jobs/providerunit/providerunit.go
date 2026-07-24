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

// ErrRouteReconciliationRequired reports a unit that reached River for a scope
// the Go capability system does not route. That is a producer-gate fault, not
// a property of the unit, so it must never terminalize the unit.
var ErrRouteReconciliationRequired = errors.New(
	"provider unit route is unavailable for this scope; producer gate and Go descriptor disagree",
)

// RouteReconciliationCategory is the durable, distinguishable failure category
// recorded when a route fault survives every River attempt. It is deliberately
// not "route_disabled": that category meant "this scope is unsupported, drop
// it", while this one means "a producer routed a scope Go does not serve and a
// human must reconcile the gate".
const RouteReconciliationCategory = "route_reconciliation_required"

// RouteFault describes a producer/consumer capability disagreement for
// alerting. It carries no credentials, URLs, or provider payloads.
type RouteFault struct {
	Provider          string
	Dataset           string
	DescriptorPresent bool
	RouteReady        bool
	RouteEnabled      bool
	Attempt           int
	MaxAttempts       int
	// Released reports whether the claim was handed back to dispatching. When
	// false the unit is still leased and will recover through lease expiry, or
	// has been recorded as reconciliation-required.
	Released bool
	// Terminal reports that River has no attempt left, so the unit was moved to
	// the durable reconciliation-required state instead of being released.
	Terminal bool
}

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
	// OnRouteFault receives producer/consumer capability disagreements so a
	// binary can alert on them. A nil hook keeps the fail-safe behavior; it
	// never converts the fault into success.
	OnRouteFault func(RouteFault)
}

func (handler *Handler) observeRouteFault(fault RouteFault) {
	if handler == nil || handler.OnRouteFault == nil {
		return
	}
	handler.OnRouteFault(fault)
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
		// TRD non-negotiable #3: a provider unit delivered to River must never
		// terminalize as route_disabled. A unit only reaches here when the
		// Python producer gate routed a scope the Go descriptor does not
		// serve, so the sync data is real and the gate is wrong. Terminalizing
		// would silently discard that scope's data for the whole run. Instead
		// hand the claim back to dispatching, alert, and fail retryably so the
		// unit stays recoverable by the Celery route or the reconciler.
		fault := RouteFault{
			Provider: claim.Provider, Dataset: claim.Dataset,
			DescriptorPresent: ok, RouteReady: descriptor.RouteReady,
			RouteEnabled: descriptor.RouteEnabled,
			Attempt:      execution.Attempt,
			MaxAttempts:  execution.Definition.MaxAttempts,
		}
		// Releasing on the terminal attempt would strand the unit: River
		// discards the job after the last attempt, leaving the unit
		// `dispatching` with no live consumer, and the producer outbox dedupe
		// row makes a stale redispatch report "queued" without enqueueing
		// anything. The sync run would then never finalize. Record an explicit
		// durable reconciliation-required state instead — terminal and
		// alertable, but never the silent route_disabled drop.
		if execution.Attempt >= execution.Definition.MaxAttempts {
			fault.Terminal = true
			handler.observeRouteFault(fault)
			if failErr := handler.Repository.Fail(
				context.WithoutCancel(ctx), claim, RouteReconciliationCategory,
				startedAt, handler.now(),
			); failErr != nil {
				return jobruntime.Retryable(failErr)
			}
			return jobruntime.Retryable(ErrRouteReconciliationRequired)
		}
		releaseErr := handler.Repository.ReleaseForRetry(
			context.WithoutCancel(ctx), claim, handler.now(),
		)
		fault.Released = releaseErr == nil
		handler.observeRouteFault(fault)
		if releaseErr != nil {
			return jobruntime.Retryable(releaseErr)
		}
		return jobruntime.Retryable(ErrRouteReconciliationRequired)
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
