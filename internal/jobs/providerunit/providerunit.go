// Package providerunit adapts the authoritative SyncRunUnit lease to the
// generic River runtime. River arguments carry only the unit identifier.
package providerunit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfamilycontract"
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

// RepositoryIdentityCategory records a unit whose repository identity cannot be
// proven identical to the Python derivation. Like RouteReconciliationCategory
// it is durable, distinguishable, and alertable — but it is reached on the
// first attempt because retrying a deterministic fault cannot change it.
const RepositoryIdentityCategory = "repository_identity_ambiguous"

// GitHubFilesInventoryFailureCategory marks an exhausted file-inventory sync;
// unlike a successful empty repository, it says inventory completeness was
// never established.
const GitHubFilesInventoryFailureCategory = "github_files_inventory_failed"

// EffectRecoveryAmbiguousCategory records a unit whose effect recovery cannot
// be reconciled: a readback found a stored row that disagrees with the effect
// being replayed, the effect was marked recovery-blocked, or the committer had
// no readback to consult (effect_committer.go:128,158,163).
//
// Every one of those is decided by state already durably persisted, so a later
// attempt re-reads the same rows and reaches the same verdict. Retrying burns
// the remaining attempts and then buries the real cause under the generic
// provider_unit_exhausted category, which is exactly the outcome that makes a
// wedged effect hard to find.
const EffectRecoveryAmbiguousCategory = "effect_recovery_ambiguous"

// ProviderDatasetUnavailableCategory records an account-level provider
// capability limitation. It is distinct from authentication and exhaustion:
// retrying the same valid credential cannot make the dataset available.
const ProviderDatasetUnavailableCategory = "provider_dataset_unavailable"

// deterministicTerminalCategory maps executor failures that no retry can clear
// onto their own durable category. Anything not listed keeps the ordinary
// bounded-retry path.
//
// This is ADAPTER-INDEPENDENT: it reclassifies ErrEffectRecoveryAmbiguous for
// every route that reaches the shared committer, not only the three derived
// destinations this lane adds. That is intended -- the error means the same
// thing wherever it comes from -- but it does change the recorded category and
// the retry count for existing adapters that previously exhausted instead.
func deterministicTerminalCategory(err error) (string, bool) {
	if errors.Is(err, providersync.ErrProviderDatasetUnavailable) {
		return ProviderDatasetUnavailableCategory, true
	}
	if errors.Is(err, providersync.ErrRepositoryIdentityAmbiguous) {
		return RepositoryIdentityCategory, true
	}
	if errors.Is(err, providersync.ErrEffectRecoveryAmbiguous) {
		return EffectRecoveryAmbiguousCategory, true
	}
	return "", false
}

func exhaustedFailureCategory(claim providersync.Claim) string {
	if claim.Provider == "github" && claim.Dataset == "files" {
		return GitHubFilesInventoryFailureCategory
	}
	return "provider_unit_exhausted"
}

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
	// LeaseMetrics observes worker_sync_lease_expired_total. It is called only
	// for a claim whose Repository.Claim reported Recovered — i.e. this
	// attempt itself is River's recovery of a unit whose previous owner's
	// lease had expired — at the point this handler durably resolves that
	// attempt to retrying (ReleaseForRetry) or failed (Fail). A nil value
	// keeps behavior unchanged.
	LeaseMetrics jobruntime.SyncLeaseObserver
}

func (handler *Handler) observeRouteFault(fault RouteFault) {
	if handler == nil || handler.OnRouteFault == nil {
		return
	}
	handler.OnRouteFault(fault)
}

// observeLeaseRecovery records the durable resolution of a claim that itself
// recovered an expired lease. It is a no-op for an ordinary (non-recovered)
// claim, since ObserveSyncLeaseExpired's contract is specifically about
// expired-lease recovery, not every retry or failure.
func (handler *Handler) observeLeaseRecovery(claim providersync.Claim, result jobruntime.SyncLeaseResult) {
	if handler == nil || handler.LeaseMetrics == nil || !claim.Recovered {
		return
	}
	_ = handler.LeaseMetrics.ObserveSyncLeaseExpired(
		jobruntime.SyncLeaseLabels{Provider: claim.Provider, DatasetFamily: claim.Dataset},
		result,
	)
}

func (handler *Handler) now() time.Time {
	if handler.Now != nil {
		return handler.Now().UTC()
	}
	return time.Now().UTC()
}

// reconcileRouteFault preserves a claimed unit for the producer/reconciler
// when either route enablement or the activated GitHub work-item family shape
// is invalid. The latter carries ErrInvalidConfiguration as its cause while
// retaining the existing durable route-reconciliation lifecycle: a malformed
// persisted unit is real work that must be repaired, not silently dropped.
func (handler *Handler) reconcileRouteFault(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.ProviderUnitArgs],
	claim providersync.Claim,
	descriptor providersync.CompleteRouteDescriptor,
	descriptorPresent bool,
	startedAt time.Time,
	configurationErr error,
) error {
	// TRD non-negotiable #3: a provider unit delivered to River must never
	// terminalize as route_disabled. A unit only reaches here when the Python
	// producer gate routed a scope the Go descriptor does not serve, or a
	// persisted GitHub family claim is malformed. The sync data is real and the
	// gate or stored claim needs reconciliation. Terminalizing it as
	// route_disabled would silently discard that scope's data for the whole run.
	fault := RouteFault{
		Provider: claim.Provider, Dataset: claim.Dataset,
		DescriptorPresent: descriptorPresent, RouteReady: descriptor.RouteReady,
		RouteEnabled: descriptor.RouteEnabled,
		Attempt:      execution.Attempt,
		MaxAttempts:  execution.Definition.MaxAttempts,
	}
	// Releasing on the terminal attempt would strand the unit: River discards
	// the job after the last attempt, leaving the unit `dispatching` with no
	// live consumer, and the producer outbox dedupe row makes a stale
	// redispatch report "queued" without enqueueing anything. Record an explicit
	// durable reconciliation-required state instead — terminal and alertable,
	// but never the silent route_disabled drop.
	if execution.Attempt >= execution.Definition.MaxAttempts {
		fault.Terminal = true
		handler.observeRouteFault(fault)
		if failErr := handler.Repository.Fail(
			context.WithoutCancel(ctx), claim, RouteReconciliationCategory,
			startedAt, handler.now(),
		); failErr != nil {
			return jobruntime.Retryable(failErr)
		}
		handler.observeLeaseRecovery(claim, jobruntime.SyncLeaseResultFailed)
		return jobruntime.Retryable(routeReconciliationError(configurationErr))
	}
	releaseErr := handler.Repository.ReleaseForRetry(
		context.WithoutCancel(ctx), claim, handler.now(),
	)
	fault.Released = releaseErr == nil
	handler.observeRouteFault(fault)
	if releaseErr != nil {
		return jobruntime.Retryable(releaseErr)
	}
	handler.observeLeaseRecovery(claim, jobruntime.SyncLeaseResultRetrying)
	return jobruntime.Retryable(routeReconciliationError(configurationErr))
}

func routeReconciliationError(configurationErr error) error {
	if configurationErr == nil {
		return ErrRouteReconciliationRequired
	}
	return fmt.Errorf("%w: %w", ErrRouteReconciliationRequired, configurationErr)
}

func validateProviderFamilyExecutionClaim(
	claim providersync.Claim,
	switches providersync.CompleteRouteSwitches,
) error {
	policy, family := providerfamilycontract.PolicyFor(claim.Provider, claim.Dataset)
	if !family || policy.Mode == providerfamilycontract.Independent {
		return nil
	}
	// GitHub retains its already-landed always-exact claim contract. Other
	// work-item providers remain on their D16 legacy claim shape until their
	// actual typed Go switch is enabled. GitLab deliberately has no switch yet,
	// so cataloguing its family cannot activate admission or execution.
	strict := false
	switch strings.ToLower(strings.TrimSpace(claim.Provider)) {
	case "github":
		strict = true
	case "jira":
		strict = switches.JiraWorkItems
	case "linear":
		strict = switches.LinearWorkItems
	}
	if err := providerfamilycontract.ValidateClaim(
		claim.Provider, claim.Dataset, claim.ProcessorFlags, strict,
	); err != nil {
		return fmt.Errorf("%w: %w", providersync.ErrInvalidConfiguration, err)
	}
	return nil
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
	descriptor, descriptorPresent := handler.Switches.Descriptor(claim.Provider, claim.Dataset)
	// This admission boundary is intentionally before LeaseSession and
	// BuildExecutor. A stale River unit can otherwise fetch credentials and
	// commit an incomplete work-item family before the completion-side
	// defense-in-depth check observes its flags.
	familyClaimErr := validateProviderFamilyExecutionClaim(claim, handler.Switches)
	if familyClaimErr != nil || !descriptorPresent ||
		!descriptor.RouteReady || !descriptor.RouteEnabled {
		return handler.reconcileRouteFault(
			ctx, execution, claim, descriptor, descriptorPresent, startedAt, familyClaimErr,
		)
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
			if len(result.WorklogObservations) > 0 {
				payload["go_worklog_observations"] = result.WorklogObservations
			}
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
	// A deterministic fault cannot succeed on a later attempt. Burning the
	// remaining attempts would only delay the outcome and then bury the real
	// cause under the generic provider_unit_exhausted category.
	if category, deterministic := deterministicTerminalCategory(err); deterministic {
		// Discarding this error would report a permanent, already-recorded
		// outcome while the category never persisted and run finalization
		// never armed, leaving the run nonterminal. Stay retryable so a later
		// attempt can record it, exactly as the route-reconciliation path does.
		if failErr := handler.Repository.Fail(
			context.WithoutCancel(ctx), session.Claim, category,
			startedAt, completedAt,
		); failErr != nil {
			return jobruntime.Retryable(failErr)
		}
		handler.observeLeaseRecovery(session.Claim, jobruntime.SyncLeaseResultFailed)
		return jobruntime.Permanent(err)
	}
	if execution.Attempt >= execution.Definition.MaxAttempts {
		// The collector's contract forbids recording a failed CAS attempt, so
		// this capture (where the prior code discarded the error entirely)
		// only adds the ability to gate the metric on true success; it does
		// not change the existing best-effort, always-retryable behavior.
		failErr := handler.Repository.Fail(
			context.WithoutCancel(ctx), session.Claim, exhaustedFailureCategory(session.Claim),
			startedAt, completedAt,
		)
		if failErr == nil {
			handler.observeLeaseRecovery(session.Claim, jobruntime.SyncLeaseResultFailed)
		}
		return jobruntime.Retryable(err)
	}
	if releaseErr := handler.Repository.ReleaseForRetry(
		context.WithoutCancel(ctx), session.Claim, completedAt,
	); releaseErr != nil {
		return jobruntime.Retryable(releaseErr)
	}
	handler.observeLeaseRecovery(session.Claim, jobruntime.SyncLeaseResultRetrying)
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
