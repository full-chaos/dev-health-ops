// Package providerunit adapts the authoritative SyncRunUnit lease to the
// generic River runtime. River arguments carry only the unit identifier.
package providerunit

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfamilycontract"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
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
// Aliased from providersync, which also stamps this category onto
// IntegrationDataset (CHAOS-4048) and cannot import this package back
// (providerunit already imports providersync) -- one literal, not two.
const ProviderDatasetUnavailableCategory = providersync.ProviderDatasetUnavailableCategory

// deterministicTerminalCategory maps executor failures that no retry can clear
// onto their own durable category. Anything not listed keeps the ordinary
// bounded-retry path.
//
// This is ADAPTER-INDEPENDENT: it reclassifies ErrEffectRecoveryAmbiguous for
// every route that reaches the shared committer, not only the three derived
// destinations this lane adds. That is intended -- the error means the same
// thing wherever it comes from -- but it does change the recorded category and
// the retry count for existing adapters that previously exhausted instead.
// Categories matching Python's vocabulary in
// src/dev_health_ops/workers/sync_units.py (_PROVIDER_ERROR_PATTERNS and
// _classify_error), so a unit that fails for the same reason in either runtime
// reports the same category.
const (
	// AuthCategory covers 401 and non-rate-limit 403.
	AuthCategory = "auth"
	// NotFoundCategory covers 404.
	NotFoundCategory = "not_found"
	// PaginationIncompleteCategory covers a fail-closed pagination or row-cap
	// refusal -- Python's PaginationException category.
	PaginationIncompleteCategory = "pagination_incomplete"
	// GitHubTestsArtifactOversizedCategory covers a github tests/cicd
	// artifact whose download exceeded the size bound -- deterministic given
	// the artifact's size, so retrying re-downloads and re-rejects the same
	// bytes on every attempt before collapsing into the generic exhausted
	// category (CHAOS-4191, same repeated-refusal waste CHAOS-3871 fixed for
	// pagination). No Python equivalent exists yet; this is a Go-only
	// addition, matching the precedent set by GitHubFilesInventoryFailureCategory
	// and EffectRecoveryAmbiguousCategory above.
	GitHubTestsArtifactOversizedCategory = "github_tests_artifact_oversized"
	// AllArtifactsUnreadableCategory covers a github tests/cicd unit whose
	// every observed artifact failed to read -- a proxy or auth edge
	// answering every artifact request with a 2xx error document. Given the
	// source's current state, every retry re-observes the identical total
	// failure, so it terminalizes on the first attempt instead of burying
	// the specific cause under provider_unit_exhausted (CHAOS-4185). No
	// Python equivalent exists: like GitHubTestsArtifactOversizedCategory,
	// this route is Go-only.
	AllArtifactsUnreadableCategory = "all_artifacts_unreadable"
	// FeatureDisabledCategory covers a unit refused by the execution-time
	// canonical-incident entitlement re-check (Jira incidents and every
	// PagerDuty dataset). Python's FEATURE_DISABLED_ERROR_CATEGORY
	// (sync/canonical_incident_gate.py:24), stamped by _classify_error
	// (workers/sync_units.py:266). Deterministic given the organization's
	// current feature state: the same disabled feature refuses identically on
	// every attempt, so retrying only re-reads the same rows five times before
	// burying the real cause under provider_unit_exhausted (CHAOS-4219).
	FeatureDisabledCategory = "feature_disabled"
)

func deterministicTerminalCategory(err error) (string, bool) {
	if errors.Is(err, providersync.ErrProviderDatasetUnavailable) {
		return ProviderDatasetUnavailableCategory, true
	}
	// A fail-closed pagination or row-cap refusal is deterministic given the
	// provider's current state: a repo with more in-window rows than the cap
	// returns the same refusal on every attempt. Retrying it re-fetched up to
	// 100 list pages and 10k detail GETs on each of 5 attempts, then again on
	// every scheduled tick, and finally reported the generic exhaustion
	// category. The refusal itself is correct; the repetition is not
	// (CHAOS-3871).
	if errors.Is(err, providersync.ErrPaginationCapExceeded) {
		return PaginationIncompleteCategory, true
	}
	if errors.Is(err, providersync.ErrGitHubTestsArtifactOversized) {
		return GitHubTestsArtifactOversizedCategory, true
	}
	if errors.Is(err, providersync.ErrGitHubTestsAllArtifactsUnreadable) {
		return AllArtifactsUnreadableCategory, true
	}
	if errors.Is(err, providersync.ErrIncidentEntitlementDisabled) {
		return FeatureDisabledCategory, true
	}
	// Authentication and not-found are already non-retryable at the HTTP layer
	// (providerfoundation.ProviderError.Retryable), so the unit handler was
	// spending four more full executions against a dead credential or a
	// deleted repository before collapsing the precise cause into
	// provider_unit_exhausted. Python fails these once, with the category.
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) {
		switch providerErr.Class {
		case providerfoundation.ErrorAuthentication:
			return AuthCategory, true
		case providerfoundation.ErrorNotFound:
			return NotFoundCategory, true
		}
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
	Plannable         bool
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
	DeferForBudgetContention(context.Context, providersync.Claim, time.Time, time.Time) error
	Fail(
		context.Context,
		providersync.Claim,
		string,
		time.Time,
		time.Time,
	) error
}

// RateLimitCategory is the terminal category for a unit whose rate-limit
// deferral episode is spent. Python fails with a rate-limit category rather
// than a generic exhaustion, so an operator can tell "the provider kept
// throttling us for two hours" from "this unit is broken".
const RateLimitCategory = "rate_limit"

// RateLimitDeferralRepository is optional so existing repository test doubles
// and older rolling binaries stay source-compatible, exactly like
// ChunkContinuationRepository. Production's PostgresRepository implements it.
type RateLimitDeferralRepository interface {
	RateLimitEpisode(context.Context, providersync.Claim) (providersync.RateLimitEpisode, error)
	DeferForRateLimit(context.Context, providersync.Claim, time.Time, time.Time) error
}

// ChunkContinuationRepository is optional so legacy repository test doubles
// and older rolling binaries remain source-compatible. Production's
// PostgresRepository implements it for the opt-in chunk route.
type ChunkContinuationRepository interface {
	DeferChunkContinuation(context.Context, providersync.Claim, time.Time, time.Time) error
}

type Handler struct {
	Repository    UnitRepository
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
	// ProviderMetrics is the process-wide provider counter registry (the same
	// instance BuildExecutor hands each claim's HTTP client). It is used here
	// for exactly one signal: a unit terminalized while it already held
	// committed rows. A nil value keeps behavior unchanged.
	ProviderMetrics *providerfoundation.Metrics
}

// observeTerminalWithCommittedRows reports a unit that WAS terminalized while
// durable rows for it already exist. Call it only after the durable transition
// succeeded: an unrecorded failure leaves the unit retryable, and a counter
// that fired anyway would report destruction that never happened.
//
// This is the alarm CHAOS-4130 lacked. A page-budget refusal cancelled units
// holding ~9,500 committed rows, deleted their checkpoints, and let the run
// re-plan the same window from page one -- a 17-minute, ~970-API-call cycle
// that repeated for days while every individual signal looked ordinary. No
// healthy route destroys a unit that has already paid for durable rows, so the
// combination is the whole signal; neither half alerts on its own.
func (handler *Handler) observeTerminalWithCommittedRows(
	claim providersync.Claim,
	result providersync.CompleteRouteExecutionResult,
	category string,
	cause error,
) {
	if handler == nil {
		return
	}
	rows := result.CommittedRows
	if written := int64(result.Effects.Written); written > rows {
		rows = written
	}
	if rows <= 0 {
		return
	}
	handler.ProviderMetrics.RecordUnitTerminalWithRows(claim.Provider, claim.Dataset)
	slog.Warn(
		"provider unit terminalized while holding committed rows",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"category", category, "committed_rows", rows, "error", cause,
	)
}

// observeAllArtifactsUnreadable reports a github tests/cicd unit that failed
// because every observed artifact was unreadable. Call it only AFTER the
// durable Fail transition succeeded, for the identical reason
// observeTerminalWithCommittedRows is: if Repository.Fail itself errors, this
// attempt stays retryable (jobruntime.Retryable(failErr) above), and a LATER
// attempt walks the route again -- which would re-detect the same condition
// and, if the metric fired here unconditionally, count one logical unit
// failure more than once (CHAOS-4185 codex round 1). Recording it only on the
// attempt whose Fail durably succeeded caps it at exactly one increment per
// unit, matching RecordUnitTerminalWithRows's contract above.
func (handler *Handler) observeAllArtifactsUnreadable(claim providersync.Claim, category string) {
	if handler == nil || category != AllArtifactsUnreadableCategory {
		return
	}
	handler.ProviderMetrics.RecordAllArtifactsUnreadable(claim.Provider, claim.Dataset)
}

// observeCicdPartialSuccess reports a github cicd/tests unit that advanced
// its watermark despite carrying non-empty incomplete evidence (CHAOS-4394).
// Call it only from the branch where Repository.Complete has ALREADY
// returned nil, for the identical reason observeTerminalWithCommittedRows and
// observeAllArtifactsUnreadable are: a metric fired before the durable
// transition commits would over-count a unit that fails to complete and gets
// recollected later (codex review round 1, P2). watermark is the SAME value
// just passed to Repository.Complete, and payload is the SAME map just
// persisted -- reading them here, after the commit, rather than re-deriving
// them, is what keeps this in sync with what was actually written.
//
// repo is deliberately logged, not passed to the counter -- see
// RecordCicdPartialSuccess's doc comment on why it stays off the Prometheus
// label.
//
// payload["incomplete"] is decoded via providersync.DecodeGitHubTestsIncomplete,
// NOT a direct type assertion (codex review round 2, P1): the real
// production caller reaches this branch through
// loadChunkedFinalResult -> PostgresRepository.LoadPreparedChunk, which
// reloads the final chunk's Result through a JSONB sidecar, so "incomplete"
// arrives here as the generic []any/map[string]any shape every time, never
// the live typed slice. A bare assertion would silently no-op this whole
// counter in production while every unit test using the live typed slice
// kept passing.
func (handler *Handler) observeCicdPartialSuccess(
	claim providersync.Claim, watermark *time.Time, payload map[string]any,
) {
	if handler == nil || watermark == nil ||
		claim.Provider != "github" || (claim.Dataset != "cicd" && claim.Dataset != "tests") {
		return
	}
	incomplete, ok := providersync.DecodeGitHubTestsIncomplete(payload["incomplete"])
	if !ok || len(incomplete) == 0 {
		return
	}
	reason := providersync.GitHubTestsCicdPartialSuccessReason(incomplete)
	repo, _ := payload["repo"].(string)
	handler.ProviderMetrics.RecordCicdPartialSuccess(reason)
	slog.Info(
		"cicd/tests unit advanced its watermark with a durable, recorded gap",
		"provider", claim.Provider, "dataset", claim.Dataset, "unit", claim.ID,
		"repository", repo, "reason", reason,
	)
}

// logLifecycle records the safe, authoritative identity of one provider-unit
// attempt. River arguments deliberately carry only the unit ID, so provider,
// dataset, mode, and run identity become available only after Claim. Keep this
// record out of generic queue metrics: provider/dataset would make that scrape
// surface unbounded as the configured provider matrix grows.
func (handler *Handler) logLifecycle(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.ProviderUnitArgs],
	claim providersync.Claim,
	event string,
	result string,
	err error,
) {
	if execution == nil {
		return
	}
	logger := execution.Logger
	if logger == nil {
		logger = slog.Default()
	}
	attributes := []any{
		"provider", claim.Provider,
		"dataset", claim.Dataset,
		"mode", claim.Mode,
		"kind", execution.Definition.Kind,
		"queue", execution.Definition.Queue,
		"job_id", execution.JobID,
		"attempt", execution.Attempt,
		"sync_run_id", claim.SyncRunID,
		"sync_unit_id", claim.ID,
	}
	if result != "" {
		attributes = append(attributes, "result", result)
	}
	if detail := lifecycleErrorDetail(err); detail != "" {
		attributes = append(attributes, "error_detail", detail)
	}
	logger.InfoContext(ctx, event, attributes...)
}

func lifecycleErrorDetail(err error) string {
	if err == nil {
		return ""
	}
	detail := logging.RedactText(err.Error())
	if detail == "" {
		return ""
	}
	return detail
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
		Plannable:   descriptor.Plannable,
		Attempt:     execution.Attempt,
		MaxAttempts: execution.Definition.MaxAttempts,
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
		handler.logLifecycle(ctx, execution, claim, "sync_provider_unit_finished", "failed", configurationErr)
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
	handler.logLifecycle(ctx, execution, claim, "sync_provider_unit_finished", "retrying", configurationErr)
	return jobruntime.Retryable(routeReconciliationError(configurationErr))
}

func routeReconciliationError(configurationErr error) error {
	if configurationErr == nil {
		return ErrRouteReconciliationRequired
	}
	return fmt.Errorf("%w: %w", ErrRouteReconciliationRequired, configurationErr)
}

func validateProviderFamilyExecutionClaim(claim providersync.Claim) error {
	policy, family := providerfamilycontract.PolicyFor(claim.Provider, claim.Dataset)
	if !family || policy.Mode == providerfamilycontract.Independent {
		return nil
	}
	// CHAOS-4054: an atomic family has exactly one canonical claim shape, and
	// capability is always on. The exactness of the claim was previously gated
	// on each provider's route switch, which meant a malformed persisted claim
	// was admitted purely because a deployment had not turned the route on.
	// With the switches gone the contract is what it always described: every
	// atomic family validates strictly.
	if err := providerfamilycontract.ValidateClaim(
		claim.Provider, claim.Dataset, claim.ProcessorFlags, true,
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
	handler.logLifecycle(ctx, execution, claim, "sync_provider_unit_started", "", nil)
	descriptor, descriptorPresent := providersync.Descriptor(claim.Provider, claim.Dataset)
	// This admission boundary is intentionally before LeaseSession and
	// BuildExecutor. A stale River unit can otherwise fetch credentials and
	// commit an incomplete work-item family before the completion-side
	// defense-in-depth check observes its flags.
	familyClaimErr := validateProviderFamilyExecutionClaim(claim)
	if familyClaimErr != nil || !descriptorPresent ||
		!descriptor.RouteReady || !descriptor.Plannable {
		return handler.reconcileRouteFault(
			ctx, execution, claim, descriptor, descriptorPresent, startedAt, familyClaimErr,
		)
	}
	session := &providersync.LeaseSession{
		Repository: handler.Repository,
		Claim:      claim, LeaseDuration: handler.LeaseDuration,
		Deadline: execution.Deadline, Now: handler.Now,
	}
	// Declared outside the success block on purpose: the chunked executor
	// reports CommittedRows on its FAILURE path too, and the terminalization
	// alarm below needs it (CHAOS-4130).
	var result providersync.CompleteRouteExecutionResult
	executor, err := handler.BuildExecutor(session)
	if err == nil {
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
				handler.observeCicdPartialSuccess(session.Claim, result.Watermark, payload)
				handler.logLifecycle(ctx, execution, session.Claim, "sync_provider_unit_finished", "succeeded", nil)
				return nil
			}
		}
	}
	completedAt := handler.now()
	// A prepared chunk can be continued after a bounded number of commits.
	// Persist the claimable not-before fence before returning an attempt-neutral
	// River snooze. Do not call ReleaseForRetry: that would turn a healthy
	// continuation into an ordinary failure attempt and erase the checkpoint's
	// lease-generation context.
	if delay, continuation := providersync.ChunkContinuationDelay(err); continuation {
		deferrer, supported := handler.Repository.(ChunkContinuationRepository)
		if !supported {
			return jobruntime.Retryable(err)
		}
		availableAt := completedAt.Add(delay)
		if deferErr := deferrer.DeferChunkContinuation(
			context.WithoutCancel(ctx), session.Claim, availableAt, completedAt,
		); deferErr != nil {
			return jobruntime.Retryable(deferErr)
		}
		handler.observeLeaseRecovery(session.Claim, jobruntime.SyncLeaseResultRetrying)
		handler.logLifecycle(ctx, execution, session.Claim, "sync_provider_unit_finished", "continued", err)
		return jobruntime.RetryableAfter(err, delay)
	}
	// A provider rate limit is the provider scheduling us, not the unit
	// failing. Python defers it up to 10 times over 2 hours without consuming
	// the failure budget; Go had no branch at all, so a 429 burned one of five
	// attempts on a 5s-5m backoff and a 30-60 minute reset window terminalized
	// the unit in two or three minutes (CHAOS-3868).
	if retryAfter, rateLimited := providerRateLimitDelay(err); rateLimited {
		if deferrer, supported := handler.Repository.(RateLimitDeferralRepository); supported {
			episode, episodeErr := deferrer.RateLimitEpisode(context.WithoutCancel(ctx), session.Claim)
			if episodeErr != nil {
				return jobruntime.Retryable(episodeErr)
			}
			plan, granted := planRateLimitDeferral(retryAfter, episode, session.Claim.ID, completedAt)
			if granted {
				if deferErr := deferrer.DeferForRateLimit(
					context.WithoutCancel(ctx), session.Claim, plan.notBefore, completedAt,
				); deferErr != nil {
					return jobruntime.Retryable(deferErr)
				}
				handler.observeLeaseRecovery(session.Claim, jobruntime.SyncLeaseResultRetrying)
				handler.logLifecycle(ctx, execution, session.Claim, "sync_provider_unit_finished", "rate_limited", err)
				return jobruntime.RateLimited(err, plan.countdown)
			}
			// The episode's count or wall-clock budget is spent. Fail with the
			// rate-limit category rather than letting it fall through to the
			// generic provider_unit_exhausted, which buries the real cause.
			if failErr := handler.Repository.Fail(
				context.WithoutCancel(ctx), session.Claim, RateLimitCategory,
				startedAt, completedAt,
			); failErr != nil {
				return jobruntime.Retryable(failErr)
			}
			handler.observeLeaseRecovery(session.Claim, jobruntime.SyncLeaseResultFailed)
			handler.logLifecycle(ctx, execution, session.Claim, "sync_provider_unit_finished", "failed", err)
			return jobruntime.Permanent(err)
		}
	}
	// A healthy shared request bucket can be full when sibling provider units
	// overlap. That is scheduling contention, not an execution failure. Persist
	// the domain deferral before returning River's attempt-neutral snooze so a
	// process restart keeps the same not-before fence and operator evidence.
	if errors.Is(err, providerfoundation.ErrBudgetContended) {
		delay := providerBudgetContentionDelay(session.Claim.ID)
		availableAt := completedAt.Add(delay)
		if deferErr := handler.Repository.DeferForBudgetContention(
			context.WithoutCancel(ctx), session.Claim, availableAt, completedAt,
		); deferErr != nil {
			return jobruntime.Retryable(deferErr)
		}
		handler.observeLeaseRecovery(session.Claim, jobruntime.SyncLeaseResultRetrying)
		handler.logLifecycle(ctx, execution, session.Claim, "sync_provider_unit_finished", "deferred", err)
		return jobruntime.BudgetContention(err, delay)
	}
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
		// Only AFTER the durable transition, for the same reason
		// observeLeaseRecovery is: a lost CAS leaves the unit retryable, and a
		// counter that already claimed the unit was destroyed would page an
		// operator about an incident that did not happen -- and inflate the
		// exact series an operator would use to judge how bad CHAOS-4130 is.
		handler.observeTerminalWithCommittedRows(session.Claim, result, category, err)
		handler.observeAllArtifactsUnreadable(session.Claim, category)
		handler.observeLeaseRecovery(session.Claim, jobruntime.SyncLeaseResultFailed)
		handler.logLifecycle(ctx, execution, session.Claim, "sync_provider_unit_finished", "failed", err)
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
			handler.observeTerminalWithCommittedRows(
				session.Claim, result, exhaustedFailureCategory(session.Claim), err,
			)
			handler.observeLeaseRecovery(session.Claim, jobruntime.SyncLeaseResultFailed)
			handler.logLifecycle(ctx, execution, session.Claim, "sync_provider_unit_finished", "failed", err)
		}
		return jobruntime.Retryable(err)
	}
	if releaseErr := handler.Repository.ReleaseForRetry(
		context.WithoutCancel(ctx), session.Claim, completedAt,
	); releaseErr != nil {
		return jobruntime.Retryable(releaseErr)
	}
	handler.observeLeaseRecovery(session.Claim, jobruntime.SyncLeaseResultRetrying)
	handler.logLifecycle(ctx, execution, session.Claim, "sync_provider_unit_finished", "retrying", err)
	return jobruntime.Retryable(err)
}

// providerRateLimitDelay reports whether the failure is a provider rate limit
// and, if so, the delay the provider asked for (already resolved from
// Retry-After or the reset headers by providerfoundation).
func providerRateLimitDelay(err error) (time.Duration, bool) {
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		return 0, false
	}
	return providerErr.RetryAfter, true
}

func providerBudgetContentionDelay(unitID string) time.Duration {
	digest := sha256.Sum256([]byte(unitID))
	jitter := time.Duration(binary.BigEndian.Uint64(digest[:8])%1000) * time.Millisecond
	return time.Second + jitter
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
