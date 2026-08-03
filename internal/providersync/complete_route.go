package providersync

import (
	"context"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type CompleteRouteBatch struct {
	Effects   []EffectBatch
	Result    map[string]any
	Watermark *time.Time
	Evidence  FetchEvidence
}

func (batch CompleteRouteBatch) validate(descriptor CompleteRouteDescriptor) error {
	if len(batch.Effects) != len(descriptor.Destinations) {
		return ErrInvalidConfiguration
	}
	got := make([]string, 0, len(batch.Effects))
	for _, effect := range batch.Effects {
		if effect.Destination == "" || !validDigest(effect.ContentDigest) ||
			!validEffectRecovery(effect.Recovery) {
			return ErrInvalidConfiguration
		}
		got = append(got, effect.Destination)
	}
	sort.Strings(got)
	want := append([]string(nil), descriptor.Destinations...)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		return ErrInvalidConfiguration
	}
	return nil
}

type CompleteRouteHandler interface {
	Collect(
		context.Context,
		Claim,
		providerfoundation.Credential,
		*providerfoundation.HTTPClient,
		time.Time,
	) (CompleteRouteBatch, error)
}

// RecoveringCompleteRouteHandler lets a route rebuild an in-flight provider
// batch from durable effect identity before ordinary source reselection. Most
// routes only need the stable normalization instant above. Bounded routes
// whose selection depends on already-written analytics rows (GitHub blame is
// the first) need the persisted generation as well, otherwise an accepted
// write can disappear from coverage and produce a new manifest before
// readback has a chance to reconcile the old one.
type RecoveringCompleteRouteHandler interface {
	CollectRecovery(
		context.Context,
		Claim,
		providerfoundation.Credential,
		*providerfoundation.HTTPClient,
		time.Time,
		EffectLedgerState,
	) (CompleteRouteBatch, error)
}

// SafeReplanningCompleteRouteHandler may authorize discarding a prepared
// provider-dependent manifest only after proving that its first ordered
// durable effect has no rows for this generation. The ledger performs a
// second, transactional state check before it removes the manifest.
type SafeReplanningCompleteRouteHandler interface {
	CanReplanRecovery(
		context.Context,
		Claim,
		EffectLedgerState,
	) (bool, error)
}

type CompleteRouteComparator interface {
	CompareCompleteRoute(
		context.Context,
		Claim,
		CompleteRouteBatch,
	) (ShadowComparison, error)
}

type CompleteRouteExecutor struct {
	Credentials       providerfoundation.CredentialResolver
	Doer              providerfoundation.HTTPDoer
	Retry             providerfoundation.RetryPolicy
	Budget            providerfoundation.BudgetStore
	BudgetLimits      map[CostClass]int
	BudgetTTL         time.Duration
	Gate              BackoffGateFactory
	Metrics           *providerfoundation.Metrics
	Handler           CompleteRouteHandler
	Comparator        CompleteRouteComparator
	Committer         EffectCommitter
	HeartbeatInterval time.Duration
	Now               func() time.Time
}

type CompleteRouteExecutionResult struct {
	Fetch      FetchEvidence
	Result     map[string]any
	Watermark  *time.Time
	Comparison ShadowComparison
	Effects    EffectCommitResult
	ShadowOnly bool
}

func (executor CompleteRouteExecutor) now() time.Time {
	if executor.Now != nil {
		return executor.Now().UTC()
	}
	return time.Now().UTC()
}

func (executor CompleteRouteExecutor) Execute(
	ctx context.Context,
	session *LeaseSession,
	descriptor CompleteRouteDescriptor,
) (CompleteRouteExecutionResult, error) {
	if ctx == nil || session == nil || !session.valid() ||
		descriptor.Provider != session.Claim.Provider ||
		descriptor.RequestedDataset != session.Claim.Dataset ||
		descriptor.RouteDataset != session.Claim.Dataset ||
		!descriptor.RouteReady || executor.Doer == nil ||
		executor.Credentials.Repository == nil ||
		executor.Credentials.Decryptor == nil ||
		executor.Budget == nil || executor.Gate == nil ||
		executor.Handler == nil || executor.Comparator == nil ||
		executor.HeartbeatInterval <= 0 || executor.BudgetTTL <= 0 ||
		executor.BudgetLimits[session.Claim.CostClass] < 1 {
		return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
	}
	if descriptor.RouteEnabled &&
		(executor.Committer.Ledger == nil || executor.Committer.Sink == nil) {
		return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
	}
	var result CompleteRouteExecutionResult
	err := session.Run(ctx, executor.HeartbeatInterval, func(
		workContext context.Context,
		guard providerfoundation.LeaseGuard,
	) error {
		credential, err := executor.Credentials.Resolve(
			workContext,
			guard,
			session.Claim.TenantScope(),
		)
		if err != nil {
			return err
		}
		client, err := (Executor{
			Doer: executor.Doer, Retry: executor.Retry,
		}).newClient(credential, guard)
		if err != nil {
			return err
		}
		client.Budget = executor.Budget
		client.BudgetKey = providerfoundation.BudgetKey{
			Provider:  session.Claim.Provider,
			OrgID:     session.Claim.OrgID,
			Host:      client.BaseURL.Hostname(),
			CostClass: string(session.Claim.CostClass),
			Limit:     executor.BudgetLimits[session.Claim.CostClass],
			TTL:       executor.BudgetTTL,
		}
		client.Gate = executor.Gate(session.Claim, client)
		if client.Gate == nil {
			return ErrInvalidConfiguration
		}
		client.Metrics = executor.Metrics
		normalizedAt := executor.now()
		var recoveredEffects *EffectLedgerState
		// Every attempt for this unit occurrence must rebuild byte-identical
		// rows, not just expired-lease recoveries. Effect digests cover the
		// serialized rows, so a wall-clock timestamp regenerated on an ordinary
		// River retry would change the digest and make PrepareEffects reject
		// the manifest with ErrEffectLedgerConflict before any readback could
		// run — wedging the unit until it exhausts. ReleaseForRetry returns the
		// unit to `dispatching`, so the next claim is *not* Recovered; gating
		// this on Recovered covered only a fraction of the real retry paths.
		if descriptor.RouteEnabled {
			state, loadErr := executor.Committer.Ledger.LoadEffects(
				workContext, session.Claim, normalizedAt,
			)
			switch {
			case loadErr == nil:
				if state.Generation != session.Claim.GenerationKey() ||
					state.Provider != session.Claim.Provider ||
					state.Dataset != session.Claim.Dataset {
					return ErrEffectLedgerConflict
				}
				normalizedAt = state.CreatedAt.UTC()
				recoveredEffects = &state
			case errors.Is(loadErr, ErrEffectLedgerNotFound):
			default:
				return loadErr
			}
		}
		if recoveredEffects != nil {
			if replanning, ok := executor.Handler.(SafeReplanningCompleteRouteHandler); ok {
				canReplan, replanErr := replanning.CanReplanRecovery(
					workContext, session.Claim, *recoveredEffects,
				)
				if replanErr != nil {
					return replanErr
				}
				if canReplan {
					ledger, ok := executor.Committer.Ledger.(EffectLedgerReplanner)
					if !ok {
						return ErrInvalidConfiguration
					}
					replannedAt := executor.now()
					if err := ledger.ResetPreparedEffectsForReplan(
						workContext, session.Claim, *recoveredEffects, replannedAt,
					); err != nil {
						return err
					}
					recoveredEffects = nil
					normalizedAt = replannedAt
				}
			}
		}
		var batch CompleteRouteBatch
		if recoveredEffects != nil {
			if recovering, ok := executor.Handler.(RecoveringCompleteRouteHandler); ok {
				batch, err = recovering.CollectRecovery(
					workContext, session.Claim, credential, client, normalizedAt,
					*recoveredEffects,
				)
			} else {
				batch, err = executor.Handler.Collect(
					workContext, session.Claim, credential, client, normalizedAt,
				)
			}
		} else {
			batch, err = executor.Handler.Collect(
				workContext, session.Claim, credential, client, normalizedAt,
			)
		}
		if err != nil {
			return err
		}
		if err := batch.validate(descriptor); err != nil {
			return err
		}
		result.Fetch, result.Result, result.Watermark =
			batch.Evidence, batch.Result, batch.Watermark
		comparison, err := executor.Comparator.CompareCompleteRoute(
			workContext, session.Claim, batch,
		)
		if err != nil {
			return err
		}
		result.Comparison = comparison
		if !comparison.Match {
			return ErrShadowMismatch
		}
		if !descriptor.RouteEnabled {
			result.ShadowOnly = true
			return nil
		}
		// The same instant that stamped these rows must become the persisted
		// ledger CreatedAt. Letting the committer read its own clock would
		// persist a later time than the rows were built with, so the next
		// attempt would reload that later time, rebuild different rows, and be
		// rejected on digest — the wedge in a second disguise.
		result.Effects, err = executor.Committer.Commit(
			workContext, session.Claim, batch.Effects, normalizedAt,
		)
		return err
	})
	return result, err
}
