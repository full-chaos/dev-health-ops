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
		executor.Handler == nil || executor.Comparator == nil ||
		executor.HeartbeatInterval <= 0 {
		return CompleteRouteExecutionResult{}, ErrInvalidConfiguration
	}
	if descriptor.RouteEnabled &&
		(executor.Budget == nil || executor.Gate == nil ||
			executor.Committer.Ledger == nil || executor.Committer.Sink == nil) {
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
		if descriptor.RouteEnabled {
			limit := executor.BudgetLimits[session.Claim.CostClass]
			if limit < 1 || executor.BudgetTTL <= 0 {
				return ErrInvalidConfiguration
			}
			client.Budget = executor.Budget
			client.BudgetKey = providerfoundation.BudgetKey{
				Provider:  session.Claim.Provider,
				OrgID:     session.Claim.OrgID,
				Host:      client.BaseURL.Hostname(),
				CostClass: string(session.Claim.CostClass),
				Limit:     limit, TTL: executor.BudgetTTL,
			}
			client.Gate = executor.Gate(session.Claim, client)
			if client.Gate == nil {
				return ErrInvalidConfiguration
			}
		}
		client.Metrics = executor.Metrics
		normalizedAt := executor.now()
		if descriptor.RouteEnabled && session.Claim.Recovered {
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
			case errors.Is(loadErr, ErrEffectLedgerNotFound):
			default:
				return loadErr
			}
		}
		batch, err := executor.Handler.Collect(
			workContext, session.Claim, credential, client, normalizedAt,
		)
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
		result.Effects, err = executor.Committer.Commit(
			workContext, session.Claim, batch.Effects,
		)
		return err
	})
	return result, err
}
