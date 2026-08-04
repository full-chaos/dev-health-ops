package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var (
	ErrGitHubWorkItemsDerivationsUnavailable = errors.New(
		"github work-item derived rows are unavailable",
	)
	ErrGitHubWorkItemsIncomplete = errors.New(
		"github work-item composite collection is incomplete",
	)
)

// GitHubWorkItemsRequestUsage is the bounded provider-neutral actual-request
// observation emitted by the composite route. Entries are aggregated by the
// same (transport, route_family, dimension) identity as ProviderRequestPlan.
type GitHubWorkItemsRequestUsage struct {
	Transport    string `json:"transport"`
	RouteFamily  string `json:"route_family"`
	Dimension    string `json:"dimension"`
	RequestCount int    `json:"request_count"`
}

// GitHubWorkItemsIncomplete preserves optional degradation as typed data.
// Cause is a stable local/provider class and never provider response text.
type GitHubWorkItemsIncomplete struct {
	Component string `json:"component"`
	SubjectID string `json:"subject_id,omitempty"`
	Cause     string `json:"cause"`
}

// GitHubWorkItemsRouteError retains physical request actuals and typed fetch
// evidence when a required phase fails or an optional phase is incomplete.
// Callers can retry without confusing a zero-effect failure for success.
type GitHubWorkItemsRouteError struct {
	Cause      error
	Usage      []GitHubWorkItemsRequestUsage
	Evidence   FetchEvidence
	Incomplete []GitHubWorkItemsIncomplete
}

func (routeErr *GitHubWorkItemsRouteError) Error() string {
	if routeErr == nil || routeErr.Cause == nil {
		return "github work-item route failed"
	}
	return routeErr.Cause.Error()
}

func (routeErr *GitHubWorkItemsRouteError) Unwrap() error {
	if routeErr == nil {
		return nil
	}
	return routeErr.Cause
}

// githubWorkItemsProjectPolicy is the unresolved Projects v2 activation seam.
// GitHubProjectV2Fetcher satisfies it, but the zero-value handler leaves it nil
// and records configured targets as policy_pending without fetching them.
type githubWorkItemsProjectPolicy interface {
	Fetch(
		context.Context,
		Claim,
		providerfoundation.Credential,
		*providerfoundation.HTTPClient,
		time.Time,
		githubIdentityResolver,
	) (GitHubProjectV2FetchResult, error)
}

// githubWorkItemsDeriver owns the nine Python-derived destination projections.
// Those implementations have not been ported yet. Requiring this seam prevents
// the composite from claiming completeness with fabricated empty metrics.
type githubWorkItemsDeriver interface {
	Derive(
		context.Context,
		Claim,
		githubWorkItemRows,
		time.Time,
	) (map[string][]json.RawMessage, error)
}

// GitHubWorkItemsRouteHandler composes the already-ported REST, PR-social, and
// semantic foundations. It deliberately owns no registration, readiness,
// effect sink/readback, or watermark. Effect construction delegates to the
// shared 16-destination foundation; this handler only supplies its row sets.
type GitHubWorkItemsRouteHandler struct {
	REST            GitHubWorkItemsRESTCollector
	Social          GitHubWorkItemPRSocialFetcher
	Projects        githubWorkItemsProjectPolicy
	Deriver         githubWorkItemsDeriver
	ResolveIdentity githubIdentityResolver
}

var githubWorkItemDerivedDestinations = []string{
	"estimate_coverage_metrics_daily",
	"investment_classifications_daily",
	"investment_metrics_daily",
	"issue_type_metrics_daily",
	"work_item_cycle_times",
	"work_item_metrics_daily",
	"work_item_state_durations_daily",
	"work_item_team_attributions",
	"work_item_user_metrics_daily",
}

func (handler GitHubWorkItemsRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "work-items" || credential.Provider != "github" ||
		credential.ID == "" || credential.ID != claim.CredentialID || client == nil ||
		client.Provider != "github" || client.BaseURL == nil || client.Doer == nil ||
		client.Lease == nil || normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	if handler.Deriver == nil {
		return CompleteRouteBatch{}, ErrGitHubWorkItemsDerivationsUnavailable
	}
	options, err := githubWorkItemsRESTOptionsForClaim(claim)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	projectTargets, err := githubProjectV2Targets(claim)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	usage := githubWorkItemsUsageAccumulator{}
	incomplete := []GitHubWorkItemsIncomplete{}

	restResult, err := handler.REST.Collect(ctx, claim, client, normalizedAt)
	usage.add(GitHubWorkItemsRequestUsage{
		Transport: "rest", RouteFamily: "work_items", Dimension: BudgetRESTCore,
		RequestCount: restResult.Evidence.Requests,
	})
	if err != nil {
		return CompleteRouteBatch{}, usage.wrap(err)
	}
	for _, partial := range restResult.Incomplete {
		incomplete = append(incomplete, GitHubWorkItemsIncomplete{
			Component: partial.Component, SubjectID: partial.SubjectID, Cause: partial.Cause,
		})
	}
	rows := restResult.Rows
	evidence := restResult.Evidence

	if len(restResult.PullRequests) > 0 {
		targets := make([]int, 0, len(restResult.PullRequests))
		for _, pull := range restResult.PullRequests {
			targets = append(targets, pull.Number)
		}
		commentsLimit := 0
		if options.fetchComments {
			commentsLimit = options.commentsLimit
		}
		socialResult, socialErr := handler.Social.Fetch(
			ctx, claim, client, targets, commentsLimit, githubWorkItemEventLimit,
		)
		usage.add(GitHubWorkItemsRequestUsage{
			Transport: socialResult.Usage.Transport, RouteFamily: socialResult.Usage.RouteFamily,
			Dimension: socialResult.Usage.Dimension, RequestCount: socialResult.Usage.RequestCount,
		})
		addGitHubWorkItemsEvidence(&evidence, socialResult.Evidence)
		if socialErr != nil {
			return CompleteRouteBatch{}, usage.wrap(socialErr)
		}
		if err := validateGitHubWorkItemsSocialResult(socialResult); err != nil {
			return CompleteRouteBatch{}, usage.wrap(err)
		}
		if !socialResult.Complete() {
			incomplete = append(incomplete, GitHubWorkItemsIncomplete{
				Component: "pr_social", Cause: socialResult.Incomplete.Cause,
			})
		} else {
			for _, pull := range restResult.PullRequests {
				payload, exists := socialResult.Payloads[pull.Number]
				if !exists {
					return CompleteRouteBatch{}, usage.wrap(providerfoundation.ErrGraphQLResponse)
				}
				adapted, adaptErr := adaptGitHubWorkItemPRSocialPayload(payload)
				if adaptErr != nil {
					return CompleteRouteBatch{}, usage.wrap(adaptErr)
				}
				bundle, normalizeErr := normalizeGitHubPullRequestBundle(
					claim, restResult.RepoFullName, restResult.RepoID, pull.Payload,
					adapted.Events, adapted.Comments, handler.ResolveIdentity, normalizedAt,
				)
				if normalizeErr != nil {
					return CompleteRouteBatch{}, usage.wrap(normalizeErr)
				}
				appendGitHubWorkItemRows(&rows, bundle)
			}
		}
	}

	projectState := "disabled"
	if len(projectTargets) > 0 {
		if handler.Projects == nil {
			projectState = "policy_pending"
			incomplete = append(incomplete, GitHubWorkItemsIncomplete{
				Component: "projects_v2", Cause: "policy_pending",
			})
		} else {
			projectResult, projectErr := handler.Projects.Fetch(
				ctx, claim, credential, client, normalizedAt, handler.ResolveIdentity,
			)
			usage.add(GitHubWorkItemsRequestUsage{
				Transport: projectResult.Usage.Transport, RouteFamily: projectResult.Usage.RouteFamily,
				Dimension: projectResult.Usage.Dimension, RequestCount: projectResult.Usage.RequestCount,
			})
			addGitHubWorkItemsEvidence(&evidence, projectResult.Evidence)
			if projectErr != nil {
				return CompleteRouteBatch{}, usage.wrap(projectErr)
			}
			if err := validateGitHubWorkItemsProjectResult(claim, projectResult, len(projectTargets)); err != nil {
				return CompleteRouteBatch{}, usage.wrap(err)
			}
			rows = mergeGitHubProjectV2Rows(rows, projectResult.Rows)
			projectState = "included"
		}
	}
	finishGitHubWorkItemsEvidence(&evidence, rows)
	if len(incomplete) > 0 {
		return CompleteRouteBatch{}, usage.wrapRoute(
			ErrGitHubWorkItemsIncomplete, evidence, incomplete,
		)
	}

	derived, err := handler.Deriver.Derive(ctx, claim, rows, normalizedAt)
	if err != nil {
		return CompleteRouteBatch{}, usage.wrap(err)
	}
	effects, err := buildGitHubWorkItemsRouteEffects(rows, derived)
	if err != nil {
		return CompleteRouteBatch{}, usage.wrap(err)
	}
	return CompleteRouteBatch{
		Effects: effects,
		Result: map[string]any{
			"work_items_synced": len(rows.WorkItems),
			"projects_v2":       projectState,
			"incomplete":        incomplete,
			"observations": map[string]any{
				"provider_usage": usage.snapshot(),
			},
		},
		Watermark: nil,
		Evidence:  evidence,
	}, nil
}

func validateGitHubWorkItemsSocialResult(result GitHubWorkItemPRSocialFetchResult) error {
	if result.Evidence.Provider != "github" || result.Evidence.Dataset != "work-items-pr-social" ||
		result.Evidence.Requests < 0 || result.Evidence.Pages < 0 ||
		result.Evidence.Pages > result.Evidence.Requests ||
		result.Usage.Transport != "graphql" || result.Usage.RouteFamily != "work_item_prs" ||
		result.Usage.Dimension != BudgetGraphQLCost || result.Usage.RequestCount < 0 ||
		result.Usage.RequestCount != result.Evidence.Requests {
		return ErrInvalidConfiguration
	}
	return nil
}

func validateGitHubWorkItemsProjectResult(
	claim Claim,
	result GitHubProjectV2FetchResult,
	targets int,
) error {
	if claim.Validate() != nil || targets < 1 || result.Targets != targets ||
		result.Evidence.Provider != "github" ||
		result.Evidence.Dataset != "projects-v2" || result.Evidence.Requests < 0 ||
		result.Evidence.Pages < 0 || result.Evidence.Pages > result.Evidence.Requests ||
		result.Usage.Transport != "graphql" || result.Usage.RouteFamily != "work_item_prs" ||
		result.Usage.Dimension != BudgetGraphQLCost || result.Usage.RequestCount < 0 ||
		result.Usage.RequestCount != result.Evidence.Requests ||
		len(result.Rows.Dependencies) != 0 || len(result.Rows.ReopenEvents) != 0 ||
		len(result.Rows.Interactions) != 0 || len(result.Rows.Sprints) != 0 ||
		len(result.Rows.AIAttributions) != 0 {
		return ErrInvalidConfiguration
	}
	if result.Evidence.Records != len(result.Rows.WorkItems)+len(result.Rows.StatusTransitions) {
		return ErrInvalidConfiguration
	}
	for _, row := range result.Rows.WorkItems {
		if err := validateGitHubProjectV2Row(claim, row); err != nil {
			return err
		}
	}
	for _, row := range result.Rows.StatusTransitions {
		if err := row.validate(claim); err != nil {
			return err
		}
	}
	return nil
}

func addGitHubWorkItemsEvidence(target *FetchEvidence, source FetchEvidence) {
	target.Requests += source.Requests
	target.Pages += source.Pages
	target.CapReached = target.CapReached || source.CapReached
}

func countGitHubWorkItemRows(rows githubWorkItemRows) int {
	return len(rows.WorkItems) + len(rows.StatusTransitions) + len(rows.Dependencies) +
		len(rows.ReopenEvents) + len(rows.Interactions) + len(rows.Sprints) +
		len(rows.AIAttributions)
}

func finishGitHubWorkItemsEvidence(evidence *FetchEvidence, rows githubWorkItemRows) {
	evidence.Provider = "github"
	evidence.Dataset = "work-items"
	evidence.Records = countGitHubWorkItemRows(rows)
}

func buildGitHubWorkItemsRouteEffects(
	rows githubWorkItemRows,
	derived map[string][]json.RawMessage,
) ([]EffectBatch, error) {
	if len(derived) != len(githubWorkItemDerivedDestinations) {
		return nil, ErrGitHubWorkItemsDerivationsUnavailable
	}
	derivedSet := make(map[string]struct{}, len(githubWorkItemDerivedDestinations))
	for _, destination := range githubWorkItemDerivedDestinations {
		derivedSet[destination] = struct{}{}
		if _, exists := derived[destination]; !exists {
			return nil, ErrGitHubWorkItemsDerivationsUnavailable
		}
	}
	for destination := range derived {
		if _, expected := derivedSet[destination]; !expected {
			return nil, ErrGitHubWorkItemsDerivationsUnavailable
		}
	}
	directAI, err := githubWorkItemsRawMessages(rows.AIAttributions)
	if err != nil {
		return nil, err
	}
	directSprints, err := githubWorkItemsRawMessages(rows.Sprints)
	if err != nil {
		return nil, err
	}
	directDependencies, err := githubWorkItemsRawMessages(rows.Dependencies)
	if err != nil {
		return nil, err
	}
	directInteractions, err := githubWorkItemsRawMessages(rows.Interactions)
	if err != nil {
		return nil, err
	}
	directReopens, err := githubWorkItemsRawMessages(rows.ReopenEvents)
	if err != nil {
		return nil, err
	}
	directTransitions, err := githubWorkItemsRawMessages(rows.StatusTransitions)
	if err != nil {
		return nil, err
	}
	directItems, err := githubWorkItemsRawMessages(rows.WorkItems)
	if err != nil {
		return nil, err
	}
	return BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{
		AIAttribution:                  directAI,
		EstimateCoverageMetricsDaily:   derived["estimate_coverage_metrics_daily"],
		InvestmentClassificationsDaily: derived["investment_classifications_daily"],
		InvestmentMetricsDaily:         derived["investment_metrics_daily"],
		IssueTypeMetricsDaily:          derived["issue_type_metrics_daily"],
		Sprints:                        directSprints,
		WorkItemCycleTimes:             derived["work_item_cycle_times"],
		WorkItemDependencies:           directDependencies,
		WorkItemInteractions:           directInteractions,
		WorkItemMetricsDaily:           derived["work_item_metrics_daily"],
		WorkItemReopenEvents:           directReopens,
		WorkItemStateDurationsDaily:    derived["work_item_state_durations_daily"],
		WorkItemTeamAttributions:       derived["work_item_team_attributions"],
		WorkItemTransitions:            directTransitions,
		WorkItemUserMetricsDaily:       derived["work_item_user_metrics_daily"],
		WorkItems:                      directItems,
	})
}

func githubWorkItemsRawMessages[T any](values []T) ([]json.RawMessage, error) {
	rows := make([]json.RawMessage, 0, len(values))
	for _, value := range values {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, ErrEffectRecoveryUnsafe
		}
		rows = append(rows, encoded)
	}
	return rows, nil
}

type githubWorkItemsUsageKey struct {
	transport   string
	routeFamily string
	dimension   string
}

type githubWorkItemsUsageAccumulator map[githubWorkItemsUsageKey]int

func (usage githubWorkItemsUsageAccumulator) add(observation GitHubWorkItemsRequestUsage) {
	if observation.RequestCount <= 0 {
		return
	}
	key := githubWorkItemsUsageKey{
		transport: observation.Transport, routeFamily: observation.RouteFamily,
		dimension: observation.Dimension,
	}
	usage[key] += observation.RequestCount
}

func (usage githubWorkItemsUsageAccumulator) snapshot() []GitHubWorkItemsRequestUsage {
	result := make([]GitHubWorkItemsRequestUsage, 0, len(usage))
	for key, count := range usage {
		result = append(result, GitHubWorkItemsRequestUsage{
			Transport: key.transport, RouteFamily: key.routeFamily,
			Dimension: key.dimension, RequestCount: count,
		})
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Transport != result[right].Transport {
			return result[left].Transport < result[right].Transport
		}
		if result[left].RouteFamily != result[right].RouteFamily {
			return result[left].RouteFamily < result[right].RouteFamily
		}
		return result[left].Dimension < result[right].Dimension
	})
	return result
}

func (usage githubWorkItemsUsageAccumulator) wrap(err error) error {
	if err == nil {
		return nil
	}
	return &GitHubWorkItemsRouteError{Cause: err, Usage: usage.snapshot()}
}

func (usage githubWorkItemsUsageAccumulator) wrapRoute(
	err error,
	evidence FetchEvidence,
	incomplete []GitHubWorkItemsIncomplete,
) error {
	if err == nil {
		return nil
	}
	return &GitHubWorkItemsRouteError{
		Cause: err, Usage: usage.snapshot(), Evidence: evidence,
		Incomplete: append([]GitHubWorkItemsIncomplete(nil), incomplete...),
	}
}

var _ CompleteRouteHandler = GitHubWorkItemsRouteHandler{}
var _ githubWorkItemsProjectPolicy = GitHubProjectV2Fetcher{}
