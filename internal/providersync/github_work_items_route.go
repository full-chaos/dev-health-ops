package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
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
//
// UNMET OBLIGATION — the activation layer must read these. Today nothing does:
// Collect is the only producer of the `incomplete` result key and the tree has
// no consumer. The degraded run is safe purely because this route is
// unregistered and hardcodes Watermark: nil on every path. Whoever registers
// the five-alias family must make alias watermark fan-out refuse to advance for
// a run carrying entries here, and must ship the test that proves a degraded
// run advances no alias watermark. Do not give this route a real watermark
// before that reader exists.
type GitHubWorkItemsIncomplete struct {
	Component string `json:"component"`
	SubjectID string `json:"subject_id,omitempty"`
	Cause     string `json:"cause"`
}

// githubWorkItemsOptionalIncompleteComponents are the collection phases that
// carry the ratified optional-data contract (D17, owner ruling 2026-08-06,
// which also resolves CHAOS-3188): an optional-data fetch failure must emit
// durable incompleteness evidence — never a silent omission, and never a
// whole-batch failure.
//
// The set is the complete list of sites `providers/github/provider.py` logs
// and continues past, so a persistently failing optional endpoint cannot block
// the five-alias family for a repository forever:
//
//	milestones               provider.py:202-217
//	issue_comments           provider.py:293-301
//	pull_requests            provider.py:339-343  (the /pulls listing itself)
//	pr_social                provider.py:369-402
//	pull_request_processing  provider.py:503-507  (the whole per-PR loop)
//
// provider.py:495-500 (the per-PR comment -> interaction conversion) has no
// entry because its failure mode does not exist in Go — see the note at the
// normalizeGitHubPullRequestBundle call site.
//
// Python satisfies only the continuing half — it logs and drops — which is the
// half of D17 it does not yet meet and is tracked separately for it. Do not
// "restore parity" by removing the recording; for this class the port is
// deliberately ahead of its source and D16's mirror rule does not apply.
//
// Components absent for a reason that is not an optional-data fetch failure at
// all are not listed here and still fail the unit closed. Projects v2 no longer
// has an entry of that kind: D18 retired the policy_pending seam, so an
// unwired Projects collector is a construction defect refused at Collect entry
// rather than a degradation recorded per batch.
var githubWorkItemsOptionalIncompleteComponents = map[string]bool{
	"milestones":              true,
	"issue_comments":          true,
	"pull_requests":           true,
	"pr_social":               true,
	"pull_request_processing": true,
}

// githubWorkItemsBlockingIncompleteCauses are failure classes that block the
// unit whatever component reports them, because they are not the provider
// declining to serve optional data:
//
//   - pagination_cap: the traversal bound was hit, so the collected set is
//     deterministically truncated. Landing it would bank a subset that every
//     later run reproduces identically — the recipe's "never both capped and
//     successful" rule, and what the REST side already does by returning
//     ErrPaginationCapExceeded rather than typed incompleteness.
//   - invalid_pagination: a missing or stalled cursor. That is a defect in our
//     own traversal, not a provider condition Python has any analogue for, and
//     it must surface as a failure rather than as a routine degradation entry.
//
// Both are produced only by the Go GraphQL social fetcher
// (gitHubWorkItemPRSocialFailureCause); no Python site can emit them.
var githubWorkItemsBlockingIncompleteCauses = map[string]bool{
	"pagination_cap":     true,
	"invalid_pagination": true,
}

// githubWorkItemsIncompleteIsOptional decides one entry. Cause is checked
// first: a blocking cause is blocking even on an optional component, which is
// the whole reason classification cannot key on Component alone.
func githubWorkItemsIncompleteIsOptional(partial GitHubWorkItemsIncomplete) bool {
	if githubWorkItemsBlockingIncompleteCauses[partial.Cause] {
		return false
	}
	return githubWorkItemsOptionalIncompleteComponents[partial.Component]
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

// githubWorkItemsProjectPolicy is the Projects v2 collection seam, ratified by
// D18: durable integration-scoped targets, the claim-resolved credential and
// client only, and no environment token or target fallback.
// GitHubProjectV2Fetcher is its production implementation.
//
// The seam remains an interface for test substitution, not for policy: there is
// no longer a legitimate "not decided yet" state, so Collect refuses a nil
// Projects outright instead of recording configured targets as an incomplete
// batch. A handler that reaches production without one is a construction
// defect, and "the code is not wired" must not be reported in the same
// vocabulary the provider uses to decline data.
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
	// Every destination column that receives normalizedAt is DateTime64(3), so
	// the nanoseconds a wall-clock now() carries cannot survive a round trip.
	// Truncating here rather than only inside REST.Collect (which truncates its
	// own by-value copy, leaving the PR-bundle and projects-v2 paths below
	// untouched) keeps the effect payload equal to what ClickHouse stores --
	// otherwise the readback compares .123456789 against a stored .123, answers
	// Absent for a row that landed, and the committer rewrites it on every
	// recovery pass forever. Same fix, same reason, as github_blame_route.go.
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	if handler.Deriver == nil {
		return CompleteRouteBatch{}, ErrGitHubWorkItemsDerivationsUnavailable
	}
	// D18 retired the policy_pending seam. This is deliberately NOT gated on
	// whether this particular claim happens to carry targets: a handler built
	// without a Projects collector is misconstructed for every claim, and
	// discovering that only on the first tenant that configured a project would
	// make the defect look like a tenant-specific data problem.
	if handler.Projects == nil {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
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
		socialComplete := socialResult.Complete()
		if !socialComplete {
			incomplete = append(incomplete, GitHubWorkItemsIncomplete{
				Component: "pr_social", Cause: socialResult.Incomplete.Cause,
			})
		}
		// An incomplete social batch must not cost us the pull requests
		// themselves: dropping them here would withhold the required
		// work-item/dependency/AI rows over an optional enrichment failure.
		// Python reaches the same place from the other side — a failed batch
		// leaves pr_events_by_number/pr_comments_by_number empty and every pull
		// request is still normalized, read with `.get(number, ())`
		// (provider.py:369-402).
		for _, pull := range restResult.PullRequests {
			subject := strconv.Itoa(pull.Number)
			payload, exists := socialResult.Payloads[pull.Number]
			if !exists && socialComplete {
				return CompleteRouteBatch{}, usage.wrap(providerfoundation.ErrGraphQLResponse)
			}
			adapted, adaptErr := adaptGitHubWorkItemPRSocialPayload(payload)
			if adaptErr != nil {
				// provider.py:503-507 wraps the WHOLE per-PR loop, so a failure
				// here stops the remaining pull requests and keeps every row
				// already collected — it does not fail the unit.
				incomplete = append(incomplete, GitHubWorkItemsIncomplete{
					Component: "pull_request_processing", SubjectID: subject,
					Cause: githubWorkItemsRESTFailureCause(adaptErr),
				})
				break
			}
			// provider.py:495-500 wraps the per-PR comment -> interaction
			// conversion in its own best-effort try. There is deliberately no
			// Go analogue here: the comments reaching this call were produced
			// by adaptGitHubWorkItemPRSocialPayload, which has already decoded
			// and re-marshalled every node, and neither
			// normalizeGitHubWorkItemComments nor the comment half of
			// extractGitHubWorkItemDependencies can fail on that input (the
			// only failure modes are a JSON decode error and a row that fails
			// validate, and adapter output can produce neither). A
			// retry-without-comments branch would be unreachable, so it is not
			// written rather than written and left unprovable. If a fallible
			// comment -> row step is ever added, this needs the same
			// continue-and-record treatment as the issue path.
			bundle, normalizeErr := normalizeGitHubPullRequestBundle(
				claim, restResult.RepoFullName, restResult.RepoID, pull.Payload,
				adapted.Events, adapted.Comments, handler.ResolveIdentity, normalizedAt,
			)
			if normalizeErr != nil {
				incomplete = append(incomplete, GitHubWorkItemsIncomplete{
					Component: "pull_request_processing", SubjectID: subject,
					Cause: githubWorkItemsRESTFailureCause(normalizeErr),
				})
				break
			}
			appendGitHubWorkItemRows(&rows, bundle)
		}
	}

	// "disabled" means this claim's integration configured no durable targets —
	// the only remaining reason Projects v2 contributes nothing. It is NOT the
	// state an operator lands in by setting GITHUB_PROJECTS_V2 in the process
	// environment: D18 puts the environment outside the Go route for
	// CREDENTIALS AND TARGETS, so env-only configuration reads here as no
	// configuration at all and issues zero GraphQL requests.
	//
	// Scoped deliberately, because "no environment reads at all" would be
	// false: github_work_items_rows.go:869 reads GITHUB_LINEAR_LINKBACK_BOTS
	// for bot-identity classification. That is presentation policy, not
	// credentials or targets, and D18 does not reach it.
	//
	// CHAOS-3506 adds the startup-readiness warning that makes this silence
	// audible at boot, which is where it belongs — reading the environment on
	// this path to warn about it would reintroduce the dependency D18 removes.
	projectState := "disabled"
	if len(projectTargets) > 0 {
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
	finishGitHubWorkItemsEvidence(&evidence, rows)
	// EVERY entry decides, not the first: a batch can mix optional degradation
	// with a blocking entry, and the blocking one is appended LATER than the
	// optional one (REST milestones record first; the per-PR and social phases
	// append after), so a check that stops early reads clean on exactly the
	// ordering production produces.
	for _, partial := range incomplete {
		if !githubWorkItemsIncompleteIsOptional(partial) {
			return CompleteRouteBatch{}, usage.wrapRoute(
				ErrGitHubWorkItemsIncomplete, evidence, incomplete,
			)
		}
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
