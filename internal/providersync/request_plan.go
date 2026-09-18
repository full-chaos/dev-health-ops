package providersync

import "sort"

const (
	BudgetRESTCore           = "rest_core"
	BudgetContentsBlob       = "contents_blob"
	BudgetSearch             = "search"
	BudgetGraphQLCost        = "graphql_cost"
	BudgetSecondaryAbuseRisk = "secondary_abuse_risk"
)

type RequestEstimate struct {
	Dimension   string `json:"dimension"`
	Units       int    `json:"units"`
	Confidence  string `json:"confidence"`
	RouteFamily string `json:"route_family"`
}

// ProviderRequestPlan is the frozen preflight reservation contract. Actual
// request accounting remains transport-owned; these estimates only decide
// whether a complete provider unit may start.
func ProviderRequestPlan(
	provider string,
	dataset string,
	spanDays int,
	flags map[string]bool,
) []RequestEstimate {
	if spanDays < 1 {
		spanDays = 1
	}
	var estimates []RequestEstimate
	switch provider {
	case "github":
		estimates = githubRequestPlan(dataset, spanDays, flags)
	case "gitlab":
		estimates = gitLabRequestPlan(dataset, spanDays)
	case "linear":
		estimates = linearRequestPlan(dataset, spanDays)
	case "jira":
		estimates = jiraRequestPlan(dataset, spanDays, flags)
	case "launchdarkly":
		if dataset == "feature-flags" {
			estimates = []RequestEstimate{
				{BudgetRESTCore, 2, "medium", "flags"},
				{BudgetRESTCore, 52, "low", "audit_log"},
				{BudgetRESTCore, 1, "medium", "code_refs"},
				{BudgetSecondaryAbuseRisk, 1, "low", "code_refs"},
			}
		}
	}
	sort.SliceStable(estimates, func(left, right int) bool {
		if estimates[left].RouteFamily == estimates[right].RouteFamily {
			return estimates[left].Dimension < estimates[right].Dimension
		}
		return estimates[left].RouteFamily < estimates[right].RouteFamily
	})
	return estimates
}

func gitLabRequestPlan(dataset string, spanDays int) []RequestEstimate {
	switch dataset {
	case "commits":
		// Ported from providers/gitlab/budget.py::_dataset_estimates. The
		// canonical Python estimator groups project metadata and repository
		// commit pages under the project REST family and scales its two-request
		// floor linearly with the requested window.
		return []RequestEstimate{{
			Dimension: BudgetRESTCore, Units: max(2, 2*spanDays),
			Confidence: "medium", RouteFamily: "project",
		}}
	case "commit-stats":
		return []RequestEstimate{{
			Dimension: BudgetRESTCore, Units: max(4, 4*spanDays),
			Confidence: "low", RouteFamily: "project",
		}}
	case "cicd", "tests":
		return []RequestEstimate{{
			Dimension: BudgetRESTCore, Units: max(6, 6*spanDays),
			Confidence: "low", RouteFamily: "pipelines",
		}}
	case "deployments":
		// Same base "cicd"/"tests" shape Python's gitlab/budget.py still
		// has for this dataset, unchanged, PLUS one term Python does not:
		// fetchGitLabDeploymentObjects' per-deployment merge-request
		// lookup (gitlab_deployments_route.go's Collect loop,
		// /repository/commits/{sha}/merge_requests, SinglePage: true, so
		// exactly one REST request per in-window deployment) has no
		// Python equivalent -- no GitLab deployments-sync module exists
		// under src/dev_health_ops/providers/gitlab/ beyond budget.py.
		//
		// The added term scales by gitLabDeploymentsMaximumPerPage, NOT
		// defaultGitLabDeploymentsMax: fetchGitLabDeploymentObjects
		// (the same helper both the deployments list fetch and the
		// per-deployment merge-request lookup use) is unconditionally
		// SinglePage, so the deployments list itself can never return
		// more than perPage records however large MaxDeployments is set
		// -- an executed probe with 100 deployments measured the real
		// per-deployment request count exactly, proving
		// defaultGitLabDeploymentsMax (1000) over-reserved this term by
		// roughly 10x against what the route can ever actually request in
		// one Collect call.
		// request_plan_test.go's live-oracle comparison states this exact
		// delta (mechanism + direction) rather than leaving this dataset
		// out of the strict comparison.
		return []RequestEstimate{
			{BudgetRESTCore, max(6, 6*spanDays), "low", "pipelines"},
			{BudgetRESTCore, gitLabDeploymentsMaximumPerPage, "low", "pipelines"},
		}
	case "incidents":
		return []RequestEstimate{
			{Dimension: BudgetRESTCore, Units: max(1, spanDays), Confidence: "medium", RouteFamily: "issues"},
			{Dimension: BudgetRESTCore, Units: 1, Confidence: "high", RouteFamily: "project"},
		}
	default:
		return nil
	}
}

func githubRequestPlan(dataset string, spanDays int, flags map[string]bool) []RequestEstimate {
	switch dataset {
	case "prs", "pr-reviews", "pr-comments":
		// github/prs is a dispatched, plannable route; its GraphQL and
		// abuse-risk terms are Python's own (github/budget.py). The GraphQL
		// term holds inside githubPRsAssumedReviewsPerPR for every spanDays:
		// ceil(prs/50) + prs*(ceil(reviews/100)-1) = 1 + prs <= 4*spanDays.
		// Every input below is one Collect does not bound, so each is a
		// NAMED assumption with a stated range (github_prs_plan.go), not a
		// derived cap. Reserving for Collect's hard caps instead
		// (nativeMaxPages list pages, up to nativeMaxPages*nativePerPage
		// detail fetches, gitHubPullRequestReviewMaxPages GraphQL requests)
		// would reserve about 10,101 REST units against the 5 this term
		// reserves at spanDays=1 (roughly 2,000x) and 100 GraphQL requests
		// against 4 (25x) for a route whose ordinary run is a handful of
		// requests -- the over-reservation starves every other unit sharing
		// the bucket, which is the failure a reservation exists to prevent.
		// The executed cells past each range
		// (TestGitHubPRsOutsideStatedDomain*) show the deficit an input
		// outside its range produces.
		return []RequestEstimate{
			{BudgetRESTCore, githubPRsRESTUnits(spanDays), "medium", "prs"},
			{BudgetGraphQLCost, max(4, 4*spanDays), "medium", "pr_social"},
			{BudgetSecondaryAbuseRisk, 1, "low", "pr_social"},
		}
	case "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments":
		floor := 1
		if dataset == "work-items" {
			floor = 2
		}
		estimates := []RequestEstimate{{
			Dimension: BudgetRESTCore, Units: max(floor, floor*spanDays),
			Confidence: "medium", RouteFamily: "work_items",
		}}
		if flags["sync_prs"] {
			estimates = append(estimates,
				RequestEstimate{
					Dimension: BudgetGraphQLCost, Units: max(3, 3*spanDays),
					Confidence: "medium", RouteFamily: "work_item_prs",
				},
				RequestEstimate{
					Dimension: BudgetSecondaryAbuseRisk, Units: 1,
					Confidence: "low", RouteFamily: "work_item_prs",
				},
			)
		}
		return estimates
	case "cicd", "tests":
		routeFamily := "tests"
		return []RequestEstimate{
			{BudgetRESTCore, 4 * spanDays, "low", routeFamily},
			{BudgetContentsBlob, 2 * spanDays, "low", routeFamily},
		}
	case "deployments":
		// Same base shape "cicd"/"tests" always had (and Python's own
		// github/budget.py still has, unchanged) -- this dataset's TWO
		// terms Python does not, both from github_deployments_route.go's
		// Collect loop and neither with a Python equivalent (no
		// deployments sync module exists under
		// src/dev_health_ops/providers/github/ beyond budget.py/
		// code_client.go): fetchGitHubDeploymentsPage's per-deployment
		// statuses lookup (up to maxDeploymentStatusPages REST requests
		// per deployment, deriving started_at/finished_at/status) and its
		// per-deployment SHA-to-pull-request lookup (exactly one REST
		// request per deployment, page-bounded at 1 in the Collect loop
		// itself, finding the PR that merged the deployed commit).
		//
		// All three REST_CORE terms below scale by the Collect loop's own
		// hard per-call cap (defaultGitHubDeploymentsMax), not spanDays:
		// deployment volume is not bounded by calendar window length the
		// way this file's other spanDays-scaled terms assume (a burst of
		// many deployments in one day is common and ordinary, unlike e.g.
		// commit or work-item volume).
		//
		// The first term replaces a flat 4*spanDays this dataset inherited,
		// unexamined, from the "cicd"/"tests" shape: an executed registry-
		// enumerated kind-coverage proof (request_budget_kind_coverage_test.go)
		// showed the repo-metadata fetch, the releases list, and the
		// deployments list are each real REST_CORE requests this dataset's
		// Collect makes that no earlier term counted at all -- at
		// defaultGitHubDeploymentsMax=1000 deployments the deployments-list
		// and releases-list fetches can each cost up to
		// githubDeploymentsListPageBudget (10) requests, proven by a 1000-
		// deployment executed run measuring 4012 real requests against a
		// 4004-unit plan (the flat 4 accounted for none of the list
		// pagination). githubDeploymentsListPageBudget is exactly the pages
		// value Collect itself derives from maxDeployments for both list
		// fetches (see its doc comment) at the domain
		// TestGitHubDeploymentsPlannedCoversCountedInsideStatedDomain holds
		// to: handler.MaxDeployments left at its zero value. A handler
		// constructed with a larger override moves the route's real pages
		// past this constant -- see
		// TestGitHubDeploymentsOutsideStatedDomainOvershootsWithNoReconciliation.
		return []RequestEstimate{
			{BudgetRESTCore, 1 + 2*githubDeploymentsListPageBudget, "low", "deployments"},
			{BudgetContentsBlob, 2 * spanDays, "low", "deployments"},
			{BudgetRESTCore, maxDeploymentStatusPages * defaultGitHubDeploymentsMax, "low", "deployments"},
			{BudgetRESTCore, defaultGitHubDeploymentsMax, "low", "deployments"},
		}
	default:
		return nil
	}
}

func linearRequestPlan(dataset string, spanDays int) []RequestEstimate {
	scaled := func(floor int, weight ...int) int {
		if spanDays <= 1 {
			return floor
		}
		perDay := 1
		if len(weight) > 0 && weight[0] > 1 {
			perDay = weight[0]
		}
		return max(floor, floor*spanDays*perDay)
	}
	switch dataset {
	case "work-items":
		return []RequestEstimate{
			{BudgetGraphQLCost, 1, "medium", "teams"},
			{BudgetGraphQLCost, scaled(5, 2), "low", "issues"},
			{BudgetGraphQLCost, 2, "low", "cycles"},
			{BudgetGraphQLCost, scaled(2), "low", "comments"},
			{BudgetGraphQLCost, scaled(1), "low", "attachments"},
			{BudgetGraphQLCost, scaled(2), "low", "history"},
			{BudgetGraphQLCost, scaled(2), "low", "relations"},
		}
	case "work-item-labels":
		return []RequestEstimate{
			{BudgetGraphQLCost, 1, "medium", "teams"},
			{BudgetGraphQLCost, 1, "medium", "team_members"},
		}
	case "work-item-projects":
		return []RequestEstimate{{BudgetGraphQLCost, 2, "medium", "projects"}}
	case "work-item-history":
		return []RequestEstimate{{BudgetGraphQLCost, scaled(3), "low", "history"}}
	case "work-item-comments":
		return []RequestEstimate{{BudgetGraphQLCost, scaled(3), "low", "comments"}}
	default:
		return nil
	}
}

func jiraRequestPlan(
	dataset string,
	spanDays int,
	flags map[string]bool,
) []RequestEstimate {
	scaled := func(floor int) int { return max(floor, floor*spanDays) }
	switch dataset {
	case "work-item-labels", "work-item-projects":
		return []RequestEstimate{{BudgetRESTCore, 1, "high", "jira_metadata"}}
	case "incidents":
		return []RequestEstimate{
			{BudgetRESTCore, scaled(1), "medium", "jira_metadata"},
			{BudgetSearch, scaled(1), "medium", "jira_jql"},
			{BudgetRESTCore, 100_000, "low", "jira_jsm_incident_admission"},
		}
	case "work-items", "work-item-history", "work-item-comments":
		estimates := []RequestEstimate{
			{BudgetSearch, scaled(2), "medium", "jira_jql"},
			{BudgetRESTCore, scaled(2), "medium", "jira_issue_enrichment"},
		}
		if dataset == "work-item-comments" {
			estimates = append(estimates, RequestEstimate{
				BudgetRESTCore, scaled(2), "low", "jira_comments",
			})
		}
		if flags["jira_fetch_worklogs"] || flags["fetch_worklogs"] {
			estimates = append(estimates, RequestEstimate{
				BudgetRESTCore, scaled(3), "low", "jira_worklogs",
			})
		}
		if flags["atlassian_gql_enabled"] || flags["gql_enabled"] {
			estimates = append(estimates, RequestEstimate{
				BudgetGraphQLCost, scaled(3), "medium", "jira_gql_enrichment",
			})
		}
		return estimates
	default:
		return nil
	}
}
