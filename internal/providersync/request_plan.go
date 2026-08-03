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
		estimates = githubRequestPlan(dataset, spanDays)
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
	default:
		return nil
	}
}

func githubRequestPlan(dataset string, spanDays int) []RequestEstimate {
	if dataset != "cicd" && dataset != "tests" && dataset != "deployments" {
		return nil
	}
	routeFamily := dataset
	if dataset == "cicd" {
		routeFamily = "tests"
	}
	return []RequestEstimate{
		{BudgetRESTCore, 4 * spanDays, "low", routeFamily},
		{BudgetContentsBlob, 2 * spanDays, "low", routeFamily},
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
