package providersync

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"
)

func TestProviderRequestPlansMatchLivePythonBudgetFunctions(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	root := filepath.Join(packageDir, "..", "..")
	output, err := exec.Command(
		python,
		filepath.Join(packageDir, "testdata", "python_provider_budget_oracle.py"),
		filepath.Join(root, "src", "dev_health_ops", "providers", "github", "budget.py"),
		filepath.Join(root, "src", "dev_health_ops", "providers", "gitlab", "budget.py"),
		filepath.Join(root, "src", "dev_health_ops", "providers", "linear", "budget.py"),
		filepath.Join(root, "src", "dev_health_ops", "providers", "jira", "budget.py"),
		filepath.Join(root, "src", "dev_health_ops", "providers", "launchdarkly", "budget.py"),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python provider budget oracle: %v: %s", err, output)
	}
	var cases []struct {
		Provider          string            `json:"provider"`
		Dataset           string            `json:"dataset"`
		SpanDays          int               `json:"span_days"`
		Flags             map[string]bool   `json:"flags"`
		Estimates         []RequestEstimate `json:"estimates"`
		ActualRouteFamily string            `json:"actual_route_family"`
		ActualDimension   string            `json:"actual_dimension"`
	}
	if err := json.Unmarshal(output, &cases); err != nil {
		t.Fatalf("decode Python provider budget oracle: %v: %s", err, output)
	}
	if len(cases) == 0 {
		t.Fatal("Python provider budget oracle returned no cases")
	}
	for _, test := range cases {
		got := ProviderRequestPlan(test.Provider, test.Dataset, test.SpanDays, test.Flags)
		want := expectedGoPlan(test.Provider, test.Dataset, test.SpanDays, test.Estimates)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf(
				"%s/%s span=%d flags=%v estimates=%+v want=%+v",
				test.Provider,
				test.Dataset,
				test.SpanDays,
				test.Flags,
				got,
				want,
			)
		}
		if test.ActualRouteFamily != "" {
			matched := false
			for _, estimate := range got {
				if estimate.RouteFamily == test.ActualRouteFamily &&
					estimate.Dimension == test.ActualDimension {
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf(
					"%s/%s actual resolver=(%s,%s) request plan=%+v",
					test.Provider, test.Dataset, test.ActualRouteFamily,
					test.ActualDimension, got,
				)
			}
		}
	}
}

func TestProviderRequestPlansFailClosedForUnknownRoutes(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ provider, dataset string }{
		{"linear", "incidents"},
		{"jira", "feature-flags"},
		{"launchdarkly", "projects"},
		{"unknown", "work-items"},
	} {
		if plan := ProviderRequestPlan(test.provider, test.dataset, 1, nil); len(plan) != 0 {
			t.Fatalf("%s/%s plan=%+v", test.provider, test.dataset, plan)
		}
	}
}

func TestGitHubWorkItemRequestPlansCoverEveryAliasAndPRPressure(t *testing.T) {
	t.Parallel()
	for _, dataset := range []string{
		"work-items", "work-item-labels", "work-item-projects",
		"work-item-history", "work-item-comments",
	} {
		restUnits := 3
		if dataset == "work-items" {
			restUnits = 6
		}
		withoutPRs := ProviderRequestPlan(
			"github", dataset, 3, map[string]bool{"sync_prs": false},
		)
		if want := []RequestEstimate{{
			Dimension: BudgetRESTCore, Units: restUnits,
			Confidence: "medium", RouteFamily: "work_items",
		}}; !reflect.DeepEqual(withoutPRs, want) {
			t.Fatalf("%s without PRs=%+v want=%+v", dataset, withoutPRs, want)
		}
		withPRs := ProviderRequestPlan(
			"github", dataset, 3, map[string]bool{"sync_prs": true},
		)
		if want := []RequestEstimate{
			{
				Dimension: BudgetGraphQLCost, Units: 9,
				Confidence: "medium", RouteFamily: "work_item_prs",
			},
			{
				Dimension: BudgetSecondaryAbuseRisk, Units: 1,
				Confidence: "low", RouteFamily: "work_item_prs",
			},
			{
				Dimension: BudgetRESTCore, Units: restUnits,
				Confidence: "medium", RouteFamily: "work_items",
			},
		}; !reflect.DeepEqual(withPRs, want) {
			t.Fatalf("%s with PRs=%+v want=%+v", dataset, withPRs, want)
		}
	}
}

// expectedGoPlan is the live Python oracle's own estimates, widened with
// every DOCUMENTED, computed Go-only delta -- a named mechanism and
// direction, never a blanket allowance. Every dataset not named here
// stays exact-equal to Python: a new, unnamed delta on ANY other
// provider/dataset combination is a test failure, not a silent pass.
//
// An override replaces Python's own term of the same (Dimension,
// RouteFamily) with Go's real, registry-enumerated-kind-coverage-proven
// value (request_budget_kind_coverage_test.go) rather than asserting
// Python's term unchanged: Python is not the oracle for correctness on
// these two datasets (github/deployments' base term and github/prs' REST
// term each under-reserved against a real Collect run before this ticket,
// proven by an executed cell in that file), only for which OTHER terms
// exist at all. An extra adds a term Python has no equivalent for.
func expectedGoPlan(provider, dataset string, spanDays int, pythonEstimates []RequestEstimate) []RequestEstimate {
	var overrides, extras []RequestEstimate
	switch {
	case provider == "github" && dataset == "deployments":
		// The base rest_core term is overridden, not left as Python's
		// flat 4*spanDays: an executed 1000-deployment run
		// (TestGitHubDeploymentsPlannedCoversCountedInsideStatedDomain)
		// measured the real repo-metadata + releases-list +
		// deployments-list cost at up to githubDeploymentsListPageBudget
		// pages each, which 4*spanDays never counted at any spanDays.
		// The two enrichment terms (statuses, pull-lookup) remain pure
		// additions -- github_deployments_route.go's per-deployment
		// statuses and SHA-to-pull-request lookups, both scaled by the
		// Collect loop's own hard per-call cap
		// (defaultGitHubDeploymentsMax), not spanDays: deployment volume
		// is not bounded by calendar window length the way this file's
		// other spanDays-scaled terms assume.
		overrides = []RequestEstimate{{
			Dimension: BudgetRESTCore, Units: 1 + 2*githubDeploymentsListPageBudget,
			Confidence: "low", RouteFamily: "deployments",
		}}
		extras = []RequestEstimate{
			{
				Dimension: BudgetRESTCore, Units: maxDeploymentStatusPages * defaultGitHubDeploymentsMax,
				Confidence: "low", RouteFamily: "deployments",
			},
			{
				Dimension: BudgetRESTCore, Units: defaultGitHubDeploymentsMax,
				Confidence: "low", RouteFamily: "deployments",
			},
		}
	case provider == "gitlab" && dataset == "deployments":
		// gitlab_deployments_route.go's per-deployment merge-request
		// lookup (fetchGitLabDeploymentObjects, SinglePage: true) --
		// exactly one REST request per in-window deployment, no Python
		// equivalent, scaled by gitLabDeploymentsMaximumPerPage (the
		// real single-page cap the whole route's List fetch is bound
		// by, not defaultGitLabDeploymentsMax). See request_plan.go's
		// "deployments" case doc comment for the full mechanism.
		extras = []RequestEstimate{{
			Dimension: BudgetRESTCore, Units: gitLabDeploymentsMaximumPerPage,
			Confidence: "low", RouteFamily: "pipelines",
		}}
	case provider == "github" && (dataset == "prs" || dataset == "pr-reviews" || dataset == "pr-comments"):
		// The REST term is overridden against a NAMED assumption
		// (github_prs_plan.go): Python's own
		// max(2,2*spanDays) already under-reserves against a single real
		// in-window PR (repo-metadata + list + one detail fetch = 3 REST
		// requests, proven by TestGitHubPRsPlannedCoversCountedInsideStatedDomain),
		// so Python is not the oracle for this term either. pr-reviews
		// and pr-comments alias to the same estimator case and are not
		// separately named here.
		overrides = []RequestEstimate{{
			Dimension: BudgetRESTCore, Units: githubPRsRESTUnits(spanDays),
			Confidence: "medium", RouteFamily: "prs",
		}}
	default:
		return pythonEstimates
	}
	byKey := func(estimate RequestEstimate) [2]string {
		return [2]string{estimate.Dimension, estimate.RouteFamily}
	}
	overrideByKey := make(map[[2]string]RequestEstimate, len(overrides))
	for _, override := range overrides {
		overrideByKey[byKey(override)] = override
	}
	want := make([]RequestEstimate, 0, len(pythonEstimates)+len(extras))
	for _, estimate := range pythonEstimates {
		if override, ok := overrideByKey[byKey(estimate)]; ok {
			want = append(want, override)
			continue
		}
		want = append(want, estimate)
	}
	want = append(want, extras...)
	sort.SliceStable(want, func(left, right int) bool {
		if want[left].RouteFamily == want[right].RouteFamily {
			return want[left].Dimension < want[right].Dimension
		}
		return want[left].RouteFamily < want[right].RouteFamily
	})
	return want
}
