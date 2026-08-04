package providersync

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
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
		if !reflect.DeepEqual(got, test.Estimates) {
			t.Fatalf(
				"%s/%s span=%d flags=%v estimates=%+v want=%+v",
				test.Provider,
				test.Dataset,
				test.SpanDays,
				test.Flags,
				got,
				test.Estimates,
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
