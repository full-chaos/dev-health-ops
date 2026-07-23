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
		filepath.Join(root, "src", "dev_health_ops", "providers", "linear", "budget.py"),
		filepath.Join(root, "src", "dev_health_ops", "providers", "jira", "budget.py"),
		filepath.Join(root, "src", "dev_health_ops", "providers", "launchdarkly", "budget.py"),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python provider budget oracle: %v: %s", err, output)
	}
	var cases []struct {
		Provider  string            `json:"provider"`
		Dataset   string            `json:"dataset"`
		SpanDays  int               `json:"span_days"`
		Flags     map[string]bool   `json:"flags"`
		Estimates []RequestEstimate `json:"estimates"`
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
