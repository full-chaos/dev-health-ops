package people

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// TestSettingsFinalTopNSafeDisablesLazyMaterialization pins
// settingsFinalTopNSafe's own contract: the returned SETTINGS text must
// disable query_plan_optimize_lazy_materialization, not just carry
// max_execution_time. A regression here (e.g. someone "simplifying" this
// back to settingsMaxExecutionTime's shape) is exactly the class this
// file exists to catch.
func TestSettingsFinalTopNSafeDisablesLazyMaterialization(t *testing.T) {
	got := settingsFinalTopNSafe()
	const want = "query_plan_optimize_lazy_materialization = 0"
	if !strings.Contains(got, want) {
		t.Fatalf("settingsFinalTopNSafe() = %q, missing %q", got, want)
	}
	if !strings.Contains(got, "max_execution_time") {
		t.Fatalf("settingsFinalTopNSafe() = %q, missing max_execution_time", got)
	}
}

// TestFetchPersonIssuesSendsLazyMaterializationOff runtime-captures the
// actual query fetchPersonIssues sends and asserts it carries the
// lazy-materialization-off setting -- a regression that swaps
// settingsFinalTopNSafe() back for settingsMaxExecutionTime() at this
// call site (the actual defect class: a FINAL + LEFT JOIN + WHERE +
// ORDER BY + LIMIT read whose SELECT list carries columns referenced only
// in the select list) fails THIS test even though every other
// fetchPersonIssuesQuery shape test still passes.
func TestFetchPersonIssuesSendsLazyMaterializationOff(t *testing.T) {
	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &issuesRowScanner{}, nil
	}}

	_, err := fetchPersonIssues(context.Background(), client, []string{"alice"}, day(2026, 8, 1), day(2026, 9, 1), 50, nil, "org-1")
	if err != nil {
		t.Fatalf("fetchPersonIssues error: %v", err)
	}
	const want = "query_plan_optimize_lazy_materialization = 0"
	if !strings.Contains(captured, want) {
		t.Fatalf("fetchPersonIssues query missing %q:\n%s", want, captured)
	}
}

// TestFetchPersonPullRequestsSendsLazyMaterializationOff is
// TestFetchPersonIssuesSendsLazyMaterializationOff's sibling for
// fetchPersonPullRequests -- the second of this package's two affected
// reads.
func TestFetchPersonPullRequestsSendsLazyMaterializationOff(t *testing.T) {
	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &prsRowScanner{}, nil
	}}

	_, err := fetchPersonPullRequests(context.Background(), client, []string{"alice"}, day(2026, 8, 1), day(2026, 9, 1), 50, nil, "org-1")
	if err != nil {
		t.Fatalf("fetchPersonPullRequests error: %v", err)
	}
	const want = "query_plan_optimize_lazy_materialization = 0"
	if !strings.Contains(captured, want) {
		t.Fatalf("fetchPersonPullRequests query missing %q:\n%s", want, captured)
	}
}
