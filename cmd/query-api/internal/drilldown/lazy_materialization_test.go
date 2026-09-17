package drilldown

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// TestSettingsMaxExecutionTimeDisablesLazyMaterialization pins
// settingsMaxExecutionTime's own contract for this package: the returned
// SETTINGS text must disable query_plan_optimize_lazy_materialization,
// not just carry max_execution_time. A regression here (e.g. someone
// "simplifying" this back to a bare max_execution_time clause) is exactly
// the class this file exists to catch, for EVERY query in this package
// that composes its trailing SETTINGS clause through this one shared
// function -- prs.go and issues.go today, and any future FINAL + JOIN +
// ORDER BY + LIMIT reader this package gains.
func TestSettingsMaxExecutionTimeDisablesLazyMaterialization(t *testing.T) {
	got := settingsMaxExecutionTime()
	const want = "query_plan_optimize_lazy_materialization = 0"
	if !strings.Contains(got, want) {
		t.Fatalf("settingsMaxExecutionTime() = %q, missing %q", got, want)
	}
	if !strings.Contains(got, "max_execution_time") {
		t.Fatalf("settingsMaxExecutionTime() = %q, missing max_execution_time", got)
	}
}

// TestBuildIssuesResponseSendsLazyMaterializationOff runtime-captures the
// actual query BuildIssuesResponse sends and asserts it carries the
// lazy-materialization-off setting -- the FINAL + LEFT JOIN + WHERE +
// ORDER BY + LIMIT shape whose SELECT list carries columns referenced
// only in the select list (status, cycle_time_hours, lead_time_hours,
// started_at).
func TestBuildIssuesResponseSendsLazyMaterializationOff(t *testing.T) {
	var captured string
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &fixtureRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildIssuesResponse(context.Background(), reader, "org-1", IssueParams{
		StartDay: day(2024, 1, 1, 0, 0, 0),
		EndDay:   day(2024, 1, 15, 0, 0, 0),
		Limit:    50,
	})
	if err != nil {
		t.Fatalf("BuildIssuesResponse: %v", err)
	}
	const want = "query_plan_optimize_lazy_materialization = 0"
	if !strings.Contains(captured, want) {
		t.Fatalf("BuildIssuesResponse query missing %q:\n%s", want, captured)
	}
}

// TestBuildPRsResponseSendsLazyMaterializationOff is
// TestBuildIssuesResponseSendsLazyMaterializationOff's sibling for
// BuildPRsResponse (prs.go) -- the second of this package's two affected
// reads, sharing the same settingsMaxExecutionTime helper.
func TestBuildPRsResponseSendsLazyMaterializationOff(t *testing.T) {
	var captured string
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &fixtureRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildPRsResponse(context.Background(), reader, "org-1", PRParams{ScopeLevel: "org", Limit: 50})
	if err != nil {
		t.Fatalf("BuildPRsResponse: %v", err)
	}
	const want = "query_plan_optimize_lazy_materialization = 0"
	if !strings.Contains(captured, want) {
		t.Fatalf("BuildPRsResponse query missing %q:\n%s", want, captured)
	}
}
