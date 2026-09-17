// Pins the binding dedup requirement: every ReplacingMergeTree table this
// package reads (repos, teams, work_items, ci_pipeline_runs,
// work_item_cycle_times, user_metrics_daily) is read FINAL, with org_id
// filtered in the SAME top-level statement as the FINAL source -- never
// a separate subquery an outer WHERE only narrows afterward. Depth-based
// (sqlshape.Depths), matching quadrant/sankey's own precedent test.
package home

import (
	"context"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

const orgIDPredicate = "org_id = {org_id:String}"

func assertOrgIDSameDepthAsMarker(t *testing.T, query, marker string) {
	t.Helper()
	if strings.Contains(query, "LIMIT 1 BY") {
		t.Fatalf("contains a LIMIT-1-BY dedup subquery shape -- expected a plain FINAL read:\n%s", query)
	}
	depths := sqlshape.Depths(query)
	markerIdx := strings.Index(query, marker)
	if markerIdx < 0 {
		t.Fatalf("expected marker %q in query:\n%s", marker, query)
	}
	markerDepth := depths[markerIdx]

	rest := query[markerIdx+len(marker):]
	orgIdx := strings.Index(rest, orgIDPredicate)
	if orgIdx < 0 {
		t.Fatalf("expected %q after %q, got:\n%s", orgIDPredicate, marker, rest)
	}
	absoluteOrgIdx := markerIdx + len(marker) + orgIdx
	orgDepth := depths[absoluteOrgIdx]

	if orgDepth != markerDepth {
		t.Fatalf("org_id predicate sits at nesting depth %d but %q sits at depth %d -- "+
			"org_id must scope the SAME statement as the dedup source", orgDepth, marker, markerDepth)
	}
}

func TestFetchCoverageWorkItemCycleTimesReadsFinal(t *testing.T) {
	var captured []string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = append(captured, query)
		return &fixtureRowScanner{rows: [][]any{{0.0, 0.0}}}, nil
	}}
	if _, err := fetchCoverage(context.Background(), client, day(2024, 1, 1), day(2024, 1, 8), "org-1"); err != nil {
		t.Fatalf("fetchCoverage: %v", err)
	}
	var sawCycleTimesFinal bool
	for _, q := range captured {
		if strings.Contains(q, "work_item_cycle_times FINAL") {
			sawCycleTimesFinal = true
			assertOrgIDSameDepthAsMarker(t, q, "work_item_cycle_times FINAL")
		}
		if strings.Contains(q, "FROM repos FINAL") {
			assertOrgIDSameDepthAsMarker(t, q, "FROM repos FINAL")
		}
		if strings.Contains(q, "FROM repo_metrics_daily FINAL") {
			assertOrgIDSameDepthAsMarker(t, q, "FROM repo_metrics_daily FINAL")
		}
	}
	if !sawCycleTimesFinal {
		t.Fatal("expected at least one work_item_cycle_times FINAL read")
	}
}

func TestFetchSourceStatusesReadsEveryTableFinal(t *testing.T) {
	var captured string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = query
		return &fixtureRowScanner{}, nil
	}}
	if _, err := fetchSourceStatuses(context.Background(), client, day(2024, 1, 1), "org-1"); err != nil {
		t.Fatalf("fetchSourceStatuses: %v", err)
	}
	for _, marker := range []string{"FROM repos FINAL", "FROM work_items FINAL", "FROM ci_pipeline_runs FINAL"} {
		assertOrgIDSameDepthAsMarker(t, captured, marker)
	}
}

func TestResolveRepoIDReadsReposFinal(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		assertOrgIDSameDepthAsMarker(t, query, "FROM repos FINAL")
		return &fixtureRowScanner{}, nil
	}}
	if _, _, err := resolveRepoID(context.Background(), client, "checkout-service", "org-1"); err != nil {
		t.Fatalf("resolveRepoID: %v", err)
	}
}

func TestResolveRepoIDsForTeamsReadsUserMetricsDailyFinal(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		assertOrgIDSameDepthAsMarker(t, query, "FROM user_metrics_daily FINAL")
		return &fixtureRowScanner{}, nil
	}}
	if _, err := resolveRepoIDsForTeams(context.Background(), client, []string{"team-1"}, "org-1"); err != nil {
		t.Fatalf("resolveRepoIDsForTeams: %v", err)
	}
}

func TestResolveScopeLabelsReadsReposAndTeamsFinal(t *testing.T) {
	score := 0.5
	rows := []RiskRow{{Scope: "repo", ScopeID: "r1", Score: &score}, {Scope: "team", ScopeID: "t1", Score: &score}}
	var captured []string
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		captured = append(captured, query)
		return &fixtureRowScanner{}, nil
	}}
	resolveScopeLabels(context.Background(), client, "org-1", rows)
	var sawRepos, sawTeams bool
	for _, q := range captured {
		if strings.Contains(q, "FROM repos FINAL") {
			sawRepos = true
			assertOrgIDSameDepthAsMarker(t, q, "FROM repos FINAL")
		}
		if strings.Contains(q, "FROM teams FINAL") {
			sawTeams = true
			assertOrgIDSameDepthAsMarker(t, q, "FROM teams FINAL")
		}
	}
	if !sawRepos || !sawTeams {
		t.Fatalf("expected both a repos FINAL and a teams FINAL read, got queries: %v", captured)
	}
}
