// Pins the binding dedup requirement: every ReplacingMergeTree table this
// package reads (repos, teams, work_items, ci_pipeline_runs,
// work_item_cycle_times, user_metrics_daily) is read FINAL, with org_id
// filtered in the SAME top-level statement as the FINAL source -- never
// a separate subquery an outer WHERE only narrows afterward. Depth-based
// (sqlshape.Depths), matching quadrant/sankey's own precedent test.
package home

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/teamscope"
)

// teamScopeAsOf is the fixed instant every team-scope assertion in this
// file resolves ownership at, so an assertion states the instant it is
// about rather than depending on when the test ran.
var teamScopeAsOf = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

const orgIDPredicate = "org_id = {org_id:String}"

// assertOrgIDSameDepthAsMarkerNamed is assertOrgIDSameDepthAsMarker's
// generalisation for a condition string that carries a caller-chosen
// predicate (e.g. teamscope.RepoCondition's own uniquely-named org-id
// binding) instead of the package's plain org_id/{org_id:String} pair.
func assertOrgIDSameDepthAsMarkerNamed(t *testing.T, query, marker, predicate string) {
	t.Helper()
	depths := sqlshape.Depths(query)
	markerIdx := strings.Index(query, marker)
	if markerIdx < 0 {
		t.Fatalf("expected marker %q in query:\n%s", marker, query)
	}
	markerDepth := depths[markerIdx]

	rest := query[markerIdx+len(marker):]
	predIdx := strings.Index(rest, predicate)
	if predIdx < 0 {
		t.Fatalf("expected %q after %q, got:\n%s", predicate, marker, rest)
	}
	absolutePredIdx := markerIdx + len(marker) + predIdx
	predDepth := depths[absolutePredIdx]

	if predDepth != markerDepth {
		t.Fatalf("%q sits at nesting depth %d but %q sits at depth %d -- both must scope the SAME statement",
			predicate, predDepth, marker, markerDepth)
	}
}

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

// TestScopeFilterForMetricTeamScopeNeverRoundTripsForRepoIDs is a
// regression pin: a repo-scoped metric filtered by team never asks the
// client to resolve or verify a repo-id LIST for that team as its own
// query. The prior shape did exactly that (one query to list every
// matching repo_id) -- a team whose true matching-repo count is large
// made that query's own result set large enough to exceed the read-only
// client's per-statement row ceiling, degrading the whole request
// instead of answering it. With no explicit repo refs alongside the team
// scope, this call must make ZERO client round trips: the team's repo_id
// membership is a condition inside the CALLER's eventual statement, never
// a standalone read of its own.
func TestScopeFilterForMetricTeamScopeNeverRoundTripsForRepoIDs(t *testing.T) {
	calls := 0
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		calls++
		return &fixtureRowScanner{}, nil
	}}
	f := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{"team-x", "team-y"}}}
	filterSQL, bindings, err := scopeFilterForMetric(context.Background(), client, "repo", f, "org-1", "team_id", "repo_id", teamScopeAsOf)
	if err != nil {
		t.Fatalf("scopeFilterForMetric: %v", err)
	}
	if calls != 0 {
		t.Fatalf("scopeFilterForMetric made %d client round trip(s) for a team-only scope, want 0:\nfilterSQL: %s", calls, filterSQL)
	}
	if !strings.Contains(filterSQL, "repo_id IN (") || !strings.Contains(filterSQL, teamscope.Marker) {
		t.Fatalf("filterSQL missing the pushed-down team membership condition:\n%s", filterSQL)
	}
	if strings.Contains(filterSQL, "repo_id IN {scope_ids:Array(String)}") {
		t.Fatalf("filterSQL carries an explicit-repo clause with no explicit repos requested:\n%s", filterSQL)
	}
	var sawIDs bool
	for _, b := range bindings {
		if b.Name == teamscope.BindingTeamIDs {
			sawIDs = true
			if fmt.Sprint(b.Value) != "[team-x team-y]" {
				t.Fatalf("%s binding = %v, want [team-x team-y]", teamscope.BindingTeamIDs, b.Value)
			}
		}
	}
	if !sawIDs {
		t.Fatalf("bindings = %+v, want %s", bindings, teamscope.BindingTeamIDs)
	}
}

// TestScopeFilterForMetricUnionsExplicitReposWithTeamScope pins the union
// semantics resolve_repo_filter_ids' own Python shape has: an explicit
// what.repos ref alongside a team scope means EITHER condition can match,
// not both required. The explicit ref still round-trips through
// resolveRepoIDs (bounded: one call per explicit ref, not per matching
// repo), the team side stays pushed down.
func TestScopeFilterForMetricUnionsExplicitReposWithTeamScope(t *testing.T) {
	const resolvedRepoID = "55555555-5555-5555-5555-555555555555"
	calls := 0
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		calls++
		return &fixtureRowScanner{rows: [][]any{{resolvedRepoID}}}, nil
	}}
	f := Filters{Scope: ScopeFilter{Level: "team", IDs: []string{"team-x"}}, What: WhatFilter{Repos: []string{resolvedRepoID}}}
	filterSQL, bindings, err := scopeFilterForMetric(context.Background(), client, "repo", f, "org-1", "team_id", "repo_id", teamScopeAsOf)
	if err != nil {
		t.Fatalf("scopeFilterForMetric: %v", err)
	}
	if calls != 1 {
		t.Fatalf("scopeFilterForMetric made %d client round trip(s), want exactly 1 (resolving the explicit ref)", calls)
	}
	if !strings.HasPrefix(filterSQL, " AND (repo_id IN {scope_ids:Array(String)} OR repo_id IN (") {
		t.Fatalf("filterSQL does not OR the explicit-repo and team conditions together:\n%s", filterSQL)
	}
	var sawScopeIDs, sawTeamIDs bool
	for _, b := range bindings {
		if b.Name == "scope_ids" {
			sawScopeIDs = true
			if fmt.Sprint(b.Value) != "["+resolvedRepoID+"]" {
				t.Fatalf("scope_ids binding = %v, want [%s]", b.Value, resolvedRepoID)
			}
		}
		if b.Name == teamscope.BindingTeamIDs {
			sawTeamIDs = true
			if fmt.Sprint(b.Value) != "[team-x]" {
				t.Fatalf("%s binding = %v, want [team-x]", teamscope.BindingTeamIDs, b.Value)
			}
		}
	}
	if !sawScopeIDs || !sawTeamIDs {
		t.Fatalf("bindings = %+v, want both scope_ids and %s", bindings, teamscope.BindingTeamIDs)
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
