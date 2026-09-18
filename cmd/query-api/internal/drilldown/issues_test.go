package drilldown

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

func TestScopeClauseTeamEmpty(t *testing.T) {
	sql, bindings := scopeClauseTeam("team", nil)
	if sql != "" || bindings != nil {
		t.Fatalf("scopeClauseTeam(team, nil) = %q, %v, want empty", sql, bindings)
	}
	sql, bindings = scopeClauseTeam("team", []string{})
	if sql != "" || bindings != nil {
		t.Fatalf("scopeClauseTeam(team, []) = %q, %v, want empty", sql, bindings)
	}
}

// TestScopeClauseTeamNonTeamLevelNoFilter pins scope_filter_for_metric's
// own asymmetry (filtering.py:138-147): an "org" or "repo" scope level
// applies NO filter on drilldown/issues, even with non-empty ids -- unlike
// drilldown/prs, which always resolves SOME repo filter.
func TestScopeClauseTeamNonTeamLevelNoFilter(t *testing.T) {
	for _, level := range []string{"org", "repo", "service", "developer", ""} {
		sql, bindings := scopeClauseTeam(level, []string{"team-x"})
		if sql != "" || bindings != nil {
			t.Fatalf("scopeClauseTeam(%q, [team-x]) = %q, %v, want empty (only scope.level==team filters)", level, sql, bindings)
		}
	}
}

func TestScopeClauseTeamNonEmpty(t *testing.T) {
	sql, bindings := scopeClauseTeam("team", []string{"team-a", "team-b"})
	if !strings.Contains(sql, "AND t.team_id IN {scope_ids:Array(String)}") {
		t.Fatalf("scopeClauseTeam sql = %q", sql)
	}
	if len(bindings) != 1 || bindings[0].Name != "scope_ids" {
		t.Fatalf("scopeClauseTeam bindings = %+v", bindings)
	}
	got, ok := bindings[0].Value.([]string)
	if !ok || len(got) != 2 || got[0] != "team-a" || got[1] != "team-b" {
		t.Fatalf("scopeClauseTeam bindings value = %+v", bindings[0].Value)
	}
}

// renderFetchIssuesQuery renders fetchIssuesQuery with the same
// substitution BuildIssuesResponse applies, for tests that only care about
// the query's fixed structure (no scope filter, no settings clause).
func renderFetchIssuesQuery() string {
	return fmt.Sprintf(fetchIssuesQuery, primaryWorkItemTeamAttributionSource, "", "")
}

// TestFetchIssuesQueryDedupsBothTables pins that neither ReplacingMergeTree
// table this query reads (work_item_cycle_times, work_item_team_attributions)
// is ever read raw: FINAL appears on both.
func TestFetchIssuesQueryDedupsBothTables(t *testing.T) {
	rendered := renderFetchIssuesQuery()
	for _, want := range []string{
		"FROM work_item_cycle_times AS wct FINAL",
		"FROM work_item_team_attributions FINAL",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("fetchIssuesQuery missing %q:\n%s", want, rendered)
		}
	}
}

// TestFetchIssuesQueryOrgFilterOnPrimaryRead pins the class ruling this
// query follows for its own top-level table: wct.org_id sits at the SAME
// nesting depth as wct's own FINAL FROM clause (depth 0 -- wct is the
// primary read, not a joined side table), never behind a join evaluated
// after a cross-tenant merge.
func TestFetchIssuesQueryOrgFilterOnPrimaryRead(t *testing.T) {
	rendered := renderFetchIssuesQuery()

	const finalMarker = "FROM work_item_cycle_times AS wct FINAL"
	const orgPredicate = "wct.org_id = {org_id:String}"
	depths := sqlshape.Depths(rendered)

	finalIdx := strings.Index(rendered, finalMarker)
	if finalIdx == -1 {
		t.Fatalf("fetchIssuesQuery missing %q:\n%s", finalMarker, rendered)
	}
	orgIdx := strings.Index(rendered, orgPredicate)
	if orgIdx == -1 {
		t.Fatalf("fetchIssuesQuery missing %q:\n%s", orgPredicate, rendered)
	}

	finalDepth, orgDepth := depths[finalIdx], depths[orgIdx]
	if orgDepth != finalDepth {
		t.Fatalf("%q sits at nesting depth %d but %q sits at depth %d -- org_id must scope the SAME "+
			"statement as wct's own FINAL dedup", orgPredicate, orgDepth, finalMarker, finalDepth)
	}
	if finalDepth != 0 {
		t.Fatalf("%q sits at nesting depth %d, want 0 (wct is the top-level read, not a subquery):\n%s",
			finalMarker, finalDepth, rendered)
	}
}

// TestFetchIssuesQueryTeamAttributionOrgFilterInsideSubquery pins that the
// team-attribution join source carries its OWN org filter at the SAME
// nesting depth as its own FINAL dedup, inside the subquery (depth > 0) --
// the same structural guard quadrant.go's sibling copy of this subquery
// satisfies, so the LEFT JOIN can never widen this route's org scope.
func TestFetchIssuesQueryTeamAttributionOrgFilterInsideSubquery(t *testing.T) {
	rendered := renderFetchIssuesQuery()
	depths := sqlshape.Depths(rendered)

	const finalMarker = "FROM work_item_team_attributions FINAL"
	const orgPredicate = "org_id = {org_id:String}"

	finalIdx := strings.Index(rendered, finalMarker)
	if finalIdx == -1 {
		t.Fatalf("fetchIssuesQuery missing %q:\n%s", finalMarker, rendered)
	}
	orgIdx := strings.Index(rendered, orgPredicate)
	if orgIdx == -1 {
		t.Fatalf("fetchIssuesQuery missing %q:\n%s", orgPredicate, rendered)
	}

	finalDepth, orgDepth := depths[finalIdx], depths[orgIdx]
	if orgDepth != finalDepth {
		t.Fatalf("%q sits at nesting depth %d but %q sits at depth %d -- org_id must scope the SAME "+
			"statement as the team-attribution FINAL dedup source", orgPredicate, orgDepth, finalMarker, finalDepth)
	}
	if finalDepth == 0 {
		t.Fatalf("%q sits at the top level (depth 0), not nested inside the LEFT JOIN subquery:\n%s", finalMarker, rendered)
	}
}

// TestFetchIssuesQueryNoJoinOnWct pins that work_item_cycle_times is read
// as the top-level FROM target, never joined -- the same "not behind a
// join" guard fetch_pull_requests' own test pins for repos, restated here
// for its sibling table.
func TestFetchIssuesQueryNoJoinOnWct(t *testing.T) {
	rendered := renderFetchIssuesQuery()
	if strings.Contains(rendered, "JOIN work_item_cycle_times") {
		t.Fatalf("fetchIssuesQuery must not join work_item_cycle_times -- it is the top-level read:\n%s", rendered)
	}
}

// TestFetchIssuesQueryOrderByWorkItemIDTiebreak pins the deterministic
// tiebreak appended after completed_at DESC: this statement's own tie
// order (and therefore this plane's own cursor pagination) stays stable
// and reproducible across repeated calls, independent of ClickHouse's
// own unspecified order among rows sharing one completed_at value.
func TestFetchIssuesQueryOrderByWorkItemIDTiebreak(t *testing.T) {
	rendered := renderFetchIssuesQuery()
	const want = "ORDER BY wct.completed_at DESC, wct.work_item_id ASC"
	if !strings.Contains(rendered, want) {
		t.Fatalf("fetchIssuesQuery missing %q:\n%s", want, rendered)
	}
}

func TestBuildIssuesResponseNilReader(t *testing.T) {
	_, err := BuildIssuesResponse(context.Background(), nil, "org-1", IssueParams{})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

func TestBuildIssuesResponseQueryError(t *testing.T) {
	boom := errors.New("boom")
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, _ string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return nil, boom
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildIssuesResponse(context.Background(), reader, "org-1", IssueParams{ScopeLevel: "org", Limit: 50})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want wrapped boom", err)
	}
}

// TestBuildIssuesResponseEmptyItemsNotNil matches Python's
// DrilldownResponse(items=[]) -- an empty result must marshal as
// "items": [], never "items": null.
func TestBuildIssuesResponseEmptyItemsNotNil(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, _ string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	resp, err := BuildIssuesResponse(context.Background(), reader, "org-1", IssueParams{ScopeLevel: "org", Limit: 50})
	if err != nil {
		t.Fatalf("BuildIssuesResponse: %v", err)
	}
	if resp.Items == nil {
		t.Fatalf("Items is nil, want empty non-nil slice")
	}
	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(body) != `{"items":[]}` {
		t.Fatalf("marshal = %s, want {\"items\":[]}", body)
	}
}

// TestBuildIssuesResponseBindsExpectedParams pins the binding set (org_id,
// start_day/end_day in YYYY-MM-DD form, limit, and no scope_ids for an org
// scope) a call issues.
func TestBuildIssuesResponseBindsExpectedParams(t *testing.T) {
	var seenBindings []dhclickhouse.Binding
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM work_item_cycle_times") {
			t.Fatalf("unexpected query: %s", query)
		}
		seenBindings = bindings
		return &fixtureRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildIssuesResponse(context.Background(), reader, "org-acme", IssueParams{
		StartDay:   day(2024, 1, 1, 0, 0, 0),
		EndDay:     day(2024, 1, 15, 0, 0, 0),
		ScopeLevel: "org",
		Limit:      50,
	})
	if err != nil {
		t.Fatalf("BuildIssuesResponse: %v", err)
	}
	if v, _ := bindingValue(seenBindings, "start_day"); v != "2024-01-01" {
		t.Fatalf("start_day binding = %v, want 2024-01-01", v)
	}
	if v, _ := bindingValue(seenBindings, "end_day"); v != "2024-01-15" {
		t.Fatalf("end_day binding = %v, want 2024-01-15", v)
	}
	if v, _ := bindingValue(seenBindings, "org_id"); v != "org-acme" {
		t.Fatalf("org_id binding = %v, want org-acme", v)
	}
	if v, _ := bindingValue(seenBindings, "limit"); v != 50 {
		t.Fatalf("limit binding = %v, want 50", v)
	}
	if _, ok := bindingValue(seenBindings, "scope_ids"); ok {
		t.Fatalf("org scope with no team filter must not bind scope_ids")
	}
}

// TestBuildIssuesResponseTeamScopeBindsScopeIDs pins that a team scope
// binds scope_ids and the rendered query carries the team filter clause.
func TestBuildIssuesResponseTeamScopeBindsScopeIDs(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "AND t.team_id IN {scope_ids:Array(String)}") {
			t.Fatalf("expected team scope filter in query:\n%s", query)
		}
		if v, _ := bindingValue(bindings, "scope_ids"); fmt.Sprint(v) != fmt.Sprint([]string{"team-x"}) {
			t.Fatalf("scope_ids binding = %v, want [team-x]", v)
		}
		return &fixtureRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildIssuesResponse(context.Background(), reader, "org-acme", IssueParams{
		StartDay:   day(2024, 1, 1, 0, 0, 0),
		EndDay:     day(2024, 1, 15, 0, 0, 0),
		ScopeLevel: "team",
		ScopeIDs:   []string{"team-x"},
		Limit:      50,
	})
	if err != nil {
		t.Fatalf("BuildIssuesResponse: %v", err)
	}
}

// TestBuildIssuesResponseTeamScopeEmptyIDsNoFilter pins the third emitted-
// statement shape scopeClauseTeam's own branch table covers -- team scope
// with an EMPTY id list -- at the SAME full BuildIssuesResponse level
// TestBuildIssuesResponseBindsExpectedParams (org) and
// TestBuildIssuesResponseTeamScopeBindsScopeIDs (team, non-empty ids)
// already pin, closing the gap TestScopeClauseTeamEmpty only covers at the
// scopeClauseTeam unit level: no team predicate, no scope_ids binding,
// same as an org-scoped request. Removing scopeClauseTeam's own `len(teamIDs)
// == 0` guard turns this red.
func TestBuildIssuesResponseTeamScopeEmptyIDsNoFilter(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "AND t.team_id IN {scope_ids:Array(String)}") {
			t.Fatalf("team scope with no ids must not emit the team filter clause:\n%s", query)
		}
		if _, ok := bindingValue(bindings, "scope_ids"); ok {
			t.Fatalf("team scope with no ids must not bind scope_ids")
		}
		return &fixtureRowScanner{rows: nil}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = BuildIssuesResponse(context.Background(), reader, "org-acme", IssueParams{
		StartDay:   day(2024, 1, 1, 0, 0, 0),
		EndDay:     day(2024, 1, 15, 0, 0, 0),
		ScopeLevel: "team",
		ScopeIDs:   nil,
		Limit:      50,
	})
	if err != nil {
		t.Fatalf("BuildIssuesResponse: %v", err)
	}
}
