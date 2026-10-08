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

// TestBlockedIssueSourceDedupsBeforeItFiltersZero pins migration 104's
// persisted-data contract at the API reader. A zero is the newest state of
// an item-day, so it must participate in argMax before the positive predicate
// runs. The outer window group then gives the result table one row per item.
func TestBlockedIssueSourceDedupsBeforeItFiltersZero(t *testing.T) {
	source, bindings := renderBlockedIssueWindowSource("team", []string{"team-a"})
	for _, want := range []string{
		"FROM work_item_blocked_durations_daily",
		"WHERE org_id = {org_id:String}",
		"day >= {start_day:Date} AND day < {end_day:Date}",
		"GROUP BY day, provider, work_item_id",
		"argMax(tuple(work_scope_id, team_id, team_name, duration_hours), computed_at)",
		"WHERE latest_snapshot.4 > 0",
		"AND b.team_id IN {scope_ids:Array(String)}",
		"GROUP BY provider, work_item_id",
		"argMax(team_name, day) AS team_name",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("blocked source missing %q:\n%s", want, source)
		}
	}
	if latest, positive := strings.Index(source, "argMax(tuple("), strings.Index(source, "WHERE latest_snapshot.4 > 0"); latest == -1 || positive == -1 || latest > positive {
		t.Fatalf("blocked source filters before latest snapshot selection:\n%s", source)
	}
	if v, ok := bindingValue(bindings, "scope_ids"); !ok || fmt.Sprint(v) != fmt.Sprint([]string{"team-a"}) {
		t.Fatalf("scope binding = %v (present=%t), want [team-a]", v, ok)
	}

	query := fmt.Sprintf(fetchBlockedIssuesQuery, source, "")
	if !strings.Contains(query, "nullIf(b.team_name, '') AS team_name") {
		t.Fatalf("blocked query does not serve team_name:\n%s", query)
	}
	if count, limit := strings.Index(query, "count() OVER () AS total_count"), strings.Index(query, "LIMIT {limit:UInt64}"); count == -1 || limit == -1 || count > limit {
		t.Fatalf("blocked count must be measured before the page limit:\n%s", query)
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

// TestBuildIssuesResponseBlockedOnlyReturnsAllProviderRowsAndWindowCount
// exercises the persisted source rather than the ordinary cycle-time query.
// The same source format is provider-agnostic, so every provider has a row in
// the result; Count is the unbounded number before the request's limit.
func TestBuildIssuesResponseBlockedOnlyReturnsAllProviderRowsAndWindowCount(t *testing.T) {
	allRows := [][]any{
		{"github:acme/api#1", "github", "team-a", "Team A", uint64(4)},
		{"gitlab:group/api#2", "gitlab", "team-a", "Team A", uint64(4)},
		{"jira:OPS-3", "jira", "team-a", "Team A", uint64(4)},
		{"linear:ENG-4", "linear", "team-a", nil, uint64(4)},
	}
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_item_cycle_times") || !strings.Contains(query, "FROM work_item_blocked_durations_daily") {
			t.Fatalf("blocked-only request read the wrong source:\n%s", query)
		}
		if v, _ := bindingValue(bindings, "org_id"); v != "org-acme" {
			t.Fatalf("org_id binding = %v, want org-acme", v)
		}
		if v, _ := bindingValue(bindings, "scope_ids"); fmt.Sprint(v) != fmt.Sprint([]string{"team-a"}) {
			t.Fatalf("scope_ids binding = %v, want [team-a]", v)
		}
		if v, _ := bindingValue(bindings, "limit"); v == 2 {
			return &fixtureRowScanner{rows: allRows[:2]}, nil
		}
		return &fixtureRowScanner{rows: allRows}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	params := IssueParams{
		StartDay: day(2026, 10, 1, 0, 0, 0), EndDay: day(2026, 10, 8, 0, 0, 0),
		ScopeLevel: "team", ScopeIDs: []string{"team-a"}, Limit: 50, BlockedOnly: true,
	}
	resp, err := BuildIssuesResponse(context.Background(), reader, "org-acme", params)
	if err != nil {
		t.Fatalf("BuildIssuesResponse: %v", err)
	}
	if resp.Count == nil || *resp.Count != 4 {
		t.Fatalf("Count = %v, want 4", resp.Count)
	}
	providers := map[string]bool{}
	for _, item := range resp.Items {
		providers[item.Provider] = true
		if item.Status != "blocked" || item.TeamID == nil || *item.TeamID != "team-a" {
			t.Fatalf("blocked item = %+v, want blocked with team-a", item)
		}
		// A provider row without a stored name serves null, never the key.
		wantName := "Team A"
		if item.Provider == "linear" {
			if item.TeamName != nil {
				t.Fatalf("%s item team_name = %q, want null", item.Provider, *item.TeamName)
			}
			continue
		}
		if item.TeamName == nil || *item.TeamName != wantName {
			t.Fatalf("%s item team_name = %v, want %q", item.Provider, item.TeamName, wantName)
		}
	}
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		if !providers[provider] {
			t.Fatalf("blocked response omitted provider %q: %+v", provider, resp.Items)
		}
	}

	params.Limit = 2
	resp, err = BuildIssuesResponse(context.Background(), reader, "org-acme", params)
	if err != nil {
		t.Fatalf("BuildIssuesResponse limited: %v", err)
	}
	if len(resp.Items) != 2 || resp.Count == nil || *resp.Count != 4 {
		t.Fatalf("limited blocked response = %+v, want two items and total count 4", resp)
	}
}

func TestBuildIssuesResponseBlockedOnlyEmptyResultReportsMeasuredZero(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "count() OVER ()") {
			t.Fatalf("blocked-only request did not measure its count:\n%s", query)
		}
		return &fixtureRowScanner{}, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	resp, err := BuildIssuesResponse(context.Background(), reader, "org-acme", IssueParams{BlockedOnly: true, Limit: 50})
	if err != nil {
		t.Fatalf("BuildIssuesResponse: %v", err)
	}
	if resp.Count == nil || *resp.Count != 0 || len(resp.Items) != 0 {
		t.Fatalf("empty blocked response = %+v, want items=[] count=0", resp)
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
