package drilldown

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// fixtureRowScanner replays a fixed slice of pre-built rows -- same shape
// as quadrant/investmentexplain's own fixtureRowScanner, extended for
// this package's own destination types (**string/**time.Time/**int64 for
// Nullable columns, matching workgraph/pr.go's established
// pointer-to-pointer Scan convention for a Nullable destination).
type fixtureRowScanner struct {
	rows  [][]any
	index int
}

func (s *fixtureRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *fixtureRowScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	for i, d := range dest {
		switch typed := d.(type) {
		case *string:
			v, _ := row[i].(string)
			*typed = v
		case *uint32:
			v, _ := row[i].(uint32)
			*typed = v
		case **string:
			if row[i] == nil {
				*typed = nil
				continue
			}
			v := row[i].(string)
			*typed = &v
		case *time.Time:
			*typed = row[i].(time.Time)
		case **time.Time:
			if row[i] == nil {
				*typed = nil
				continue
			}
			v := row[i].(time.Time)
			*typed = &v
		case **int64:
			if row[i] == nil {
				*typed = nil
				continue
			}
			v := row[i].(int64)
			*typed = &v
		default:
			return fmt.Errorf("fixtureRowScanner: unsupported dest type %T", d)
		}
	}
	return nil
}

func (s *fixtureRowScanner) Err() error   { return nil }
func (s *fixtureRowScanner) Close() error { return nil }

// fakeQueryClient dispatches on query text, same convention as
// quadrant/investmentexplain's own fakeQueryClient.
type fakeQueryClient struct {
	t       *testing.T
	handler func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

func (c fakeQueryClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return c.handler(c.t, query, bindings)
}

func bindingValue(bindings []dhclickhouse.Binding, name string) (any, bool) {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

func day(y int, m time.Month, d, hh, mm, ss int) time.Time {
	return time.Date(y, m, d, hh, mm, ss, 0, time.UTC)
}

// loadGolden decodes a testdata JSON file -- captured from the REAL
// Python build_..._response equivalent for this route (time_window +
// scope_filter_for_metric + fetch_pull_requests, monkeypatched
// ClickHouse readers, DrilldownResponse(items=...).model_dump(mode=
// "json")) via a one-off `uv run python3` invocation (see this package's
// PRItem doc comment and the PR's own TEST-EVIDENCE for the exact
// script), THEN hand-adjusted to replace each naive Python timestamp
// with its RFC 3339 form (created_at/merged_at/first_review_at) -- the
// declared, already-established divergence PRItem's own doc comment
// explains. Every other field is byte-identical to the captured Python
// JSON. DisallowUnknownFields makes a field-name mismatch (a key this Go
// type does not declare) a hard test failure, not a silent drop.
func loadGolden(t *testing.T, name string) PRsResponse {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp PRsResponse
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return resp
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

// TestGoldenGetDefaultOrgScope replays testdata/get_default_org_scope.json:
// scope.level="org", scope.ids=[] -- ResolveRepoFilterIDs short-circuits
// to no repo refs at all (no ClickHouse round trip for scope resolution,
// same as Python's resolve_repo_filter_ids for this shape), so the fake
// client only needs to answer the main pull-request query. One row
// exercises every non-null field including a computed
// review_latency_hours; the other exercises every nullable field as
// NULL at once (title/author_name/merged_at/first_review_at/
// review_latency_hours).
func TestGoldenGetDefaultOrgScope(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM git_pull_requests") {
			t.Fatalf("unexpected query for get-default-org-scope fixture:\n%s", query)
		}
		if strings.Contains(query, "scope_ids") {
			t.Fatalf("org scope with no ids must not add a scope filter:\n%s", query)
		}
		if v, _ := bindingValue(bindings, "org_id"); v != "org-acme" {
			t.Fatalf("org_id binding = %v, want org-acme", v)
		}
		if v, _ := bindingValue(bindings, "limit"); v != 50 {
			t.Fatalf("limit binding = %v, want 50 (GET has no limit param)", v)
		}
		return &fixtureRowScanner{rows: [][]any{
			{
				"12345678-1234-5678-1234-567812345678", uint32(42),
				"Add retry logic", "alice",
				day(2024, 1, 10, 9, 0, 0),
				day(2024, 1, 11, 15, 30, 0),
				day(2024, 1, 10, 14, 0, 0),
				int64(5),
			},
			{
				"87654321-4321-8765-4321-876543218765", uint32(7),
				nil, nil,
				day(2024, 1, 9, 3, 15, 0),
				nil, nil, nil,
			},
		}}, nil
	}}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildPRsResponse(context.Background(), reader, "org-acme", PRParams{
		StartDay:   day(2024, 1, 1, 0, 0, 0),
		EndDay:     day(2024, 1, 15, 0, 0, 0),
		ScopeLevel: "org",
		Limit:      50,
	})
	if err != nil {
		t.Fatalf("BuildPRsResponse: %v", err)
	}
	want := loadGolden(t, "get_default_org_scope.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenGetRepoScopeExplicitDates replays
// testdata/get_repo_scope_explicit_dates.json: scope.level="repo",
// scope.ids=["acme/webapp"] (a non-UUID ref, resolved by repos.repo
// name). Exercises ResolveRepoFilterIDs' "repo" branch and the resulting
// " AND pr.repo_id IN {scope_ids:Array(String)}" clause.
func TestGoldenGetRepoScopeExplicitDates(t *testing.T) {
	const resolvedRepoID = "11111111-1111-1111-1111-111111111111"

	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		// Checked BEFORE the bare "FROM repos" case: the pull-request
		// query now embeds its own "FROM repos FINAL" org-scoping
		// subquery (see fetchPullRequestsQuery's doc comment), so it
		// matches "FROM repos" too -- git_pull_requests must be
		// recognized first or this fixture would answer the wrong rows
		// for it.
		case strings.Contains(query, "FROM git_pull_requests"):
			if v, _ := bindingValue(bindings, "scope_ids"); fmt.Sprint(v) != fmt.Sprint([]string{resolvedRepoID}) {
				t.Fatalf("scope_ids binding = %v, want [%s]", v, resolvedRepoID)
			}
			if v, _ := bindingValue(bindings, "org_id"); v != "org-acme" {
				t.Fatalf("org_id binding = %v, want org-acme", v)
			}
			return &fixtureRowScanner{rows: [][]any{
				{
					resolvedRepoID, uint32(101),
					"Fix flaky test", "bob",
					day(2024, 2, 2, 8, 0, 0),
					day(2024, 2, 2, 20, 0, 0),
					nil, nil,
				},
			}}, nil
		case strings.Contains(query, "FROM repos"):
			if v, _ := bindingValue(bindings, "repo_name"); v != "acme/webapp" {
				t.Fatalf("repo_name binding = %v, want acme/webapp", v)
			}
			return &fixtureRowScanner{rows: [][]any{{resolvedRepoID}}}, nil
		default:
			t.Fatalf("unexpected query for get-repo-scope fixture:\n%s", query)
			return nil, nil
		}
	}}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildPRsResponse(context.Background(), reader, "org-acme", PRParams{
		StartDay:   day(2024, 2, 1, 0, 0, 0),
		EndDay:     day(2024, 2, 6, 0, 0, 0),
		ScopeLevel: "repo",
		ScopeIDs:   []string{"acme/webapp"},
		Limit:      50,
	})
	if err != nil {
		t.Fatalf("BuildPRsResponse: %v", err)
	}
	want := loadGolden(t, "get_repo_scope_explicit_dates.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenPostTeamScopeLimitFallback replays
// testdata/post_team_scope_limit_fallback.json: scope.level="team",
// scope.ids=["team-x"], POST body limit=0 (the "payload.limit or 50"
// falsy fallback -- the route file resolves this to 50 BEFORE calling
// BuildPRsResponse, so PRParams.Limit is already 50 here, same division
// of labor investment_explain_route.go's buildExplainOptions
// establishes). Exercises ResolveRepoFilterIDs' "team" branch:
// resolveRepoIDsForTeams (user_metrics_daily) then resolveRepoID per
// resolved id (a UUID-shaped ref, resolved by repos.id).
func TestGoldenPostTeamScopeLimitFallback(t *testing.T) {
	const resolvedRepoID = "22222222-2222-2222-2222-222222222222"

	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "FROM user_metrics_daily"):
			if v, _ := bindingValue(bindings, "team_ids"); fmt.Sprint(v) != fmt.Sprint([]string{"team-x"}) {
				t.Fatalf("team_ids binding = %v, want [team-x]", v)
			}
			return &fixtureRowScanner{rows: [][]any{{resolvedRepoID}}}, nil
		// Checked BEFORE the bare "FROM repos" case -- see the sibling
		// switch's own comment above.
		case strings.Contains(query, "FROM git_pull_requests"):
			if v, _ := bindingValue(bindings, "limit"); v != 50 {
				t.Fatalf("limit binding = %v, want 50 (limit=0 falls back)", v)
			}
			return &fixtureRowScanner{rows: [][]any{
				{
					resolvedRepoID, uint32(3),
					"Team-x change", "carol",
					day(2024, 3, 1, 0, 0, 0),
					nil,
					day(2024, 3, 1, 6, 30, 0),
					int64(6),
				},
			}}, nil
		case strings.Contains(query, "FROM repos"):
			if v, _ := bindingValue(bindings, "repo_id"); v != resolvedRepoID {
				t.Fatalf("repo_id binding = %v, want %s", v, resolvedRepoID)
			}
			return &fixtureRowScanner{rows: [][]any{{resolvedRepoID}}}, nil
		default:
			t.Fatalf("unexpected query for post-team-scope fixture:\n%s", query)
			return nil, nil
		}
	}}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildPRsResponse(context.Background(), reader, "org-acme", PRParams{
		StartDay:   day(2024, 2, 1, 0, 0, 0),
		EndDay:     day(2024, 3, 2, 0, 0, 0),
		ScopeLevel: "team",
		ScopeIDs:   []string{"team-x"},
		Limit:      50,
	})
	if err != nil {
		t.Fatalf("BuildPRsResponse: %v", err)
	}
	want := loadGolden(t, "post_team_scope_limit_fallback.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// loadIssuesGolden is loadGolden's IssuesResponse counterpart -- same
// capture technique (time_window + scope_filter_for_metric + fetch_issues,
// monkeypatched query_dicts, DrilldownResponse(items=...).model_dump(mode=
// "json")) via a one-off `uv run python3` invocation (see this package's
// IssueItem doc comment and the PR's own TEST-EVIDENCE for the exact
// script), THEN hand-adjusted to replace each naive Python timestamp with
// its RFC 3339 form (started_at/completed_at) -- the declared divergence
// IssueItem's own doc comment explains. Every other field is byte-identical
// to the captured Python JSON.
func loadIssuesGolden(t *testing.T, name string) IssuesResponse {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp IssuesResponse
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return resp
}

// issuesFixtureRowScanner replays a fixed slice of pre-built rows in
// fetchIssuesQuery's own column order (work_item_id, provider, status,
// team_id, cycle_time_hours, lead_time_hours, started_at, completed_at) --
// a package-local scanner rather than reusing fixtureRowScanner because
// IssueItem's nullable numeric columns need a **float64 case
// fixtureRowScanner's PRItem-shaped switch does not declare (PRItem has no
// nullable float field).
type issuesFixtureRowScanner struct {
	rows  [][]any
	index int
}

func (s *issuesFixtureRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *issuesFixtureRowScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	for i, d := range dest {
		switch typed := d.(type) {
		case *string:
			v, _ := row[i].(string)
			*typed = v
		case **string:
			if row[i] == nil {
				*typed = nil
				continue
			}
			v := row[i].(string)
			*typed = &v
		case **float64:
			if row[i] == nil {
				*typed = nil
				continue
			}
			v := row[i].(float64)
			*typed = &v
		case **time.Time:
			if row[i] == nil {
				*typed = nil
				continue
			}
			v := row[i].(time.Time)
			*typed = &v
		default:
			return fmt.Errorf("issuesFixtureRowScanner: unsupported dest type %T", d)
		}
	}
	return nil
}

func (s *issuesFixtureRowScanner) Err() error   { return nil }
func (s *issuesFixtureRowScanner) Close() error { return nil }

// TestGoldenGetDefaultOrgScopeIssues replays
// testdata/get_default_org_scope_issues.json: scope.level="org",
// scope.ids=[] -- scope_filter_for_metric's own asymmetry means NO team
// filter is added (metric_scope="team" but filters.scope.level!="team"),
// so the fake client only needs to answer the one fetch_issues query. One
// row exercises every non-null field; the other exercises every nullable
// field as NULL at once (team_id/cycle_time_hours/lead_time_hours/
// started_at/completed_at).
func TestGoldenGetDefaultOrgScopeIssues(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM work_item_cycle_times") {
			t.Fatalf("unexpected query for get-default-org-scope-issues fixture:\n%s", query)
		}
		if strings.Contains(query, "scope_ids") {
			t.Fatalf("org scope must not add a team scope filter:\n%s", query)
		}
		if v, _ := bindingValue(bindings, "org_id"); v != "org-acme" {
			t.Fatalf("org_id binding = %v, want org-acme", v)
		}
		if v, _ := bindingValue(bindings, "limit"); v != 50 {
			t.Fatalf("limit binding = %v, want 50 (GET has no limit param)", v)
		}
		return &issuesFixtureRowScanner{rows: [][]any{
			{
				"ghpr:acme/webapp#42", "github", "done", "team-platform",
				12.5, 30.25,
				day(2024, 1, 10, 9, 0, 0),
				day(2024, 1, 11, 15, 30, 0),
			},
			{
				"jira:PROJ-7", "jira", "backlog", nil,
				nil, nil,
				nil, nil,
			},
		}}, nil
	}}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildIssuesResponse(context.Background(), reader, "org-acme", IssueParams{
		StartDay:   day(2024, 1, 1, 0, 0, 0),
		EndDay:     day(2024, 1, 15, 0, 0, 0),
		ScopeLevel: "org",
		Limit:      50,
	})
	if err != nil {
		t.Fatalf("BuildIssuesResponse: %v", err)
	}
	want := loadIssuesGolden(t, "get_default_org_scope_issues.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenGetTeamScopeExplicitDatesIssues replays
// testdata/get_team_scope_explicit_dates_issues.json: scope.level="team",
// scope.ids=["team-y"] -- exercises scopeClauseTeam's non-empty branch and
// the resulting " AND t.team_id IN {scope_ids:Array(String)}" clause.
func TestGoldenGetTeamScopeExplicitDatesIssues(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM work_item_cycle_times") {
			t.Fatalf("unexpected query for get-team-scope-issues fixture:\n%s", query)
		}
		if !strings.Contains(query, "AND t.team_id IN {scope_ids:Array(String)}") {
			t.Fatalf("expected team scope filter in query:\n%s", query)
		}
		if v, _ := bindingValue(bindings, "scope_ids"); fmt.Sprint(v) != fmt.Sprint([]string{"team-y"}) {
			t.Fatalf("scope_ids binding = %v, want [team-y]", v)
		}
		if v, _ := bindingValue(bindings, "org_id"); v != "org-acme" {
			t.Fatalf("org_id binding = %v, want org-acme", v)
		}
		return &issuesFixtureRowScanner{rows: [][]any{
			{
				"ghpr:acme/webapp#101", "github", "in_review", "team-y",
				4.0, 8.0,
				day(2024, 2, 2, 8, 0, 0),
				day(2024, 2, 2, 20, 0, 0),
			},
		}}, nil
	}}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildIssuesResponse(context.Background(), reader, "org-acme", IssueParams{
		StartDay:   day(2024, 2, 1, 0, 0, 0),
		EndDay:     day(2024, 2, 6, 0, 0, 0),
		ScopeLevel: "team",
		ScopeIDs:   []string{"team-y"},
		Limit:      50,
	})
	if err != nil {
		t.Fatalf("BuildIssuesResponse: %v", err)
	}
	want := loadIssuesGolden(t, "get_team_scope_explicit_dates_issues.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenPostRepoScopeIgnoredLimitFallbackIssues replays
// testdata/post_repo_scope_ignored_limit_fallback_issues.json: POST body
// scope.level="repo" with explicit ids -- proving scope_filter_for_metric's
// own asymmetry (a non-team scope level adds NO filter on drilldown/issues,
// even with ids present) together with the "payload.limit or 50" fallback
// (limit=0 in PRParams here is already resolved to 50 by the route file,
// same division of labor TestGoldenPostTeamScopeLimitFallback documents for
// prs).
func TestGoldenPostRepoScopeIgnoredLimitFallbackIssues(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !strings.Contains(query, "FROM work_item_cycle_times") {
			t.Fatalf("unexpected query for post-repo-scope-ignored-issues fixture:\n%s", query)
		}
		if strings.Contains(query, "scope_ids") {
			t.Fatalf("repo scope must not add a team scope filter on drilldown/issues:\n%s", query)
		}
		if v, _ := bindingValue(bindings, "limit"); v != 50 {
			t.Fatalf("limit binding = %v, want 50 (limit=0 falls back)", v)
		}
		return &issuesFixtureRowScanner{rows: [][]any{
			{
				"gitlab:acme/svc!9", "gitlab", "done", "team-z",
				2.0, 3.5,
				day(2024, 3, 1, 0, 0, 0),
				day(2024, 3, 1, 6, 30, 0),
			},
		}}, nil
	}}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildIssuesResponse(context.Background(), reader, "org-acme", IssueParams{
		StartDay:   day(2024, 2, 23, 0, 0, 0),
		EndDay:     day(2024, 3, 2, 0, 0, 0),
		ScopeLevel: "repo",
		ScopeIDs:   []string{"some-repo"},
		Limit:      50,
	})
	if err != nil {
		t.Fatalf("BuildIssuesResponse: %v", err)
	}
	want := loadIssuesGolden(t, "post_repo_scope_ignored_limit_fallback_issues.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}
