package explain

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"
)

// queryCapturingClient records the last statement/bindings it was asked
// to run and answers with a fixed row set -- same convention as
// drilldown/repofilter_test.go's own copy.
type queryCapturingClient struct {
	rows         [][]any
	lastQuery    string
	lastBindings []dhclickhouse.Binding
}

func (c *queryCapturingClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.lastQuery = query
	c.lastBindings = bindings
	return &fixtureRowScanner{rows: c.rows}, nil
}

func bindingValue(bindings []dhclickhouse.Binding, name string) (any, bool) {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

// assertSameDepth pins that every marker in markers appears in query AND
// sits at the same parenthesis nesting depth as the first one -- same
// convention as drilldown/repofilter_test.go's own copy.
func assertSameDepth(t *testing.T, query string, markers ...string) {
	t.Helper()
	if len(markers) == 0 {
		return
	}
	depths := sqlshape.Depths(query)
	var wantDepth int
	for i, marker := range markers {
		idx := strings.Index(query, marker)
		if idx == -1 {
			t.Fatalf("query missing %q:\n%s", marker, query)
		}
		depth := depths[idx]
		if i == 0 {
			wantDepth = depth
			continue
		}
		if depth != wantDepth {
			t.Fatalf("%q sits at nesting depth %d but %q sits at depth %d -- both must scope the "+
				"SAME statement\nquery:\n%s", marker, depth, markers[0], wantDepth, query)
		}
	}
}

// TestResolveRepoIDByUUIDQueryShape pins resolveRepoID's UUID-ref branch:
// repos (ReplacingMergeTree) is read with FINAL, never raw, and the org
// filter sits inside this SAME read, at the SAME nesting depth as FINAL
// and the id equality.
func TestResolveRepoIDByUUIDQueryShape(t *testing.T) {
	client := &queryCapturingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, _, err := reader.resolveRepoID(context.Background(), "12345678-1234-5678-1234-567812345678", "org-1"); err != nil {
		t.Fatalf("resolveRepoID: %v", err)
	}
	assertSameDepth(t, client.lastQuery, "FROM repos FINAL", "toString(id) = {repo_id:String}", "org_id = {org_id:String}")
}

// TestResolveRepoIDsForTeamsQueryShape pins that user_metrics_daily
// (ReplacingMergeTree(computed_at), team_id outside its sorting key) is
// read with FINAL, org filter at the same depth as FINAL and team_id.
func TestResolveRepoIDsForTeamsQueryShape(t *testing.T) {
	client := &queryCapturingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := reader.resolveRepoIDsForTeams(context.Background(), []string{"team-x"}, "org-1"); err != nil {
		t.Fatalf("resolveRepoIDsForTeams: %v", err)
	}
	assertSameDepth(t, client.lastQuery, "FROM user_metrics_daily FINAL", "team_id IN {team_ids:Array(String)}", "org_id = {org_id:String}")
}

// TestResolveScopeDisplayNamesRepoQueryShape pins that resolve_scope_
// display_names' "repo" branch reads repos (ReplacingMergeTree(last_synced))
// with FINAL -- the declared divergence from Python's raw read (see
// identity.go's own doc comment) -- org filter at the same depth.
func TestResolveScopeDisplayNamesRepoQueryShape(t *testing.T) {
	client := &queryCapturingClient{rows: [][]any{}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_ = reader.resolveScopeDisplayNames(context.Background(), "org-1", "repo", []string{"11111111-1111-1111-1111-111111111111"})
	assertSameDepth(t, client.lastQuery, "FROM repos FINAL", "toString(id) IN {scope_ids:Array(String)}", "org_id = {org_id:String}")
}

// TestResolveScopeDisplayNamesTeamQueryShape is the "team" branch's own
// copy -- teams is ReplacingMergeTree(updated_at) since its original
// creation (002_teams.sql), same declared divergence.
func TestResolveScopeDisplayNamesTeamQueryShape(t *testing.T) {
	client := &queryCapturingClient{rows: [][]any{}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_ = reader.resolveScopeDisplayNames(context.Background(), "org-1", "team", []string{"team-x"})
	assertSameDepth(t, client.lastQuery, "FROM teams FINAL", "toString(id) IN {scope_ids:Array(String)}", "org_id = {org_id:String}")
}

// TestMetricFromClauseQueryShape pins metricFromClause's own dedup
// subquery shape (class ruling (a)+(b)): argMax(...) dedup, org filter
// and any scope filter all sit INSIDE the same subquery, before the
// GROUP BY that collapses to one row per natural key. Uses a
// NON-nullable column (items_completed) so the marker is the plain
// argMax form; TestMetricValueProjectionUsesTupleArgMaxForNullableColumn
// is the Nullable-column shape's own test.
func TestMetricFromClauseQueryShape(t *testing.T) {
	clause := metricFromClause("work_item_metrics_daily", "items_completed", " AND team_id IN {scope_ids:Array(String)}", "start_day", "end_day")
	assertSameDepth(t, clause,
		"argMax(items_completed, computed_at)",
		"team_id IN {scope_ids:Array(String)}",
		"org_id = {org_id:String}",
	)
	if !strings.Contains(clause, "WHERE day >= {start_day:Date} AND day < {end_day:Date}") {
		t.Fatalf("clause missing start/end day window:\n%s", clause)
	}
}

// TestMetricValueProjectionUsesTupleArgMaxForNullableColumn pins the
// class ruling (b) fix: a Nullable(Float64) metric column
// (cycle_time_p50_hours, pr_first_review_p50_hours) is deduped via
// `(argMax(tuple(col), version)).1`, never a bare `argMax(col, version)`
// -- see metricValueProjection's own doc comment for why a bare argMax
// on a Nullable column can silently return a stale version.
func TestMetricValueProjectionUsesTupleArgMaxForNullableColumn(t *testing.T) {
	got := metricValueProjection("cycle_time_p50_hours")
	want := "(argMax(tuple(cycle_time_p50_hours), computed_at)).1 AS cycle_time_p50_hours"
	if got != want {
		t.Fatalf("metricValueProjection(cycle_time_p50_hours) = %q, want %q", got, want)
	}
	got = metricValueProjection("pr_first_review_p50_hours")
	want = "(argMax(tuple(pr_first_review_p50_hours), computed_at)).1 AS pr_first_review_p50_hours"
	if got != want {
		t.Fatalf("metricValueProjection(pr_first_review_p50_hours) = %q, want %q", got, want)
	}
	// A non-nullable column stays a plain argMax.
	if got := metricValueProjection("items_completed"); got != "argMax(items_completed, computed_at) AS items_completed" {
		t.Fatalf("metricValueProjection(items_completed) = %q, want plain argMax", got)
	}
}

// TestFetchMetricValueBindsDateAsString pins the {name:Date}-vs-time.Time
// formatting fix (internal/analytics/validate.go's own dateBindingValue,
// this package's metrics.go duplicates it) -- a bound {start_day:Date}/
// {end_day:Date} parameter must be a plain "YYYY-MM-DD" STRING, never a
// raw time.Time (the exact defect class six other call sites in this
// repo shared before that fix).
func TestFetchMetricValueBindsDateAsString(t *testing.T) {
	client := &queryCapturingClient{rows: [][]any{{12.5}}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	start := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	if _, err := reader.fetchMetricValue(context.Background(), "repo_metrics_daily", "total_loc_touched", "sum", start, end, "", nil, "org-1"); err != nil {
		t.Fatalf("fetchMetricValue: %v", err)
	}
	if v, _ := bindingValue(client.lastBindings, "start_day"); v != "2024-03-01" {
		t.Fatalf("start_day binding = %v (%T), want string \"2024-03-01\"", v, v)
	}
	if v, _ := bindingValue(client.lastBindings, "end_day"); v != "2024-03-15" {
		t.Fatalf("end_day binding = %v (%T), want string \"2024-03-15\"", v, v)
	}
}

// TestScopeFilterForMetricRepoUsesRealOrgID pins the declared, deliberate
// divergence from Python's own explain.py:150-152 (see response.go's
// scopeFilterForMetric doc comment): the repo-scope resolution chain
// must receive the CALLER's real org id, never "".
func TestScopeFilterForMetricRepoUsesRealOrgID(t *testing.T) {
	client := &queryCapturingClient{rows: [][]any{}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, _, err = reader.scopeFilterForMetric(context.Background(), "repo", "repo", []string{"acme/webapp"}, nil, "org-real")
	if err != nil {
		t.Fatalf("scopeFilterForMetric: %v", err)
	}
	if v, _ := bindingValue(client.lastBindings, "org_id"); v != "org-real" {
		t.Fatalf("resolveRepoID org_id binding = %v, want org-real (Python's own explain.py bug passes \"\")", v)
	}
}
