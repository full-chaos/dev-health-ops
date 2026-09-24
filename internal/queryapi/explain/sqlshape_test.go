package explain

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/sqlshape"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// teamScopeAsOf is the fixed instant every team-scope assertion in this
// file resolves ownership at, so an assertion states the instant it is
// about rather than depending on when the test ran.
var teamScopeAsOf = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

// queryCapturingClient records the last statement/bindings it was asked
// to run and answers with a fixed row set -- same convention as
// drilldown/repofilter_test.go's own copy. calls counts every Query
// invocation, for a test that pins the ABSENCE of a round trip (a
// pushed-down condition that never asks the client for an intermediate
// id list makes zero calls of its own).
type queryCapturingClient struct {
	rows         [][]any
	lastQuery    string
	lastBindings []dhclickhouse.Binding
	calls        int
}

func (c *queryCapturingClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.calls++
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

// TestScopeFilterForMetricTeamScopeNeverRoundTripsForRepoIDs is a
// regression pin: a repo-scoped metric filtered by team never asks the
// client to resolve or verify a repo-id LIST for that team as its own
// query. The prior shape did exactly that (one query to list every
// matching repo_id, then one further query PER resolved id to re-verify
// it) -- a team whose true matching-repo count is large made that first
// query's own result set large enough to exceed the read-only client's
// per-statement row ceiling, degrading the whole request instead of
// answering it. With no explicit repo refs alongside the team scope, this
// call must make ZERO client round trips: the team's repo_id membership
// is a condition inside the CALLER's eventual statement, never a
// standalone read of its own.
func TestScopeFilterForMetricTeamScopeNeverRoundTripsForRepoIDs(t *testing.T) {
	client := &queryCapturingClient{rows: [][]any{}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	filterSQL, bindings, err := reader.scopeFilterForMetric(context.Background(), "repo", "team", []string{"team-x", "team-y"}, nil, "org-1", teamScopeAsOf)
	if err != nil {
		t.Fatalf("scopeFilterForMetric: %v", err)
	}
	if client.calls != 0 {
		t.Fatalf("scopeFilterForMetric made %d client round trip(s) for a team-only scope, want 0:\nfilterSQL: %s", client.calls, filterSQL)
	}
	if !strings.Contains(filterSQL, "repo_id IN (") || !strings.Contains(filterSQL, teamscope.Marker) {
		t.Fatalf("filterSQL missing the pushed-down team membership condition:\n%s", filterSQL)
	}
	if strings.Contains(filterSQL, "repo_id IN {scope_ids:Array(String)}") {
		t.Fatalf("filterSQL carries an explicit-repo clause with no explicit repos requested:\n%s", filterSQL)
	}
	if v, _ := bindingValue(bindings, teamscope.BindingTeamIDs); fmt.Sprint(v) != "[team-x team-y]" {
		t.Fatalf("%s binding = %v, want [team-x team-y]", teamscope.BindingTeamIDs, v)
	}
}

// TestScopeFilterForMetricUnionsExplicitReposWithTeamScope pins the union
// semantics resolve_repo_filter_ids' own Python shape has: an explicit
// what.repos ref alongside a team scope means EITHER condition can match,
// not both required. The explicit ref still round-trips through
// resolveRepoID (bounded: one call per explicit ref, not per matching
// repo), the team side stays pushed down.
func TestScopeFilterForMetricUnionsExplicitReposWithTeamScope(t *testing.T) {
	const resolvedRepoID = "55555555-5555-5555-5555-555555555555"
	client := &queryCapturingClient{rows: [][]any{{resolvedRepoID}}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	filterSQL, bindings, err := reader.scopeFilterForMetric(context.Background(), "repo", "team", []string{"team-x"}, []string{resolvedRepoID}, "org-1", teamScopeAsOf)
	if err != nil {
		t.Fatalf("scopeFilterForMetric: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("scopeFilterForMetric made %d client round trip(s), want exactly 1 (resolving the explicit ref)", client.calls)
	}
	if !strings.HasPrefix(filterSQL, " AND (repo_id IN {scope_ids:Array(String)} OR repo_id IN (") {
		t.Fatalf("filterSQL does not OR the explicit-repo and team conditions together:\n%s", filterSQL)
	}
	if v, _ := bindingValue(bindings, "scope_ids"); fmt.Sprint(v) != "["+resolvedRepoID+"]" {
		t.Fatalf("scope_ids binding = %v, want [%s]", v, resolvedRepoID)
	}
	if v, _ := bindingValue(bindings, teamscope.BindingTeamIDs); fmt.Sprint(v) != "[team-x]" {
		t.Fatalf("%s binding = %v, want [team-x]", teamscope.BindingTeamIDs, v)
	}
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
	_, _, err = reader.scopeFilterForMetric(context.Background(), "repo", "repo", []string{"acme/webapp"}, nil, "org-real", teamScopeAsOf)
	if err != nil {
		t.Fatalf("scopeFilterForMetric: %v", err)
	}
	if v, _ := bindingValue(client.lastBindings, "org_id"); v != "org-real" {
		t.Fatalf("resolveRepoID org_id binding = %v, want org-real (Python's own explain.py bug passes \"\")", v)
	}
}

// TestFetchMetricContributorsUsesMetricAggregator pins the class ruling
// fix: contributor ranking uses the metric's OWN aggregator, never a
// hardcoded avg -- Python's own fetch_metric_contributors
// (api/queries/explain.py) hardcodes avg() regardless of the metric,
// while this route's own headline read (fetch_metric_value/
// fetchMetricValue) already keys off the metric's configured aggregator.
func TestFetchMetricContributorsUsesMetricAggregator(t *testing.T) {
	client := &queryCapturingClient{rows: [][]any{}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	start := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)

	if _, err := reader.fetchMetricContributors(context.Background(), "work_item_metrics_daily", "items_completed", "team_id", "sum", start, end, "", nil, "org-1"); err != nil {
		t.Fatalf("fetchMetricContributors: %v", err)
	}
	if !strings.Contains(client.lastQuery, "toFloat64(sum(items_completed)) AS value") {
		t.Fatalf("sum-aggregator metric did not rank by sum():\n%s", client.lastQuery)
	}
	if strings.Contains(client.lastQuery, "avg(items_completed)") {
		t.Fatalf("sum-aggregator metric still ranks by avg():\n%s", client.lastQuery)
	}

	if _, err := reader.fetchMetricContributors(context.Background(), "repo_metrics_daily", "pr_first_review_p50_hours", "repo_id", "avg", start, end, "", nil, "org-1"); err != nil {
		t.Fatalf("fetchMetricContributors: %v", err)
	}
	if !strings.Contains(client.lastQuery, "toFloat64(avg(pr_first_review_p50_hours)) AS value") {
		t.Fatalf("avg-aggregator metric did not rank by avg():\n%s", client.lastQuery)
	}
}

// TestFetchMetricDriverDeltaUsesMetricAggregator is
// TestFetchMetricContributorsUsesMetricAggregator's own copy for
// fetchMetricDriverDelta's current/previous CTE pair -- both must use the
// metric's own aggregator, not just one of the two.
func TestFetchMetricDriverDeltaUsesMetricAggregator(t *testing.T) {
	client := &queryCapturingClient{rows: [][]any{}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	start := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	compareStart := time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC)
	compareEnd := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)

	if _, err := reader.fetchMetricDriverDelta(context.Background(), "work_item_metrics_daily", "items_completed", "team_id", "sum", start, end, compareStart, compareEnd, "", nil, "org-1"); err != nil {
		t.Fatalf("fetchMetricDriverDelta: %v", err)
	}
	if got := strings.Count(client.lastQuery, "toFloat64(sum(items_completed)) AS value"); got != 2 {
		t.Fatalf("expected both current/previous CTEs to rank by sum(), found %d occurrences:\n%s", got, client.lastQuery)
	}
	if strings.Contains(client.lastQuery, "avg(items_completed)") {
		t.Fatalf("sum-aggregator metric still ranks by avg() somewhere:\n%s", client.lastQuery)
	}
}

// TestMetricStatusFilterSQL pins metricStatusFilterSQL's own two shapes:
// empty for a metric with no StatusFilter, and a plain equality clause
// for one that has one (blocked_work's "blocked").
func TestMetricStatusFilterSQL(t *testing.T) {
	if got := metricStatusFilterSQL(""); got != "" {
		t.Fatalf("metricStatusFilterSQL(\"\") = %q, want empty", got)
	}
	if got, want := metricStatusFilterSQL("blocked"), " AND status = 'blocked'"; got != want {
		t.Fatalf("metricStatusFilterSQL(\"blocked\") = %q, want %q", got, want)
	}
}

// multiQueryCapturingClient is queryCapturingClient's own copy that keeps
// every query BuildExplainResponse issues across one call, not just the
// last one -- needed here because a single request issues four separate
// reads (current value, previous value, drivers, contributors) that must
// ALL carry blocked_work's status filter.
type multiQueryCapturingClient struct {
	rows    [][]any
	queries []string
}

func (c *multiQueryCapturingClient) Query(_ context.Context, query string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.queries = append(c.queries, query)
	return &fixtureRowScanner{rows: c.rows}, nil
}

// TestBuildExplainResponseBlockedWorkAppliesStatusFilterEverywhere pins
// that blocked_work's status filter reaches every one of the metric's
// reads (the headline's current AND previous windows, the driver-delta
// read, the contributor read) -- not just the headline, matching the
// class ruling (a) placement (same subquery WHERE, same nesting depth as
// org_id) at every one of those call sites.
func TestBuildExplainResponseBlockedWorkAppliesStatusFilterEverywhere(t *testing.T) {
	client := &multiQueryCapturingClient{rows: [][]any{}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := BuildExplainResponse(context.Background(), reader, "org-acme", Params{
		Metric:       "blocked_work",
		StartDay:     day(2024, 4, 1),
		EndDay:       day(2024, 4, 15),
		CompareStart: day(2024, 3, 18),
		CompareEnd:   day(2024, 4, 1),
		ScopeLevel:   "org",
	}); err != nil {
		t.Fatalf("BuildExplainResponse: %v", err)
	}
	if len(client.queries) != 4 {
		t.Fatalf("expected 4 queries (value current, value previous, drivers, contributors), got %d:\n%v", len(client.queries), client.queries)
	}
	for _, query := range client.queries {
		if !strings.Contains(query, "status = 'blocked'") {
			t.Fatalf("query missing blocked_work's status filter:\n%s", query)
		}
		assertSameDepth(t, query, "status = 'blocked'", "org_id = {org_id:String}")
	}
}
