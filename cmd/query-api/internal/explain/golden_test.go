package explain

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
// as drilldown/investmentexplain/quadrant's own fixtureRowScanner,
// specialised to the destination types this package's Scan calls use:
// *string (id/scope_id/display_name -- always non-nullable columns) and
// **float64 (value/delta_pct -- a ClickHouse aggregate that can
// legitimately return NULL; nil row entry -> nil pointer, matching SQL
// NULL).
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
		case **float64:
			if row[i] == nil {
				*typed = nil
				continue
			}
			v := row[i].(float64)
			*typed = &v
		default:
			return fmt.Errorf("fixtureRowScanner: unsupported dest type %T", d)
		}
	}
	return nil
}

func (s *fixtureRowScanner) Err() error   { return nil }
func (s *fixtureRowScanner) Close() error { return nil }

// fakeQueryClient dispatches on query text (most-specific-first), same
// convention as drilldown/investmentexplain/quadrant's own fakeQueryClient.
type fakeQueryClient struct {
	t       *testing.T
	handler func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

func (c fakeQueryClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return c.handler(c.t, query, bindings)
}

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// loadGolden decodes a testdata JSON file -- captured/hand-derived from
// build_explain_response's own field-for-field contract
// (api/services/explain.py:121-261, api/models/schemas.py's
// ExplainResponse/Contributor), THEN cross-checked against this test's
// own fake-client fixture rows (see each Test function's own doc comment
// for the worked arithmetic). DisallowUnknownFields makes a field-name
// mismatch a hard test failure, not a silent drop.
func loadGolden(t *testing.T, name string) Response {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp Response
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

// explainQueryDispatch answers the fixed set of queries BuildExplainResponse
// can issue, in the SAME most-specific-first order drilldown/repofilter_test.go's
// own comment establishes: a query shape that would match more than one
// case (e.g. "FROM repos FINAL" alone matches BOTH resolveRepoID and
// resolveScopeDisplayNames' repo branch) must be disambiguated by its
// MORE specific sibling first.
type explainQueryDispatch struct {
	t                *testing.T
	resolveRepoIDRow []any // nil -> not expected to be called
	displayNameRows  [][]any
	valueCurrent     float64
	valuePrevious    float64
	currentStartDay  string // "YYYY-MM-DD" -- distinguishes the current-window fetchMetricValue call from the previous/compare one (identical query TEXT, different bound start_day)
	driverRows       [][]any
	contributorRows  [][]any
}

func (d *explainQueryDispatch) handle(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	switch {
	case strings.Contains(query, "AS scope_id, repo AS display_name"), strings.Contains(query, "AS scope_id, name AS display_name"):
		return &fixtureRowScanner{rows: d.displayNameRows}, nil
	case strings.Contains(query, "FROM repos FINAL") && d.resolveRepoIDRow != nil:
		return &fixtureRowScanner{rows: [][]any{d.resolveRepoIDRow}}, nil
	case strings.Contains(query, "delta_pct"):
		return &fixtureRowScanner{rows: d.driverRows}, nil
	case strings.Contains(query, "ORDER BY value DESC"):
		return &fixtureRowScanner{rows: d.contributorRows}, nil
	case strings.Contains(query, "FROM ("):
		startDay, _ := bindingValue(bindings, "start_day")
		if fmt.Sprint(startDay) == d.currentStartDay {
			return &fixtureRowScanner{rows: [][]any{{d.valueCurrent}}}, nil
		}
		return &fixtureRowScanner{rows: [][]any{{d.valuePrevious}}}, nil
	default:
		t.Fatalf("unexpected query:\n%s", query)
		return nil, nil
	}
}

// TestGoldenOrgScopeThroughput replays testdata/org_scope_throughput.json:
// metric="throughput" (team scope, sum aggregator), scope.level="org" --
// scopeFilterForMetric's "team" branch requires scope.level=="team", so
// an org-level request applies NO scope filter and never touches
// ResolveRepoFilterIDs/resolveRepoIDsForTeams. Exercises: fetchMetricValue
// (current 120.0, previous 100.0 -> delta_pct 20.0), fetchMetricDriverDelta
// (2 rows) and fetchMetricContributors (the SAME 2 ids, delta forced to
// 0.0 by buildContributor), and resolveScopeDisplayNames("team", ...) for
// BOTH ids: "team-a" resolves to "Team Alpha"; the second, a
// UUID-SHAPED team_id, does not resolve (A8: even if a row came back
// with a bare-UUID display_name it would be filtered), so it falls back
// to shortToken.
func TestGoldenOrgScopeThroughput(t *testing.T) {
	dispatch := &explainQueryDispatch{
		t:               t,
		valueCurrent:    120.0,
		valuePrevious:   100.0,
		currentStartDay: "2024-01-01",
		displayNameRows: [][]any{{"team-a", "Team Alpha"}},
		driverRows: [][]any{
			{"team-a", 80.0, 25.0},
			{"22222222-2222-2222-2222-222222222222", 40.0, -10.0},
		},
		contributorRows: [][]any{
			{"team-a", 80.0},
			{"22222222-2222-2222-2222-222222222222", 40.0},
		},
	}
	client := fakeQueryClient{t: t, handler: dispatch.handle}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildExplainResponse(context.Background(), reader, "org-acme", Params{
		Metric:       "throughput",
		StartDay:     day(2024, 1, 1),
		EndDay:       day(2024, 1, 15),
		CompareStart: day(2023, 12, 18),
		CompareEnd:   day(2024, 1, 1),
		ScopeLevel:   "org",
	})
	if err != nil {
		t.Fatalf("BuildExplainResponse: %v", err)
	}
	want := loadGolden(t, "org_scope_throughput.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenUnknownMetricFallsBackToCycleTime replays
// testdata/unknown_metric_falls_back.json: metric="totally_bogus" is not
// a metricConfigs key, so resolveMetricConfig borrows cycle_time's
// table/column/label/unit/group_by/scope/aggregator/transform
// (api/services/explain.py:146) -- but the RESPONSE's own "metric" field
// still echoes "totally_bogus" verbatim, never "cycle_time". No drivers/
// contributors rows -> both lists are empty (never null), and
// resolveScopeDisplayNames is never called (collectRowIDs is empty).
func TestGoldenUnknownMetricFallsBackToCycleTime(t *testing.T) {
	dispatch := &explainQueryDispatch{
		t:               t,
		valueCurrent:    48.0,
		valuePrevious:   24.0,
		currentStartDay: "2024-02-01",
		driverRows:      [][]any{},
		contributorRows: [][]any{},
	}
	client := fakeQueryClient{t: t, handler: dispatch.handle}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildExplainResponse(context.Background(), reader, "org-acme", Params{
		Metric:       "totally_bogus",
		StartDay:     day(2024, 2, 1),
		EndDay:       day(2024, 2, 15),
		CompareStart: day(2024, 1, 18),
		CompareEnd:   day(2024, 2, 1),
		ScopeLevel:   "org",
	})
	if err != nil {
		t.Fatalf("BuildExplainResponse: %v", err)
	}
	want := loadGolden(t, "unknown_metric_falls_back.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenRepoScopeReviewLatency replays
// testdata/repo_scope_review_latency.json: metric="review_latency"
// (repo scope, avg aggregator), scope.level="repo", scope.ids=["acme/webapp"]
// (a non-UUID ref, resolved by repos.repo name) -- exercises
// scopeFilterForMetric's "repo" branch end to end: ResolveRepoFilterIDs
// -> resolveRepoID (the name branch) -> scopeClauseRepo, all threading
// the REAL org id "org-acme" throughout (the declared divergence from
// Python's own explain.py:150-152 bug, see response.go's
// scopeFilterForMetric doc comment -- pinned separately, at the query-
// shape level, by sqlshape_test.go's
// TestScopeFilterForMetricRepoUsesRealOrgID). The evidence_link's own
// scope_id is the RAW request value "acme/webapp" (_primary_scope_id
// reads filters.scope.ids[0] verbatim), not the resolved UUID.
func TestGoldenRepoScopeReviewLatency(t *testing.T) {
	const resolvedRepoID = "33333333-3333-3333-3333-333333333333"
	dispatch := &explainQueryDispatch{
		t:                t,
		resolveRepoIDRow: []any{resolvedRepoID},
		valueCurrent:     5.0,
		valuePrevious:    10.0,
		currentStartDay:  "2024-03-01",
		displayNameRows:  [][]any{{resolvedRepoID, "acme/webapp"}},
		driverRows: [][]any{
			{resolvedRepoID, 5.0, -50.0},
		},
		contributorRows: [][]any{
			{resolvedRepoID, 5.0},
		},
	}
	client := fakeQueryClient{t: t, handler: dispatch.handle}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildExplainResponse(context.Background(), reader, "org-acme", Params{
		Metric:       "review_latency",
		StartDay:     day(2024, 3, 1),
		EndDay:       day(2024, 3, 15),
		CompareStart: day(2024, 2, 15),
		CompareEnd:   day(2024, 3, 1),
		ScopeLevel:   "repo",
		ScopeIDs:     []string{"acme/webapp"},
	})
	if err != nil {
		t.Fatalf("BuildExplainResponse: %v", err)
	}
	want := loadGolden(t, "repo_scope_review_latency.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenSumAggregatorMetricRanksBySum replays
// testdata/repo_scope_deploy_freq_sum_ranking.json: metric="deploy_freq"
// (repo scope, sum aggregator), scope.level="org" (no repo/team filter
// applied). It pins the ranking-aggregator fix at the VALUE level: the
// driver/contributor "value" fixtures below are not arbitrary -- each is
// independently computed here, in this comment, from a stated set of raw
// per-day deployments_count contributions a real fetchMetricContributors/
// fetchMetricDriverDelta call would have summed:
//
//   - "repo-a": raw per-day values [3, 4, 5, 2, 1] -> sum = 15, avg = 3.0.
//     The fixture uses 15.0 (sum) -- what fetchMetricContributors/
//     fetchMetricDriverDelta now ask ClickHouse for, matching
//     deploy_freq's own config.Aggregator="sum" and its headline read
//     (fetchMetricValue already keyed off the same aggregator before this
//     change). Python's own fetch_metric_contributors/
//     fetch_metric_driver_delta hardcode avg() regardless of the metric,
//     so the SAME raw rows would have ranked repo-a at 3.0 there --
//     understating a repo that shipped 15 deploys as if it shipped 3.
//   - the UUID-shaped id "44444444-4444-4444-4444-444444444444": raw
//     per-day values [9, 7] -> sum = 16, avg = 8.0. The fixture uses 16.0
//     (sum) for the same reason.
//
// The headline value/delta_pct (30.0/50.0) and the driver delta_pct
// figures (20.0/-5.0) are not under this ruling (fetchMetricValue already
// used config.Aggregator, and delta_pct is a SQL-side CASE this test
// treats as opaque, same as every other golden in this file) -- picked
// for readability only.
func TestGoldenSumAggregatorMetricRanksBySum(t *testing.T) {
	dispatch := &explainQueryDispatch{
		t:               t,
		valueCurrent:    30.0,
		valuePrevious:   20.0,
		currentStartDay: "2024-05-01",
		displayNameRows: [][]any{{"repo-a", "webapp"}},
		driverRows: [][]any{
			{"repo-a", 15.0, 20.0},
			{"44444444-4444-4444-4444-444444444444", 16.0, -5.0},
		},
		contributorRows: [][]any{
			{"repo-a", 15.0},
			{"44444444-4444-4444-4444-444444444444", 16.0},
		},
	}
	client := fakeQueryClient{t: t, handler: dispatch.handle}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildExplainResponse(context.Background(), reader, "org-acme", Params{
		Metric:       "deploy_freq",
		StartDay:     day(2024, 5, 1),
		EndDay:       day(2024, 5, 15),
		CompareStart: day(2024, 4, 17),
		CompareEnd:   day(2024, 5, 1),
		ScopeLevel:   "org",
	})
	if err != nil {
		t.Fatalf("BuildExplainResponse: %v", err)
	}
	want := loadGolden(t, "repo_scope_deploy_freq_sum_ranking.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenBlockedWorkSumsOnlyBlockedStatus replays
// testdata/team_scope_blocked_work_status_filter.json: metric="blocked_work"
// (team scope, sum aggregator), scope.level="team", scope.ids=["team-ops"].
// It pins the status-filter fix at the VALUE level: team-ops's
// work_item_state_durations_daily rows for the current window, by status,
// are stated here and independently summed in this comment, never copied
// from a Go run:
//
//	blocked:     [4, 3, 5]  -> sum 12
//	in_progress: [2, 2]     -> sum 4
//	done:        [1]        -> sum 1
//	-----------------------------------
//	blocked-only sum:            12  (this fixture's value, current window)
//	every-status sum:            17  (what Python's own fetch_metric_value/
//	                                  fetch_metric_contributors/
//	                                  fetch_metric_driver_delta would have
//	                                  summed instead -- none of the three
//	                                  carries a status predicate)
//
// The headline value (12.0), the driver value (12.0) and the contributor
// value (12.0) all agree because this fixture uses ONE team across all
// three reads, so the same blocked-only total applies to the headline,
// the one driver row and the one contributor row alike -- a real
// blocked_work response would rarely show all three equal, but doing so
// here keeps the arithmetic in one place. The previous window's
// blocked-only total is taken as 8.0 (not decomposed by status; only the
// CURRENT window's per-status split matters for this test), giving
// delta_pct = (12-8)/8*100 = 50.0.
func TestGoldenBlockedWorkSumsOnlyBlockedStatus(t *testing.T) {
	dispatch := &explainQueryDispatch{
		t:               t,
		valueCurrent:    12.0,
		valuePrevious:   8.0,
		currentStartDay: "2024-06-01",
		displayNameRows: [][]any{{"team-ops", "Team Ops"}},
		driverRows: [][]any{
			{"team-ops", 12.0, 50.0},
		},
		contributorRows: [][]any{
			{"team-ops", 12.0},
		},
	}
	client := fakeQueryClient{t: t, handler: dispatch.handle}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildExplainResponse(context.Background(), reader, "org-acme", Params{
		Metric:       "blocked_work",
		StartDay:     day(2024, 6, 1),
		EndDay:       day(2024, 6, 15),
		CompareStart: day(2024, 5, 18),
		CompareEnd:   day(2024, 6, 1),
		ScopeLevel:   "team",
		ScopeIDs:     []string{"team-ops"},
	})
	if err != nil {
		t.Fatalf("BuildExplainResponse: %v", err)
	}
	want := loadGolden(t, "team_scope_blocked_work_status_filter.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}
