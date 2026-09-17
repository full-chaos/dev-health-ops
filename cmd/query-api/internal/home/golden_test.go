// Golden parity test for GET+POST /api/v1/home's org-scope default
// request -- replays testdata/org_default.json, captured by running the
// EXISTING Python build_home_response once with every ClickHouse/
// Postgres reader monkeypatched to a canned dispatcher, org scope,
// range_days=7 compare_days=7 end_date=2024-01-08.
//
// CAPTURE COMMAND (verbatim, from the ops repo root, this worktree's
// venv):
//
//	.venv/bin/python <<'PYEOF'
//	<see capture_home_golden.py in this PR's TEST-EVIDENCE for the full
//	script: it monkeypatches query_dicts in
//	dev_health_ops.api.services.{home,filtering}, dev_health_ops.api.
//	queries.{freshness,metrics,explain,scopes} (query_dicts is imported
//	by name into each of those five modules, so patching the shared
//	definition alone does not reach them), and clickhouse_client in
//	dev_health_ops.api.services.home, then calls build_home_response
//	with a fresh epoch_scoped(create_cache(...)) and semantic_session=
//	None (Postgres freshness intentionally out of scope for this
//	fixture -- latest_successful_sync_at is null)>
//	PYEOF
//
// events[].ts is real wall-clock in Python's own capture (datetime.now
// (timezone.utc) has no injectable clock in the reference) and was
// normalized to the fixed instant "2024-01-08T12:00:00Z" post-capture;
// this test passes that exact instant as BuildResponse's own now
// parameter, so the replay is byte-exact without editing any computed
// value.
package home

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

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

func bindingValue(bindings []dhclickhouse.Binding, name string) (any, bool) {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

func isCurrentWindow(bindings []dhclickhouse.Binding) bool {
	v, ok := bindingValue(bindings, "start_day")
	return ok && v == "2024-01-02"
}

type seriesPoint struct {
	day   time.Time
	value float64
}

func orgGoldenHandler(t *testing.T) func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	metricFixtures := map[string]struct {
		current, previous float64
		series            []seriesPoint
	}{
		"cycle_time_p50_hours":      {48.0, 60.0, []seriesPoint{{day(2024, 1, 2), 50.0}, {day(2024, 1, 5), 46.0}}},
		"pr_first_review_p50_hours": {5.0, 4.0, []seriesPoint{{day(2024, 1, 2), 4.5}, {day(2024, 1, 5), 5.5}}},
		"items_completed":           {20.0, 15.0, []seriesPoint{{day(2024, 1, 2), 9.0}, {day(2024, 1, 5), 11.0}}},
		"deployments_count":         {3.0, 3.0, []seriesPoint{{day(2024, 1, 2), 1.0}, {day(2024, 1, 5), 2.0}}},
		"total_loc_touched":         {1000.0, 800.0, []seriesPoint{{day(2024, 1, 2), 400.0}, {day(2024, 1, 5), 600.0}}},
		"wip_congestion_ratio":      {0.5, 0.4, []seriesPoint{{day(2024, 1, 2), 0.45}, {day(2024, 1, 5), 0.55}}},
		"change_failure_rate":       {0.1, 0.08, []seriesPoint{{day(2024, 1, 2), 0.09}, {day(2024, 1, 5), 0.11}}},
		"rework_churn_ratio_30d":    {0.2, 0.15, []seriesPoint{{day(2024, 1, 2), 0.18}, {day(2024, 1, 5), 0.22}}},
		"pr_rework_ratio":           {0.12, 0.10, []seriesPoint{{day(2024, 1, 2), 0.11}, {day(2024, 1, 5), 0.13}}},
		"success_rate":              {0.95, 0.90, []seriesPoint{{day(2024, 1, 2), 0.93}, {day(2024, 1, 5), 0.97}}},
	}

	return func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		q := query

		switch {
		case strings.Contains(q, "maxOrNull(computed_at) AS last_ingested_at"):
			return &fixtureRowScanner{rows: [][]any{{time.Date(2024, 1, 8, 10, 0, 0, 0, time.UTC)}}}, nil

		case strings.Contains(q, "countDistinct(id)) AS total"):
			return &fixtureRowScanner{rows: [][]any{{10.0}}}, nil
		case strings.Contains(q, "countDistinct(repo_id)) AS covered"):
			return &fixtureRowScanner{rows: [][]any{{8.0}}}, nil
		case strings.Contains(q, "work_scope_id != ''") && strings.Contains(q, "work_item_cycle_times"):
			return &fixtureRowScanner{rows: [][]any{{40.0, 50.0}}}, nil
		case strings.Contains(q, "cycle_time_hours IS NOT NULL"):
			return &fixtureRowScanner{rows: [][]any{{45.0, 50.0}}}, nil

		case strings.Contains(q, "FROM repos FINAL") && strings.Contains(q, "FROM work_items FINAL") && strings.Contains(q, "'ci' AS source"):
			return &fixtureRowScanner{rows: [][]any{
				{"github", time.Date(2024, 1, 8, 9, 0, 0, 0, time.UTC)},
				{"jira", time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)},
				{"ci", time.Date(2024, 1, 7, 9, 0, 0, 0, time.UTC)},
			}}, nil

		case strings.Contains(q, "FROM investment_metrics_daily"):
			return &fixtureRowScanner{rows: [][]any{
				{"feature_delivery", 100.0, int64(20), int64(500)},
				{"maintenance", 50.0, int64(10), int64(300)},
			}}, nil

		case strings.Contains(q, "SUM(pr_rework_ratio * prs_merged)"):
			fx := metricFixtures["pr_rework_ratio"]
			if strings.Contains(q, "GROUP BY day") && strings.Contains(q, "ORDER BY day") {
				return seriesScanner(fx.series), nil
			}
			if isCurrentWindow(bindings) {
				return &fixtureRowScanner{rows: [][]any{{fx.current}}}, nil
			}
			return &fixtureRowScanner{rows: [][]any{{fx.previous}}}, nil

		case strings.Contains(q, "duration_hours") && strings.Contains(q, "FROM work_item_state_durations_daily"):
			if isCurrentWindow(bindings) {
				return &fixtureRowScanner{rows: [][]any{{day(2024, 1, 2), 6.0}, {day(2024, 1, 5), 4.0}}}, nil
			}
			return &fixtureRowScanner{rows: [][]any{{day(2023, 12, 26), 8.0}}}, nil

		case strings.Contains(q, "delta_pct") && strings.Contains(q, "LEFT JOIN previous"):
			return &fixtureRowScanner{rows: [][]any{{"team-alpha", 10.0, 20.0}}}, nil

		case strings.Contains(q, "FROM recommendations_daily"):
			t.Fatal("recommendations_daily must not be read at org scope")
			return nil, nil

		case strings.Contains(q, "FROM compounding_risk_daily"):
			return &fixtureRowScanner{rows: [][]any{
				{"repo", "repo-1", 0.8, "high", time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)},
			}}, nil

		case strings.Contains(q, "FROM repos FINAL") && strings.Contains(q, "display_name"):
			return &fixtureRowScanner{rows: [][]any{{"repo-1", "checkout-service"}}}, nil
		case strings.Contains(q, "FROM teams FINAL") && strings.Contains(q, "display_name"):
			return &fixtureRowScanner{rows: [][]any{{"team-1", "Team Alpha"}}}, nil
		}

		for column, fx := range metricFixtures {
			marker := "(" + column + ")) AS value"
			if !strings.Contains(q, marker) {
				continue
			}
			if strings.Contains(q, "GROUP BY day") && strings.Contains(q, "ORDER BY day") {
				return seriesScanner(fx.series), nil
			}
			if isCurrentWindow(bindings) {
				return &fixtureRowScanner{rows: [][]any{{fx.current}}}, nil
			}
			return &fixtureRowScanner{rows: [][]any{{fx.previous}}}, nil
		}

		t.Fatalf("unexpected query for org golden fixture:\n%s", query)
		return nil, nil
	}
}

func seriesScanner(points []seriesPoint) *fixtureRowScanner {
	rows := make([][]any, 0, len(points))
	for _, p := range points {
		rows = append(rows, []any{p.day, p.value})
	}
	return &fixtureRowScanner{rows: rows}
}

func ptrTime(t time.Time) *time.Time { return &t }

func TestGoldenOrgScopeDefault(t *testing.T) {
	client := fakeQueryClient{t: t, handler: orgGoldenHandler(t)}

	f := Filters{
		Time:  TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day(2024, 1, 8))},
		Scope: ScopeFilter{Level: "org"},
	}
	now := time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC)

	got, err := BuildResponse(context.Background(), client, nil, "org-1", f, now)
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	want := loadGolden(t, "org_default.json")
	assertResponseEqual(t, *got, want)
}

// assertResponseEqual is shared by every golden test in this package
// (org/team/repo scope).
func assertResponseEqual(t *testing.T, got, want Response) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response mismatch\n got:  %+v\n want: %+v", got, want)
	}
}
