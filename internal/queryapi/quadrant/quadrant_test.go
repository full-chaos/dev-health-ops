package quadrant

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// TestQuadrantDefinitionsMatchPython pins the four fixed-axis definitions
// against api/services/quadrant.py's QUADRANT_DEFINITIONS (quadrant.py:
// 278-303) field for field -- a drift here is a silent product-shape
// change, not a refactor.
func TestQuadrantDefinitionsMatchPython(t *testing.T) {
	cases := []struct {
		key  string
		want QuadrantDefinition
	}{
		{"churn_throughput", QuadrantDefinition{
			Type:           "churn_throughput",
			X:              AxisDefinition{Metric: "churn", Label: "Churn", Unit: "loc"},
			Y:              AxisDefinition{Metric: "throughput", Label: "Throughput", Unit: "items"},
			EvidenceMetric: "throughput",
		}},
		{"cycle_throughput", QuadrantDefinition{
			Type:           "cycle_throughput",
			X:              AxisDefinition{Metric: "cycle_time", Label: "Cycle Time", Unit: "days"},
			Y:              AxisDefinition{Metric: "throughput", Label: "Throughput", Unit: "items"},
			EvidenceMetric: "cycle_time",
		}},
		{"wip_throughput", QuadrantDefinition{
			Type:           "wip_throughput",
			X:              AxisDefinition{Metric: "wip", Label: "WIP", Unit: "items"},
			Y:              AxisDefinition{Metric: "throughput", Label: "Throughput", Unit: "items"},
			EvidenceMetric: "throughput",
		}},
		{"review_load_latency", QuadrantDefinition{
			Type:           "review_load_latency",
			X:              AxisDefinition{Metric: "review_load", Label: "Review Load", Unit: "reviews"},
			Y:              AxisDefinition{Metric: "review_latency", Label: "Review Latency", Unit: "hours"},
			EvidenceMetric: "review_latency",
		}},
	}
	if len(QuadrantDefinitions) != len(cases) {
		t.Fatalf("QuadrantDefinitions has %d entries, want %d", len(QuadrantDefinitions), len(cases))
	}
	for _, tc := range cases {
		got, ok := QuadrantDefinitions[tc.key]
		if !ok {
			t.Fatalf("QuadrantDefinitions missing %q", tc.key)
		}
		if got != tc.want {
			t.Fatalf("QuadrantDefinitions[%q] = %+v, want %+v", tc.key, got, tc.want)
		}
	}
}

// TestAttributionQuirkOnlyCycleThroughputTeam pins the exact condition
// build_quadrant_response's _fetch_metric_rows closure gates on
// (quadrant.py:546-551): `definition.type == "cycle_throughput" and
// group_scope == "team" and spec.use_primary_team_attribution`. Both x
// (cycle_time) and y (throughput) specs on TeamMetrics carry
// UsePrimaryTeamAttribution=true, but the attributed reader must fire ONLY
// for the cycle_throughput definition at team scope -- wip_throughput's y
// axis is the SAME throughput spec (UsePrimaryTeamAttribution=true) but
// must still use the plain team_id reader, because its definition.Type is
// not "cycle_throughput". This is the ticket's own "AS-IS" attribution
// quirk (CHAOS-5550, ruling R99).
func TestAttributionQuirkOnlyCycleThroughputTeam(t *testing.T) {
	throughputSpec := TeamMetrics["throughput"]
	cycleTimeSpec := TeamMetrics["cycle_time"]
	wipSpec := TeamMetrics["wip"]

	if !throughputSpec.UsePrimaryTeamAttribution {
		t.Fatal("TeamMetrics[throughput].UsePrimaryTeamAttribution = false, want true")
	}
	if !cycleTimeSpec.UsePrimaryTeamAttribution {
		t.Fatal("TeamMetrics[cycle_time].UsePrimaryTeamAttribution = false, want true")
	}
	if wipSpec.UsePrimaryTeamAttribution {
		t.Fatal("TeamMetrics[wip].UsePrimaryTeamAttribution = true, want false")
	}

	usesAttributedReader := func(definitionType, scope string, spec MetricSpec) bool {
		return definitionType == "cycle_throughput" && scope == "team" && spec.UsePrimaryTeamAttribution
	}

	cases := []struct {
		name           string
		definitionType string
		scope          string
		spec           MetricSpec
		want           bool
	}{
		{"cycle_throughput team x=cycle_time", "cycle_throughput", "team", cycleTimeSpec, true},
		{"cycle_throughput team y=throughput", "cycle_throughput", "team", throughputSpec, true},
		{"cycle_throughput repo x=cycle_time", "cycle_throughput", "repo", RepoMetrics["cycle_time"], false},
		{"wip_throughput team y=throughput", "wip_throughput", "team", throughputSpec, false},
		{"churn_throughput repo y=throughput", "churn_throughput", "repo", RepoMetrics["throughput"], false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := usesAttributedReader(tc.definitionType, tc.scope, tc.spec); got != tc.want {
				t.Fatalf("usesAttributedReader(%q, %q, ...) = %v, want %v", tc.definitionType, tc.scope, got, tc.want)
			}
		})
	}
}

// TestDedupFromUsesFinalForEveryDailyTable pins dedupFrom against the
// CURRENT ClickHouse engine of every daily-family table this package's
// MetricSpecs name, not src/dev_health_ops/clickhouse_dedup.py's
// dedup_from reference text -- migration 096 converted
// user_metrics_daily/repo_metrics_daily to ReplacingMergeTree(computed_at)
// with a sorting key matching their reader dedup key, so a LIMIT-1-BY
// subquery (the reference's own shape) is no longer correct: it is an
// all-tenant sort with no org_id predicate inside it, scanning every
// org's rows before the caller's outer WHERE narrows to one. Declared
// Python-plane divergence: Python's dedup_from was not updated for
// migration 096 and still emits the LIMIT-1-BY shape for these two
// tables. Alias comes before FINAL: "<table> FINAL AS <alias>" is a
// ClickHouse syntax error (Code 62), confirmed live -- "<table> AS
// <alias> FINAL" is the only accepted order.
func TestDedupFromUsesFinalForEveryDailyTable(t *testing.T) {
	cases := []struct {
		table string
		want  string
	}{
		{"work_item_metrics_daily AS m", "work_item_metrics_daily AS m FINAL"},
		{"work_item_user_metrics_daily AS m", "work_item_user_metrics_daily AS m FINAL"},
		{"user_metrics_daily AS m", "user_metrics_daily AS m FINAL"},
		{"repo_metrics_daily AS m", "repo_metrics_daily AS m FINAL"},
		{"git_pull_requests AS m", "git_pull_requests AS m FINAL"},
	}
	for _, tc := range cases {
		if got := dedupFrom(tc.table); got != tc.want {
			t.Fatalf("dedupFrom(%q) = %q, want %q", tc.table, got, tc.want)
		}
		if strings.Contains(dedupFrom(tc.table), "LIMIT 1 BY") {
			t.Fatalf("dedupFrom(%q) still emits a LIMIT-1-BY dedup subquery", tc.table)
		}
	}
}

// TestTimeWindowMatchesPython pins timeWindow against time_window
// (services/filtering.py:78-92) for the (start_day, end_day) pair this
// route reads.
func TestTimeWindowMatchesPython(t *testing.T) {
	end := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)

	t.Run("range_days only", func(t *testing.T) {
		start, endDay, _ := timeWindow(30, nil, &end)
		if !endDay.Equal(time.Date(2026, 3, 11, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("endDay = %v", endDay)
		}
		if !start.Equal(time.Date(2026, 2, 9, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("startDay = %v", start)
		}
	})

	t.Run("explicit start_date before end", func(t *testing.T) {
		start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
		gotStart, gotEnd, _ := timeWindow(30, &start, &end)
		if !gotEnd.Equal(time.Date(2026, 3, 11, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("endDay = %v", gotEnd)
		}
		if !gotStart.Equal(start) {
			t.Fatalf("startDay = %v, want %v", gotStart, start)
		}
	})

	t.Run("explicit start_date not before end_day clamps to end_day-1", func(t *testing.T) {
		start := time.Date(2026, 3, 11, 0, 0, 0, 0, time.UTC)
		gotStart, gotEnd, _ := timeWindow(30, &start, &end)
		if !gotEnd.Equal(time.Date(2026, 3, 11, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("endDay = %v", gotEnd)
		}
		if !gotStart.Equal(time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("startDay = %v", gotStart)
		}
	})
}

// TestNormalizeRangeDaysClamps pins the max(1, min(value, 180)) clamp
// (quadrant.py:326-327).
func TestNormalizeRangeDaysClamps(t *testing.T) {
	cases := map[int]int{-5: 1, 0: 1, 1: 1, 30: 30, 180: 180, 181: 180, 1000: 180}
	for in, want := range cases {
		if got := normalizeRangeDays(in); got != want {
			t.Fatalf("normalizeRangeDays(%d) = %d, want %d", in, got, want)
		}
	}
}

// TestChurnThroughputForcesRepoGrain pins the CHAOS-2079 override
// (quadrant.py:493-494): churn_throughput at org/team scope always
// resolves to repo grain; repo scope is left alone.
func TestChurnThroughputForcesRepoGrain(t *testing.T) {
	unusedQueryClient := unusedQueryClientStub{t: t}
	for _, scopeType := range []string{"org", "team"} {
		resp, err := BuildResponse(context.Background(), unusedQueryClient, "org-1", Params{
			Type: "churn_throughput", ScopeType: scopeType, RangeDays: 30, Bucket: "week",
		})
		if err != nil {
			t.Fatalf("BuildResponse(scope_type=%q) error: %v", scopeType, err)
		}
		if resp.Axes.X.Metric != "churn" || resp.Axes.Y.Metric != "throughput" {
			t.Fatalf("unexpected axes for scope_type=%q: %+v", scopeType, resp.Axes)
		}
	}
}

// TestPersonScopeRequiresScopeID pins quadrant.py:500-503: person/developer
// scope with no scope_id fails fast (400) before any ClickHouse call.
func TestPersonScopeRequiresScopeID(t *testing.T) {
	for _, scopeType := range []string{"developer", "person"} {
		_, err := BuildResponse(context.Background(), unusedQueryClientStub{t: t}, "org-1", Params{
			Type: "wip_throughput", ScopeType: scopeType, RangeDays: 30, Bucket: "week",
		})
		reqErr, ok := AsRequestError(err)
		if !ok || reqErr.Status != 400 {
			t.Fatalf("scope_type=%q: err = %v, want *RequestError{Status: 400}", scopeType, err)
		}
	}
}

// TestPersonScopeChurnThroughputNotForcedToRepo pins that the CHAOS-2079
// churn_throughput override (quadrant.py:493-494) is gated on
// normalized_scope in {"org", "team"} only -- person scope must reach the
// person-scope-id check (400, no ClickHouse call) rather than being
// silently redirected to repo grain.
func TestPersonScopeChurnThroughputNotForcedToRepo(t *testing.T) {
	_, err := BuildResponse(context.Background(), unusedQueryClientStub{t: t}, "org-1", Params{
		Type: "churn_throughput", ScopeType: "person", RangeDays: 30, Bucket: "week",
	})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 400 {
		t.Fatalf("err = %v, want *RequestError{Status: 400} (missing scope_id)", err)
	}
}

// TestUnknownTypeIsNotFound pins the 404 branch (quadrant.py:480-482).
func TestUnknownTypeIsNotFound(t *testing.T) {
	_, err := BuildResponse(context.Background(), unusedQueryClientStub{t: t}, "org-1", Params{
		Type: "not_a_real_type", ScopeType: "org", RangeDays: 30, Bucket: "week",
	})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 404 {
		t.Fatalf("err = %v, want *RequestError{Status: 404}", err)
	}
}

// TestInvalidBucketIsBadRequest pins the 400 branch (quadrant.py:505-506).
func TestInvalidBucketIsBadRequest(t *testing.T) {
	_, err := BuildResponse(context.Background(), unusedQueryClientStub{t: t}, "org-1", Params{
		Type: "wip_throughput", ScopeType: "org", RangeDays: 30, Bucket: "day",
	})
	reqErr, ok := AsRequestError(err)
	if !ok || reqErr.Status != 400 {
		t.Fatalf("err = %v, want *RequestError{Status: 400}", err)
	}
}

// unusedQueryClientStub panics if Query is ever called -- used for
// requests this test suite expects to fail (or resolve) before any
// ClickHouse call, and for churn_throughput's grain-override assertion,
// which needs a QueryClient that answers zero rows rather than panicking.
type unusedQueryClientStub struct{ t *testing.T }

func (u unusedQueryClientStub) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return emptyRowScannerStub{}, nil
}

type emptyRowScannerStub struct{}

func (emptyRowScannerStub) Next() bool        { return false }
func (emptyRowScannerStub) Scan(...any) error { return nil }
func (emptyRowScannerStub) Err() error        { return nil }
func (emptyRowScannerStub) Close() error      { return nil }

// TestTeamReviewSpecsWeighRowsByOwnerShare pins the shape of the team review
// reads (CHAOS-9562, D5998): every value expression reads o.owner_share, and the
// load is a ratio of a weighted sum to the weight of its distinct readers.
func TestTeamReviewSpecsWeighRowsByOwnerShare(t *testing.T) {
	load := quadrantMetricQuery(ownerTeamSpec(TeamMetrics["review_load"]), "week", "")
	for _, want := range []string{
		"sum(toFloat64(m.reviews_given) * o.owner_share) AS reader_value",
		"max(o.owner_share) AS reader_weight",
		"GROUP BY bucket, entity_id, entity_label, reader",
		"sum(reader_value) / nullIf(sum(reader_weight), 0)",
	} {
		if !strings.Contains(load, want) {
			t.Errorf("review_load query lacks %q:\n%s", want, load)
		}
	}
	latency := quadrantMetricQuery(ownerTeamSpec(TeamMetrics["review_latency"]), "week", "")
	if !strings.Contains(latency, "sum(m.pr_first_review_p50_hours * o.owner_share) / nullIf(sum(o.owner_share), 0)") {
		t.Errorf("review_latency query is not the share-weighted mean:\n%s", latency)
	}
	if !strings.Contains(latency, "AND m.pr_first_review_p50_hours IS NOT NULL") {
		t.Errorf("review_latency query weighs rows without a latency:\n%s", latency)
	}
}

// TestWorkItemTeamQuadrantQuerySplitsAnItemByOwnerShare pins the shape of the
// attributed team reads (CHAOS-9562, D5983): the weight of a team in an item is
// computed before any team selection, throughput sums each item's weight once per
// team, and cycle time is the weighted mean.
func TestWorkItemTeamQuadrantQuerySplitsAnItemByOwnerShare(t *testing.T) {
	asOf := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	selected := extraScope{SQL: "\n              AND t.team_id IN {scope_team_ids:Array(String)}"}
	throughput, bindings, err := workItemTeamQuadrantQuery("throughput", "week", "org-1", asOf, selected)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"toFloat64(sum(item_weight)) AS value",
		"GROUP BY bucket, team_id, work_item_id",
		"OVER (PARTITION BY w.work_item_id)",
		"AS owner_share",
		"LEFT JOIN (",
	} {
		if !strings.Contains(throughput, want) {
			t.Errorf("throughput query lacks %q:\n%s", want, throughput)
		}
	}
	if strings.Index(throughput, "OVER (PARTITION BY w.work_item_id)") > strings.Index(throughput, "AND t.team_id IN {scope_team_ids:Array(String)}") {
		t.Error("the team selection is applied before the item weights are computed")
	}
	names := map[string]bool{}
	for _, b := range bindings {
		names[b.Name] = true
	}
	for _, name := range []string{"owner_shares_org_id", "owner_shares_as_of"} {
		if !names[name] {
			t.Errorf("owner-share bindings lack %q: %v", name, bindings)
		}
	}
	if names["owner_shares_team_ids"] {
		t.Error("the owner shares are narrowed to the selected teams; they must be computed over every owner")
	}

	cycle, _, err := workItemTeamQuadrantQuery("cycle_time", "week", "org-1", asOf, extraScope{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(cycle, "toFloat64(sum(cycle_time_hours * weight) / sum(weight)) AS value") ||
		!strings.Contains(cycle, "AND cycle_time_hours IS NOT NULL") {
		t.Errorf("cycle_time query is not the weighted mean over rows with a cycle time:\n%s", cycle)
	}
	if _, _, err := workItemTeamQuadrantQuery("wip", "week", "org-1", asOf, extraScope{}); err == nil {
		t.Error("an unsupported metric gave no error")
	}
}

// TestTeamChurnSpecSumsChurnTimesOwnerShare pins the additive rule of the team
// churn read (CHAOS-9693, D5998): churn x owner share, summed per owning team.
func TestTeamChurnSpecSumsChurnTimesOwnerShare(t *testing.T) {
	churn := quadrantMetricQuery(ownerTeamSpec(TeamMetrics["churn"]), "week", "")
	if !strings.Contains(churn, "toFloat64(sum(toFloat64(m.loc_touched) * o.owner_share)) AS value") {
		t.Errorf("team churn query is not the sum of churn x owner share:\n%s", churn)
	}
}
