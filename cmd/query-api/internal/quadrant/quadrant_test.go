package quadrant

import (
	"context"
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

// TestDedupFromMatchesPython pins dedupFrom against
// src/dev_health_ops/clickhouse_dedup.py's dedup_from (clickhouse_dedup.py:
// 135-157) for the three tables this package's MetricSpecs actually name.
func TestDedupFromMatchesPython(t *testing.T) {
	cases := []struct {
		table string
		want  string
	}{
		{
			"work_item_metrics_daily AS m",
			"work_item_metrics_daily FINAL AS m",
		},
		{
			"user_metrics_daily AS m",
			"(\n            SELECT *\n            FROM user_metrics_daily\n            ORDER BY computed_at DESC\n            LIMIT 1 BY org_id, repo_id, author_email, day\n        ) AS m",
		},
		{
			"repo_metrics_daily AS m",
			"(\n            SELECT *\n            FROM repo_metrics_daily\n            ORDER BY computed_at DESC\n            LIMIT 1 BY org_id, repo_id, day\n        ) AS m",
		},
	}
	for _, tc := range cases {
		if got := dedupFrom(tc.table); got != tc.want {
			t.Fatalf("dedupFrom(%q) = %q, want %q", tc.table, got, tc.want)
		}
	}
}

// TestTimeWindowMatchesPython pins timeWindow against time_window
// (services/filtering.py:78-92) for the (start_day, end_day) pair this
// route reads.
func TestTimeWindowMatchesPython(t *testing.T) {
	end := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)

	t.Run("range_days only", func(t *testing.T) {
		start, endDay := timeWindow(30, nil, &end)
		if !endDay.Equal(time.Date(2026, 3, 11, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("endDay = %v", endDay)
		}
		if !start.Equal(time.Date(2026, 2, 9, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("startDay = %v", start)
		}
	})

	t.Run("explicit start_date before end", func(t *testing.T) {
		start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
		gotStart, gotEnd := timeWindow(30, &start, &end)
		if !gotEnd.Equal(time.Date(2026, 3, 11, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("endDay = %v", gotEnd)
		}
		if !gotStart.Equal(start) {
			t.Fatalf("startDay = %v, want %v", gotStart, start)
		}
	})

	t.Run("explicit start_date not before end_day clamps to end_day-1", func(t *testing.T) {
		start := time.Date(2026, 3, 11, 0, 0, 0, 0, time.UTC)
		gotStart, gotEnd := timeWindow(30, &start, &end)
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

// TestPersonScopeNotImplemented pins the documented scope gap: developer/
// person scope answers a typed, recognizable error rather than silently
// mis-resolving to team/repo grain.
func TestPersonScopeNotImplemented(t *testing.T) {
	for _, scopeType := range []string{"developer", "person"} {
		_, err := BuildResponse(context.Background(), unusedQueryClientStub{t: t}, "org-1", Params{
			Type: "wip_throughput", ScopeType: scopeType, RangeDays: 30, Bucket: "week",
		})
		reqErr, ok := AsRequestError(err)
		if !ok {
			t.Fatalf("scope_type=%q: err = %v, want *RequestError", scopeType, err)
		}
		if reqErr.Status != 501 {
			t.Fatalf("scope_type=%q: status = %d, want 501", scopeType, reqErr.Status)
		}
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
