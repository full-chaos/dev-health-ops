package home

import (
	"context"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/prrework"
)

// TestBuildResponseEmptyCurrentWindowIsNoData guards the distinction between
// an empty window and a stored zero. Scalar aggregates return one result row
// when their source has none, so this fixture returns the real empty aggregate
// shape (count 0, value 0). It also answers the pre-CHAOS-8169 query shape
// with its one value column, which reproduced the baseline's false zero claim.
func TestBuildResponseEmptyCurrentWindowIsNoData(t *testing.T) {
	base := orgGoldenHandler(t)
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM work_item_team_attributions") {
			t.Fatal("an empty current window must not read Home signal attribution")
		}
		if strings.Contains(query, "FROM work_item_metrics_daily") ||
			strings.Contains(query, "FROM repo_metrics_daily") ||
			strings.Contains(query, "FROM deploy_metrics_daily") ||
			strings.Contains(query, "FROM cicd_metrics_daily") {
			if strings.Contains(query, "AS row_count") {
				return &fixtureRowScanner{rows: [][]any{{int64(0), 0.0}}}, nil
			}
			return &fixtureRowScanner{}, nil
		}
		// Change failure rate: an empty window sums to zeros over zero stored
		// rows, which the shared rule reads as no stored counts.
		if strings.Contains(query, "FROM repo_change_failure_daily") && !strings.Contains(query, "delta_pct") {
			if strings.Contains(query, changefailure.ViewSumsSQL) {
				return &fixtureRowScanner{rows: [][]any{{uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0)}}}, nil
			}
			return &fixtureRowScanner{}, nil
		}
		if strings.Contains(query, "FROM work_item_state_durations_daily") ||
			strings.Contains(query, "FROM compounding_risk_daily") {
			return &fixtureRowScanner{}, nil
		}
		return base(t, query, bindings)
	}}

	got, err := BuildResponse(
		context.Background(),
		client,
		nil,
		"org-1",
		Filters{Time: TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day(2024, 1, 8))}, Scope: ScopeFilter{Level: "org"}},
		time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if got.HealthState.Status != "no_data" {
		t.Fatalf("health status = %q, want no_data", got.HealthState.Status)
	}
	if got.HealthState.Headline != "" || got.HealthState.Summary != "" {
		t.Fatalf("no-data health must not make a claim: %+v", got.HealthState)
	}
	if len(got.Signals) != 0 || len(got.Summary) != 0 || len(got.Events) != 0 {
		t.Fatalf("empty window created derived claims: signals=%d summary=%d events=%d", len(got.Signals), len(got.Summary), len(got.Events))
	}
	if got.Constraint != nil {
		t.Fatalf("constraint = %+v, want nil for an empty window", got.Constraint)
	}
	for _, delta := range got.Deltas {
		if delta.HasData || delta.HasPriorData {
			t.Fatalf("delta %q has data flags %+v, want current/prior false", delta.Metric, delta)
		}
	}
}

func TestBuildResponseNoCurrentDataDoesNotTurnPriorDataIntoAClaim(t *testing.T) {
	base := orgGoldenHandler(t)
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if !isCurrentWindow(bindings) {
			return base(t, query, bindings)
		}
		if (strings.Contains(query, "FROM work_item_metrics_daily") ||
			strings.Contains(query, "FROM repo_metrics_daily") ||
			strings.Contains(query, "FROM deploy_metrics_daily") ||
			strings.Contains(query, "FROM cicd_metrics_daily")) && strings.Contains(query, "AS row_count") {
			return &fixtureRowScanner{rows: [][]any{{int64(0), 0.0}}}, nil
		}
		if strings.Contains(query, "FROM repo_change_failure_daily") && strings.Contains(query, changefailure.ViewSumsSQL) {
			return &fixtureRowScanner{rows: [][]any{{uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0)}}}, nil
		}
		if strings.Contains(query, prrework.ViewSumsSQL) {
			return &fixtureRowScanner{rows: [][]any{{uint64(0), uint64(0), uint64(0), uint64(0), uint64(0), uint64(0)}}}, nil
		}
		if strings.Contains(query, "FROM work_item_state_durations_daily") {
			return &fixtureRowScanner{}, nil
		}
		return base(t, query, bindings)
	}}

	got, err := BuildResponse(
		context.Background(),
		client,
		nil,
		"org-1",
		Filters{Time: TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day(2024, 1, 8))}, Scope: ScopeFilter{Level: "org"}},
		time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("BuildResponse: %v", err)
	}
	if got.HealthState.Status != "no_data" || len(got.Signals) != 0 || len(got.Summary) != 0 || len(got.Events) != 0 || got.Constraint != nil {
		t.Fatalf("current-window absence must suppress derived claims even with prior values: %+v", got)
	}
	for _, delta := range got.Deltas {
		if delta.HasData {
			t.Fatalf("delta %q hasData = true, want false for the empty current window", delta.Metric)
		}
	}
	if !got.Deltas[0].HasPriorData {
		t.Fatalf("cycle_time hasPriorData = false, want true from the populated comparison window: %+v", got.Deltas[0])
	}
}

func TestFetchMetricValueStoredZeroHasData(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(_ *testing.T, _ string, _ []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{rows: [][]any{{int64(1), 0.0}}}, nil
	}}

	got, err := fetchMetricValue(
		context.Background(),
		client,
		"repo_metrics_daily",
		"total_loc_touched",
		day(2024, 1, 1),
		day(2024, 1, 2),
		"",
		nil,
		"sum",
		"org-1",
	)
	if err != nil {
		t.Fatalf("fetchMetricValue: %v", err)
	}
	if got.Value != 0 || !got.HasData {
		t.Fatalf("stored zero = %+v, want value 0 with hasData true", got)
	}
}
