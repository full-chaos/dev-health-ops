//go:build integration

package home

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
)

// The prior value beside a delta is the stored value of the comparison window,
// not a number back-solved from the current value and the percent (CHAOS-9817).
// Rows come from the real writer; the response from BuildResponse.
func TestHomeServesTheMeasuredPriorValueOfTheComparisonWindow(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	f := DefaultFilters()
	startDay, _, compareStart, _, err := TimeWindow(f, now)
	if err != nil {
		t.Fatal(err)
	}
	computedAt := now.Add(-time.Hour)
	loc := func(v int) *int { return &v }
	cases := []struct {
		name       string
		org        string
		prior      *int // total_loc_touched in the comparison window; nil = no row
		current    *int
		wantPrior  *float64
		wantSignal *string
	}{
		{"prior measured, current 0", "prior-org-fall-to-zero", loc(120), loc(0), ptr(120.0), strPtr("120 loc")},
		{"prior measured, current lower", "prior-org-fall", loc(120), loc(60), ptr(120.0), strPtr("120 loc")},
		{"prior measured 0, current measured 0", "prior-org-zero-zero", loc(0), loc(0), ptr(0.0), strPtr("0 loc")},
		{"prior measured 0, current above", "prior-org-from-zero", loc(0), loc(50), ptr(0.0), strPtr("0 loc")},
		{"prior missing", "prior-org-missing", nil, loc(50), nil, nil},
		{"prior fraction", "prior-org-fraction", loc(7), loc(3), ptr(7.0), strPtr("7 loc")},
	}
	for _, tc := range cases {
		repo := uuid.New()
		var metrics []repouser.RepoMetric
		if tc.prior != nil {
			metrics = append(metrics, repouser.RepoMetric{RepoID: repo, Day: compareStart, CommitsCount: 3, TotalLOCTouched: *tc.prior, ComputedAt: computedAt})
		}
		if tc.current != nil {
			metrics = append(metrics, repouser.RepoMetric{RepoID: repo, Day: startDay, CommitsCount: 3, TotalLOCTouched: *tc.current, ComputedAt: computedAt})
		}
		if _, _, _, err := writer.WriteResult(ctx, repouser.Result{RepoMetrics: metrics}, tc.org); err != nil {
			t.Fatalf("%s: write: %v", tc.name, err)
		}
		resp, err := BuildResponse(ctx, client, nil, tc.org, f, now)
		if err != nil {
			t.Fatalf("%s: BuildResponse: %v", tc.name, err)
		}
		var signal *Signal
		for i := range resp.Signals {
			if resp.Signals[i].ID == "metric:churn" {
				signal = &resp.Signals[i]
			}
		}
		if signal == nil {
			t.Fatalf("%s: the response has no churn signal", tc.name)
		}
		var delta *MetricDelta
		for i := range resp.Deltas {
			if resp.Deltas[i].Metric == "churn" {
				delta = &resp.Deltas[i]
			}
		}
		if delta == nil {
			t.Fatalf("%s: the response has no churn delta", tc.name)
		}
		switch {
		case tc.wantPrior == nil && delta.PriorValue != nil:
			t.Errorf("%s: delta priorValue = %v, want null", tc.name, *delta.PriorValue)
		case tc.wantPrior != nil && delta.PriorValue == nil:
			t.Errorf("%s: delta priorValue is null, want %v", tc.name, *tc.wantPrior)
		case tc.wantPrior != nil && *delta.PriorValue != *tc.wantPrior:
			t.Errorf("%s: delta priorValue = %v, want %v", tc.name, *delta.PriorValue, *tc.wantPrior)
		}
		switch {
		case tc.wantSignal == nil && signal.PriorValue != nil:
			t.Errorf("%s: signal prior value = %q, want none", tc.name, *signal.PriorValue)
		case tc.wantSignal != nil && signal.PriorValue == nil:
			t.Errorf("%s: signal prior value is none, want %q", tc.name, *tc.wantSignal)
		case tc.wantSignal != nil && *signal.PriorValue != *tc.wantSignal:
			t.Errorf("%s: signal prior value = %q, want %q", tc.name, *signal.PriorValue, *tc.wantSignal)
		}
	}
}

func ptr(v float64) *float64  { return &v }
func strPtr(v string) *string { return &v }
