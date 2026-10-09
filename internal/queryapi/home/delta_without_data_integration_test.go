//go:build integration

package home

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
)

// A delta states a move between two measured values. A window with no value
// (unknown, not applicable, never counted) reads 0 and 0 against a real prior
// is -100 %; Home must serve no delta and no event for it, for every metric.
// Rows come from the real writers; the response from BuildResponse.
func TestHomeServesNoDeltaAndNoEventWhenAWindowHasNoValue(t *testing.T) {
	ctx := context.Background()
	admin, client := newHomeTestClickHouse(ctx, t)
	writer, err := repouser.NewWriter(admin)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	f := DefaultFilters()
	startDay, endDay, compareStart, compareEnd, err := TimeWindow(f, now)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = endDay, compareEnd
	computedAt := now.Add(-time.Hour)
	measured := changefailure.Counts{Deployments: 10, FailedHeuristic: 5, IncidentsDirect: 1}
	zero := changefailure.Counts{Deployments: 4, IncidentsDirect: 1}
	unknown := changefailure.Counts{Deployments: 4}
	notApplicable := changefailure.Counts{IncidentsDirect: 1}
	cases := []struct {
		name         string
		org          string
		prior        *changefailure.Counts
		current      *changefailure.Counts
		wantDelta    bool
		wantEvent    bool
		wantDeltaPct float64
	}{
		{"prior measured, current unknown", "delta-org-unknown", &measured, &unknown, false, false, 0},
		{"prior measured, current not applicable", "delta-org-na", &measured, &notApplicable, false, false, 0},
		{"prior measured, current never counted", "delta-org-none", &measured, nil, false, false, 0},
		{"prior unknown, current measured", "delta-org-prior-unknown", &unknown, &measured, false, false, 0},
		{"prior not applicable, current measured", "delta-org-prior-na", &notApplicable, &measured, false, false, 0},
		{"prior never counted, current measured", "delta-org-prior-none", nil, &measured, false, false, 0},
		// Control: two measured values keep the move and the event.
		{"prior measured 50 %, current measured 0 %", "delta-org-both", &measured, &zero, true, true, -100},
	}
	for _, tc := range cases {
		repo := uuid.New()
		var rows []repouser.ChangeFailureDaily
		if tc.prior != nil {
			rows = append(rows, repouser.ChangeFailureDaily{RepoID: repo, Day: compareStart, Counts: *tc.prior, ComputedAt: computedAt})
		}
		if tc.current != nil {
			rows = append(rows, repouser.ChangeFailureDaily{RepoID: repo, Day: startDay, Counts: *tc.current, ComputedAt: computedAt})
		}
		if len(rows) > 0 {
			if _, err := writer.WriteChangeFailure(ctx, rows, tc.org); err != nil {
				t.Fatal(err)
			}
		}
		// Other metrics have data in both windows, as in a real organization.
		if _, _, _, err := writer.WriteResult(ctx, repouser.Result{RepoMetrics: []repouser.RepoMetric{
			{RepoID: repo, Day: compareStart, CommitsCount: 3, TotalLOCTouched: 120, ComputedAt: computedAt},
			{RepoID: repo, Day: startDay, CommitsCount: 3, TotalLOCTouched: 120, ComputedAt: computedAt},
		}}, tc.org); err != nil {
			t.Fatal(err)
		}
		resp, err := BuildResponse(ctx, client, nil, tc.org, f, now)
		if err != nil {
			t.Fatalf("%s: BuildResponse: %v", tc.name, err)
		}
		labels := map[string]bool{}
		sawCFR := false
		for _, d := range resp.Deltas {
			// The rule holds for every metric, not only change failure rate.
			if (!d.HasData || !d.HasPriorData) && d.DeltaPct != 0 {
				t.Errorf("%s: metric %s has hasData=%v hasPriorData=%v and deltaPct %v, want 0", tc.name, d.Metric, d.HasData, d.HasPriorData, d.DeltaPct)
			}
			if !d.HasData || !d.HasPriorData {
				labels[d.Label] = true
			}
			if d.Metric != "change_failure_rate" {
				continue
			}
			sawCFR = true
			if tc.wantDelta && d.DeltaPct != tc.wantDeltaPct {
				t.Errorf("%s: change failure rate deltaPct = %v, want %v", tc.name, d.DeltaPct, tc.wantDeltaPct)
			}
			if !tc.wantDelta && d.DeltaPct != 0 {
				t.Errorf("%s: change failure rate deltaPct = %v, want 0 (no delta)", tc.name, d.DeltaPct)
			}
		}
		if !sawCFR {
			t.Fatalf("%s: the response has no change_failure_rate delta", tc.name)
		}
		gotEvent := false
		for _, e := range resp.Events {
			for label := range labels {
				if strings.HasPrefix(e.Text, label+" shifted") {
					t.Errorf("%s: event %q for a metric whose window has no value", tc.name, e.Text)
				}
			}
			if strings.HasPrefix(e.Text, "Change Failure Rate shifted") {
				gotEvent = true
			}
		}
		if gotEvent != tc.wantEvent {
			t.Errorf("%s: change failure rate event = %v, want %v", tc.name, gotEvent, tc.wantEvent)
		}
	}
}
