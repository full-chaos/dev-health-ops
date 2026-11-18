package testopsrisk

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// Stability is read only through the shared rule, which leaves out a repo-day whose newest pipeline row has no
// completed run; the daily statement carries it whole, with the join_use_nulls setting it relies on.
func TestDailyStabilityReadsTheSharedCompletedRunRule(t *testing.T) {
	if !strings.Contains(dailyQuery, testops.StabilityLiveRowsSQL) {
		t.Fatalf("dailyQuery does not read stability through testops.StabilityLiveRowsSQL:\n%s", dailyQuery)
	}
	if strings.Count(dailyQuery, "FROM testops_pipeline_stability") != 1 {
		t.Errorf("dailyQuery reads testops_pipeline_stability outside the shared rule:\n%s", dailyQuery)
	}
	if !strings.HasSuffix(dailyQuery, "SETTINGS join_use_nulls = 1") {
		t.Errorf("dailyQuery lost join_use_nulls = 1: a stability row with no pipeline row would read a completed count of 0")
	}
	for _, want := range []string{
		"argMax(success_count + failure_count, computed_at) AS completed_runs",
		"WHERE pipeline.completed_runs IS NULL OR pipeline.completed_runs > 0",
	} {
		if !strings.Contains(testops.StabilityLiveRowsSQL, want) {
			t.Errorf("StabilityLiveRowsSQL lacks %q", want)
		}
	}
}

// A day with no stability value is absent: not a spark point, not the latest value, never 0.
func TestNilStabilityReadsAbsent(t *testing.T) {
	day1 := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	client := &fakeClient{daily: [][]any{
		{&day1, nil, nil, nil, nil, nil, nil, nil, 0.8},
		{&day2, nil, nil, nil, nil, nil, nil, nil, nil},
	}}
	got, err := Resolve(context.Background(), client, "org-1", model.TestOpsRiskInput{StartDate: date("2026-11-13"), EndDate: date("2026-11-14")})
	if err != nil {
		t.Fatal(err)
	}
	if got.PipelineStability == nil || *got.PipelineStability != 0.8 {
		t.Errorf("PipelineStability = %v, want 0.8 (the day with a value; the absent day is not 0)", got.PipelineStability)
	}
	if len(got.StabilitySpark) != 1 || !got.StabilitySpark[0].Ts.Time().Equal(day1) || got.StabilitySpark[0].Value != 80 {
		t.Errorf("StabilitySpark = %+v, want one point %s = 80", got.StabilitySpark, day1.Format(dateLayout))
	}
}
