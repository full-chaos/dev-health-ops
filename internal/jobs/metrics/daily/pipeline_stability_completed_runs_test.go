package daily

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

func stabilityGroup(success, failure, cancelled int) testops.PipelineMetric {
	metric := testops.PipelineMetric{
		OrgID: "org", PipelinesCount: success + failure + cancelled,
		SuccessCount: success, FailureCount: failure, CancelledCount: cancelled,
	}
	if completed := success + failure; completed > 0 {
		metric.SuccessRate = float64(success) / float64(completed)
		metric.FailureRate = float64(failure) / float64(completed)
	}
	return metric
}

// A (team, service) group of the day with no completed run is left out of the stability success rate, never read as
// a rate of 0. The later groups move one place in the weights: [1.0, none, 0.0] weighs 1.0 by 1 and 0.0 by 1.5 (0.4),
// not by 2.0 as its old position would (0.3333). The positions are the groups of one day, not days.
func TestPipelineStabilityLeavesOutAGroupWithNoCompletedRun(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000013802")
	day := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	withGap := computePipelineStability(repoID, day, []testops.PipelineMetric{
		stabilityGroup(1, 0, 0), stabilityGroup(0, 0, 3), stabilityGroup(0, 1, 0),
	}, day)
	without := computePipelineStability(repoID, day, []testops.PipelineMetric{
		stabilityGroup(1, 0, 0), stabilityGroup(0, 1, 0),
	}, day)
	if withGap == nil || without == nil {
		t.Fatalf("computePipelineStability = %v / %v, want rows", withGap, without)
	}
	const keptPositions = (1.0*1 + 0.0*2.0) / (1 + 2.0)
	t.Logf("success_rate_7d: group left out = %.4f, groups without it = %.4f, kept positions would be %.4f",
		withGap.SuccessRate7d, without.SuccessRate7d, keptPositions)
	if math.Abs(withGap.SuccessRate7d-0.4) > 1e-9 {
		t.Errorf("success_rate_7d = %v, want 0.4 (the group with no completed run left out, the next group moved up)", withGap.SuccessRate7d)
	}
	if withGap.SuccessRate7d != without.SuccessRate7d || withGap.SuccessRateTrend != without.SuccessRateTrend {
		t.Errorf("left-out group: 7d %v trend %v, want the two-group values 7d %v trend %v",
			withGap.SuccessRate7d, withGap.SuccessRateTrend, without.SuccessRate7d, without.SuccessRateTrend)
	}
	// The failure clustering still walks every group: the left-out group sits between the two others, so the failing
	// group has no failing neighbour in either form.
	if withGap.FailureClusteringScore != without.FailureClusteringScore {
		t.Errorf("failure clustering %v, want %v", withGap.FailureClusteringScore, without.FailureClusteringScore)
	}
}

// A day whose groups have no completed run has no stability row: no success rate to score, never 0.
func TestPipelineStabilityOfADayWithNoCompletedRunIsAbsent(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000013803")
	day := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	if row := computePipelineStability(repoID, day, []testops.PipelineMetric{stabilityGroup(0, 0, 2), stabilityGroup(0, 0, 1)}, day); row != nil {
		t.Fatalf("computePipelineStability = %+v, want nil (no completed run)", row)
	}
}
