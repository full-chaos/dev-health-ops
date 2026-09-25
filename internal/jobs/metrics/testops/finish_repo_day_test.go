package testops

import (
	"math"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

// CHAOS-6774: testops_pipeline_metrics_daily holds one row per (org, repo, day)
// (ReplacingMergeTree ORDER BY (org_id, repo_id, day), migration 096), so the
// native write path merges the per-(team, service) groups first.

func pipelineRun(repoID uuid.UUID, hour int, status string, seconds int, team, service *string, retries uint32) PipelineRunRow {
	return PipelineRunRow{
		RepoID: repoID, Status: strPtr(status), StartedAt: floatTime(2026, 8, 27, hour, 0, 0),
		FinishedAt: floatTimePtr(2026, 8, 27, hour, seconds/60, seconds%60), RetryCount: retries,
		TeamID: team, ServiceID: service, OrgID: "org-1",
	}
}

func withQueue(row PipelineRunRow, seconds float64) PipelineRunRow {
	row.QueueSeconds = &seconds
	return row
}

func finishRepoDay(t *testing.T, rows ...PipelineRunRow) *PipelineMetric {
	t.Helper()
	acc := NewPipelineAccumulator(rows[0].RepoID, "acme/widgets", nil)
	for _, row := range rows {
		acc.Add(row)
	}
	return acc.FinishRepoDay()
}

func TestFinishRepoDayOfNoRowsIsNil(t *testing.T) {
	if got := NewPipelineAccumulator(uuid.New(), "", nil).FinishRepoDay(); got != nil {
		t.Fatalf("FinishRepoDay() on no rows = %#v, want nil", got)
	}
}

func TestFinishRepoDayOfOneGroupIsExactlyWhatFinishReturns(t *testing.T) {
	repoID := uuid.New()
	rows := []PipelineRunRow{
		pipelineRun(repoID, 1, "success", 60, strPtr("t1"), strPtr("svc"), 0),
		pipelineRun(repoID, 2, "failed", 120, strPtr("t1"), strPtr("svc"), 1),
	}
	acc := NewPipelineAccumulator(repoID, "acme/widgets", nil)
	for _, row := range rows {
		acc.Add(row)
	}
	perGroup := acc.Finish()
	if len(perGroup) != 1 {
		t.Fatalf("Finish() = %d groups, want 1", len(perGroup))
	}
	merged := acc.FinishRepoDay()
	if merged == nil || !reflect.DeepEqual(*merged, perGroup[0]) {
		t.Fatalf("FinishRepoDay() = %#v, want Finish()[0] = %#v", merged, perGroup[0])
	}
}

func TestFinishRepoDayMergesGroupsFromSumsAndTheUnionOfSamples(t *testing.T) {
	repoID := uuid.New()
	got := finishRepoDay(t,
		pipelineRun(repoID, 1, "success", 60, strPtr("t1"), strPtr("a"), 0),
		pipelineRun(repoID, 2, "failed", 120, strPtr("t1"), strPtr("a"), 0),
		pipelineRun(repoID, 3, "success", 300, strPtr("t1"), strPtr("b"), 1),
	)
	if got == nil {
		t.Fatal("FinishRepoDay() = nil")
	}
	if got.PipelinesCount != 3 || got.SuccessCount != 2 || got.FailureCount != 1 || got.CancelledCount != 0 {
		t.Fatalf("counts = %d/%d/%d/%d, want 3/2/1/0 (summed over both groups)", got.PipelinesCount, got.SuccessCount, got.FailureCount, got.CancelledCount)
	}
	// 2/3 from the summed counts; the mean of the two groups' own rates
	// (1/2 and 1/1) would be 0.75.
	for name, pair := range map[string][2]float64{
		"success": {got.SuccessRate, 2.0 / 3}, "failure": {got.FailureRate, 1.0 / 3}, "rerun": {got.RerunRate, 1.0 / 3},
	} {
		if math.Abs(pair[0]-pair[1]) > 1e-12 {
			t.Errorf("%s rate = %v, want %v", name, pair[0], pair[1])
		}
	}
	// samples 60, 120, 300 pooled: median 120, p95 = 120 + 0.9*(300-120) = 282.
	if got.MedianDurationSeconds == nil || *got.MedianDurationSeconds != 120 {
		t.Errorf("median = %v, want 120 over the union", got.MedianDurationSeconds)
	}
	if got.P95DurationSeconds == nil || math.Abs(*got.P95DurationSeconds-282) > 1e-9 {
		t.Errorf("p95 = %v, want 282 over the union", got.P95DurationSeconds)
	}
	if got.TeamID == nil || *got.TeamID != "t1" {
		t.Errorf("team = %v, want t1 (every group agrees)", got.TeamID)
	}
	if got.ServiceID != nil {
		t.Errorf("service = %q, want nil (groups a and b disagree)", *got.ServiceID)
	}
	if got.OrgID != "org-1" {
		t.Errorf("org = %q", got.OrgID)
	}
}

func TestFinishRepoDayPoolsTheQueueSamplesOfEveryGroup(t *testing.T) {
	repoID := uuid.New()
	got := finishRepoDay(t,
		withQueue(pipelineRun(repoID, 1, "success", 60, strPtr("t1"), strPtr("a"), 0), 1),
		withQueue(pipelineRun(repoID, 2, "success", 60, strPtr("t1"), strPtr("a"), 0), 3),
		withQueue(pipelineRun(repoID, 3, "success", 60, strPtr("t1"), strPtr("b"), 0), 5),
	)
	if got == nil {
		t.Fatal("FinishRepoDay() = nil")
	}
	// pooled samples 1, 3, 5: mean 3, p95 = 3 + 0.9*(5-3) = 4.8. The groups' own
	// means are 2 and 5, so an average of the groups (3.5), or the first
	// group's alone (2), would be wrong.
	if got.AvgQueueSeconds == nil || math.Abs(*got.AvgQueueSeconds-3) > 1e-9 {
		t.Errorf("avg queue = %v, want 3 over the pooled samples", got.AvgQueueSeconds)
	}
	if got.P95QueueSeconds == nil || math.Abs(*got.P95QueueSeconds-4.8) > 1e-9 {
		t.Errorf("p95 queue = %v, want 4.8 over the pooled samples", got.P95QueueSeconds)
	}
}

func TestFinishRepoDayTeamAndServiceAreSetOnlyWhenEveryGroupAgrees(t *testing.T) {
	repoID := uuid.New()
	cases := []struct {
		name             string
		a, b             PipelineRunRow
		wantTeam, wantSv *string
	}{
		{"same team and service", pipelineRun(repoID, 1, "success", 60, strPtr("t1"), strPtr("s"), 0), pipelineRun(repoID, 2, "success", 60, strPtr("t1"), strPtr("s"), 0), strPtr("t1"), strPtr("s")},
		{"teams differ", pipelineRun(repoID, 1, "success", 60, strPtr("t1"), strPtr("s"), 0), pipelineRun(repoID, 2, "success", 60, strPtr("t2"), strPtr("s"), 0), nil, strPtr("s")},
		{"services differ", pipelineRun(repoID, 1, "success", 60, strPtr("t1"), strPtr("s"), 0), pipelineRun(repoID, 2, "success", 60, strPtr("t1"), strPtr("z"), 0), strPtr("t1"), nil},
		{"nil service versus empty service are distinct", pipelineRun(repoID, 1, "success", 60, strPtr("t1"), nil, 0), pipelineRun(repoID, 2, "success", 60, strPtr("t1"), strPtr(""), 0), strPtr("t1"), nil},
		{"both without a team", pipelineRun(repoID, 1, "success", 60, nil, strPtr("s"), 0), pipelineRun(repoID, 2, "success", 60, nil, strPtr("s"), 0), nil, strPtr("s")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := finishRepoDay(t, tc.a, tc.b)
			if got == nil {
				t.Fatal("FinishRepoDay() = nil")
			}
			if !reflect.DeepEqual(got.TeamID, tc.wantTeam) || !reflect.DeepEqual(got.ServiceID, tc.wantSv) {
				t.Fatalf("team=%v service=%v, want team=%v service=%v", deref(got.TeamID), deref(got.ServiceID), deref(tc.wantTeam), deref(tc.wantSv))
			}
		})
	}
}

func deref(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}
