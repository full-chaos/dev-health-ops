package testops

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// pipelineBucketCounts are the six bucket counts of a row, in the order success, failure, cancelled, skipped,
// unfinished, other.
type pipelineBucketCounts [6]int

func bucketCountsOf(metric PipelineMetric) pipelineBucketCounts {
	return pipelineBucketCounts{
		metric.SuccessCount, metric.FailureCount, metric.CancelledCount,
		metric.SkippedCount, metric.UnfinishedCount, metric.OtherCount,
	}
}

func (counts pipelineBucketCounts) sum() int {
	total := 0
	for _, count := range counts {
		total += count
	}
	return total
}

// pipelineStatusSeed holds each stored status with the bucket it must land in (index into pipelineBucketCounts).
var pipelineStatusSeed = []struct {
	status *string
	bucket int
}{
	{strPtr("success"), 0}, {strPtr("succeeded"), 0}, {strPtr("passed"), 0}, {strPtr(" Success "), 0},
	{strPtr("failure"), 1}, {strPtr("failed"), 1}, {strPtr("error"), 1}, {strPtr("errors"), 1},
	{strPtr("timeout"), 1}, {strPtr("timed_out"), 1},
	{strPtr("cancelled"), 2}, {strPtr("canceled"), 2}, {strPtr("cancel"), 2},
	{strPtr("skipped"), 3}, {strPtr("SKIPPED"), 3},
	{strPtr("running"), 4}, {strPtr("queued"), 4}, {strPtr(" Queued"), 4},
	{strPtr("stale"), 5}, {strPtr("completed"), 5}, {strPtr("canceling"), 5},
	{strPtr("some_new_provider_state"), 5}, {strPtr(""), 5}, {nil, 5},
}

// Every counted run status lands in exactly one of the six buckets, and in the bucket named for it.
func TestEveryPipelineRunStatusLandsInExactlyOneBucket(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000014019")
	started := time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC)
	for _, seed := range pipelineStatusSeed {
		accumulator := NewPipelineAccumulator(repoID, "", nil)
		accumulator.Add(PipelineRunRow{RepoID: repoID, Status: seed.status, StartedAt: started, OrgID: "org-1"})
		metric := accumulator.FinishRepoDay()
		if metric == nil {
			t.Fatalf("status %v: no row", derefStr(seed.status))
		}
		var want pipelineBucketCounts
		want[seed.bucket] = 1
		if got := bucketCountsOf(*metric); got != want || metric.PipelinesCount != 1 {
			t.Errorf("status %q: buckets %v (pipelines %d), want %v (pipelines 1)", derefStr(seed.status), got, metric.PipelinesCount, want)
		}
	}
}

// pipelines_count is the sum of the six buckets, per group and on the merged repo-day row; a deleted run is in none.
func TestPipelinesCountIsTheSumOfTheSixBuckets(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000014019")
	started := time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC)
	accumulator := NewPipelineAccumulator(repoID, "", nil)
	var want pipelineBucketCounts
	for index, seed := range pipelineStatusSeed {
		team := strPtr([]string{"team-a", "team-b"}[index%2])
		accumulator.Add(PipelineRunRow{RepoID: repoID, Status: seed.status, StartedAt: started, TeamID: team, OrgID: "org-1"})
		want[seed.bucket]++
	}
	accumulator.Add(PipelineRunRow{RepoID: repoID, Status: strPtr(StatusDeleted), StartedAt: started, OrgID: "org-1"})

	groups := accumulator.Finish()
	if len(groups) < 2 {
		t.Fatalf("the seed makes %d groups, want at least 2", len(groups))
	}
	var groupSum pipelineBucketCounts
	for _, group := range groups {
		counts := bucketCountsOf(group)
		if counts.sum() != group.PipelinesCount {
			t.Errorf("group %v: buckets %v sum to %d, pipelines_count %d", derefStr(group.TeamID), counts, counts.sum(), group.PipelinesCount)
		}
		for index := range counts {
			groupSum[index] += counts[index]
		}
	}
	merged := accumulator.FinishRepoDay()
	if merged == nil {
		t.Fatal("FinishRepoDay() = nil")
	}
	got := bucketCountsOf(*merged)
	if got != want || groupSum != want {
		t.Errorf("merged buckets %v, group sum %v, want %v", got, groupSum, want)
	}
	if merged.PipelinesCount != len(pipelineStatusSeed) || got.sum() != merged.PipelinesCount {
		t.Errorf("pipelines_count %d, bucket sum %d, want both %d", merged.PipelinesCount, got.sum(), len(pipelineStatusSeed))
	}
}

// An unfinished run is stale when its row was last synced more than OpenRunRefreshLookback before computed_at. A row
// synced exactly at the cutoff or later is not stale, and a run in another bucket is never stale.
func TestUnfinishedRunIsStaleWhenItsRowIsOlderThanTheOpenRunLookback(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-000000014019")
	computedAt := time.Date(2026, 11, 18, 3, 0, 0, 0, time.UTC)
	cutoff := computedAt.Add(-6 * time.Hour)
	if got := computedAt.Add(-OpenRunRefreshLookback); !got.Equal(cutoff) {
		t.Fatalf("OpenRunRefreshLookback = %s, want the 6 h of the GitHub open-run lookback", OpenRunRefreshLookback)
	}
	rows := []struct {
		status     string
		lastSynced time.Time
	}{
		{"running", cutoff.Add(-time.Millisecond)}, // stale
		{"queued", cutoff.Add(-2 * time.Hour)},     // stale
		{"running", cutoff.Add(-48 * time.Hour)},   // stale
		{"running", cutoff},                        // at the cutoff: not stale
		{"queued", cutoff.Add(time.Hour)},          // fresh
		{"stale", cutoff.Add(-48 * time.Hour)},     // other, never stale
		{"success", cutoff.Add(-48 * time.Hour)},   // completed, never stale
	}
	build := func(measure bool) PipelineMetric {
		accumulator := NewPipelineAccumulator(repoID, "", nil)
		if measure {
			accumulator.MeasureStaleAsOf(computedAt)
		}
		for _, row := range rows {
			accumulator.Add(PipelineRunRow{
				RepoID: repoID, Status: strPtr(row.status), StartedAt: cutoff.Add(-72 * time.Hour),
				LastSynced: row.lastSynced, OrgID: "org-1",
			})
		}
		return *accumulator.FinishRepoDay()
	}
	if got := build(true); got.UnfinishedCount != 5 || got.UnfinishedStaleCount != 3 {
		t.Errorf("unfinished %d, stale %d; want 5 and 3", got.UnfinishedCount, got.UnfinishedStaleCount)
	}
	if got := build(false); got.UnfinishedCount != 5 || got.UnfinishedStaleCount != 0 {
		t.Errorf("without MeasureStaleAsOf: unfinished %d, stale %d; want 5 and 0", got.UnfinishedCount, got.UnfinishedStaleCount)
	}
}
