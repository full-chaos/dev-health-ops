package providersync

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Base controls for two r1 review classes of the runs-phase coalescing. Every cell here runs the tests route with no
// coalescing field set, so this file also builds and runs on the base binary; the coalescing cells compare their flag-on
// result with these numbers.

// reseekRewrittenRuns runs the moved-page scenario at one durable position: 6 runs (one pipeline row each) on one
// listing page, attempts until 3 runs are written, then the second written run (46) is deleted, so the resume finds its
// anchor moved and re-seeks from the oldest second it listed. It returns the runs written more than once.
func reseekRewrittenRuns(t *testing.T, handler CompleteRouteHandler) int {
	t.Helper()
	since, before := listingSince.Add(-time.Hour), listingSince.Add(time.Hour)
	now := before.Add(time.Hour)
	claim, session := runsGroupWindowSession(t, now, since, before)
	descriptor := cicdDescriptor(t)
	descriptor.ChunkPolicy.MaxChunksPerAttempt = 1
	descriptor.ChunkPolicy.MaxSourceItems = 3
	runs := makeListingRuns(42, 6, since, time.Second)
	for index := range runs {
		runs[index].updated = before
	}
	fake := newListingFake(t, runs)
	sink := newRunsGroupSink()
	executor := runsGroupExecutor(now, handler, NewInProcessChunkLedger(claim), sink, fake, nil)
	for attempt := 1; len(sink.pipelineRows) < 3; attempt++ {
		if attempt > 10 {
			t.Fatalf("3 runs not written in 10 attempts: %v", sink.pipelineRows)
		}
		if _, err := executor.Execute(context.Background(), session, descriptor); !errors.As(err, new(ChunkContinuationError)) {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
	if len(sink.pipelineRows) != 3 || sink.pipelineRows["46"] != 1 {
		t.Fatalf("before the deletion: written runs %v, want 47, 46 and 45 once", sink.pipelineRows)
	}
	resumeDeletionDelete(fake, 46)
	runUntilDone(t, executor, session, descriptor)
	rewritten := 0
	for _, writes := range sink.pipelineRows {
		if writes > 1 {
			rewritten++
		}
	}
	if len(sink.pipelineRows) != 6 {
		t.Fatalf("written runs %v, want all 6 (the deleted 46 was written before it was deleted)", sink.pipelineRows)
	}
	return rewritten
}

// wallDurableRuns runs 3 runs whose first jobs read takes MaxWallTime-1s and whose second takes 1s+late on the
// injected clock, and returns the runs written by the first attempt and the cursor's pipeline count.
func wallDurableRuns(t *testing.T, handler CompleteRouteHandler, late time.Duration) (int, int) {
	t.Helper()
	since, before := listingSince.Add(-time.Hour), listingSince.Add(time.Hour)
	now := before.Add(time.Hour)
	claim, session := runsGroupWindowSession(t, now, since, before)
	descriptor := cicdDescriptor(t)
	wall := descriptor.ChunkPolicy.MaxWallTime
	runs := makeListingRuns(10, 3, since, time.Second)
	for index := range runs {
		runs[index].updated = before
	}
	fake := newListingFake(t, runs)
	clock := &coalesceClock{now: now}
	sink, store := newRunsGroupSink(), NewInProcessChunkLedger(claim)
	reads := 0
	executor := runsGroupExecutor(now, handler, store, sink, coalesceDoerFunc(func(request *http.Request) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "/jobs") {
			reads++
			switch reads {
			case 1:
				clock.advance(wall - time.Second)
			case 2:
				clock.advance(time.Second + late)
			}
		}
		return fake.Do(request)
	}), clock)
	_, err := executor.Execute(context.Background(), session, descriptor)
	var stopped ChunkContinuationError
	if !errors.As(err, &stopped) || stopped.Reason != ChunkStopWallTime {
		t.Fatalf("first attempt err=%v, want a wall continuation", err)
	}
	checkpoint, err := store.LoadChunkCheckpoint(context.Background(), claim, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := decodeGitHubTestsChunkCursor(checkpoint.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	durable := len(sink.pipelineRows)
	runUntilDone(t, executor, session, descriptor)
	if len(sink.pipelineRows) != 3 || sink.duplicates() != 0 {
		t.Fatalf("after the resume: written runs %v, duplicates %d", sink.pipelineRows, sink.duplicates())
	}
	return durable, cursor.Pipelines
}

// A re-seek after a moved page re-writes the surviving run of the re-seek second: the existing design
// (githubTestsReseek), idempotent under ReplacingMergeTree. One run is written twice.
func TestGitHubTestsMovedPageReseekRewriteBaseControl(t *testing.T) {
	if rewritten := reseekRewrittenRuns(t, GitHubTestsRouteHandler{}); rewritten != 1 {
		t.Fatalf("runs written twice after the re-seek = %d, want 1 (the run of the re-seek second)", rewritten)
	}
}

// A run whose jobs read ends at or after the wall is still committed by the attempt it was read in: 2 runs.
func TestGitHubTestsRunReadAtTheWallBaseControl(t *testing.T) {
	for _, late := range []time.Duration{0, time.Millisecond} {
		if durable, pipelines := wallDurableRuns(t, GitHubTestsRouteHandler{}, late); durable != 2 || pipelines != 2 {
			t.Fatalf("late %s: first attempt wrote %d runs (cursor pipelines %d), want 2 and 2", late, durable, pipelines)
		}
	}
}
