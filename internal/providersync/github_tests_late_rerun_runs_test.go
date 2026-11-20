package providersync

import (
	"fmt"
	"testing"
	"time"
)

// lateRerunRunAt is the run as GitHub shows it at t (ok=false before its creation): attempt 1 fails 5 minutes after its
// creation; attempt 2 starts at rerun and succeeds one hour later.
func lateRerunRunAt(created, rerun, at time.Time) (listingRun, bool) {
	run := listingRun{id: 1, created: created, attempt: 1}
	switch {
	case at.Before(created):
		return run, false
	case at.Before(created.Add(5 * time.Minute)):
		run.startedAt, run.updated, run.status = created, at, "in_progress"
	case at.Before(rerun):
		run.startedAt, run.updated, run.status, run.conclusion = created, created.Add(5*time.Minute), "completed", "failure"
	case at.Before(rerun.Add(time.Hour)):
		run.startedAt, run.updated, run.status, run.attempt = rerun, at, "in_progress", 2
	default:
		run.startedAt, run.updated, run.status, run.conclusion, run.attempt = rerun, rerun.Add(time.Hour), "completed", "success", 2
	}
	return run, true
}

// lateRerunWrite is one pipeline row a claim of the chain wrote.
type lateRerunWrite struct {
	claim         int
	since, before time.Time
	row           githubTestsPipelineRow
}

// lateRerunChain walks contiguous incremental claims (since = previous before, overlap 0) on CollectChunks, from s0 to end,
// with the open phase wired to a store that keeps the last write of a run (ReplacingMergeTree(last_synced) at a later clock).
// GitHub answers each claim with the run as it is at the claim's before.
func lateRerunChain(t *testing.T, handler GitHubTestsRouteHandler, s0, end time.Time, width time.Duration, created, rerun time.Time) (*fakeOpenRuns, []lateRerunWrite) {
	t.Helper()
	store := &fakeOpenRuns{}
	handler.OpenRuns = store
	var writes []lateRerunWrite
	for index := 0; s0.Add(time.Duration(index)*width).Before(end); index++ {
		since, before := s0.Add(time.Duration(index)*width), s0.Add(time.Duration(index+1)*width)
		var runs []listingRun
		if run, exists := lateRerunRunAt(created, rerun, before); exists {
			runs = []listingRun{run}
		}
		claim := listingClaim(since, before)
		claim.Mode = "incremental"
		walk := walkListing(t, handler, claim, newListingFake(t, runs), 0)
		for _, row := range walk.pipelines {
			writes = append(writes, lateRerunWrite{claim: index + 1, since: since, before: before, row: row})
			replaced := false
			for stored := range store.rows {
				if store.rows[stored].RepoID == row.RepoID && store.rows[stored].RunID == row.RunID {
					store.rows[stored], replaced = row, true
				}
			}
			if !replaced {
				store.rows = append(store.rows, row)
			}
		}
	}
	return store, writes
}

func lateRerunRowText(row githubTestsPipelineRow) string {
	status, finished := "<nil>", "<nil>"
	if row.Status != nil {
		status = *row.Status
	}
	if row.FinishedAt != nil {
		finished = row.FinishedAt.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf("status=%s retry_count=%d started=%s finished=%s", status, row.RetryCount, row.StartedAt.UTC().Format(time.RFC3339), finished)
}

// A completed run re-run after it left the 6 h refresh lookback must refresh its ci_pipeline_runs row to the new attempt.
// The chain runs to the second UTC midnight after the re-run's finish plus one width, so a daily relist is measured and
// its next deep claim is in the chain too. The first attempt-2 write finishes on or after its claim's since day (the
// daily recompute plans the windows' days), and no write of the run is older than its claim's since minus max(1 d, width).
func TestLateRerunRunsChainRefreshesThePipelineRow(t *testing.T) {
	s0 := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for _, cell := range []struct {
		name    string
		width   time.Duration
		created time.Duration // offset from claim 1's before (negative = inside claim 1)
		rerun   time.Duration // re-run start, offset from claim 1's before
	}{
		{"loss 1h rerun 30h later", time.Hour, -10 * time.Minute, 30 * time.Hour},
		{"loss 1h rerun 72h later", time.Hour, -10 * time.Minute, 72 * time.Hour},
		{"loss 1d rerun 3d later", 24 * time.Hour, -10 * time.Minute, 72 * time.Hour},
		{"loss 1h rerun 6d later", time.Hour, -10 * time.Minute, 6 * 24 * time.Hour},
		{"loss 7d rerun in claim 3", 7 * 24 * time.Hour, -24 * time.Hour, 8 * 24 * time.Hour},
		{"control 1h rerun 3h later", time.Hour, -10 * time.Minute, 3 * time.Hour},
	} {
		b1 := s0.Add(cell.width)
		created, rerun := b1.Add(cell.created), b1.Add(cell.rerun)
		finish := rerun.Add(time.Hour)
		end := finish.Add(3 * cell.width)
		if midnights := finish.Truncate(24 * time.Hour).Add(48*time.Hour + cell.width); midnights.After(end) {
			end = midnights
		}
		store, writes := lateRerunChain(t, GitHubTestsRouteHandler{}, s0, end, cell.width, created, rerun)
		var attempt2 []int
		firstChecked := false
		for _, write := range writes {
			floor := write.since.Add(-max(24*time.Hour, write.before.Sub(write.since)))
			if write.row.FinishedAt != nil && write.row.FinishedAt.Before(floor) {
				t.Errorf("%s: claim %d [%s, %s] wrote the run finished %s, before its floor %s: a day outside the plan",
					cell.name, write.claim, write.since.Format(time.RFC3339), write.before.Format(time.RFC3339),
					write.row.FinishedAt.UTC().Format(time.RFC3339), floor.Format(time.RFC3339))
			}
			if write.row.RetryCount != 1 {
				continue
			}
			attempt2 = append(attempt2, write.claim)
			if !firstChecked && write.row.FinishedAt != nil {
				firstChecked = true
				if write.row.FinishedAt.UTC().Truncate(24 * time.Hour).Before(write.since.Truncate(24 * time.Hour)) {
					t.Errorf("%s: first finished attempt-2 write (claim %d) finishes %s, before its since day %s",
						cell.name, write.claim, write.row.FinishedAt.UTC().Format(time.RFC3339), write.since.Format(time.DateOnly))
				}
			}
		}
		if len(store.rows) != 1 {
			t.Fatalf("%s: stored rows=%d, want 1", cell.name, len(store.rows))
		}
		row := store.rows[0]
		t.Logf("CELL %s: width=%s created=%s rerun=%s finish=%s; stored %s; attempt-2 writes in claims %v",
			cell.name, cell.width, created.Format(time.RFC3339), rerun.Format(time.RFC3339), finish.Format(time.RFC3339),
			lateRerunRowText(row), attempt2)
		if row.Status == nil || *row.Status != "success" || row.RetryCount != 1 || !row.StartedAt.Equal(rerun) ||
			row.FinishedAt == nil || !row.FinishedAt.Equal(finish) {
			t.Errorf("%s: stored %s, want the re-run: status=success retry_count=1 started=%s finished=%s",
				cell.name, lateRerunRowText(row), rerun.Format(time.RFC3339), finish.Format(time.RFC3339))
		}
	}
}
