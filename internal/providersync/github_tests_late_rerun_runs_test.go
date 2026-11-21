package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
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
	for index := 0; s0.Add(time.Duration(index) * width).Before(end); index++ {
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
		want    []int         // the claims that write attempt 2
	}{
		{"loss 1h rerun 30h later", time.Hour, -10 * time.Minute, 30 * time.Hour, []int{48}},
		{"loss 1h rerun 72h later", time.Hour, -10 * time.Minute, 72 * time.Hour, []int{96}},
		{"loss 1d rerun 3d later", 24 * time.Hour, -10 * time.Minute, 72 * time.Hour, []int{4, 5, 6}},
		{"loss 1h rerun 6d later", time.Hour, -10 * time.Minute, 6 * 24 * time.Hour, []int{168}},
		{"loss 7d rerun in claim 3", 7 * 24 * time.Hour, -24 * time.Hour, 8 * 24 * time.Hour, []int{3}},
		{"control 1h rerun 3h later", time.Hour, -10 * time.Minute, 3 * time.Hour, []int{4, 5, 6}},
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
		if fmt.Sprint(attempt2) != fmt.Sprint(cell.want) {
			t.Errorf("%s: attempt-2 writes in claims %v, want %v", cell.name, attempt2, cell.want)
		}
		if row.Status == nil || *row.Status != "success" || row.RetryCount != 1 || !row.StartedAt.Equal(rerun) ||
			row.FinishedAt == nil || !row.FinishedAt.Equal(finish) {
			t.Errorf("%s: stored %s, want the re-run: status=success retry_count=1 started=%s finished=%s",
				cell.name, lateRerunRowText(row), rerun.Format(time.RFC3339), finish.Format(time.RFC3339))
		}
	}
}

// lateRerunRuns builds count runs created three days before since, one a minute, each re-run so that it last changed one
// hour before since: attempt 2, completed. They are older than the runs range and changed after the deep floor.
func lateRerunRuns(first, count int, since time.Time) []listingRun {
	runs := make([]listingRun, 0, count)
	for index := range count {
		runs = append(runs, listingRun{id: first + index, created: since.Add(-72*time.Hour + time.Duration(index)*time.Minute),
			startedAt: since.Add(-2 * time.Hour), updated: since.Add(-time.Hour), status: "completed", conclusion: "success", attempt: 2})
	}
	return runs
}

// lateRerunDeepRanges counts the listings of the deep range the claim sent: the created ranges that end one second
// before the runs range (a peeled deep range ends earlier and is not counted).
func lateRerunDeepRanges(fake *listingFake, claim Claim) int {
	hi := claim.SinceAt.UTC().Add(-githubTestsRefreshLookbackDefault - time.Second).Format(time.RFC3339)
	count := 0
	for _, created := range fake.listRanges {
		if strings.HasSuffix(created, ".."+hi) {
			count++
		}
	}
	return count
}

func lateRerunIncomplete(t *testing.T, walk listingWalk) []GitHubTestsIncomplete {
	t.Helper()
	incomplete, ok := walk.final.Result[GitHubTestsResultKeyIncomplete].([]GitHubTestsIncomplete)
	if !ok {
		t.Fatalf("result incomplete is %T", walk.final.Result[GitHubTestsResultKeyIncomplete])
	}
	return incomplete
}

func lateRerunMidnightClaim(mode string) Claim {
	since, before := time.Date(2026, 7, 20, 23, 0, 0, 0, time.UTC), time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	claim := listingClaim(since, before)
	claim.Mode = mode
	return claim
}

// Only the claim that holds a UTC midnight lists the deep range, and the deep range reads no run whose last change came
// less than the refresh lookback after its creation: a normal run costs no jobs request there.
func TestLateRerunRunsRangeOnlyOnTheMidnightClaimAndOnlyLateChanges(t *testing.T) {
	s0 := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	var runs []listingRun
	for hour := range 72 {
		created := s0.Add(time.Duration(hour)*time.Hour + 10*time.Minute)
		runs = append(runs, listingRun{id: 100 + hour, created: created, updated: created.Add(5 * time.Minute), status: "completed", conclusion: "success"})
	}
	var deepIn []int
	for index := range 72 {
		since, before := s0.Add(time.Duration(index)*time.Hour), s0.Add(time.Duration(index+1)*time.Hour)
		claim := listingClaim(since, before)
		claim.Mode = "incremental"
		fake := newListingFake(t, runs)
		walkListing(t, GitHubTestsRouteHandler{}, claim, fake, 0)
		if lateRerunDeepRanges(fake, claim) > 0 {
			deepIn = append(deepIn, index+1)
		}
		for _, run := range runs {
			if fake.jobGets[run.id] > 0 && run.created.Before(since.Add(-githubTestsRefreshLookbackDefault)) {
				t.Errorf("claim %d [%s, %s]: jobs read %d times for run %d created %s, a normal run older than the runs range",
					index+1, since.Format(time.RFC3339), before.Format(time.RFC3339), fake.jobGets[run.id], run.id, run.created.Format(time.RFC3339))
			}
		}
	}
	if fmt.Sprint(deepIn) != fmt.Sprint([]int{24, 48, 72}) {
		t.Fatalf("deep range sent by claims %v, want [24 48 72] (the claims that hold a UTC midnight)", deepIn)
	}
}

// A run created exactly at the start of the runs range is listed by the runs range only: the deep range ends one second
// before it, so the run is read once in the claim.
func TestLateRerunRunsSeamRunIsReadOnce(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	seam := claim.SinceAt.Add(-githubTestsRefreshLookbackDefault)
	run := listingRun{id: 7, created: seam, startedAt: seam.Add(6 * time.Hour), updated: claim.SinceAt.Add(30 * time.Minute),
		status: "completed", conclusion: "success", attempt: 2}
	fake := newListingFake(t, []listingRun{run})
	walk := walkListing(t, GitHubTestsRouteHandler{}, claim, fake, 0)
	if lateRerunDeepRanges(fake, claim) != 1 {
		t.Fatalf("deep ranges=%d, want 1 (%v)", lateRerunDeepRanges(fake, claim), fake.listRanges)
	}
	rows := 0
	for _, row := range walk.pipelines {
		if row.RunID == "7" {
			rows++
		}
	}
	if fake.jobGets[7] != 1 || rows != 1 {
		t.Fatalf("seam run: jobs reads=%d pipeline rows=%d, want 1 and 1 (ranges %v)", fake.jobGets[7], rows, fake.listRanges)
	}
}

// The deep range belongs to an incremental claim with both edges that holds a UTC midnight M, since < M <= before. A
// retry of that claim keeps its window, so it lists the deep range again.
func TestLateRerunRunsMidnightEdges(t *testing.T) {
	day := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	window := func(since, before time.Time, mode string) Claim {
		claim := listingClaim(since, before)
		claim.Mode = mode
		return claim
	}
	open := window(day.Add(-time.Hour), day, "incremental")
	open.SinceAt = nil
	for _, cell := range []struct {
		name  string
		claim Claim
		deep  bool
	}{
		{"midnight == before", window(day.Add(-time.Hour), day, "incremental"), true},
		{"midnight == since", window(day, day.Add(time.Hour), "incremental"), false},
		{"no midnight", window(day.Add(time.Hour), day.Add(2*time.Hour), "incremental"), false},
		{"two-day claim", window(day.Add(-36*time.Hour), day.Add(12*time.Hour), "incremental"), true},
		{"backfill holds a midnight", window(day.Add(-time.Hour), day, "backfill"), false},
		{"full resync holds a midnight", window(day.Add(-time.Hour), day, "full_resync"), false},
		{"open start", open, false},
	} {
		if got := githubTestsHoldsMidnight(cell.claim); got != cell.deep {
			t.Errorf("%s: holds midnight=%v, want %v", cell.name, got, cell.deep)
		}
		if cell.claim.SinceAt == nil {
			continue
		}
		late := lateRerunRuns(1, 1, cell.claim.SinceAt.UTC())
		for attempt := 1; attempt <= 2; attempt++ {
			fake := newListingFake(t, late)
			walk := walkListing(t, GitHubTestsRouteHandler{}, cell.claim, fake, 0)
			refreshed := len(walk.pipelines) == 1 && walk.pipelines[0].RetryCount == 1
			if got := lateRerunDeepRanges(fake, cell.claim) == 1; got != cell.deep || refreshed != cell.deep {
				t.Errorf("%s attempt %d: deep range=%v late run refreshed=%v, want %v (ranges %v)", cell.name, attempt, got, refreshed, cell.deep, fake.listRanges)
			}
		}
	}
}

// A midnight claim held by the runs-range page budget (no watermark) is claimed again with the same window, so it still
// owns the midnight: once its runs range fits, the deep range refreshes the late re-run.
func TestLateRerunRunsHeldClaimReownsTheMidnight(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	runs := lateRerunRuns(1, 1, claim.SinceAt.UTC())
	runs = append(runs, makeListingRuns(1000, 150, claim.SinceAt.UTC(), 20*time.Second)...)
	held := walkListing(t, GitHubTestsRouteHandler{MaxRuns: 100}, claim, newListingFake(t, runs), 0)
	if held.final.Watermark != nil {
		t.Fatalf("held claim watermark=%v, want nil (runs range over its budget)", held.final.Watermark)
	}
	retry := walkListing(t, GitHubTestsRouteHandler{}, claim, newListingFake(t, runs), 0)
	if retry.final.Watermark == nil || !retry.final.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("retry watermark=%v, want %s", retry.final.Watermark, claim.BeforeAt)
	}
	if row, ok := retry.pipelineIDs()["1"]; !ok || row.RetryCount != 1 {
		t.Fatalf("retry of the held midnight claim did not refresh the late re-run: %+v present=%v", row, ok)
	}
}

// A deep range of three pages walked one chunk per invocation resumes where it stopped, counts each page once against
// its budget, and ends with no observation. The budget is one page above the real pages: a resume inside a page re-fetches
// it inside the visitor's own allowance (the runs range has the same edge).
func TestLateRerunRunsDeepRangeResumesAcrossChunks(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	runs := lateRerunRuns(1, 250, claim.SinceAt.UTC())
	walk := walkListing(t, GitHubTestsRouteHandler{LateRerunMaxPages: 4}, claim, newListingFake(t, runs), 1)
	if walk.passes < 4 {
		t.Fatalf("passes=%d, want the deep range walked over several invocations", walk.passes)
	}
	if incomplete := lateRerunIncomplete(t, walk); len(incomplete) != 0 || walk.final.Result[GitHubTestsResultKeyReportsComplete] != true {
		t.Fatalf("incomplete=%v reports_complete=%v, want none and true", incomplete, walk.final.Result[GitHubTestsResultKeyReportsComplete])
	}
	if walk.cursor.LateRunPages != 3 {
		t.Fatalf("deep pages=%d, want 3", walk.cursor.LateRunPages)
	}
	refreshed := 0
	for _, row := range walk.pipelineIDs() {
		if row.RetryCount == 1 {
			refreshed++
		}
	}
	if refreshed != 250 || walk.final.Watermark == nil {
		t.Fatalf("refreshed=%d watermark=%v, want 250 and the window end", refreshed, walk.final.Watermark)
	}
}

// A deep range stopped by its page budget or by a saturated second records ONE late_rerun_runs observation: the unit
// reads reports_complete=false (coverage shows the gap) and still advances its watermark (the window was walked).
func TestLateRerunRunsStopIsOneAdvancingObservation(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	inWindow := listingRun{id: 9000, created: claim.SinceAt.Add(10 * time.Minute), updated: claim.SinceAt.Add(15 * time.Minute),
		status: "completed", conclusion: "success"}
	sameSecond := lateRerunRuns(1, 1001, claim.SinceAt.UTC())
	for index := range sameSecond {
		sameSecond[index].created = sameSecond[0].created
	}
	for _, cell := range []struct {
		name    string
		handler GitHubTestsRouteHandler
		runs    []listingRun
		cause   string
		atLeast int
		most    int
	}{
		{"page budget", GitHubTestsRouteHandler{LateRerunMaxPages: 1}, lateRerunRuns(1, 150, claim.SinceAt.UTC()), githubTestsPageBudgetCause, 100, 100},
		{"saturated second", GitHubTestsRouteHandler{}, sameSecond, githubTestsListingSaturatedCause, 1000, 1001},
	} {
		runs := append(append([]listingRun{}, cell.runs...), inWindow)
		walk := walkListing(t, cell.handler, claim, newListingFake(t, runs), 0)
		// The artifacts phase's deep range lists the same old runs on the same page budget and records its own
		// late_rerun_reports stop; this cell pins the runs phase's component.
		want := []GitHubTestsIncomplete{{Component: githubTestsLateRerunRunsComponent, Cause: cell.cause, Count: 1}}
		if incomplete := lateRerunIncompleteOf(lateRerunIncomplete(t, walk), githubTestsLateRerunRunsComponent); fmt.Sprint(incomplete) != fmt.Sprint(want) {
			t.Errorf("%s: incomplete=%v, want %v", cell.name, incomplete, want)
		}
		if walk.final.Result[GitHubTestsResultKeyReportsComplete] != false {
			t.Errorf("%s: reports_complete=%v, want false (the gap shows in coverage)", cell.name, walk.final.Result[GitHubTestsResultKeyReportsComplete])
		}
		if walk.final.Watermark == nil || !walk.final.Watermark.Equal(*claim.BeforeAt) {
			t.Errorf("%s: watermark=%v, want the window end (an advancing pair)", cell.name, walk.final.Watermark)
		}
		if err := validateGitHubTestsCompletion(claim, walk.final); err != nil {
			t.Errorf("%s: the completion comparator refused the result: %v", cell.name, err)
		}
		refreshed := 0
		ids := walk.pipelineIDs()
		for _, row := range ids {
			if row.RetryCount == 1 {
				refreshed++
			}
		}
		if _, ok := ids["9000"]; !ok || refreshed < cell.atLeast || refreshed > cell.most {
			t.Errorf("%s: window run written=%v, late re-runs refreshed=%d, want the window run and %d..%d", cell.name, ok, refreshed, cell.atLeast, cell.most)
		}
	}
}

// The non-chunked Collect (the oracle) lists the same deep range with the same keep rule, and records a stop of it as the
// late_rerun_runs page-budget observation.
func TestLateRerunRunsOracleMatchesTheChunkedRoute(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	runs := lateRerunRuns(1, 3, claim.SinceAt.UTC())
	runs = append(runs, listingRun{id: 50, created: claim.SinceAt.Add(-72 * time.Hour), updated: claim.SinceAt.Add(-72*time.Hour + 5*time.Minute),
		status: "completed", conclusion: "failure"})
	collect := func(handler GitHubTestsRouteHandler, runs []listingRun) (map[string]uint32, []GitHubTestsIncomplete) {
		batch, err := handler.Collect(context.Background(), claim, providerfoundation.Credential{},
			githubTestsClient(t, fakehttp.Client(newListingFake(t, runs))), normalizedAt)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]uint32{}
		for _, effect := range batch.Effects {
			if effect.Destination != "ci_pipeline_runs" {
				continue
			}
			rows, decodeErr := decodeEffectRows[githubTestsPipelineRow](effect)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			for _, row := range rows {
				out[row.RunID] = row.RetryCount
			}
		}
		incomplete, _ := batch.Result["incomplete"].([]GitHubTestsIncomplete)
		return out, incomplete
	}
	oracle, incomplete := collect(GitHubTestsRouteHandler{}, runs)
	chunked := map[string]uint32{}
	for id, row := range walkListing(t, GitHubTestsRouteHandler{}, claim, newListingFake(t, runs), 0).pipelineIDs() {
		chunked[id] = row.RetryCount
	}
	if fmt.Sprint(oracle) != fmt.Sprint(chunked) || fmt.Sprint(oracle) != "map[1:1 2:1 3:1]" || len(incomplete) != 0 {
		t.Fatalf("oracle rows %v incomplete %v, chunked rows %v; want map[1:1 2:1 3:1] on both and no observation", oracle, incomplete, chunked)
	}
	_, incomplete = collect(GitHubTestsRouteHandler{LateRerunMaxPages: 1}, lateRerunRuns(1, 150, claim.SinceAt.UTC()))
	want := []GitHubTestsIncomplete{{Component: githubTestsLateRerunRunsComponent, Cause: githubTestsPageBudgetCause, Count: 1}}
	if incomplete = lateRerunIncompleteOf(incomplete, githubTestsLateRerunRunsComponent); fmt.Sprint(incomplete) != fmt.Sprint(want) {
		t.Fatalf("oracle over its deep budget: incomplete=%v, want %v", incomplete, want)
	}
}

// The late_rerun_runs pairs are in the closed vocabulary and advance the watermark.
func TestLateRerunRunsPairsAreKnownAndAdvancing(t *testing.T) {
	for _, cause := range []string{githubTestsPageBudgetCause, githubTestsListingSaturatedCause} {
		observation := GitHubTestsIncomplete{Component: githubTestsLateRerunRunsComponent, Cause: cause, Count: 1}
		if !githubTestsIncompleteInVocabulary(observation) {
			t.Errorf("%v is not in the closed vocabulary", observation)
		}
		if githubTestsBlocksWatermark([]GitHubTestsIncomplete{observation}, nil, 0, nil, nil) {
			t.Errorf("%v holds the watermark, want an advancing pair", observation)
		}
	}
}

// Handler bounds of the deep range: the page budget is at most 1000 (zero or less = 1000).
func TestLateRerunRunsPageBudgetBounds(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	for _, pages := range []int{githubTestsLateRerunMaxPages + 1} {
		handler := GitHubTestsRouteHandler{LateRerunMaxPages: pages}
		client := githubTestsClient(t, fakehttp.Client(newListingFake(t, nil)))
		err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, time.Now(), "",
			func(ChunkRouteEmission) error { return nil })
		if err != ErrInvalidConfiguration {
			t.Errorf("LateRerunMaxPages=%d: err=%v, want ErrInvalidConfiguration", pages, err)
		}
	}
	if (GitHubTestsRouteHandler{}).lateRerunMaxPages() != 1000 || (GitHubTestsRouteHandler{}).lateRerunLookback() != 7*24*time.Hour {
		t.Fatal("deep range defaults changed: want 1000 pages and 7 days")
	}
}

// lateRerunBudgetAPI serves the deep range of a midnight claim from count runs, one every step from oldest (perPage a
// page, 100 when zero; GitHub's 1,000-result cap per created search, stable unique seconds). Only the oldest run (id 1)
// changed late: attempt 2. Every other listing goes to the listing fake.
type lateRerunBudgetAPI struct {
	*listingFake
	oldest  time.Time
	count   int
	step    time.Duration
	perPage int
}

func (api *lateRerunBudgetAPI) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Path != "/repos/acme/api/actions/runs" {
		return api.listingFake.Do(request)
	}
	query := request.URL.Query()
	lo, hi, ok := strings.Cut(query.Get("created"), "..")
	first, _ := listingTime(lo, false)
	last, _ := listingTime(hi, true)
	if !ok || first.After(api.oldest) || last.Before(api.oldest) {
		return api.listingFake.Do(request)
	}
	api.listingFake.mu.Lock()
	api.listingFake.listRanges = append(api.listingFake.listRanges, query.Get("created"))
	api.listingFake.mu.Unlock()
	newest := min(api.count, int(last.Sub(api.oldest)/api.step)+1)
	total := min(githubTestsSearchResultCap, newest)
	page := max(1, func() int { value, _ := strconv.Atoi(query.Get("page")); return value }())
	perPage := api.perPage
	if perPage == 0 {
		perPage = 100
	}
	from, to := min((page-1)*perPage, total), min(page*perPage, total)
	header := http.Header{"Content-Type": {"application/json"}}
	if to < total {
		next := *request.URL
		query.Set("page", strconv.Itoa(page+1))
		next.RawQuery = query.Encode()
		header.Set("Link", "<"+next.String()+">; rel=\"next\"")
	}
	items := make([]string, 0, to-from)
	for index := from; index < to; index++ {
		id := newest - index
		created := api.oldest.Add(time.Duration(id-1) * api.step)
		run := listingRun{id: id, created: created, updated: created.Add(time.Minute), status: "completed", conclusion: "failure"}
		if id == 1 {
			run.updated = api.oldest.Add(6*24*time.Hour + 30*time.Minute)
			run.startedAt, run.attempt, run.conclusion = run.updated.Add(-time.Hour), 2, "success"
		}
		items = append(items, run.json())
	}
	return api.respond(request, header, `{"workflow_runs":[`+strings.Join(items, ",")+`]}`, 200), nil
}

// lateRerunYieldWalk drives CollectChunks and yields (a continuation) after the chunks yieldAt picks, from the real cursors.
func lateRerunYieldWalk(t *testing.T, handler GitHubTestsRouteHandler, claim Claim, doer providerfoundation.HTTPDoer, yieldAt func(chunk int, after githubTestsChunkCursor) bool) listingWalk {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(doer))
	stop := errors.New("yield")
	walk, resume := listingWalk{}, ""
	for pass := 1; ; pass++ {
		if pass > 100000 {
			t.Fatal("the walk never reached a final emission")
		}
		err := handler.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, claim.BeforeAt.Add(time.Hour), resume,
			func(emission ChunkRouteEmission) error {
				resume = emission.CursorAfter
				for _, effect := range emission.Batch.Effects {
					if effect.Destination == "ci_pipeline_runs" {
						rows, decodeErr := decodeEffectRows[githubTestsPipelineRow](effect)
						if decodeErr != nil {
							t.Fatal(decodeErr)
						}
						walk.pipelines = append(walk.pipelines, rows...)
					}
				}
				after, decodeErr := decodeGitHubTestsChunkCursor(resume)
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if emission.Final {
					walk.final, walk.cursor = emission.Batch, after
					return nil
				}
				walk.chunks++
				if yieldAt(walk.chunks, after) {
					return stop
				}
				return nil
			})
		if err == nil {
			walk.passes = pass
			return walk
		}
		if !errors.Is(err, stop) {
			t.Fatal(err)
		}
	}
}

// A page is charged when the walk enters it; a resume inside a page re-fetches it for free, and a checkpointed page
// always finishes. A deep range with exactly its budget of pages, resumed inside a page, writes the late re-run and
// records no observation; so does the production shape: the 1,000-page default and the default chunk size. Its pages
// hold 3 runs (2,993 runs: three peeled ranges, exactly 1,000 pages, 2 runs on the last), so a resume lands inside a
// page at most chunk ends, and the walk ends at the budget: each page a resume charged again would lose the re-run. The
// first page of each of the two peeled ranges is free (CHAOS-14435: it lists the cut second again), so 998 are charged.
func TestLateRerunRunsResumeInsideAPageCostsNoPage(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	for _, cell := range []struct {
		name            string
		budget, count   int
		perPage         int
		resumeAt        int
		periodic, heavy bool
	}{
		{"fresh at the limit", 2, 150, 0, 0, false, false},
		{"resume inside the next-to-last page", 2, 150, 0, 64, false, false},
		{"resume inside the last page", 2, 180, 0, 128, false, false},
		{"resume at a page boundary", 2, 150, 0, 100, false, false},
		{"1,000 pages, default budget and chunk size", 0, 2993, 3, DefaultChunkPolicy().MaxChunksPerAttempt, true, true},
		{"1,000 pages, resume inside page 1,000", 0, 2993, 3, 2994, false, true},
	} {
		oldest, step := claim.SinceAt.Add(-72*time.Hour), time.Second
		if cell.heavy {
			oldest, step = claim.SinceAt.Add(-7*24*time.Hour), 4*time.Second
		}
		api := &lateRerunBudgetAPI{listingFake: newListingFake(t, nil), oldest: oldest, count: cell.count, step: step, perPage: cell.perPage}
		walk := lateRerunYieldWalk(t, GitHubTestsRouteHandler{LateRerunMaxPages: cell.budget}, claim, api, func(chunk int, after githubTestsChunkCursor) bool {
			return after.LateRuns && cell.resumeAt > 0 && (chunk == cell.resumeAt || cell.periodic && chunk%cell.resumeAt == 0)
		})
		incomplete := lateRerunIncomplete(t, walk)
		if len(walk.pipelines) != 1 || walk.pipelines[0].RetryCount != 1 || len(incomplete) != 0 || walk.final.Watermark == nil {
			t.Errorf("%s: rows=%d incomplete=%v watermark=%v deep pages=%d, want the late re-run, no observation, the window end",
				cell.name, len(walk.pipelines), incomplete, walk.final.Watermark, walk.cursor.LateRunPages)
		}
		if cell.heavy && (walk.cursor.LateRunPages != githubTestsLateRerunMaxPages-2 || walk.passes < 2) {
			t.Errorf("%s: deep pages=%d passes=%d, want the 1,000 pages less the 2 free peel pages over more than one invocation", cell.name, walk.cursor.LateRunPages, walk.passes)
		}
	}
}

// The runs range has the same rule: a window of exactly its page budget, resumed inside a page, is walked whole.
func TestRunsRangeResumeInsideAPageCostsNoPage(t *testing.T) {
	since := time.Date(2026, 7, 20, 1, 0, 0, 0, time.UTC)
	claim := listingClaim(since, since.Add(time.Hour))
	claim.Mode = "incremental"
	runs := makeListingRuns(1, 150, since.Add(time.Minute), 10*time.Second)
	walk := lateRerunYieldWalk(t, GitHubTestsRouteHandler{MaxRuns: 200}, claim, newListingFake(t, runs), func(chunk int, after githubTestsChunkCursor) bool {
		return after.Phase == githubTestsRunsPhase && chunk == 64
	})
	if len(walk.pipelineIDs()) != 150 || len(lateRerunIncomplete(t, walk)) != 0 || walk.cursor.RunPages != 2 {
		t.Fatalf("rows=%d incomplete=%v run pages=%d, want 150 runs, no observation, 2 pages", len(walk.pipelineIDs()), lateRerunIncomplete(t, walk), walk.cursor.RunPages)
	}
}

// A deep range that peels at GitHub's cap reads the runs of the cut second once (the runs listing's cut-second dedupe):
// one jobs read and one pipeline row per run, as many rows as the oracle writes.
func TestLateRerunRunsPeelReadsTheCutSecondOnce(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	for _, cell := range []struct {
		name   string
		count  int
		tie    bool
		chunks int
	}{
		{"one run at the cut", 1000, false, 0},
		{"two runs share the cut second", 1001, true, 0},
		{"two runs share the cut second, resumed", 1001, true, 64},
	} {
		runs := lateRerunRuns(1, cell.count, claim.SinceAt.UTC())
		if cell.tie {
			runs[1].created = runs[0].created
		}
		fake := newListingFake(t, runs)
		walk := walkListing(t, GitHubTestsRouteHandler{}, claim, fake, cell.chunks)
		batch, err := GitHubTestsRouteHandler{}.Collect(context.Background(), claim, providerfoundation.Credential{},
			githubTestsClient(t, fakehttp.Client(newListingFake(t, runs))), claim.BeforeAt.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		oracle := 0
		for _, effect := range batch.Effects {
			if effect.Destination == "ci_pipeline_runs" {
				oracle += len(effect.Rows)
			}
		}
		extra := 0
		for _, reads := range fake.jobGets {
			extra += max(0, reads-1)
		}
		if len(walk.pipelines) != cell.count || oracle != cell.count || extra != 0 {
			t.Errorf("%s: chunked rows=%d oracle rows=%d extra jobs reads=%d, want %d, %d, 0", cell.name, len(walk.pipelines), oracle, extra, cell.count, cell.count)
		}
	}
}

// A late re-run seen in progress is written by the deep range and read again by the open phase of the same claim: the row
// after the double processing is the row after one (same key, same values; the cost is one more jobs read).
func TestLateRerunRunsDoubleProcessingWritesTheSameRow(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	created := claim.SinceAt.Add(-72 * time.Hour)
	young := listingClaim(created.Add(-time.Minute), created.Add(time.Hour))
	young.Mode = "incremental"
	store := &fakeOpenRuns{rows: walkListing(t, GitHubTestsRouteHandler{}, young,
		newListingFake(t, []listingRun{{id: 1, created: created, updated: created.Add(time.Minute), status: "in_progress"}}), 0).pipelines}
	// The re-run started before the runs range reaches, so its open row is one the open phase re-reads.
	late := listingRun{id: 1, created: created, startedAt: claim.SinceAt.Add(-7 * time.Hour), updated: claim.SinceAt.Add(-time.Hour), status: "in_progress", attempt: 2}
	twice := walkListing(t, GitHubTestsRouteHandler{OpenRuns: store}, claim, newListingFake(t, []listingRun{late}), 0)
	once := walkListing(t, GitHubTestsRouteHandler{}, claim, newListingFake(t, []listingRun{late}), 0)
	if len(twice.pipelines) != 2 || len(once.pipelines) != 1 {
		t.Fatalf("premise: rows with the open phase=%d without=%d, want 2 and 1", len(twice.pipelines), len(once.pipelines))
	}
	for _, row := range twice.pipelines {
		if lateRerunRowJSON(row) != lateRerunRowJSON(once.pipelines[0]) {
			t.Fatalf("row after double processing %s, want the row after one %s", lateRerunRowJSON(row), lateRerunRowJSON(once.pipelines[0]))
		}
	}
}

func lateRerunRowJSON(row githubTestsPipelineRow) string {
	encoded, err := json.Marshal(row)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

// A walk bound reached inside the deep range records only late_rerun_runs: the artifacts phase still reads the window
// (its bound counts no deep record), and the watermark advances. A bound reached by the window itself still holds it.
func TestLateRerunRunsWalkBoundInsideTheDeepRange(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	for _, cell := range []struct {
		name                  string
		window, deep, bound   int
		chunks                int
		wantLate, wantBlocked bool
	}{
		{"inside a deep page", 1, 3, 2, 0, true, false},
		{"inside a deep page, resumed", 1, 3, 2, 1, true, false},
		{"at a deep next link", 0, 101, 100, 0, true, false},
		{"exactly at the deep range end", 0, 1, 1, 0, false, false},
		{"below the bound", 1, 2, 10, 0, false, false},
		{"the window reaches the bound", 1, 2, 1, 0, true, true},
		{"deep stop, then a two-page artifacts listing", 150, 3, 151, 0, true, false},
	} {
		runs := append(lateRerunRuns(1, cell.deep, claim.SinceAt.UTC()),
			makeListingRuns(9000, cell.window, claim.SinceAt.Add(5*time.Minute), time.Second)...)
		fake := newListingFake(t, runs)
		walk := walkListing(t, GitHubTestsRouteHandler{MaxWalkRecords: cell.bound, OpenRuns: &fakeOpenRuns{}}, claim, fake, cell.chunks)
		var late, blocked bool
		for _, observation := range lateRerunIncomplete(t, walk) {
			late = late || observation.Component == githubTestsLateRerunRunsComponent
			blocked = blocked || observation.Component == githubTestsArtifactInventoryComponent || observation.Component == githubTestsRunInventoryComponent
		}
		windowRead := cell.window == 0 || fake.artifactGets[9000] > 0
		openStopped := walk.final.Result[GitHubTestsResultKeyOpenCapReached] == true
		if late != cell.wantLate || blocked != cell.wantBlocked || (walk.final.Watermark == nil) != cell.wantBlocked || windowRead == cell.wantBlocked ||
			openStopped != cell.wantBlocked {
			t.Errorf("%s: incomplete=%v watermark=%v window artifacts read=%v open phase stopped at the bound=%v, want late=%v blocked=%v",
				cell.name, lateRerunIncomplete(t, walk), walk.final.Watermark, windowRead, openStopped, cell.wantLate, cell.wantBlocked)
		}
	}
}

// A run with no created_at is never read by the deep range (GitHub cannot list it by created), even from a listing that
// returns it; the runs range keeps its own rule for such a run (it falls back to the start time).
func TestLateRerunRunsRefuseARunWithNoCreationTime(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	noCreated := `{"id":5,"name":"CI","status":"completed","conclusion":"success","run_started_at":"` + claim.SinceAt.Add(-3*time.Hour).Format(time.RFC3339) +
		`","updated_at":"` + claim.SinceAt.Add(-time.Hour).Format(time.RFC3339) + `","run_attempt":2,"event":"push","head_sha":"abc","head_branch":"main","html_url":"https://github.com/acme/api/actions/runs/5","pull_requests":[]}`
	run, err := decodeGitHubWorkflowRun(json.RawMessage(noCreated))
	if err != nil {
		t.Fatal(err)
	}
	if githubTestsLateRerunWanted(run, claim, githubTestsRefreshLookbackDefault) {
		t.Fatal("deep keep rule read a run with no created_at")
	}
	pipeline, include := normalizeGitHubTestsPipeline(claim, "repo", run, time.Now())
	inWindow := pipeline
	inWindow.StartedAt = claim.SinceAt.Add(10 * time.Minute)
	if !include || !githubTestsRunWanted(run, inWindow, claim) || githubTestsRunWanted(run, pipeline, claim) {
		t.Fatalf("runs-range rule for a run with no created_at changed: include=%v in-window=%v before-window=%v, want true, true, false",
			include, githubTestsRunWanted(run, inWindow, claim), githubTestsRunWanted(run, pipeline, claim))
	}
	deep, jobs := 0, 0
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		header := http.Header{"Content-Type": {"application/json"}}
		switch request.URL.Path {
		case "/repos/acme/api":
			return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
		case "/repos/acme/api/actions/runs":
			if strings.HasSuffix(request.URL.Query().Get("created"), claim.SinceAt.Add(-githubTestsRefreshLookbackDefault-time.Second).Format(time.RFC3339)) {
				deep++
				return githubTestsHTTPResponse(request, header, `{"workflow_runs":[`+noCreated+`]}`), nil
			}
			return githubTestsHTTPResponse(request, header, `{"workflow_runs":[]}`), nil
		case "/repos/acme/api/actions/runs/5/jobs":
			jobs++
			return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
		}
		t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	})
	batch, err := GitHubTestsRouteHandler{}.Collect(context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(doer)), claim.BeforeAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for _, effect := range batch.Effects {
		if effect.Destination == "ci_pipeline_runs" {
			rows += len(effect.Rows)
		}
	}
	if deep != 1 || jobs != 0 || rows != 0 {
		t.Fatalf("oracle: deep listings=%d jobs reads=%d rows=%d, want 1, 0, 0", deep, jobs, rows)
	}
	walk := lateRerunYieldWalk(t, GitHubTestsRouteHandler{}, claim, doer, func(int, githubTestsChunkCursor) bool { return false })
	if deep != 2 || jobs != 0 || len(walk.pipelines) != 0 {
		t.Fatalf("chunked: deep listings=%d jobs reads=%d rows=%d, want 2, 0, 0", deep, jobs, len(walk.pipelines))
	}
}

// lateRerunRouteDoer answers the runs listings of a midnight claim: deep (ending one second below the runs range) and
// every other listing get their own run payloads; jobs and artifacts reads are counted per run id.
func lateRerunRouteDoer(t *testing.T, claim Claim, deep, other []string, jobs map[string]int) jiraWorkItemsDoerFunc {
	deepHi := claim.SinceAt.Add(-githubTestsRefreshLookbackDefault - time.Second).Format(time.RFC3339)
	return func(request *http.Request) (*http.Response, error) {
		header := http.Header{"Content-Type": {"application/json"}}
		path := request.URL.Path
		switch {
		case path == "/repos/acme/api":
			return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
		case path == "/repos/acme/api/actions/runs":
			items := other
			if strings.HasSuffix(request.URL.Query().Get("created"), deepHi) {
				items = deep
			}
			return githubTestsHTTPResponse(request, header, `{"workflow_runs":[`+strings.Join(items, ",")+`]}`), nil
		case strings.HasSuffix(path, "/jobs"):
			jobs[strings.Split(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/")[0]]++
			return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
		case strings.HasSuffix(path, "/artifacts"):
			return githubTestsHTTPResponse(request, header, `{"artifacts":[]}`), nil
		}
		t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

func lateRerunRunPayload(id int, created, updated *time.Time, started time.Time) string {
	parts := []string{fmt.Sprintf(`"id":%d`, id), `"name":"CI"`, `"status":"completed"`, `"conclusion":"success"`,
		`"run_started_at":"` + started.UTC().Format(time.RFC3339) + `"`, `"run_attempt":2`, `"event":"push"`, `"head_sha":"abc"`,
		`"head_branch":"main"`, fmt.Sprintf(`"html_url":"https://github.com/acme/api/actions/runs/%d"`, id), `"pull_requests":[]`}
	if created != nil {
		parts = append(parts, `"created_at":"`+created.UTC().Format(time.RFC3339)+`"`)
	}
	if updated != nil {
		parts = append(parts, `"updated_at":"`+updated.UTC().Format(time.RFC3339)+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// lateRerunBothRoutes runs the oracle Collect and the chunked route over the same doer shape and returns, per route,
// the pipeline row ids and the jobs reads.
func lateRerunBothRoutes(t *testing.T, claim Claim, deep, other []string) (map[string]map[string]int, map[string]map[string]int) {
	t.Helper()
	rows, reads := map[string]map[string]int{"oracle": {}, "chunked": {}}, map[string]map[string]int{"oracle": {}, "chunked": {}}
	batch, err := GitHubTestsRouteHandler{}.Collect(context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(lateRerunRouteDoer(t, claim, deep, other, reads["oracle"]))), claim.BeforeAt.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range batch.Effects {
		if effect.Destination != "ci_pipeline_runs" {
			continue
		}
		decoded, decodeErr := decodeEffectRows[githubTestsPipelineRow](effect)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		for _, row := range decoded {
			rows["oracle"][row.RunID]++
		}
	}
	walk := lateRerunYieldWalk(t, GitHubTestsRouteHandler{}, claim, lateRerunRouteDoer(t, claim, deep, other, reads["chunked"]),
		func(int, githubTestsChunkCursor) bool { return false })
	for _, row := range walk.pipelines {
		rows["chunked"][row.RunID]++
	}
	return rows, reads
}

// A run with no updated_at is never read by the deep range (no late change can be shown), on both routes; the runs
// range still reads such a run of its lookback part, as before.
func TestLateRerunRunsRefuseARunWithNoUpdateTime(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	deepCreated, rangeCreated := claim.SinceAt.Add(-72*time.Hour), claim.SinceAt.Add(-time.Hour)
	deep := []string{lateRerunRunPayload(21, &deepCreated, nil, claim.SinceAt.Add(-2*time.Hour))}
	other := []string{lateRerunRunPayload(22, &rangeCreated, nil, claim.SinceAt.Add(-30*time.Minute))}
	rows, reads := lateRerunBothRoutes(t, claim, deep, other)
	for _, route := range []string{"oracle", "chunked"} {
		if rows[route]["21"] != 0 || reads[route]["21"] != 0 || rows[route]["22"] != 1 || reads[route]["22"] != 1 {
			t.Errorf("%s: deep run with no updated_at rows=%d jobs reads=%d (want 0, 0); runs-range run with no updated_at rows=%d jobs reads=%d (want 1, 1)",
				route, rows[route]["21"], reads[route]["21"], rows[route]["22"], reads[route]["22"])
		}
	}
}

// A run created at or after the runs range's floor belongs to the runs range: if a deep listing answers it, the deep
// range skips it on both routes, so it is read once (one row, one jobs read) and never twice in a batch.
func TestLateRerunRunsDeepRangeSkipsARunOfTheRunsRange(t *testing.T) {
	claim := lateRerunMidnightClaim("incremental")
	created, updated := claim.SinceAt.Add(-5*time.Hour-30*time.Minute), claim.SinceAt.Add(45*time.Minute)
	run := lateRerunRunPayload(31, &created, &updated, claim.SinceAt.Add(30*time.Minute))
	rows, reads := lateRerunBothRoutes(t, claim, []string{run}, []string{run})
	for _, route := range []string{"oracle", "chunked"} {
		if rows[route]["31"] != 1 || reads[route]["31"] != 1 {
			t.Errorf("%s: rows=%d jobs reads=%d, want 1 and 1", route, rows[route]["31"], reads[route]["31"])
		}
	}
}

// lateRerunIncompleteOf keeps the observations of one component.
func lateRerunIncompleteOf(incomplete []GitHubTestsIncomplete, component string) []GitHubTestsIncomplete {
	var out []GitHubTestsIncomplete
	for _, observation := range incomplete {
		if observation.Component == component {
			out = append(out, observation)
		}
	}
	return out
}
