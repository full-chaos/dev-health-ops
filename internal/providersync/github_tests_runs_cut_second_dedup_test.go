package providersync

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The runs phase peels like the artifacts phase: the next range lists the cut second again. A run whose jobs the previous
// range read whole at that second, with a final status, is not read again; an open run, a new attempt or a failed read is.

// runsCutDoer serves the cut-dedup doer plus jobs: one job per run, or more than githubTestsMaxJobsPerRun over pages
// for a capped run, a hard failure of a run's first jobs page, and a flip of an open run to completed after its first
// jobs read. jobReads counts first jobs pages per run.
type runsCutDoer struct {
	*cutDedupDoer
	jobReads      map[int]int
	capped        map[int]bool
	failJobs      map[int]int
	flipAfterJobs map[int]bool
}

func newRunsCutDoer(t *testing.T, runs []listingRun) *runsCutDoer {
	return &runsCutDoer{cutDedupDoer: newCutDedupDoer(t, runs), jobReads: map[int]int{}, capped: map[int]bool{},
		failJobs: map[int]int{}, flipAfterJobs: map[int]bool{}}
}

func (doer *runsCutDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	if !strings.HasPrefix(path, "/repos/acme/api/actions/runs/") || !strings.HasSuffix(path, "/jobs") {
		return doer.cutDedupDoer.Do(request)
	}
	id, err := strconv.Atoi(strings.Split(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/")[0])
	if err != nil {
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	header := http.Header{"Content-Type": {"application/json"}}
	page := 1
	if raw := request.URL.Query().Get("page"); raw != "" {
		page, _ = strconv.Atoi(raw)
	}
	if page == 1 {
		doer.jobReads[id]++
		if doer.failJobs[id] > 0 {
			doer.failJobs[id]--
			return &http.Response{StatusCode: http.StatusUnprocessableEntity, Header: header, Body: io.NopCloser(strings.NewReader(`{"message":"injected"}`)), Request: request}, nil
		}
		if doer.flipAfterJobs[id] && doer.jobReads[id] == 1 {
			doer.fake.mu.Lock()
			for index := range doer.fake.runs {
				if doer.fake.runs[index].id == id {
					doer.fake.runs[index].status, doer.fake.runs[index].conclusion = "completed", "success"
				}
			}
			doer.fake.mu.Unlock()
		}
	}
	count, last := 1, 1
	if doer.capped[id] {
		count, last = githubTestsPerRunPerPage, githubTestsMaxJobsPerRun/githubTestsPerRunPerPage+1
	}
	jobs := make([]string, 0, count)
	for index := 0; index < count; index++ {
		jobs = append(jobs, fmt.Sprintf(`{"id":%d,"name":"unit","status":"completed","conclusion":"success","started_at":"2026-07-20T06:30:01Z","completed_at":"2026-07-20T06:30:09Z","labels":["ubuntu-latest"]}`,
			id*100000+page*1000+index))
	}
	if page < last {
		next := *request.URL
		forward := next.Query()
		forward.Set("page", strconv.Itoa(page+1))
		next.RawQuery = forward.Encode()
		header.Set("Link", "<"+next.String()+">; rel=\"next\"")
	}
	return githubTestsHTTPResponse(request, header, `{"jobs":[`+strings.Join(jobs, ",")+`]}`), nil
}

func (doer *runsCutDoer) jobReadsOf(ids []int) []int {
	out := make([]int, 0, len(ids))
	for _, id := range ids {
		out = append(out, doer.jobReads[id])
	}
	return out
}

func runsCutCounters(walk cutDedupWalk) string {
	return fmt.Sprintf("pipelines=%v jobs=%v acceptance=%v", walk.final.Result["pipeline_runs_synced"], walk.final.Result["job_runs_synced"],
		walk.final.Result["acceptance_checks_synced"])
}

// runsCutWant is the counters of distinct runs: one job per run, and one acceptance row per job.
func runsCutWant(distinct int) string {
	return fmt.Sprintf("pipelines=%d jobs=%d acceptance=%d", distinct, distinct, distinct)
}

func runsCutRowsOf(walk cutDedupWalk, ids []int) []int {
	out := make([]int, 0, len(ids))
	for _, id := range ids {
		out = append(out, walk.rows["ci_pipeline_runs"][strconv.Itoa(id)])
	}
	return out
}

func runsCutPhase(before, after githubTestsChunkCursor) bool {
	return before.Phase == githubTestsRunsPhase || after.Phase == githubTestsRunsPhase
}

// The fcc layout (998 newer runs, then 5 runs at the cut second, 5004 and 5003 in the first range): each cut-second run
// is read once and written once, with no resume, with resumes at the points the list must survive (after the first
// cut-second run, at the end of the first range, after the first run of the next range), and with a resume every 7
// emissions. The counters count each of the 1,003 runs once.
func TestGitHubTestsRunsCutSecondRunsAreReadOnce(t *testing.T) {
	for _, tc := range []struct {
		name  string
		yield func() func(before, after githubTestsChunkCursor) bool
	}{
		{"no resume", func() func(before, after githubTestsChunkCursor) bool { return nil }},
		{"resume at the list's edges", func() func(before, after githubTestsChunkCursor) bool {
			return func(before, after githubTestsChunkCursor) bool {
				return runsCutPhase(before, after) && (after.RangeHi == "" && after.RangeItems == 999 ||
					before.RangeHi == "" && after.RangeHi != "" ||
					after.RangeHi != "" && after.RangeItems == 1)
			}
		}},
		{"resume every 7 emissions", func() func(before, after githubTestsChunkCursor) bool {
			emissions := 0
			return func(githubTestsChunkCursor, githubTestsChunkCursor) bool {
				emissions++
				return emissions%7 == 0
			}
		}},
	} {
		doer := newRunsCutDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
		walk := walkCutDedup(t, cutDedupClaim(false), doer, tc.yield(), nil)
		if got, want := doer.jobReadsOf(cutDedupIDs), []int{1, 1, 1, 1, 1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: jobs reads of %v = %v, want %v", tc.name, cutDedupIDs, got, want)
		}
		if got, want := runsCutRowsOf(walk, cutDedupIDs), []int{1, 1, 1, 1, 1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: pipeline rows of %v = %v, want %v", tc.name, cutDedupIDs, got, want)
		}
		if got, want := runsCutCounters(walk), runsCutWant(1003); got != want {
			t.Fatalf("%s: counters %s, want %s", tc.name, got, want)
		}
	}
}

// 3,000 runs, one at each cut second, at least 3 peels per phase: every run is read once and the counters count 3,000,
// without and with resumes; no run is lost (a skipped run still counts toward its range's 1,000 results).
func TestGitHubTestsRunsCutSecondRunsAreReadOnceOverManyPeels(t *testing.T) {
	runs := makeListingRuns(1, 3000, listingSince, 10*time.Second)
	claim := listingClaim(listingSince, listingSince.Add(24*time.Hour))
	for _, every := range []int{0, 13} {
		doer := newRunsCutDoer(t, runs)
		doer.fake.requestCeiling = 4000000
		emissions := 0
		yield := func(githubTestsChunkCursor, githubTestsChunkCursor) bool {
			emissions++
			return every > 0 && emissions%every == 0
		}
		walk := walkCutDedup(t, claim, doer, yield, nil)
		twice := 0
		for _, run := range runs {
			if doer.jobReads[run.id] != 1 {
				twice++
			}
		}
		if twice != 0 || len(walk.rows["ci_pipeline_runs"]) != 3000 {
			t.Fatalf("resume every %d: %d runs not read exactly once, %d distinct pipeline rows; want 0 and 3,000", every, twice, len(walk.rows["ci_pipeline_runs"]))
		}
		if got, want := runsCutCounters(walk), runsCutWant(3000); got != want {
			t.Fatalf("resume every %d: counters %s, want %s", every, got, want)
		}
		if got := distinctRanges(doer.fake); got < 6 {
			t.Fatalf("resume every %d: %d distinct created ranges, want at least 3 peels per phase", every, got)
		}
	}
}

// A cut-second run still open at its first read is read and written again when listed again (completed by then).
func TestGitHubTestsRunsCutSecondOpenRunIsReadAgain(t *testing.T) {
	runs := cutDedupRuns(cutDedupBefore.Add(-time.Hour))
	for index := range runs {
		if runs[index].id == 5004 {
			runs[index].status, runs[index].conclusion = "in_progress", ""
		}
	}
	doer := newRunsCutDoer(t, runs)
	doer.flipAfterJobs[5004] = true
	walk := walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
	if got, want := doer.jobReadsOf(cutDedupIDs), []int{1, 1, 1, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("jobs reads of %v = %v, want %v (5004 open at its first read)", cutDedupIDs, got, want)
	}
	if got, want := runsCutRowsOf(walk, cutDedupIDs), []int{1, 1, 1, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pipeline rows of %v = %v, want %v (5004 written again, completed)", cutDedupIDs, got, want)
	}
}

// A run listed again with a new attempt (a rerun) is a different key: it is read again.
func TestGitHubTestsRunsCutSecondRunWithANewAttemptIsReadAgain(t *testing.T) {
	doer := newRunsCutDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
	doer.attemptAt[5003] = cutDedupSecond.Format(time.RFC3339)
	walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
	if got, want := doer.jobReadsOf(cutDedupIDs), []int{1, 1, 1, 2, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("jobs reads of %v = %v, want %v", cutDedupIDs, got, want)
	}
}

// A cut-second run over the per-run jobs cap is recorded (the cap is deterministic): its jobs are read once and the cap
// is counted once.
func TestGitHubTestsRunsCutSecondCappedRunIsReadOnce(t *testing.T) {
	doer := newRunsCutDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
	doer.capped[5004] = true
	walk := walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
	if got, want := doer.jobReadsOf(cutDedupIDs), []int{1, 1, 1, 1, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("jobs reads of %v = %v, want %v", cutDedupIDs, got, want)
	}
	var incomplete []GitHubTestsIncomplete
	if err := decodeCompletionValue(walk.final.Result, "incomplete", &incomplete); err != nil {
		t.Fatal(err)
	}
	capped := 0
	for _, observation := range incomplete {
		if observation.Component == githubTestsRunJobsComponent && observation.Cause == githubTestsPerRunCapCause {
			capped += observation.Count
		}
	}
	if capped != 1 {
		t.Fatalf("run_jobs/per_run_cap count = %d, want 1 (one capped run): %+v", capped, incomplete)
	}
}

// A hard error on a cut-second run's jobs read ends the invocation before the run's emission; the resume reads it, and
// the next range does not read it again. Its row is written once.
func TestGitHubTestsRunsCutSecondFailedReadIsReadOnResume(t *testing.T) {
	doer := newRunsCutDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
	doer.failJobs[5004] = 1
	walk := walkCutDedup(t, cutDedupClaim(false), doer, nil, func(err error) bool {
		var providerErr *providerfoundation.ProviderError
		return errors.As(err, &providerErr) && providerErr.StatusCode == http.StatusUnprocessableEntity
	})
	if walk.errors != 1 {
		t.Fatalf("injected errors seen = %d, want 1", walk.errors)
	}
	if got, want := doer.jobReadsOf(cutDedupIDs), []int{1, 1, 1, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("jobs reads of %v = %v, want %v (5004: the failed read and its resume)", cutDedupIDs, got, want)
	}
	if got, want := runsCutRowsOf(walk, cutDedupIDs), []int{1, 1, 1, 1, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pipeline rows of %v = %v, want %v", cutDedupIDs, got, want)
	}
}

// The runs phase and the artifacts phase share the cut lists (ak/ck): the cursor that enters the artifacts phase carries
// neither, so the artifacts phase lists the artifacts of every cut-second run once. A runs-phase cursor keeps both on
// decode.
func TestGitHubTestsRunsCutListIsClearedForTheArtifactsPhase(t *testing.T) {
	doer := newRunsCutDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
	// Each cut-second run holds a report, so the artifacts phase reads it whole once (a run with no report is read again
	// by design).
	doer.withReports(cutDedupIDs...)
	walk := walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
	withList, entered := 0, 0
	for index := 0; index+1 < len(walk.cursors); index += 2 {
		before, _ := decodeGitHubTestsChunkCursor(walk.cursors[index])
		after, _ := decodeGitHubTestsChunkCursor(walk.cursors[index+1])
		if after.Phase == githubTestsRunsPhase && after.CarriedCutKeys != "" {
			withList++
		}
		if before.Phase == githubTestsRunsPhase && after.Phase == githubTestsArtifactsPhase {
			entered++
			if after.ActiveCutKeys != "" || after.CarriedCutKeys != "" {
				t.Fatalf("the cursor entering the artifacts phase carries the runs lists ak=%q ck=%q", after.ActiveCutKeys, after.CarriedCutKeys)
			}
		}
	}
	if withList == 0 || entered != 1 {
		t.Fatalf("premise: %d runs-phase cursors carried a cut list, %d entered the artifacts phase; want > 0 and 1", withList, entered)
	}
	if got, want := doer.reads(cutDedupIDs), []int{1, 1, 1, 1, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("artifact lists of %v = %v, want %v", cutDedupIDs, got, want)
	}
	kept, err := decodeGitHubTestsChunkCursor(`{"phase":"runs","repo":"acme/api","archives_seen":0,"archives_unreadable":0,"rh":"2026-07-20T06:30:00Z","ro":"2026-07-20T06:30:00Z","ri":3,"ak":"5,6.2","ck":"7"}`)
	if err != nil || kept.ActiveCutKeys != "5,6.2" || kept.CarriedCutKeys != "7" {
		t.Fatalf("runs-phase cut lists = ak %q ck %q, %v; want both kept", kept.ActiveCutKeys, kept.CarriedCutKeys, err)
	}
}
