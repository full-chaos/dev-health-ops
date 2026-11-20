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
// for a capped run, a partial jobs shape (runsCutJobShapes), a hard failure of a run's first jobs page, and a flip of an
// open run to completed after its first jobs read. listRewrite edits each runs listing body. jobReads counts first jobs
// pages per run.
type runsCutDoer struct {
	*cutDedupDoer
	jobReads      map[int]int
	capped        map[int]bool
	jobShape      map[int]string
	failJobs      map[int]int
	flipAfterJobs map[int]bool
	listRewrite   func(string) string
}

func newRunsCutDoer(t *testing.T, runs []listingRun) *runsCutDoer {
	return &runsCutDoer{cutDedupDoer: newCutDedupDoer(t, runs), jobReads: map[int]int{}, capped: map[int]bool{},
		jobShape: map[int]string{}, failJobs: map[int]int{}, flipAfterJobs: map[int]bool{}}
}

func runsCutJob(id, page, index int) string {
	return fmt.Sprintf(`{"id":%d,"name":"unit","status":"completed","conclusion":"success","started_at":"2026-07-20T06:30:01Z","completed_at":"2026-07-20T06:30:09Z","labels":["ubuntu-latest"]}`,
		id*100000+page*1000+index)
}

// runsCutJobShapes are jobs responses: the body and the Link header of every page. Every shape but runsCutWholeShape is
// not a whole read.
const runsCutWholeShape = "whole, 1 declared"

var runsCutJobShapes = map[string]func(id, page int, request *http.Request) (string, string){
	runsCutWholeShape: func(id, page int, _ *http.Request) (string, string) {
		return `{"total_count":1,"jobs":[` + runsCutJob(id, page, 0) + `]}`, ""
	},
	"missing array, 2 declared": func(int, int, *http.Request) (string, string) { return `{"total_count":2}`, "" },
	"null array, 2 declared":    func(int, int, *http.Request) (string, string) { return `{"total_count":2,"jobs":null}`, "" },
	"short page, 2 declared": func(id, page int, _ *http.Request) (string, string) {
		return `{"total_count":2,"jobs":[` + runsCutJob(id, page, 0) + `]}`, ""
	},
	"bad Link header": func(id, page int, _ *http.Request) (string, string) {
		return `{"jobs":[` + runsCutJob(id, page, 0) + `]}`, `garbage; rel="next"`
	},
	"full page, no Link": func(id, page int, _ *http.Request) (string, string) {
		jobs := make([]string, 0, githubTestsPerRunPerPage)
		for index := 0; index < githubTestsPerRunPerPage; index++ {
			jobs = append(jobs, runsCutJob(id, page, index))
		}
		return `{"jobs":[` + strings.Join(jobs, ",") + `]}`, ""
	},
	"job with no id": func(id, page int, _ *http.Request) (string, string) {
		return `{"total_count":2,"jobs":[` + runsCutJob(id, page, 0) + `,{"name":"unit","status":"completed"}]}`, ""
	},
	"null job": func(id, page int, _ *http.Request) (string, string) {
		return `{"total_count":2,"jobs":[` + runsCutJob(id, page, 0) + `,null]}`, ""
	},
	"unreadable total": func(id, page int, _ *http.Request) (string, string) {
		return `{"total_count":"many","jobs":[` + runsCutJob(id, page, 0) + `]}`, ""
	},
	"page budget": func(id, page int, request *http.Request) (string, string) {
		next := *request.URL
		forward := next.Query()
		forward.Set("page", strconv.Itoa(page+1))
		next.RawQuery = forward.Encode()
		return `{"jobs":[` + runsCutJob(id, page, 0) + `]}`, "<" + next.String() + ">; rel=\"next\""
	},
}

func (doer *runsCutDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	if path == "/repos/acme/api/actions/runs" && doer.listRewrite != nil {
		response, err := doer.cutDedupDoer.Do(request)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			doer.t.Fatal(readErr)
		}
		response.Body = io.NopCloser(strings.NewReader(doer.listRewrite(string(body))))
		return response, nil
	}
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
	if shape, ok := runsCutJobShapes[doer.jobShape[id]]; ok {
		body, link := shape(id, page, request)
		if link != "" {
			header.Set("Link", link)
		}
		return githubTestsHTTPResponse(request, header, body), nil
	}
	count, last := 1, 1
	if doer.capped[id] {
		count, last = githubTestsPerRunPerPage, githubTestsMaxJobsPerRun/githubTestsPerRunPerPage+1
	}
	jobs := make([]string, 0, count)
	for index := 0; index < count; index++ {
		jobs = append(jobs, runsCutJob(id, page, index))
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

// Only a run GitHub calls completed is recorded: a missing, empty or unknown status, and an open status with a stale
// success conclusion, are read again by the revisit (5004 is read in the first range and listed again).
func TestGitHubTestsRunsCutSecondRunWithoutACompletedStatusIsReadAgain(t *testing.T) {
	for _, tc := range []struct {
		name, status, conclusion string
		want                     int
	}{
		{"completed", "completed", "success", 1},
		{"missing status", "DROPKEY", "success", 2},
		{"empty status", "", "success", 2},
		{"unknown status", "mystery", "success", 2},
		{"in_progress, stale success", "in_progress", "success", 2},
		{"queued, stale success", "queued", "success", 2},
	} {
		runs := cutDedupRuns(cutDedupBefore.Add(-time.Hour))
		for index := range runs {
			if runs[index].id == 5004 {
				runs[index].status, runs[index].conclusion = tc.status, tc.conclusion
			}
		}
		doer := newRunsCutDoer(t, runs)
		doer.listRewrite = func(body string) string { return strings.ReplaceAll(body, `"status":"DROPKEY",`, "") }
		walkCutDedup(t, cutDedupClaim(false), doer, nil, nil)
		if got := doer.jobReads[5004]; got != tc.want {
			t.Errorf("%s: run 5004 jobs reads = %d, want %d", tc.name, got, tc.want)
		}
		if got := doer.jobReadsOf(cutDedupIDs[:4]); !reflect.DeepEqual(got, []int{1, 1, 1, 1}) {
			t.Errorf("%s: jobs reads of the other cut-second runs = %v, want once each", tc.name, got)
		}
	}
}

// runsCutPartialJobs walks the fcc layout once per jobs shape of 5004 (read in the first range, listed again by the
// next): a whole read is read once, every other shape twice. The other cut-second runs are read once.
func runsCutPartialJobs(t *testing.T, names []string, resume bool) {
	t.Helper()
	for _, name := range names {
		if runsCutJobShapes[name] == nil {
			t.Fatalf("unknown jobs shape %q", name)
		}
		doer := newRunsCutDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour)))
		doer.jobShape[5004] = name
		var yield func(before, after githubTestsChunkCursor) bool
		if resume {
			yield = func(before, after githubTestsChunkCursor) bool {
				return runsCutPhase(before, after) && after.RangeHi == "" && after.RangeItems == 999
			}
		}
		walkCutDedup(t, cutDedupClaim(false), doer, yield, nil)
		want := 2
		if name == runsCutWholeShape {
			want = 1
		}
		if got := doer.jobReads[5004]; got != want {
			t.Errorf("%s (resume=%v): run 5004 jobs reads = %d, want %d", name, resume, got, want)
		}
		if got := doer.jobReadsOf(cutDedupIDs[:4]); !reflect.DeepEqual(got, []int{1, 1, 1, 1}) {
			t.Errorf("%s (resume=%v): jobs reads of the other cut-second runs = %v, want once each", name, resume, got)
		}
	}
}

// A jobs read with fewer jobs than total_count declares (or an unreadable total) is not whole: it is read again.
func TestGitHubTestsRunsCutSecondJobsShortOfTheDeclaredTotalAreReadAgain(t *testing.T) {
	runsCutPartialJobs(t, []string{runsCutWholeShape, "missing array, 2 declared", "null array, 2 declared", "short page, 2 declared", "unreadable total"}, false)
	runsCutPartialJobs(t, []string{"short page, 2 declared"}, true)
}

// A jobs walk with no proven end (a bad Link header, a full page without Link, the page budget) or a job it could not
// keep is not whole: it is read again.
func TestGitHubTestsRunsCutSecondJobsWalkNotProvenWholeIsReadAgain(t *testing.T) {
	runsCutPartialJobs(t, []string{"bad Link header", "full page, no Link", "page budget", "job with no id", "null job"}, false)
	runsCutPartialJobs(t, []string{"job with no id"}, true)
}
