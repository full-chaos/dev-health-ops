package providersync

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// runsTwiceCell is one layout of the runs listing at the 1,000-result cap: count runs, the oldest ties of them in one
// second, ids from 13,000,000,001 (11 digits) when long, each run at attempt 2. The deep layout is created three days
// before the window (the deep range of the midnight claim), the regular one inside the window.
type runsTwiceCell struct {
	name               string
	count, ties, chunk int
	long, fullJobs     bool
	saturated          bool
}

func runsTwiceRuns(cell runsTwiceCell, rangeName string, since time.Time) []listingRun {
	first := 1
	if cell.long {
		first = 13_000_000_001
	}
	var runs []listingRun
	if rangeName == "deep" {
		runs = lateRerunRuns(first, cell.count, since)
	} else {
		for index := range cell.count {
			created := since.Add(time.Minute + time.Duration(index)*time.Second)
			runs = append(runs, listingRun{id: first + index, created: created, updated: created.Add(time.Minute),
				status: "completed", conclusion: "success", attempt: 2})
		}
	}
	for index := 1; index < cell.ties; index++ {
		runs[index].created = runs[0].created
	}
	return runs
}

// runsTwiceFullJobs answers run 1's jobs with exactly 100 jobs, total_count 100 and no Link header.
func runsTwiceFullJobs(fake *listingFake) providerfoundation.HTTPDoer {
	jobs := make([]string, 0, githubTestsPerRunPerPage)
	for index := range githubTestsPerRunPerPage {
		jobs = append(jobs, fmt.Sprintf(`{"id":%d,"name":"matrix","status":"completed","conclusion":"success"}`, index+1000))
	}
	body := `{"total_count":100,"jobs":[` + strings.Join(jobs, ",") + "]}"
	return jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/repos/acme/api/actions/runs/1/jobs" {
			return fake.Do(request)
		}
		fake.mu.Lock()
		fake.jobGets[1]++
		fake.mu.Unlock()
		return fake.respond(request, http.Header{"Content-Type": {"application/json"}}, body, 200), nil
	})
}

func runsTwiceExtraJobs(fake *listingFake) int {
	extra := 0
	for _, reads := range fake.jobGets {
		extra += max(0, reads-1)
	}
	return extra
}

// runsTwiceCheck walks one cell on both routes (CollectChunks, resumed every chunk emissions when set, and Collect) and
// wants each run read once: one pipeline row per id and one jobs request per id on each route.
func runsTwiceCheck(t *testing.T, cell runsTwiceCell, rangeName string) {
	t.Helper()
	claim := lateRerunMidnightClaim("incremental")
	runs := runsTwiceRuns(cell, rangeName, claim.SinceAt.UTC())
	walkFake, oracleFake := newListingFake(t, runs), newListingFake(t, runs)
	handler := GitHubTestsRouteHandler{}
	var walk listingWalk
	var oracleDoer providerfoundation.HTTPDoer = oracleFake
	if cell.fullJobs {
		walk = lateRerunYieldWalk(t, handler, claim, runsTwiceFullJobs(walkFake), func(int, githubTestsChunkCursor) bool { return false })
		oracleDoer = runsTwiceFullJobs(oracleFake)
	} else {
		walk = walkListing(t, handler, claim, walkFake, cell.chunk)
	}
	batch, oracleErr := handler.Collect(context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(oracleDoer)), claim.BeforeAt.Add(time.Hour))
	oracleRows := 0
	for _, effect := range batch.Effects {
		if effect.Destination == "ci_pipeline_runs" {
			oracleRows += len(effect.Rows)
		}
	}
	var observed []string
	for _, observation := range lateRerunIncomplete(t, walk) {
		observed = append(observed, observation.Component+"/"+observation.Cause)
	}
	unique := len(walk.pipelineIDs())
	t.Logf("%s/%s: chunk_rows=%d unique=%d extra_jobs=%d oracle_rows=%d oracle_extra_jobs=%d oracle_err=%v watermark=%t incomplete=%v",
		rangeName, cell.name, len(walk.pipelines), unique, runsTwiceExtraJobs(walkFake), oracleRows, runsTwiceExtraJobs(oracleFake),
		oracleErr, walk.final.Watermark != nil, observed)
	if unique != len(runs) && !cell.saturated || cell.saturated && unique != githubTestsSearchResultCap {
		t.Errorf("%s/%s: %d distinct runs written, want every listable run", rangeName, cell.name, unique)
	}
	if len(walk.pipelines) != unique || runsTwiceExtraJobs(walkFake) != 0 {
		t.Errorf("%s/%s: CollectChunks wrote %d rows for %d runs with %d extra jobs requests, want each run once",
			rangeName, cell.name, len(walk.pipelines), unique, runsTwiceExtraJobs(walkFake))
	}
	if runsTwiceExtraJobs(oracleFake) != 0 {
		t.Errorf("%s/%s: Collect made %d extra jobs requests, want each run once", rangeName, cell.name, runsTwiceExtraJobs(oracleFake))
	}
	switch {
	case cell.saturated && rangeName == "regular":
		// The regular range of a saturated second withholds the watermark and Collect refuses at the cap, as on main.
		if !errors.Is(oracleErr, ErrPaginationCapExceeded) || walk.final.Watermark != nil ||
			!slices.Contains(observed, githubTestsRunInventorySaturatedComponent+"/"+githubTestsListingSaturatedCause) {
			t.Errorf("regular/%s: oracle err=%v watermark=%t incomplete=%v, want the run inventory saturated and no watermark",
				cell.name, oracleErr, walk.final.Watermark != nil, observed)
		}
	case cell.saturated:
		if oracleErr != nil || oracleRows != unique || walk.final.Watermark == nil ||
			!slices.Contains(observed, githubTestsLateRerunRunsComponent+"/"+githubTestsListingSaturatedCause) {
			t.Errorf("deep/%s: oracle rows=%d err=%v watermark=%t incomplete=%v, want the deep stop and the watermark",
				cell.name, oracleRows, oracleErr, walk.final.Watermark != nil, observed)
		}
	default:
		if oracleErr != nil || oracleRows != unique || walk.final.Watermark == nil || len(observed) != 0 {
			t.Errorf("%s/%s: oracle rows=%d err=%v watermark=%t incomplete=%v, want %d rows, the watermark and no observation",
				rangeName, cell.name, oracleRows, oracleErr, walk.final.Watermark != nil, observed, unique)
		}
	}
}

func runsTwiceRanges(t *testing.T, cells []runsTwiceCell) {
	t.Helper()
	for _, rangeName := range []string{"regular", "deep"} {
		for _, cell := range cells {
			t.Run(rangeName+"/"+cell.name, func(t *testing.T) { runsTwiceCheck(t, cell, rangeName) })
		}
	}
}

// Four or five 11-digit cut-second keys at attempt 2 (over 52 bytes) are read once, fresh and resumed.
func TestGitHubTestsRunsCutSecondLongKeysAreReadOnce(t *testing.T) {
	runsTwiceRanges(t, []runsTwiceCell{
		{name: "four_long_ids", count: 1001, ties: 5, long: true},
		{name: "five_long_ids", count: 1001, ties: 6, long: true},
		{name: "five_long_ids_resumed", count: 1001, ties: 6, long: true, chunk: 64},
	})
}

// A cut-second run whose jobs answer is exactly one full page with total_count equal to it and no Link is whole.
func TestGitHubTestsRunsCutSecondFullJobsPageWithItsTotalIsReadOnce(t *testing.T) {
	runsTwiceRanges(t, []runsTwiceCell{{name: "exactly_100_jobs_total_100_no_link", count: 1000, fullJobs: true}})
}

// A range whose 1,000 runs share one second is listed once: the same runs are not listed and read again before the
// saturated second is recorded.
func TestGitHubTestsRunsSaturatedSecondIsReadOnce(t *testing.T) {
	runsTwiceRanges(t, []runsTwiceCell{
		{name: "saturated_second", count: 1001, ties: 1001, saturated: true},
		{name: "saturated_second_resumed", count: 1001, ties: 1001, saturated: true, chunk: 64},
	})
}

// Controls: a cut second that the cut list already holds is read once.
func TestGitHubTestsRunsCutSecondControlsAreReadOnce(t *testing.T) {
	runsTwiceRanges(t, []runsTwiceCell{
		{name: "one_cut_run", count: 1000},
		{name: "shared_cut_second", count: 1001, ties: 2},
		{name: "shared_cut_second_resumed", count: 1001, ties: 2, chunk: 64},
		{name: "three_long_ids_fit", count: 1001, ties: 4, long: true},
	})
}
