package providersync

import (
	"context"
	"encoding/json"
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
// second, ids from 13,000,000,001 (11 digits) when long, each run at attempt 2. Every run is listed, but only the runs of
// the cut second (the newest 10 of a saturated second) changed since the window start (regular) or late (deep): only those
// are read, so a cell costs one jobs request and one row per run that matters (the race cost of 1,000 reads).
type runsTwiceCell struct {
	name               string
	count, ties, chunk int
	long, fullJobs     bool
	saturated          bool
}

const runsTwiceSaturatedReads = 10

func (cell runsTwiceCell) read(index int) bool {
	if cell.saturated {
		return index >= cell.count-runsTwiceSaturatedReads
	}
	return index < max(1, cell.ties)
}

// runsTwiceRuns: the regular layout is created 60 h before the window, inside the 72 h refresh lookback of
// runsTwiceHandler and before the artifacts range (read only when it changed after the window start; no artifacts
// listing reads it), the deep layout three days before the window (read only when it changed late).
func runsTwiceRuns(cell runsTwiceCell, rangeName string, since time.Time) ([]listingRun, int) {
	first := 1
	if cell.long {
		first = 13_000_000_001
	}
	runs := make([]listingRun, 0, cell.count)
	for index := range cell.count {
		run := listingRun{id: first + index, status: "completed", conclusion: "success", attempt: 2}
		if rangeName == "deep" {
			run.created = since.Add(-72*time.Hour + time.Duration(index)*time.Minute)
		} else {
			run.created = since.Add(-60*time.Hour + time.Duration(index)*time.Second)
		}
		run.updated = run.created.Add(time.Minute)
		runs = append(runs, run)
	}
	for index := 1; index < cell.ties; index++ {
		runs[index].created = runs[0].created
	}
	reads := 0
	for index := range runs {
		if !cell.read(index) {
			continue
		}
		reads++
		if rangeName == "deep" {
			runs[index].startedAt, runs[index].updated = since.Add(-2*time.Hour), since.Add(-time.Hour)
		} else {
			runs[index].updated = since.Add(time.Minute)
		}
	}
	return runs, reads
}

// runsTwiceClaim: the deep layout needs the claim that holds a UTC midnight; the regular layout takes an hour that does
// not, so no deep range or deep re-list walks it again.
func runsTwiceClaim(rangeName string) Claim {
	if rangeName == "deep" {
		return lateRerunMidnightClaim("incremental")
	}
	since := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	claim := listingClaim(since, since.Add(time.Hour))
	claim.Mode = "incremental"
	return claim
}

// runsTwiceHandler: the regular layout is listed by a runs range of a 72 h refresh lookback.
func runsTwiceHandler(rangeName string) GitHubTestsRouteHandler {
	if rangeName == "regular" {
		return GitHubTestsRouteHandler{RefreshLookback: 72 * time.Hour}
	}
	return GitHubTestsRouteHandler{}
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
	claim := runsTwiceClaim(rangeName)
	runs, reads := runsTwiceRuns(cell, rangeName, claim.SinceAt.UTC())
	walkFake, oracleFake := newListingFake(t, runs), newListingFake(t, runs)
	handler := runsTwiceHandler(rangeName)
	var walk listingWalk
	var oracleDoer providerfoundation.HTTPDoer = oracleFake
	if cell.fullJobs {
		walk = lateRerunYieldWalk(t, handler, claim, runsTwiceFullJobs(walkFake), func(int, githubTestsChunkCursor) bool { return false })
		oracleDoer = runsTwiceFullJobs(oracleFake)
	} else {
		walk = walkListing(t, handler, claim, walkFake, cell.chunk)
	}
	// A resumed cell walks the fixture of a fresh cell: Collect answers the same, so it runs in the fresh cell only.
	oracle := cell.chunk == 0
	var batch CompleteRouteBatch
	var oracleErr error
	if oracle {
		batch, oracleErr = handler.Collect(context.Background(), claim, providerfoundation.Credential{},
			githubTestsClient(t, fakehttp.Client(oracleDoer)), claim.BeforeAt.Add(time.Hour))
	}
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
	if unique != reads {
		t.Errorf("%s/%s: %d distinct runs written, want the %d changed runs", rangeName, cell.name, unique, reads)
	}
	if len(walk.pipelines) != unique || runsTwiceExtraJobs(walkFake) != 0 {
		t.Errorf("%s/%s: CollectChunks wrote %d rows for %d runs with %d extra jobs requests, want each run once",
			rangeName, cell.name, len(walk.pipelines), unique, runsTwiceExtraJobs(walkFake))
	}
	if runsTwiceExtraJobs(oracleFake) != 0 {
		t.Errorf("%s/%s: Collect made %d extra jobs requests, want each run once", rangeName, cell.name, runsTwiceExtraJobs(oracleFake))
	}
	// The saturated range is listed once: GitHub's cap of runs in pages, no second listing of the same second.
	wantPages := githubTestsSearchResultCap / 100
	if rangeName == "deep" {
		wantPages++ // the empty runs range of the midnight claim
	}
	if pages := walk.cursor.RunPages + walk.cursor.LateRunPages; cell.saturated && pages != wantPages {
		t.Errorf("%s/%s: %d runs-phase listing pages, want %d (the saturated range once)", rangeName, cell.name, pages, wantPages)
	}
	switch {
	case cell.saturated && rangeName == "regular":
		// The regular range of a saturated second withholds the watermark and Collect refuses at the cap, as on main.
		if oracle && !errors.Is(oracleErr, ErrPaginationCapExceeded) || walk.final.Watermark != nil ||
			!slices.Contains(observed, githubTestsRunInventorySaturatedComponent+"/"+githubTestsListingSaturatedCause) {
			t.Errorf("regular/%s: oracle err=%v watermark=%t incomplete=%v, want the run inventory saturated and no watermark",
				cell.name, oracleErr, walk.final.Watermark != nil, observed)
		}
	case cell.saturated:
		if oracle && (oracleErr != nil || oracleRows != unique) || walk.final.Watermark == nil ||
			!slices.Contains(observed, githubTestsLateRerunRunsComponent+"/"+githubTestsListingSaturatedCause) {
			t.Errorf("deep/%s: oracle rows=%d err=%v watermark=%t incomplete=%v, want the deep stop and the watermark",
				cell.name, oracleRows, oracleErr, walk.final.Watermark != nil, observed)
		}
	default:
		if oracle && (oracleErr != nil || oracleRows != unique) || walk.final.Watermark == nil || len(observed) != 0 {
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

// Four 11-digit cut-second keys at attempt 2 (55 bytes, over 52) are read once on both routes.
func TestGitHubTestsRunsCutSecondLongKeysAreReadOnce(t *testing.T) {
	runsTwiceRanges(t, []runsTwiceCell{{name: "four_long_ids", count: 1001, ties: 5, long: true}})
}

// Five long cut-second keys are carried over a resume every 64 emissions and read once.
func TestGitHubTestsRunsCutSecondLongKeysAreReadOnceAcrossResumes(t *testing.T) {
	runsTwiceRanges(t, []runsTwiceCell{{name: "five_long_ids_resumed", count: 1001, ties: 6, long: true, chunk: 64}})
}

// A cut-second run whose jobs answer is exactly one full page with total_count equal to it and no Link is whole.
func TestGitHubTestsRunsCutSecondFullJobsPageWithItsTotalIsReadOnce(t *testing.T) {
	runsTwiceRanges(t, []runsTwiceCell{{name: "exactly_100_jobs_total_100_no_link", count: 1000, fullJobs: true}})
}

// A range whose 1,000 runs share one second is listed once: the same runs are not listed and read again before the
// saturated second is recorded. Ten of them are read: their keys pass 52 bytes, as on main.
func TestGitHubTestsRunsSaturatedSecondIsReadOnce(t *testing.T) {
	runsTwiceRanges(t, []runsTwiceCell{{name: "saturated_second", count: 1001, ties: 1001, saturated: true}})
}

// The count of runs at the saturated second is carried over a resume every 64 emissions.
func TestGitHubTestsRunsSaturatedSecondIsReadOnceAcrossResumes(t *testing.T) {
	runsTwiceRanges(t, []runsTwiceCell{{name: "saturated_second_resumed", count: 1001, ties: 1001, saturated: true, chunk: 64}})
}

// Control: three long cut-second keys (41 bytes) fit the 52 bytes of main and are read once on both routes, as on main.
func TestGitHubTestsRunsCutSecondControlsAreReadOnce(t *testing.T) {
	runsTwiceRanges(t, []runsTwiceCell{{name: "three_long_ids_fit", count: 1001, ties: 4, long: true}})
}

func runsTwiceDeep(t *testing.T, cells []runsTwiceCell) {
	t.Helper()
	for _, cell := range cells {
		t.Run("deep/"+cell.name, func(t *testing.T) { runsTwiceCheck(t, cell, "deep") })
	}
}

// Deep-range controls: one run at the cut second, and two runs sharing it (one in the first range), are read once on
// both routes, as on main. The regular range keeps these controls in TestGitHubTestsRunsCutSecondRunsAreReadOnce.
func TestGitHubTestsRunsCutSecondDeepRangeControlsAreReadOnce(t *testing.T) {
	runsTwiceDeep(t, []runsTwiceCell{
		{name: "one_cut_run", count: 1000},
		{name: "shared_cut_second", count: 1001, ties: 2},
	})
}

// Deep-range control: the shared cut second is read once across a resume every 64 emissions, as on main.
func TestGitHubTestsRunsCutSecondDeepRangeControlsAreReadOnceAcrossResumes(t *testing.T) {
	runsTwiceDeep(t, []runsTwiceCell{{name: "shared_cut_second_resumed", count: 1001, ties: 2, chunk: 64}})
}

// The runs phase keeps up to githubTestsMaxRunsCutKeysBytes of cut keys and the artifacts phase 52: one key past the cap
// overflows the list (those runs are read again, the named residual), and a decoded list past its phase cap is a named
// checkpoint conflict.
func TestGitHubTestsRunsCutListCapIsPerPhase(t *testing.T) {
	at := "2026-07-20T06:30:00Z"
	second, _ := githubTestsCutSecond(at)
	widest := strings.Split(githubTestsWidestRunsCutKeys(t), ",")
	fill := func(phase string, keys []string) githubTestsChunkCursor {
		cursor := githubTestsChunkCursor{Phase: phase, RangeOldest: at}
		for _, key := range keys {
			cursor = githubTestsRecordCutRun(cursor, second, key)
		}
		return cursor
	}
	if got := fill(githubTestsRunsPhase, widest).ActiveCutKeys; len(got) != githubTestsMaxRunsCutKeysBytes {
		t.Fatalf("runs phase: %d bytes of keys kept, want all %d", len(got), githubTestsMaxRunsCutKeysBytes)
	}
	if got := fill(githubTestsRunsPhase, append(slices.Clone(widest), "6")).ActiveCutKeys; got != githubTestsCutKeysOverflow {
		t.Fatalf("runs phase: one key past %d bytes kept %d bytes, want the overflow mark", githubTestsMaxRunsCutKeysBytes, len(got))
	}
	if got := fill(githubTestsArtifactsPhase, widest).ActiveCutKeys; got != githubTestsCutKeysOverflow {
		t.Fatalf("artifacts phase: past %d bytes kept %d bytes, want the overflow mark", githubTestsMaxCutKeysBytes, len(got))
	}
	for _, tc := range []struct {
		phase, keys string
		valid       bool
	}{
		{githubTestsRunsPhase, githubTestsWidestRunsCutKeys(t), true},
		{githubTestsRunsPhase, githubTestsWidestRunsCutKeys(t) + ",6", false},
		{githubTestsArtifactsPhase, githubTestsWidestCutKeys(t), true},
		{githubTestsArtifactsPhase, githubTestsWidestCutKeys(t) + "9", false},
	} {
		for _, carried := range []bool{false, true} {
			cursor := githubTestsChunkCursor{Phase: tc.phase, Repo: "acme/api", RangeHi: "2026-07-20T23:59:59Z", RangeOldest: at}
			if carried {
				cursor.CarriedCutKeys = tc.keys
			} else {
				cursor.ActiveCutKeys = tc.keys
			}
			raw, err := json.Marshal(cursor)
			if err != nil {
				t.Fatal(err)
			}
			_, err = decodeGitHubTestsChunkCursor(string(raw))
			if tc.valid && err != nil || !tc.valid && !errors.Is(err, ErrChunkCheckpointConflict) {
				t.Errorf("%s phase, %d-byte list (carried %t): decode err=%v, want valid=%t", tc.phase, len(tc.keys), carried, err, tc.valid)
			}
		}
	}
}

// The count of runs at the oldest second is a plain cursor int: negative, above the range's runs or without an oldest
// second is a corrupt cursor.
func TestGitHubTestsRunsRangeOldestItemsDecode(t *testing.T) {
	for _, tc := range []struct {
		name          string
		items, oldest int
		at            string
		valid         bool
	}{
		{"absent", 5, 0, "", true},
		{"within the range", 5, 5, "2026-07-20T06:30:00Z", true},
		{"negative", 5, -1, "2026-07-20T06:30:00Z", false},
		{"above the range", 5, 6, "2026-07-20T06:30:00Z", false},
		{"no oldest second", 5, 1, "", false},
	} {
		raw, err := json.Marshal(githubTestsChunkCursor{Phase: githubTestsRunsPhase, Repo: "acme/api", RangeItems: tc.items,
			RangeOldestItems: tc.oldest, RangeOldest: tc.at})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeGitHubTestsChunkCursor(string(raw))
		if tc.valid && (err != nil || decoded.RangeOldestItems != tc.oldest) || !tc.valid && !errors.Is(err, ErrChunkCheckpointConflict) {
			t.Errorf("%s: decode = %d err=%v, want valid=%t", tc.name, decoded.RangeOldestItems, err, tc.valid)
		}
	}
}
