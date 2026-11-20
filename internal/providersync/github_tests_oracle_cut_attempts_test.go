package providersync

import (
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// The oracle (Collect) and CollectChunks read a run listed again at a peel's cut second the same way: a repeat is skipped
// only after the same (run, attempt) had a whole read. A newer attempt, an incomplete jobs result or an artifact read with a
// loss is read again.

func oracleCutVisits(fake *listingFake, query string) int {
	count := 0
	for _, listed := range fake.listRanges {
		if listed == query {
			count++
		}
	}
	return count
}

func oracleCutPeeledQuery(claim Claim, handler GitHubTestsRouteHandler, rangeName, hi string) string {
	if rangeName == "deep" {
		return githubTestsLateRerunRunsQuery(claim, handler.refreshLookback(), handler.lateRerunLookback(), hi).Get("created")
	}
	return githubTestsRunsQuery(claim, handler.refreshLookback(), hi).Get("created")
}

func oracleCutPipelineAttempts(t *testing.T, batch CompleteRouteBatch, runID string) []uint32 {
	t.Helper()
	var attempts []uint32
	for _, effect := range batch.Effects {
		if effect.Destination != "ci_pipeline_runs" {
			continue
		}
		rows, err := decodeEffectRows[githubTestsPipelineRow](effect)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if row.RunID == runID {
				attempts = append(attempts, row.RetryCount+1)
			}
		}
	}
	return attempts
}

// oracleCutRunsRead runs one route over a saturated runs-phase range whose cut second holds run 13000000002 (attempt 2),
// so the peeled listing lists it again. change: stable; new_attempt (attempt 3 on the peeled listing); incomplete_jobs (the
// first jobs response declares 2 jobs and returns 1).
func oracleCutRunsRead(t *testing.T, rangeName, route, change string) (attempts []uint32, jobGets int) {
	t.Helper()
	claim := runsTwiceClaim(rangeName)
	handler := runsTwiceHandler(rangeName)
	runs, _ := runsTwiceRuns(runsTwiceCell{count: 1001, ties: 6, long: true}, rangeName, claim.SinceAt.UTC())
	fake := newListingFake(t, runs)
	at, id := runs[1].created.UTC().Format(time.RFC3339), "13000000002"
	peeled := oracleCutPeeledQuery(claim, handler, rangeName, at)
	doer := jiraWorkItemsDoerFunc(func(req *http.Request) (*http.Response, error) {
		if change == "new_attempt" && req.URL.Path == "/repos/acme/api/actions/runs" && req.URL.Query().Get("created") == peeled {
			fake.runs[1].attempt = 3
			fake.runs[1].updated = claim.SinceAt.UTC().Add(time.Minute)
		}
		if change == "incomplete_jobs" && strings.HasSuffix(req.URL.Path, "/"+id+"/jobs") {
			fake.jobGets[runs[1].id]++
			body := `{"total_count":2,"jobs":[{"id":50,"name":"test","status":"completed","conclusion":"success"}`
			if fake.jobGets[runs[1].id] > 1 {
				body += `,{"id":51,"name":"test2","status":"completed","conclusion":"success"}`
			}
			return fake.respond(req, http.Header{"Content-Type": {"application/json"}}, body+"]}", 200), nil
		}
		return fake.Do(req)
	})
	switch route {
	case "Collect":
		batch, err := handler.Collect(t.Context(), claim, providerfoundation.Credential{}, githubTestsClient(t, fakehttp.Client(doer)), claim.BeforeAt.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		attempts = oracleCutPipelineAttempts(t, batch, id)
	default:
		walk := lateRerunYieldWalk(t, handler, claim, doer, func(int, githubTestsChunkCursor) bool { return false })
		for _, row := range walk.pipelines {
			if row.RunID == id {
				attempts = append(attempts, row.RetryCount+1)
			}
		}
	}
	if listed := oracleCutVisits(fake, peeled); listed != 1 {
		t.Fatalf("%s listed the peeled range %d times, want 1", route, listed)
	}
	return attempts, fake.jobGets[runs[1].id]
}

func oracleCutRunsCells(t *testing.T, rangeName string) {
	wants := map[string]struct {
		attempts []uint32
		gets     int
	}{
		"stable":          {[]uint32{2}, 1},
		"new_attempt":     {[]uint32{2, 3}, 2},
		"incomplete_jobs": {[]uint32{2, 2}, 2},
	}
	for _, change := range []string{"stable", "new_attempt", "incomplete_jobs"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			oracle, oracleGets := oracleCutRunsRead(t, rangeName, "Collect", change)
			chunked, chunkedGets := oracleCutRunsRead(t, rangeName, "CollectChunks", change)
			want := wants[change]
			if !slices.Equal(oracle, want.attempts) || oracleGets != want.gets {
				t.Errorf("Collect: attempts=%v jobs reads=%d, want %v and %d", oracle, oracleGets, want.attempts, want.gets)
			}
			if !slices.Equal(chunked, want.attempts) || chunkedGets != want.gets {
				t.Errorf("CollectChunks: attempts=%v jobs reads=%d, want %v and %d", chunked, chunkedGets, want.attempts, want.gets)
			}
		})
	}
}

func TestGitHubTestsOracleCutRunsRegular(t *testing.T) { oracleCutRunsCells(t, "regular") }

func TestGitHubTestsOracleCutRunsDeep(t *testing.T) { oracleCutRunsCells(t, "deep") }

// oracleCutArtifactsRead runs one route over the cutDedup layout (five report runs share the cut second of a saturated
// artifacts listing). change: stable; new_attempt (5004 is attempt 2 on the peeled listing); loss (the first download of
// 5004's report fails); partial_loss (5004 lists a second report whose first download fails, so its first read holds a
// report and a loss). It returns the artifact-list reads of the five runs and the suite rows per run.
func oracleCutArtifactsRead(t *testing.T, route, change string) (reads []int, suites []int) {
	t.Helper()
	doer := newCutDedupDoer(t, cutDedupRuns(cutDedupBefore.Add(-time.Hour))).withReports(cutDedupIDs...)
	switch change {
	case "new_attempt":
		doer.attemptAt[5004] = cutDedupSecond.Format(time.RFC3339)
	case "loss", "partial_loss":
		doer.failDownload[5004] = 1
	}
	var served providerfoundation.HTTPDoer = doer
	if change == "partial_loss" {
		doer.failDownload[5004] = 0
		doer.failDownload[95004] = 1
		served = jiraWorkItemsDoerFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/repos/acme/api/actions/runs/5004/artifacts" {
				return doer.Do(req)
			}
			doer.lists[5004]++
			return githubTestsHTTPResponse(req, http.Header{"Content-Type": {"application/json"}},
				`{"artifacts":[{"id":5004,"name":"test-results-5004","expired":false},{"id":95004,"name":"test-results-95004","expired":false}]}`), nil
		})
	}
	rows := map[string]int{}
	switch route {
	case "Collect":
		batch, err := GitHubTestsRouteHandler{}.Collect(t.Context(), cutDedupClaim(false), providerfoundation.Credential{}, githubTestsClient(t, served), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))
		if err != nil {
			t.Fatal(err)
		}
		for _, effect := range batch.Effects {
			if effect.Destination != "test_suite_results" {
				continue
			}
			decoded, err := decodeEffectRows[testSuiteResultRow](effect)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range decoded {
				rows[row.RunID]++
			}
		}
	default:
		rows = walkCutDedup(t, cutDedupClaim(false), served, nil, nil).rows["test_suite_results"]
	}
	for _, id := range cutDedupIDs {
		suites = append(suites, rows[strconv.Itoa(id)])
	}
	return doer.reads(cutDedupIDs), suites
}

func oracleCutArtifactsCells(t *testing.T, changes ...string) {
	wantReads := map[string][]int{
		"stable":       {1, 1, 1, 1, 1},
		"new_attempt":  {1, 1, 1, 1, 2},
		"loss":         {1, 1, 1, 1, 2},
		"partial_loss": {1, 1, 1, 1, 2},
	}
	for _, change := range changes {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			oracleReads, oracleSuites := oracleCutArtifactsRead(t, "Collect", change)
			chunkedReads, chunkedSuites := oracleCutArtifactsRead(t, "CollectChunks", change)
			if !reflect.DeepEqual(chunkedReads, wantReads[change]) {
				t.Fatalf("CollectChunks: artifact lists of %v = %v, want %v", cutDedupIDs, chunkedReads, wantReads[change])
			}
			if !reflect.DeepEqual(oracleReads, chunkedReads) || !reflect.DeepEqual(oracleSuites, chunkedSuites) {
				t.Errorf("Collect: artifact lists %v suite rows %v, CollectChunks: %v and %v", oracleReads, oracleSuites, chunkedReads, chunkedSuites)
			}
		})
	}
}

func TestGitHubTestsOracleCutArtifactsAttempt(t *testing.T) {
	oracleCutArtifactsCells(t, "stable", "new_attempt")
}

func TestGitHubTestsOracleCutArtifactsLoss(t *testing.T) {
	oracleCutArtifactsCells(t, "loss", "partial_loss")
}
