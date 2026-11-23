package providersync

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// floorRetryDoer is the listing fake with one transient failure: the FIRST by-id read of run failID answers 500 with
// failRemaining requests left of a 5,000 limit; every later request goes to the fake (4,000 left). It counts the wire
// attempts of each by-id read.
type floorRetryDoer struct {
	fake          *listingFake
	failID        int
	failRemaining int
	mu            sync.Mutex
	byIDAttempts  map[int]int
}

func (doer *floorRetryDoer) Do(request *http.Request) (*http.Response, error) {
	const prefix = "/repos/acme/api/actions/runs/"
	if path := request.URL.Path; strings.HasPrefix(path, prefix) && !strings.Contains(strings.TrimPrefix(path, prefix), "/") {
		id, err := strconv.Atoi(strings.TrimPrefix(path, prefix))
		if err == nil {
			doer.mu.Lock()
			doer.byIDAttempts[id]++
			first := doer.byIDAttempts[id] == 1
			doer.mu.Unlock()
			if id == doer.failID && first {
				header := http.Header{"Content-Type": {"application/json"}}
				header.Set("X-RateLimit-Remaining", strconv.Itoa(doer.failRemaining))
				header.Set("X-RateLimit-Limit", "5000")
				return &http.Response{StatusCode: http.StatusInternalServerError, Header: header,
					Body: io.NopCloser(strings.NewReader(`{"message":"Server Error"}`)), Request: request}, nil
			}
		}
	}
	return doer.fake.Do(request)
}

// floorRetryWalk runs the open phase of the hourly claim over three stale open runs (7100..7102) with a client that
// retries once (MaxAttempts 2), and returns the final result and the doer.
func floorRetryWalk(t *testing.T, failRemaining int) (map[string]any, *floorRetryDoer) {
	t.Helper()
	repoID := listingRepoID(t)
	stale := listingSince.Add(-30 * 24 * time.Hour)
	fake := newListingFake(t, makeListingRuns(1, 2, listingSince, time.Minute))
	var rows []githubTestsPipelineRow
	for index := 0; index < 3; index++ {
		id := 7100 + index
		started := stale.Add(time.Duration(index) * time.Minute)
		rows = append(rows, openRow(repoID, strconv.Itoa(id), started, "running"))
		fake.byID[id] = listingRun{id: id, created: started, updated: started.Add(time.Hour), status: "completed", conclusion: "success"}
	}
	doer := &floorRetryDoer{fake: fake, failID: 7100, failRemaining: failRemaining, byIDAttempts: map[int]int{}}
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
		func(request *http.Request) error { request.Header.Set("Authorization", "Bearer secret"); return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var final map[string]any
	err = GitHubTestsRouteHandler{OpenRuns: &fakeOpenRuns{rows: rows}}.CollectChunks(ctx, openWindowClaim(),
		providerfoundation.Credential{}, client, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(emission ChunkRouteEmission) error {
			if emission.Final {
				final = emission.Batch.Result
			}
			return nil
		})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if final == nil {
		t.Fatal("the walk emitted no final batch")
	}
	return final, doer
}

// CHAOS-14435 commit 1 (#6802 r2 finding B): the open phase's rate floor must be read before EVERY wire attempt, retries
// included. A retryable 500 that reports 100 requests left (under the floor of 1,000 = a fifth of 5,000) must not be
// retried: the retry would spend a request under the floor, and its success headers (4,000 left) would hide the low
// reading from the per-run check. Want: one attempt, the floor marker with the low reading, no read of the next runs.
func TestGitHubTestsOpenPhaseFloorRefusesTheRetryUnderTheFloor(t *testing.T) {
	final, doer := floorRetryWalk(t, 100)
	t.Logf("CELL R1 by-id attempts 7100=%d 7101=%d 7102=%d open_runs_reread=%v open_phase_rate_floor=%v",
		doer.byIDAttempts[7100], doer.byIDAttempts[7101], doer.byIDAttempts[7102], final["open_runs_reread"], final["open_phase_rate_floor"])
	if got := doer.byIDAttempts[7100]; got != 1 {
		t.Errorf("run 7100: %d wire attempts, want 1: the retry was sent although the 500 reported 100 left, under the floor of 1000", got)
	}
	if marker, hit := final["open_phase_rate_floor"]; !hit || marker != int64(100) {
		t.Errorf("open_phase_rate_floor = %v (present %v), want 100: the low reading of the refused attempt", marker, hit)
	}
	if doer.byIDAttempts[7101] != 0 || doer.byIDAttempts[7102] != 0 {
		t.Errorf("runs 7101/7102 read %d/%d times, want 0: the open phase must stop under the floor",
			doer.byIDAttempts[7101], doer.byIDAttempts[7102])
	}
}

// Control: the same 500 with 4,000 left (above the floor) is retried as on main, and the open phase reads every run.
func TestGitHubTestsOpenPhaseFloorAboveRetriesAsMain(t *testing.T) {
	final, doer := floorRetryWalk(t, 4000)
	t.Logf("CELL R1c by-id attempts 7100=%d 7101=%d 7102=%d open_runs_reread=%v open_phase_rate_floor=%v",
		doer.byIDAttempts[7100], doer.byIDAttempts[7101], doer.byIDAttempts[7102], final["open_runs_reread"], final["open_phase_rate_floor"])
	if got := doer.byIDAttempts[7100]; got != 2 {
		t.Errorf("run 7100: %d wire attempts, want 2 (one retry, as main)", got)
	}
	if got := final["open_runs_reread"]; got != 3 {
		t.Errorf("open_runs_reread = %v, want 3", got)
	}
	if marker, hit := final["open_phase_rate_floor"]; hit {
		t.Errorf("open_phase_rate_floor = %v, want absent", marker)
	}
}
