package providersync

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// floorRetryDoer is the listing fake with one failing answer: the FIRST by-id read of run failID answers failStatus with
// failRemaining requests left of a 5,000 limit (no rate headers at all when failRemaining < 0) and failBody; every later
// request goes to the fake (4,000 left). It counts the wire attempts of each by-id read.
type floorRetryDoer struct {
	fake          *listingFake
	failID        int
	failStatus    int
	failRemaining int
	failBody      string
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
				if doer.failRemaining >= 0 {
					header.Set("X-RateLimit-Remaining", strconv.Itoa(doer.failRemaining))
					header.Set("X-RateLimit-Limit", "5000")
				}
				return &http.Response{StatusCode: doer.failStatus, Header: header,
					Body: io.NopCloser(strings.NewReader(doer.failBody)), Request: request}, nil
			}
		}
	}
	return doer.fake.Do(request)
}

// floorRecordingGate is a shared backoff gate that records each Penalize and fails it on a cancelled context, as the Valkey
// gate does (its command runs on that context).
type floorRecordingGate struct {
	mu        sync.Mutex
	penalties []error // ctx.Err() at each Penalize
}

func (gate *floorRecordingGate) Wait(context.Context) (time.Duration, error) { return 0, nil }

func (gate *floorRecordingGate) Penalize(ctx context.Context, _ time.Duration) error {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.penalties = append(gate.penalties, ctx.Err())
	return ctx.Err()
}

type floorRetryCase struct {
	status    int
	remaining int // < 0: the failing answer carries no rate headers
	body      string
	noHeaders bool // every answer of the fake carries no rate headers
}

type floorRetryResult struct {
	final map[string]any
	doer  *floorRetryDoer
	gate  *floorRecordingGate
	logs  string
}

// floorRetryWalk runs the open phase of the hourly claim over three stale open runs (7100..7102) with a client that
// retries once (MaxAttempts 2) and holds a recording shared gate, and returns the final result, the doer, the gate and
// the log lines of the walk.
func floorRetryWalk(t *testing.T, tc floorRetryCase) floorRetryResult {
	t.Helper()
	repoID := listingRepoID(t)
	stale := listingSince.Add(-30 * 24 * time.Hour)
	fake := newListingFake(t, makeListingRuns(1, 2, listingSince, time.Minute))
	fake.noHeaders = tc.noHeaders
	var rows []githubTestsPipelineRow
	for index := 0; index < 3; index++ {
		id := 7100 + index
		started := stale.Add(time.Duration(index) * time.Minute)
		rows = append(rows, openRow(repoID, strconv.Itoa(id), started, "running"))
		fake.byID[id] = listingRun{id: id, created: started, updated: started.Add(time.Hour), status: "completed", conclusion: "success"}
	}
	body := tc.body
	if body == "" {
		body = `{"message":"Server Error"}`
	}
	doer := &floorRetryDoer{fake: fake, failID: 7100, failStatus: tc.status, failRemaining: tc.remaining, failBody: body, byIDAttempts: map[int]int{}}
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
		func(request *http.Request) error { request.Header.Set("Authorization", "Bearer secret"); return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	gate := &floorRecordingGate{}
	client.Gate = gate
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(previous)
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
	return floorRetryResult{final: final, doer: doer, gate: gate, logs: logged.String()}
}

func (result floorRetryResult) logf(t *testing.T, name string) {
	t.Helper()
	t.Logf("CELL %s by-id attempts 7100=%d 7101=%d 7102=%d jobs 7100=%d open_runs_reread=%v open_phase_rate_floor=%v penalize_ctx_errs=%v",
		name, result.doer.byIDAttempts[7100], result.doer.byIDAttempts[7101], result.doer.byIDAttempts[7102],
		result.doer.fake.jobGets[7100], result.final["open_runs_reread"], result.final["open_phase_rate_floor"], result.gate.penalties)
}

// assertFloorStop: one attempt for run 7100, the floor marker with the low reading, no read of the next runs.
func (result floorRetryResult) assertFloorStop(t *testing.T, remaining int64) {
	t.Helper()
	if got := result.doer.byIDAttempts[7100]; got != 1 {
		t.Errorf("run 7100: %d wire attempts, want 1: the retry was sent although the answer reported %d left, under the floor of 1000", got, remaining)
	}
	if marker, hit := result.final["open_phase_rate_floor"]; !hit || marker != remaining {
		t.Errorf("open_phase_rate_floor = %v (present %v), want %d: the low reading of the refused retry", marker, hit, remaining)
	}
	if result.doer.byIDAttempts[7101] != 0 || result.doer.byIDAttempts[7102] != 0 {
		t.Errorf("runs 7101/7102 read %d/%d times, want 0: the open phase must stop under the floor",
			result.doer.byIDAttempts[7101], result.doer.byIDAttempts[7102])
	}
	if got := result.final["open_runs_reread"]; got != 0 {
		t.Errorf("open_runs_reread = %v, want 0: the floor stop does not advance the open cursor", got)
	}
}

// CHAOS-14518 (#6802 r2 finding B): the open phase's rate floor must stop a retry. A retryable 500 that reports 100
// requests left (under the floor of 1,000 = a fifth of 5,000) must not be retried: the retry would spend a request under
// the floor, and its success headers (4,000 left) would hide the low reading from the per-run check.
func TestGitHubTestsOpenPhaseFloorRefusesTheRetryUnderTheFloor(t *testing.T) {
	result := floorRetryWalk(t, floorRetryCase{status: http.StatusInternalServerError, remaining: 100})
	result.logf(t, "R1")
	result.assertFloorStop(t, 100)
}

// Control: the same 500 with 4,000 left (above the floor) is retried as on main, and the open phase reads every run.
func TestGitHubTestsOpenPhaseFloorAboveRetriesAsMain(t *testing.T) {
	result := floorRetryWalk(t, floorRetryCase{status: http.StatusInternalServerError, remaining: 4000})
	result.logf(t, "R1c")
	if got := result.doer.byIDAttempts[7100]; got != 2 {
		t.Errorf("run 7100: %d wire attempts, want 2 (one retry, as main)", got)
	}
	if got := result.final["open_runs_reread"]; got != 3 {
		t.Errorf("open_runs_reread = %v, want 3", got)
	}
	if marker, hit := result.final["open_phase_rate_floor"]; hit {
		t.Errorf("open_phase_rate_floor = %v, want absent", marker)
	}
}

// PR-READ-6860-1 P1: a 429 under the floor still arms the shared backoff gate: the client's Penalize runs on a live
// context (the Valkey gate runs its command on it), and no provider_gate_penalize_failed line is written. Then the retry
// is refused and the open phase stops at the floor.
func TestGitHubTestsOpenPhaseFloor429PenalizesTheGateOnALiveContext(t *testing.T) {
	result := floorRetryWalk(t, floorRetryCase{status: http.StatusTooManyRequests, remaining: 100, body: `{"message":"too many requests"}`})
	result.logf(t, "P1")
	if len(result.gate.penalties) != 1 || result.gate.penalties[0] != nil {
		t.Errorf("Penalize calls (ctx.Err() each) = %v, want exactly one on a live context", result.gate.penalties)
	}
	if strings.Contains(result.logs, "provider_gate_penalize_failed") {
		t.Errorf("a provider_gate_penalize_failed line was written: the 429 did not arm the shared gate")
	}
	result.assertFloorStop(t, 100)
}

// PR-READ-6860-1 P2-b: a 403 the client classes as a rate limit (here by its message; retryable) is a retry the floor
// must stop too.
func TestGitHubTestsOpenPhaseFloor403RateLimitUnderTheFloorIsNotRetried(t *testing.T) {
	result := floorRetryWalk(t, floorRetryCase{status: http.StatusForbidden, remaining: 100,
		body: `{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again."}`})
	result.logf(t, "403")
	result.assertFloorStop(t, 100)
}

// An answer the client does not retry is not gated: a 200 or a 404 under the floor lets the run finish as on main. The
// 200's run reads its jobs (the next request of the run is not refused; its answer reports 4,000 left, so the phase goes
// on, as main). The 404 marks the row deleted, and the per-run check stops the phase before the next run (as main).
func TestGitHubTestsOpenPhaseFloorDoesNotGateANonRetryableAnswer(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		wantJobs  int
		wantRead  int
		wantFloor any
	}{
		{"200 under the floor", http.StatusOK, `{"id":7100,"name":"CI","status":"completed","conclusion":"success","created_at":"2026-06-20T00:00:00Z","run_started_at":"2026-06-20T00:00:00Z","updated_at":"2026-06-20T01:00:00Z","run_attempt":1,"event":"push","head_sha":"abc","head_branch":"main","html_url":"https://github.com/acme/api/actions/runs/7100","pull_requests":[]}`, 1, 3, nil},
		{"404 under the floor", http.StatusNotFound, `{"message":"Not Found"}`, 0, 1, int64(100)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := floorRetryWalk(t, floorRetryCase{status: tc.status, remaining: 100, body: tc.body})
			result.logf(t, "NG "+tc.name)
			if got := result.doer.byIDAttempts[7100]; got != 1 {
				t.Errorf("run 7100: %d attempts, want 1", got)
			}
			if got := result.doer.fake.jobGets[7100]; got != tc.wantJobs {
				t.Errorf("run 7100 jobs reads = %d, want %d: the run's next request is not gated", got, tc.wantJobs)
			}
			if got := result.final["open_runs_reread"]; got != tc.wantRead {
				t.Errorf("open_runs_reread = %v, want %d", got, tc.wantRead)
			}
			if marker := result.final["open_phase_rate_floor"]; marker != tc.wantFloor {
				t.Errorf("open_phase_rate_floor = %v, want %v (the per-run check, as main)", marker, tc.wantFloor)
			}
		})
	}
}

// Fail closed, as main: with no rate headers on any answer, the open phase stops before its first read (marker -1) and
// sends no by-id request.
func TestGitHubTestsOpenPhaseFloorFailsClosedOnMissingRateHeaders(t *testing.T) {
	result := floorRetryWalk(t, floorRetryCase{status: http.StatusInternalServerError, remaining: -1, noHeaders: true})
	result.logf(t, "FC")
	if marker := result.final["open_phase_rate_floor"]; marker != int64(-1) {
		t.Errorf("open_phase_rate_floor = %v, want -1", marker)
	}
	for id := 7100; id <= 7102; id++ {
		if result.doer.byIDAttempts[id] != 0 {
			t.Errorf("run %d read %d times, want 0", id, result.doer.byIDAttempts[id])
		}
	}
}

// floorListingDoer fails the FIRST attempt of every runs-listing request (runs, deep runs, artifacts, deep relist) with a
// 500 that reports 100 left, then lets the retry through to the fake.
type floorListingDoer struct {
	fake     *listingFake
	mu       sync.Mutex
	attempts map[string]int
}

func (doer *floorListingDoer) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Path == "/repos/acme/api/actions/runs" {
		doer.mu.Lock()
		key := request.URL.RawQuery
		doer.attempts[key]++
		first := doer.attempts[key] == 1
		doer.mu.Unlock()
		if first {
			header := http.Header{"Content-Type": {"application/json"}}
			header.Set("X-RateLimit-Remaining", "100")
			header.Set("X-RateLimit-Limit", "5000")
			return &http.Response{StatusCode: http.StatusInternalServerError, Header: header,
				Body: io.NopCloser(strings.NewReader(`{"message":"Server Error"}`)), Request: request}, nil
		}
	}
	return doer.fake.Do(request)
}

// The gate is off outside the open phase: every runs-listing request of the runs range, the deep runs range, the
// artifacts listing and the deep relist (a claim that holds a UTC midnight) is retried after a 500 under the floor, as on
// main, and the walk lists every run.
func TestGitHubTestsFloorGateIsOffOutsideTheOpenPhase(t *testing.T) {
	since := listingSince.Add(-30 * time.Minute)
	before := listingSince.Add(30 * time.Minute)
	claim := listingClaim(since, before)
	runs := append(makeListingRuns(1, 5, since, time.Minute), makeListingRuns(101, 5, since.Add(-3*24*time.Hour), time.Minute)...)
	for index := range runs[5:] {
		runs[5+index].updated = since.Add(10 * time.Minute)
	}
	fake := newListingFake(t, runs)
	doer := &floorListingDoer{fake: fake, attempts: map[string]int{}}
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
		func(request *http.Request) error { request.Header.Set("Authorization", "Bearer secret"); return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	var final map[string]any
	err = GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "", func(emission ChunkRouteEmission) error {
			if emission.Final {
				final = emission.Batch.Result
			}
			return nil
		})
	if err != nil || final == nil {
		t.Fatalf("walk: err=%v final=%v", err, final != nil)
	}
	deep := 0
	for query, attempts := range doer.attempts {
		values, _ := url.ParseQuery(query)
		if lo, _, _ := strings.Cut(values.Get("created"), ".."); lo != "" && lo < since.Add(-24*time.Hour).Format(time.RFC3339) {
			deep++
		}
		if attempts != 2 {
			t.Errorf("listing %q: %d attempts, want 2 (the retry is sent outside the open phase, as main)", query, attempts)
		}
	}
	t.Logf("CELL OFF listings=%d deep=%d pipeline_runs_synced=%v", len(doer.attempts), deep, final["pipeline_runs_synced"])
	if len(doer.attempts) < 3 || deep == 0 {
		t.Errorf("listings=%d deep=%d, want the runs, artifacts and deep listings all walked", len(doer.attempts), deep)
	}
}
