package providersync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// Contract cases of the open-phase rate floor (CHAOS-14518) at the three request sites of one re-read run: the run by id,
// its jobs pages and its branch protection read. Run 7100 is a pull-request run (so the branch protection of "main" is
// read); runs 7101 and 7102 follow. Every other answer reports 4,000 left of 5,000.

const (
	floorSiteRun    = "run"
	floorSiteJobs   = "jobs"
	floorSitePolicy = "policy"
)

// floorAnswer is one scripted answer: status, the rate headers ("" omits one) and the body.
type floorAnswer struct {
	status           int
	remaining, limit string
	body             string
	link             string            // a Link header value, "" none
	raw              map[string]string // extra headers set as given (an empty value stays present)
}

func (answer floorAnswer) response(request *http.Request) *http.Response {
	header := http.Header{"Content-Type": {"application/json"}}
	if answer.remaining != "" {
		header.Set("X-RateLimit-Remaining", answer.remaining)
	}
	if answer.limit != "" {
		header.Set("X-RateLimit-Limit", answer.limit)
	}
	if answer.link != "" {
		header.Set("Link", answer.link)
	}
	for name, value := range answer.raw {
		header[http.CanonicalHeaderKey(name)] = []string{value}
	}
	return &http.Response{StatusCode: answer.status, Header: header, Body: io.NopCloser(strings.NewReader(answer.body)), Request: request}
}

// floorSiteDoer answers the three sites of run 7100 from a script (site -> attempt number -> answer; a missing entry is the
// default success) and the rest from the listing fake. It counts every wire attempt it is asked for.
type floorSiteDoer struct {
	fake     *listingFake
	script   map[string]map[int]floorAnswer
	mu       sync.Mutex
	attempts map[string]int
	total    int
	lowSent  bool // a scripted answer under the floor (or without a usable pair) was sent
}

const floorPullRun = `{"id":7100,"name":"CI","status":"completed","conclusion":"success","created_at":"2026-06-20T00:00:00Z","run_started_at":"2026-06-20T00:00:00Z","updated_at":"2026-06-20T01:00:00Z","run_attempt":1,"event":"pull_request","head_sha":"abc","head_branch":"feature","html_url":"https://github.com/acme/api/actions/runs/7100","pull_requests":[{"number":5,"base":{"ref":"main"},"head":{"ref":"feature"}}]}`

func floorJob(id int) string {
	return fmt.Sprintf(`{"id":%d,"run_id":7100,"name":"job-%d","status":"completed","conclusion":"success","started_at":"2026-06-20T00:00:00Z","completed_at":"2026-06-20T00:10:00Z","html_url":"https://github.com/acme/api/actions/runs/7100/job/%d"}`, id, id, id)
}

func (doer *floorSiteDoer) Do(request *http.Request) (*http.Response, error) {
	doer.mu.Lock()
	doer.total++
	site := ""
	switch path := request.URL.Path; {
	case path == "/repos/acme/api/actions/runs/7100":
		site = floorSiteRun
	case path == "/repos/acme/api/actions/runs/7100/jobs":
		site = floorSiteJobs
	case path == "/repos/acme/api/branches/main/protection/required_status_checks":
		site = floorSitePolicy
	}
	var answer floorAnswer
	scripted := false
	if site != "" {
		doer.attempts[site]++
		answer, scripted = doer.script[site][doer.attempts[site]]
		if scripted && (answer.remaining == "" || answer.limit == "" || answer.remaining == "100") {
			doer.lowSent = true
		}
	}
	doer.mu.Unlock()
	if scripted {
		return answer.response(request), nil
	}
	ok := floorAnswer{status: http.StatusOK, remaining: "4000", limit: "5000"}
	switch site {
	case floorSiteRun:
		ok.body = floorPullRun
		return ok.response(request), nil
	case floorSitePolicy:
		ok.body = `{"contexts":[],"checks":[]}`
		return ok.response(request), nil
	}
	return doer.fake.Do(request)
}

// floorReleaseBudget is a request budget whose release fails once, for the first call that ends after a low answer was
// sent (the refused call).
type floorReleaseBudget struct {
	doer   *floorSiteDoer
	mu     sync.Mutex
	failed bool
}

type floorReservation struct{ budget *floorReleaseBudget }

func (budget *floorReleaseBudget) Acquire(context.Context, providerfoundation.BudgetKey) (providerfoundation.Reservation, error) {
	return floorReservation{budget: budget}, nil
}

func (reservation floorReservation) Release(context.Context) error {
	budget := reservation.budget
	budget.doer.mu.Lock()
	low := budget.doer.lowSent
	budget.doer.mu.Unlock()
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if low && !budget.failed {
		budget.failed = true
		return errors.New("release refused")
	}
	return nil
}

type floorSiteResult struct {
	final    CompleteRouteBatch
	err      error
	doer     *floorSiteDoer
	logs     string
	metrics  string
	failedRl bool
}

// floorSiteWalk runs the open phase over runs 7100..7102 with a client that retries once and holds a shared gate.
func floorSiteWalk(t *testing.T, script map[string]map[int]floorAnswer, handler GitHubTestsRouteHandler, budget bool) floorSiteResult {
	t.Helper()
	repoID := listingRepoID(t)
	stale := listingSince.Add(-30 * 24 * time.Hour)
	fake := newListingFake(t, makeListingRuns(1, 2, listingSince, time.Minute))
	var rows []githubTestsPipelineRow
	for index := 0; index < 3; index++ {
		id := 7100 + index
		started := stale.Add(time.Duration(index) * time.Minute)
		rows = append(rows, openRow(repoID, fmt.Sprint(id), started, "running"))
		fake.byID[id] = listingRun{id: id, created: started, updated: started.Add(time.Hour), status: "completed", conclusion: "success"}
	}
	doer := &floorSiteDoer{fake: fake, script: script, attempts: map[string]int{}}
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(doer),
		func(request *http.Request) error { request.Header.Set("Authorization", "Bearer secret"); return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	client.Gate = &floorRecordingGate{}
	client.Metrics = providerfoundation.NewMetrics()
	releases := &floorReleaseBudget{doer: doer}
	if budget {
		client.Budget = releases
	}
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(previous)
	handler.OpenRuns = &fakeOpenRuns{rows: rows}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	result := floorSiteResult{doer: doer}
	result.err = handler.CollectChunks(ctx, openWindowClaim(), providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "", func(emission ChunkRouteEmission) error {
			if emission.Final {
				result.final = emission.Batch
			}
			return nil
		})
	var metrics bytes.Buffer
	_ = client.Metrics.WritePrometheus(&metrics)
	result.logs, result.metrics, result.failedRl = logged.String(), metrics.String(), releases.failed
	return result
}

func (result floorSiteResult) marker() any { return result.final.Result["open_phase_rate_floor"] }

// Contract case: a 429 or a 403 rate limit without rate headers, or a 5xx whose rate headers do not carry both the
// remaining count and the limit, after an earlier reading above the floor: the retry is not sent (fail closed on the
// current answer), and the phase stops at the floor with the marker -1 (headroom unknown). At each of the three sites.
// A 5xx with no rate header at all keeps main's path (TestGitHubTestsOpenPhaseFloorHeaderless5xxKeepsTheTransientPath).
func TestGitHubTestsOpenPhaseFloorFailsClosedOnTheCurrentAnswer(t *testing.T) {
	shapes := []struct {
		name   string
		answer floorAnswer
	}{
		{"429 without rate headers", floorAnswer{status: http.StatusTooManyRequests, body: `{"message":"too many requests"}`}},
		{"403 rate limit without rate headers", floorAnswer{status: http.StatusForbidden, body: `{"message":"You have exceeded a secondary rate limit."}`}},
		{"500 with 100 left and no limit", floorAnswer{status: http.StatusInternalServerError, remaining: "100", body: `{"message":"Server Error"}`}},
	}
	for _, site := range []string{floorSiteRun, floorSiteJobs, floorSitePolicy} {
		for _, shape := range shapes {
			t.Run(site+"/"+shape.name, func(t *testing.T) {
				result := floorSiteWalk(t, map[string]map[int]floorAnswer{site: {1: shape.answer}}, GitHubTestsRouteHandler{}, false)
				t.Logf("CELL FC-CUR %s/%s attempts=%v err=%v marker=%v", site, shape.name, result.doer.attempts, result.err, result.marker())
				if result.err != nil {
					t.Fatalf("walk: %v", result.err)
				}
				if got := result.doer.attempts[site]; got != 1 {
					t.Errorf("%s: %d wire attempts, want 1: the retry was sent although the answer showed no headroom", site, got)
				}
				if marker := result.marker(); marker != int64(-1) {
					t.Errorf("open_phase_rate_floor = %v, want -1 (headroom unknown on the current answer)", marker)
				}
			})
		}
	}
}

// Contract case: a refused jobs retry, at the first jobs page and at a later one: the final evidence counts every wire
// attempt the unit sent.
func TestGitHubTestsOpenPhaseFloorRefusalKeepsTheRequestCount(t *testing.T) {
	low := floorAnswer{status: http.StatusInternalServerError, remaining: "100", limit: "5000", body: `{"message":"Server Error"}`}
	for _, tc := range []struct {
		name   string
		script map[int]floorAnswer
	}{
		{"first jobs page", map[int]floorAnswer{1: low}},
		{"second jobs page", map[int]floorAnswer{
			1: {status: http.StatusOK, remaining: "4000", limit: "5000", body: `{"total_count":2,"jobs":[` + floorJob(1) + `]}`,
				link: `<https://api.github.com/repos/acme/api/actions/runs/7100/jobs?page=2&per_page=100>; rel="next"`},
			2: low,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := floorSiteWalk(t, map[string]map[int]floorAnswer{floorSiteJobs: tc.script}, GitHubTestsRouteHandler{}, false)
			t.Logf("CELL REQ %s attempts=%v total_wire=%d evidence_requests=%d marker=%v err=%v", tc.name, result.doer.attempts,
				result.doer.total, result.final.Evidence.Requests, result.marker(), result.err)
			if result.err != nil {
				t.Fatalf("walk: %v", result.err)
			}
			if result.marker() != int64(100) {
				t.Errorf("open_phase_rate_floor = %v, want 100", result.marker())
			}
			if got, want := result.final.Evidence.Requests, result.doer.total; got != want {
				t.Errorf("final evidence Requests = %d, want %d: every wire attempt the unit sent", got, want)
			}
		})
	}
}

// Contract case: a floor refusal whose call also fails to release its request-budget reservation: the floor stop decides
// the path, and the release failure is still counted by the client's existing counter
// (dev_health_provider_budget_release_errors_total).
func TestGitHubTestsOpenPhaseFloorRefusalStillCountsABudgetReleaseFailure(t *testing.T) {
	low := floorAnswer{status: http.StatusInternalServerError, remaining: "100", limit: "5000", body: `{"message":"Server Error"}`}
	for _, site := range []string{floorSiteRun, floorSiteJobs, floorSitePolicy} {
		t.Run(site, func(t *testing.T) {
			result := floorSiteWalk(t, map[string]map[int]floorAnswer{site: {1: low}}, GitHubTestsRouteHandler{}, true)
			t.Logf("CELL REL %s release_failed=%v err=%v marker=%v attempts=%v", site, result.failedRl, result.err, result.marker(), result.doer.attempts)
			if !result.failedRl {
				t.Fatal("premise: the refused call's release did not fail")
			}
			if result.err != nil {
				t.Fatalf("walk: %v, want the floor stop", result.err)
			}
			if result.marker() != int64(100) || result.doer.attempts[site] != 1 {
				t.Errorf("marker=%v attempts=%d, want the floor stop (100, 1 attempt)", result.marker(), result.doer.attempts[site])
			}
			if !strings.Contains(result.metrics, `dev_health_provider_budget_release_errors_total{provider="github"} 1`) {
				t.Errorf("the release failure was not counted:\n%s", result.metrics)
			}
		})
	}
}

// Contract case: the branch protection read of run 7100 is refused at the floor after its jobs read stopped at a per-run
// bound (here the jobs page budget, on short pages). The run is not emitted, so no truncation of it is published: no
// run_jobs observation in the final result and no per-run truncation log line.
// floorShortJobPages is six jobs pages of one job each, each with a next link: the jobs read stops at a page budget of 6.
func floorShortJobPages() map[int]floorAnswer {
	jobs := map[int]floorAnswer{}
	for page := 1; page <= 6; page++ {
		jobs[page] = floorAnswer{status: http.StatusOK, remaining: "4000", limit: "5000", body: `{"jobs":[` + floorJob(page) + `]}`,
			link: fmt.Sprintf(`<https://api.github.com/repos/acme/api/actions/runs/7100/jobs?page=%d&per_page=100>; rel="next"`, page+1)}
	}
	return jobs
}

// Positive control of the next cell: the same jobs truncation, with a branch protection read that succeeds. Run 7100 is
// emitted, and its truncation is published: the run_jobs observation in the final result and the per-run truncation line.
func TestGitHubTestsOpenPhaseFloorPublishesTheTruncationOfAnEmittedRun(t *testing.T) {
	result := floorSiteWalk(t, map[string]map[int]floorAnswer{floorSiteJobs: floorShortJobPages()}, GitHubTestsRouteHandler{MaxJobPages: 6}, false)
	incomplete, _ := DecodeGitHubTestsIncomplete(result.final.Result["incomplete"])
	t.Logf("CELL TRUNC+ attempts=%v marker=%v incomplete=%v reread=%v err=%v", result.doer.attempts, result.marker(), incomplete, result.final.Result["open_runs_reread"], result.err)
	if result.err != nil {
		t.Fatalf("walk: %v", result.err)
	}
	published := false
	for _, observation := range incomplete {
		published = published || observation.Component == githubTestsRunJobsComponent && observation.Cause == githubTestsPerRunPageBudgetCause
	}
	if !published || !strings.Contains(result.logs, "provider per-run item cap reached") {
		t.Errorf("run 7100 was emitted (reread=%v) but its truncation is not published: incomplete=%v logged=%v",
			result.final.Result["open_runs_reread"], incomplete, strings.Contains(result.logs, "provider per-run item cap reached"))
	}
}

func TestGitHubTestsOpenPhaseFloorRefusalPublishesNoTruncationOfAnUnemittedRun(t *testing.T) {
	jobs := floorShortJobPages()
	low := floorAnswer{status: http.StatusInternalServerError, remaining: "100", limit: "5000", body: `{"message":"Server Error"}`}
	result := floorSiteWalk(t, map[string]map[int]floorAnswer{floorSiteJobs: jobs, floorSitePolicy: {1: low}},
		GitHubTestsRouteHandler{MaxJobPages: 6}, false)
	incomplete, _ := DecodeGitHubTestsIncomplete(result.final.Result["incomplete"])
	t.Logf("CELL TRUNC attempts=%v marker=%v incomplete=%v err=%v", result.doer.attempts, result.marker(), incomplete, result.err)
	if result.err != nil {
		t.Fatalf("walk: %v", result.err)
	}
	if result.doer.attempts[floorSiteJobs] != 6 || result.doer.attempts[floorSitePolicy] != 1 || result.marker() != int64(100) {
		t.Fatalf("premise: jobs=%d policy=%d marker=%v, want 6 short jobs pages, one refused policy read and the floor stop",
			result.doer.attempts[floorSiteJobs], result.doer.attempts[floorSitePolicy], result.marker())
	}
	for _, observation := range incomplete {
		if observation.Component == githubTestsRunJobsComponent {
			t.Errorf("final result publishes %+v for run 7100, which was not emitted", observation)
		}
	}
	if strings.Contains(result.logs, "provider per-run item cap reached") {
		t.Errorf("a per-run truncation line was logged for run 7100, which was not emitted")
	}
}

// Contract case: a 5xx with no rate header at all keeps main's path, at the oldest open run and at its branch protection
// read: the answer is retried; at the run a last failure skips the run as a transient, at the branch protection read the
// policy is unknown and the run is written. The later open runs (7101, 7102) are read, and there is no floor marker.
func TestGitHubTestsOpenPhaseFloorHeaderless5xxKeepsTheTransientPath(t *testing.T) {
	headerless := floorAnswer{status: http.StatusBadGateway, body: `{"message":"Bad Gateway"}`}
	for _, tc := range []struct {
		site     string
		wantRead int
	}{
		{floorSiteRun, 2},    // 7100 skipped, 7101 and 7102 re-read
		{floorSitePolicy, 3}, // 7100 written with an unknown policy, then 7101 and 7102
	} {
		t.Run(tc.site, func(t *testing.T) {
			result := floorSiteWalk(t, map[string]map[int]floorAnswer{tc.site: {1: headerless, 2: headerless}}, GitHubTestsRouteHandler{}, false)
			later := result.doer.fake.runGets
			t.Logf("CELL H5XX %s attempts=%v later_reads=%v reread=%v skipped=%v marker=%v err=%v", tc.site, result.doer.attempts, later,
				result.final.Result["open_runs_reread"], result.final.Result["open_runs_skipped"], result.marker(), result.err)
			if result.err != nil {
				t.Fatalf("walk: %v", result.err)
			}
			if got := result.doer.attempts[tc.site]; got != 2 {
				t.Errorf("%s: %d wire attempts, want 2 (retried, as main)", tc.site, got)
			}
			if marker, hit := result.final.Result["open_phase_rate_floor"]; hit {
				t.Errorf("open_phase_rate_floor = %v, want absent", marker)
			}
			read := map[int]bool{}
			for _, id := range later {
				read[id] = true
			}
			if !read[7101] || !read[7102] {
				t.Errorf("later open runs read = %v, want 7101 and 7102: the header-less 5xx must not end the phase", later)
			}
			if got := result.final.Result["open_runs_reread"]; got != tc.wantRead {
				t.Errorf("open_runs_reread = %v, want %d", got, tc.wantRead)
			}
		})
	}
}

// Contract case: the rate headers the client's parser reads (parseRequestRateLimit: X-RateLimit-* and the RateLimit-*
// aliases) count for the floor at every site. A 503 with the alias pair under the floor is not retried (marker 100); a
// 503 with only an alias remaining count is not a header-less answer: it fails closed (marker -1). (A present but empty
// header is no header to the parser: TestGitHubTestsOpenPhaseFloorEntryTable, row 503/empty.)
func TestGitHubTestsOpenPhaseFloorReadsEveryRateHeaderName(t *testing.T) {
	shapes := []struct {
		name   string
		raw    map[string]string
		marker int64
	}{
		{"alias pair under the floor", map[string]string{"RateLimit-Remaining": "100", "RateLimit-Limit": "5000"}, 100},
		{"alias remaining only", map[string]string{"RateLimit-Remaining": "100"}, -1},
	}
	for _, site := range []string{floorSiteRun, floorSiteJobs, floorSitePolicy} {
		for _, shape := range shapes {
			t.Run(site+"/"+shape.name, func(t *testing.T) {
				answer := floorAnswer{status: http.StatusServiceUnavailable, body: `{"message":"Service Unavailable"}`, raw: shape.raw}
				result := floorSiteWalk(t, map[string]map[int]floorAnswer{site: {1: answer, 2: answer}}, GitHubTestsRouteHandler{}, false)
				t.Logf("CELL NAMES %s/%s attempts=%v marker=%v err=%v", site, shape.name, result.doer.attempts, result.marker(), result.err)
				if result.err != nil {
					t.Fatalf("walk: %v", result.err)
				}
				if got := result.doer.attempts[site]; got != 1 {
					t.Errorf("%s: %d wire attempts, want 1: the retry was sent although the answer's rate headers show no headroom", site, got)
				}
				if marker := result.marker(); marker != shape.marker {
					t.Errorf("open_phase_rate_floor = %v, want %d", marker, shape.marker)
				}
			})
		}
	}
}

// Contract case: a 403 the client does not class as a rate limit (a permission denial without rate headers) on the
// branch protection read of run 7100 does not stop the floor: run 7100 is written with an unknown policy (as main), and
// the later open runs 7101 and 7102 are read.
func TestGitHubTestsOpenPhaseFloorPermissionDenial403DoesNotStopThePhase(t *testing.T) {
	denial := floorAnswer{status: http.StatusForbidden, body: `{"message":"Resource not accessible by integration"}`}
	result := floorSiteWalk(t, map[string]map[int]floorAnswer{floorSitePolicy: {1: denial}}, GitHubTestsRouteHandler{}, false)
	t.Logf("CELL DENY403 attempts=%v later_reads=%v reread=%v marker=%v err=%v", result.doer.attempts, result.doer.fake.runGets,
		result.final.Result["open_runs_reread"], result.marker(), result.err)
	if result.err != nil {
		t.Fatalf("walk: %v", result.err)
	}
	if got := result.doer.attempts[floorSitePolicy]; got != 1 {
		t.Errorf("policy: %d attempts, want 1 (a 403 permission denial is not retried)", got)
	}
	if marker, hit := result.final.Result["open_phase_rate_floor"]; hit {
		t.Errorf("open_phase_rate_floor = %v, want absent: a permission denial is not a rate limit", marker)
	}
	read := map[int]bool{}
	for _, id := range result.doer.fake.runGets {
		read[id] = true
	}
	if !read[7101] || !read[7102] {
		t.Errorf("later open runs read = %v, want 7101 and 7102", result.doer.fake.runGets)
	}
	if got := result.final.Result["open_runs_reread"]; got != 3 {
		t.Errorf("open_runs_reread = %v, want 3 (run 7100 written with an unknown policy, then 7101 and 7102)", got)
	}
}

// floorEntryRow is one row of the gate's entry table: an answer at one site of run 7100 (the run by id, or its branch
// protection read), first attempt only; a retry gets a healthy answer.
type floorEntryRow struct {
	site, class, shape string
	answer             floorAnswer
	action             string // see TestGitHubTestsOpenPhaseFloorEntryTable
	marker             any
	mainSame           bool // the row's action equals main (and the 487ea61b1367 tip)
}

// floorEntryRows is the table {site} x {answer class} x {rate header shape}. One source answers each question: the
// headroom is the client's parser's reading of the answer's own headers (aliases included; an empty value is no value);
// the gate arms only on an answer the client retries (its classification: 5xx, 429, and a 403 the client classes a rate
// limit, here by its body); the floor is strict (remaining < floor, as the per-run check); a 5xx with no rate header
// keeps main's transient path.
func floorEntryRows() []floorEntryRow {
	classes := []struct {
		name      string
		status    int
		body      map[string]string // site -> body
		retried   bool
		transient bool
	}{
		{"200", http.StatusOK, map[string]string{floorSiteRun: floorPullRun, floorSitePolicy: `{"contexts":[],"checks":[]}`}, false, false},
		{"404", http.StatusNotFound, nil, false, false},
		{"403 permission", http.StatusForbidden, map[string]string{"": `{"message":"Resource not accessible by integration"}`}, false, false},
		{"403 rate limit", http.StatusForbidden, map[string]string{"": `{"message":"You have exceeded a secondary rate limit."}`}, true, false},
		{"429", http.StatusTooManyRequests, nil, true, false},
		{"5xx", http.StatusServiceUnavailable, nil, true, true},
	}
	shapes := []struct {
		name    string
		raw     map[string]string
		reading string // "absent", "unknown", "ok", "low", "floor"
	}{
		{"none", nil, "absent"},
		{"canonical", map[string]string{"X-RateLimit-Remaining": "4000", "X-RateLimit-Limit": "5000"}, "ok"},
		{"alias", map[string]string{"RateLimit-Remaining": "4000", "RateLimit-Limit": "5000"}, "ok"},
		{"alias low", map[string]string{"RateLimit-Remaining": "100", "RateLimit-Limit": "5000"}, "low"},
		{"one of two", map[string]string{"X-RateLimit-Remaining": "100"}, "unknown"},
		{"empty", map[string]string{"X-RateLimit-Remaining": "", "X-RateLimit-Limit": ""}, "absent"},
		{"low", map[string]string{"X-RateLimit-Remaining": "100", "X-RateLimit-Limit": "5000"}, "low"},
		{"at the floor", map[string]string{"X-RateLimit-Remaining": "1000", "X-RateLimit-Limit": "5000"}, "floor"},
	}
	var rows []floorEntryRow
	for _, site := range []string{floorSiteRun, floorSitePolicy} {
		for _, class := range classes {
			body := class.body[site]
			if body == "" {
				body = class.body[""]
			}
			if body == "" {
				body = `{"message":"error"}`
			}
			for _, shape := range shapes {
				row := floorEntryRow{site: site, class: class.name, shape: shape.name,
					answer: floorAnswer{status: class.status, body: body, raw: shape.raw}, mainSame: true}
				lastOfRun := site == floorSitePolicy || class.status != http.StatusOK // no later request of run 7100 follows
				switch {
				case site == floorSiteRun && class.name == "403 permission":
					row.action = "unit fails"
				case !class.retried && shape.reading == "low" && lastOfRun:
					row.action, row.marker = "not gated, stop before next run", int64(100)
				case !class.retried:
					row.action = "not gated"
				case class.transient && shape.reading == "absent":
					row.action = "retry sent"
				case shape.reading == "low":
					row.action, row.marker, row.mainSame = "refused", int64(100), false
				case shape.reading == "absent" || shape.reading == "unknown":
					row.action, row.marker, row.mainSame = "refused", int64(-1), false
				default:
					row.action = "retry sent"
				}
				rows = append(rows, row)
			}
		}
	}
	return rows
}

// The entry table of the open-phase floor gate. Each row checks the wire attempts at the site, the floor marker, and
// whether the later open runs (7101, 7102) are read:
//   - "retry sent": 2 attempts, no marker, later runs read;
//   - "refused": 1 attempt, the marker, no later run;
//   - "not gated": 1 attempt, no marker, later runs read (at the branch protection read: run 7100 written with the policy
//     the answer gives, unknown for a 404 or 403);
//   - "not gated, stop before next run": 1 attempt, the per-run check's marker, no later run;
//   - "unit fails": a 403 permission denial of the run read fails the unit, as main.
func TestGitHubTestsOpenPhaseFloorEntryTable(t *testing.T) {
	for _, row := range floorEntryRows() {
		t.Run(row.site+"/"+row.class+"/"+row.shape, func(t *testing.T) {
			result := floorSiteWalk(t, map[string]map[int]floorAnswer{row.site: {1: row.answer}}, GitHubTestsRouteHandler{}, false)
			read := map[int]bool{}
			for _, id := range result.doer.fake.runGets {
				read[id] = true
			}
			later := read[7101] && read[7102]
			marker := result.final.Result["open_phase_rate_floor"]
			t.Logf("ROW %s/%s/%s want=%q main_same=%v attempts=%d marker=%v later_read=%v reread=%v err=%v", row.site, row.class,
				row.shape, row.action, row.mainSame, result.doer.attempts[row.site], marker, later, result.final.Result["open_runs_reread"], result.err)
			if row.action == "unit fails" {
				if result.err == nil || !strings.Contains(result.err.Error(), "authentication") {
					t.Errorf("unit fails: err = %v, want the authentication failure", result.err)
				}
				return
			}
			if result.err != nil {
				t.Fatalf("walk: %v", result.err)
			}
			wantAttempts, wantLater := 1, true
			switch row.action {
			case "retry sent":
				wantAttempts = 2
			case "refused", "not gated, stop before next run":
				wantLater = false
			}
			if got := result.doer.attempts[row.site]; got != wantAttempts {
				t.Errorf("%s: %d wire attempts at %s, want %d", row.action, got, row.site, wantAttempts)
			}
			if marker != row.marker {
				t.Errorf("%s: open_phase_rate_floor = %v, want %v", row.action, marker, row.marker)
			}
			if later != wantLater {
				t.Errorf("%s: later open runs read = %v, want %v", row.action, later, wantLater)
			}
			if row.action == "not gated" && row.site == floorSitePolicy && result.final.Result["open_runs_reread"] != 3 {
				t.Errorf("not gated: open_runs_reread = %v, want 3 (run 7100 written, then 7101 and 7102)", result.final.Result["open_runs_reread"])
			}
		})
	}
}
