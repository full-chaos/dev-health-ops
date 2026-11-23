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
	link             string // a Link header value, "" none
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

// Contract case: a retryable answer (5xx, 429, a 403 rate limit) whose OWN headers cannot show headroom (no rate headers,
// or a remaining count without a limit) after an earlier reading above the floor: the retry is not sent (fail closed on
// the current answer), and the phase stops at the floor with the marker -1 (headroom unknown). At each of the three sites.
func TestGitHubTestsOpenPhaseFloorFailsClosedOnTheCurrentAnswer(t *testing.T) {
	shapes := []struct {
		name   string
		answer floorAnswer
	}{
		{"500 without rate headers", floorAnswer{status: http.StatusInternalServerError, body: `{"message":"Server Error"}`}},
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
	result := floorSiteWalk(t, map[string]map[int]floorAnswer{floorSiteRun: {1: low}}, GitHubTestsRouteHandler{}, true)
	t.Logf("CELL REL release_failed=%v err=%v marker=%v attempts=%v", result.failedRl, result.err, result.marker(), result.doer.attempts)
	if !result.failedRl {
		t.Fatal("premise: the refused call's release did not fail")
	}
	if result.err != nil {
		t.Fatalf("walk: %v, want the floor stop", result.err)
	}
	if result.marker() != int64(100) || result.doer.attempts[floorSiteRun] != 1 {
		t.Errorf("marker=%v attempts=%d, want the floor stop (100, 1 attempt)", result.marker(), result.doer.attempts[floorSiteRun])
	}
	if !strings.Contains(result.metrics, `dev_health_provider_budget_release_errors_total{provider="github"} 1`) {
		t.Errorf("the release failure was not counted:\n%s", result.metrics)
	}
}

// Contract case: the branch protection read of run 7100 is refused at the floor after its jobs read stopped at a per-run
// bound (here the jobs page budget, on short pages). The run is not emitted, so no truncation of it is published: no
// run_jobs observation in the final result and no per-run truncation log line.
func TestGitHubTestsOpenPhaseFloorRefusalPublishesNoTruncationOfAnUnemittedRun(t *testing.T) {
	jobs := map[int]floorAnswer{}
	for page := 1; page <= 6; page++ {
		jobs[page] = floorAnswer{status: http.StatusOK, remaining: "4000", limit: "5000", body: `{"jobs":[` + floorJob(page) + `]}`,
			link: fmt.Sprintf(`<https://api.github.com/repos/acme/api/actions/runs/7100/jobs?page=%d&per_page=100>; rel="next"`, page+1)}
	}
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
