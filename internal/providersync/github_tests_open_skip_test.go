package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// openSkipPoisonBody is the provider's error text. It must never reach a log attribute.
const openSkipPoisonBody = `{"message":"poison-body-text"}`

// openSkipReply is one scripted answer to a by-id run read or a jobs page. A zero status and no err hands the request
// to the listing fake, so a script can turn a failing run into a good one on a later hit.
type openSkipReply struct {
	status int
	header map[string]string
	body   string
	err    error
}

// openSkipDoer is the listing fake with scripted answers per run id for the by-id read and the jobs page.
type openSkipDoer struct {
	inner   *listingFake
	mu      sync.Mutex
	runs    map[int][]openSkipReply
	jobs    map[int][]openSkipReply
	runHits map[int]int
	jobHits map[int]int
	cancel  context.CancelFunc
	// repoStatus, when set, answers the repository read with that status; repoName replaces its full_name.
	repoStatus int
	repoName   string
}

func (doer *openSkipDoer) setRepoStatus(status int) {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	doer.repoStatus = status
}

func newOpenSkipDoer(inner *listingFake) *openSkipDoer {
	return &openSkipDoer{inner: inner, runs: map[int][]openSkipReply{}, jobs: map[int][]openSkipReply{},
		runHits: map[int]int{}, jobHits: map[int]int{}}
}

func (doer *openSkipDoer) Do(request *http.Request) (*http.Response, error) {
	const prefix = "/repos/acme/api/actions/runs/"
	if request.URL.Path == "/repos/acme/api" {
		doer.mu.Lock()
		status, name := doer.repoStatus, doer.repoName
		doer.mu.Unlock()
		if status != 0 {
			header := http.Header{"Content-Type": {"application/json"}, "X-Ratelimit-Remaining": {"4000"}, "X-Ratelimit-Limit": {"5000"}}
			return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(openSkipPoisonBody)), Request: request}, nil
		}
		if name != "" {
			response, err := doer.inner.Do(request)
			if err != nil {
				return nil, err
			}
			var repo map[string]any
			if err := json.NewDecoder(response.Body).Decode(&repo); err != nil {
				return nil, err
			}
			repo["full_name"] = name
			encoded, err := json.Marshal(repo)
			if err != nil {
				return nil, err
			}
			response.Body = io.NopCloser(bytes.NewReader(encoded))
			response.ContentLength = int64(len(encoded))
			response.Header.Del("Content-Length")
			return response, nil
		}
	}
	if rest, ok := strings.CutPrefix(request.URL.Path, prefix); ok {
		script, hits := doer.runs, doer.runHits
		if trimmed, isJobs := strings.CutSuffix(rest, "/jobs"); isJobs {
			rest, script, hits = trimmed, doer.jobs, doer.jobHits
		}
		if id, err := strconv.Atoi(rest); err == nil {
			doer.mu.Lock()
			replies := script[id]
			hit := hits[id]
			hits[id]++
			doer.mu.Unlock()
			if len(replies) > 0 {
				reply := replies[min(hit, len(replies)-1)]
				if reply.err != nil {
					if doer.cancel != nil && errors.Is(reply.err, context.Canceled) {
						doer.cancel()
					}
					return nil, reply.err
				}
				if reply.status != 0 {
					header := http.Header{"Content-Type": {"application/json"}, "X-Ratelimit-Remaining": {"4000"}, "X-Ratelimit-Limit": {"5000"}}
					for key, value := range reply.header {
						header.Set(key, value)
					}
					return &http.Response{StatusCode: reply.status, Header: header, Body: io.NopCloser(strings.NewReader(reply.body)), Request: request}, nil
				}
			}
		}
	}
	return doer.inner.Do(request)
}

func (doer *openSkipDoer) hitsOf(id int) int {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	return doer.runHits[id]
}

type openSkipWalk struct {
	pipelines []githubTestsPipelineRow
	final     CompleteRouteBatch
	finalSeen bool
	err       error
	metrics   string
}

func (walk openSkipWalk) rows() map[string]githubTestsPipelineRow {
	out := map[string]githubTestsPipelineRow{}
	for _, row := range walk.pipelines {
		out[row.RunID] = row
	}
	return out
}

func (walk openSkipWalk) result(key string) string {
	value, found := walk.final.Result[key]
	if !found {
		return "<absent>"
	}
	return fmt.Sprint(value)
}

// walkOpenSkip drives one unit (a new generation, no resume cursor) to its final emission or its error. maxChunks > 0
// yields after that many chunks of each invocation and resumes from the last cursor, as the executor's attempt bound
// does. A fakeOpenRuns store follows the written rows as ClickHouse would.
func walkOpenSkip(t *testing.T, handler GitHubTestsRouteHandler, claim Claim, doer *openSkipDoer, maxChunks int) openSkipWalk {
	t.Helper()
	metrics := providerfoundation.NewMetrics()
	client := githubTestsClient(t, fakehttp.Client(doer))
	client.Metrics = metrics
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	doer.cancel = cancel
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	walk := openSkipWalk{}
	stop := errors.New("yield")
	resume := ""
	for pass := 1; pass <= 100000; pass++ {
		emitted, last := 0, resume
		err := handler.CollectChunks(ctx, claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				// The executor tells the route of each chunk it committed; every emission this walk accepts counts as one.
				defer handler.ObserveCommittedChunk(claim, PreparedProviderChunk{CursorAfter: emission.CursorAfter}, metrics)
				for _, effect := range emission.Batch.Effects {
					if effect.Destination != "ci_pipeline_runs" {
						continue
					}
					rows, decodeErr := decodeEffectRows[githubTestsPipelineRow](effect)
					if decodeErr != nil {
						t.Fatal(decodeErr)
					}
					walk.pipelines = append(walk.pipelines, rows...)
					if store, ok := handler.OpenRuns.(*fakeOpenRuns); ok {
						store.finalize(rows)
					}
				}
				if emission.Final {
					walk.final, walk.finalSeen = emission.Batch, true
					return nil
				}
				emitted++
				if maxChunks > 0 && emitted >= maxChunks {
					return stop
				}
				return nil
			})
		if errors.Is(err, stop) && !walk.finalSeen {
			resume = last
			continue
		}
		walk.err = err
		var rendered bytes.Buffer
		if renderErr := metrics.WritePrometheus(&rendered); renderErr != nil {
			t.Fatal(renderErr)
		}
		walk.metrics = rendered.String()
		return walk
	}
	t.Fatal("the walk never ended")
	return walk
}

// openSkipStore holds count stored open rows of the repository from id first, one a minute from 40 days before the
// window; GitHub reports every one completed.
func openSkipStore(repoID string, fake *listingFake, first, count int) *fakeOpenRuns {
	stale := listingSince.Add(-40 * 24 * time.Hour)
	reader := &fakeOpenRuns{}
	for index := 0; index < count; index++ {
		id := first + index
		started := stale.Add(time.Duration(index) * time.Minute)
		reader.rows = append(reader.rows, openRow(repoID, strconv.Itoa(id), started, "running"))
		fake.byID[id] = listingRun{id: id, created: started, updated: started.Add(time.Hour), status: "completed", conclusion: "success"}
	}
	return reader
}

// openSkipLines are the WARN records that name a skipped run (the run attribute) with a reason.
func openSkipLines(records []slog.Record) []map[string]any {
	var out []map[string]any
	for _, record := range records {
		attrs := membershipLogAttrs(record)
		if _, named := attrs["run"]; !named || record.Level != slog.LevelWarn {
			continue
		}
		if _, hasReason := attrs["reason"]; hasReason {
			out = append(out, attrs)
		}
	}
	return out
}

func assertNoBodyInLogs(t *testing.T, records []slog.Record) {
	t.Helper()
	for _, record := range records {
		if strings.Contains(record.Message, "poison-body-text") {
			t.Fatalf("a log message carries the provider's error text: %q", record.Message)
		}
		for key, value := range membershipLogAttrs(record) {
			if strings.Contains(fmt.Sprint(value), "poison-body-text") {
				t.Fatalf("log attribute %s carries the provider's error text: %v", key, value)
			}
		}
	}
}

func openSkipCounterLine(reason string, count int) string {
	return fmt.Sprintf(`dev_health_provider_open_run_skipped_total{provider="github",dataset="tests",reason=%q} %d`, reason, count)
}

// 13966 RED on main: a re-read of one run that fails with a per-run error skips that run (it writes nothing for it),
// names it in one WARN line and the counter, and the walk drains the runs behind it. The unit succeeds.
func TestOpenPhaseSkipsARunWithAPerRunErrorAndDrainsTheRest(t *testing.T) {
	repoID := listingRepoID(t)
	for _, tc := range []struct {
		name       string
		runReply   *openSkipReply
		jobsReply  *openSkipReply
		wantReason string
		wantStatus int // -1: not checked
	}{
		{"410 gone", &openSkipReply{status: 410, body: openSkipPoisonBody}, nil, "gone", 410},
		{"422 unprocessable", &openSkipReply{status: 422, body: openSkipPoisonBody}, nil, "unprocessable", 422},
		{"409 conflict", &openSkipReply{status: 409, body: openSkipPoisonBody}, nil, "conflict", 409},
		{"400 client error", &openSkipReply{status: 400, body: openSkipPoisonBody}, nil, "client_error", 400},
		{"451 client error", &openSkipReply{status: 451, body: openSkipPoisonBody}, nil, "client_error", 451},
		{"a run payload that does not decode", &openSkipReply{status: 200, body: "poison-body-text not json"}, nil, "invalid_payload", -1},
		{"jobs page 404", nil, &openSkipReply{status: 404, body: openSkipPoisonBody}, "jobs_not_found", 404},
		{"one 502", &openSkipReply{status: 502, body: openSkipPoisonBody}, nil, "transient", 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := captureMembershipLogs(t)
			fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
			reader := openSkipStore(repoID, fake, 7200, 3)
			doer := newOpenSkipDoer(fake)
			if tc.runReply != nil {
				doer.runs[7201] = []openSkipReply{*tc.runReply}
			}
			if tc.jobsReply != nil {
				doer.jobs[7201] = []openSkipReply{*tc.jobsReply}
			}
			walk := walkOpenSkip(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), doer, 0)
			if walk.err != nil || !walk.finalSeen {
				t.Fatalf("unit err=%v final=%v, want the unit to succeed past the failing run", walk.err, walk.finalSeen)
			}
			rows := walk.rows()
			for _, id := range []string{"7200", "7202"} {
				if got := stringPtrValue(rows[id].Status); got != "success" {
					t.Fatalf("run %s status = %q, want success (the runs around the failing one drain)", id, got)
				}
			}
			if _, written := rows["7201"]; written {
				t.Fatalf("the skipped run was written: %#v; a skip writes nothing", rows["7201"])
			}
			if got := walk.result("open_runs_reread"); got != "2" {
				t.Fatalf("open_runs_reread = %s, want 2 (a skip is not a re-read)", got)
			}
			if got := walk.result("open_runs_skipped"); got != "1" {
				t.Fatalf("open_runs_skipped = %s, want 1", got)
			}
			lines := openSkipLines(*records)
			if len(lines) != 1 {
				t.Fatalf("got %d skip lines, want 1: %v", len(lines), lines)
			}
			if lines[0]["run"] != "7201" || lines[0]["reason"] != tc.wantReason {
				t.Fatalf("skip line = %v, want run 7201 reason %s", lines[0], tc.wantReason)
			}
			if tc.wantStatus >= 0 && fmt.Sprint(lines[0]["status"]) != strconv.Itoa(tc.wantStatus) {
				t.Fatalf("skip line status = %v, want %d", lines[0]["status"], tc.wantStatus)
			}
			assertNoBodyInLogs(t, *records)
			if want := openSkipCounterLine(tc.wantReason, 1); !strings.Contains(walk.metrics, want) {
				t.Fatalf("missing %q in:\n%s", want, walk.metrics)
			}
			if tc.runReply != nil && doer.hitsOf(7201) != 1 {
				t.Fatalf("the failing run was read %d times in the unit, want once", doer.hitsOf(7201))
			}
		})
	}
}

// 13966 cells C4, C4b, C4c: an error that hits every run (rate limit, auth) or a cancel still fails the unit, as on
// main, and stops at the failing run. GREEN on main.
func TestOpenPhaseStillFailsTheUnitOnAnInstallationWideError(t *testing.T) {
	repoID := listingRepoID(t)
	for _, tc := range []struct {
		name      string
		reply     openSkipReply
		wantClass providerfoundation.ErrorClass
	}{
		{"429", openSkipReply{status: 429, body: openSkipPoisonBody}, providerfoundation.ErrorRateLimited},
		{"403 primary limit remaining 0", openSkipReply{status: 403, header: map[string]string{"X-Ratelimit-Remaining": "0"}, body: openSkipPoisonBody}, providerfoundation.ErrorRateLimited},
		{"403 secondary limit", openSkipReply{status: 403, body: `{"message":"You have exceeded a secondary rate limit"}`}, providerfoundation.ErrorRateLimited},
		{"401", openSkipReply{status: 401, body: openSkipPoisonBody}, providerfoundation.ErrorAuthentication},
		{"403 not a limit", openSkipReply{status: 403, body: `{"message":"Resource not accessible by integration"}`}, providerfoundation.ErrorAuthentication},
		{"context cancel", openSkipReply{err: context.Canceled}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := captureMembershipLogs(t)
			fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
			reader := openSkipStore(repoID, fake, 7200, 3)
			doer := newOpenSkipDoer(fake)
			doer.runs[7201] = []openSkipReply{tc.reply}
			walk := walkOpenSkip(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), doer, 0)
			if walk.err == nil || walk.finalSeen {
				t.Fatalf("unit err=%v final=%v, want the unit to fail", walk.err, walk.finalSeen)
			}
			if tc.wantClass != "" {
				var providerErr *providerfoundation.ProviderError
				if !errors.As(walk.err, &providerErr) || providerErr.Class != tc.wantClass {
					t.Fatalf("unit err=%v, want class %s", walk.err, tc.wantClass)
				}
			}
			if doer.hitsOf(7202) != 0 {
				t.Fatal("the walk went past an installation-wide error")
			}
			if lines := openSkipLines(*records); len(lines) != 0 {
				t.Fatalf("an installation-wide error was logged as a skip: %v", lines)
			}
			if strings.Contains(walk.metrics, "dev_health_provider_open_run_skipped_total{") {
				t.Fatalf("an installation-wide error was counted as a skip:\n%s", walk.metrics)
			}
		})
	}
}

// 13966 cell C6: three transient skips in a row with no good run between them fail the unit (GitHub is failing, not
// one run); two and then a good run do not.
func TestOpenPhaseTransientBreaker(t *testing.T) {
	repoID := listingRepoID(t)
	bad := []openSkipReply{{status: 502, body: openSkipPoisonBody}}
	t.Run("three in a row fail the unit", func(t *testing.T) {
		captureMembershipLogs(t)
		fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
		reader := openSkipStore(repoID, fake, 7300, 5)
		doer := newOpenSkipDoer(fake)
		doer.runs[7301], doer.runs[7302], doer.runs[7303] = bad, bad, bad
		walk := walkOpenSkip(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), doer, 0)
		var providerErr *providerfoundation.ProviderError
		if !errors.As(walk.err, &providerErr) || providerErr.Class != providerfoundation.ErrorTransient || providerErr.StatusCode != 502 {
			t.Fatalf("unit err=%v, want the third transient error", walk.err)
		}
		if doer.hitsOf(7303) != 1 || doer.hitsOf(7304) != 0 {
			t.Fatalf("by-id reads 7303=%d 7304=%d, want the breaker to trip on the third run (1, 0)", doer.hitsOf(7303), doer.hitsOf(7304))
		}
		if want := openSkipCounterLine("transient", 2); !strings.Contains(walk.metrics, want) {
			t.Fatalf("missing %q in:\n%s", want, walk.metrics)
		}
	})
	t.Run("two then a good run do not", func(t *testing.T) {
		captureMembershipLogs(t)
		fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
		reader := openSkipStore(repoID, fake, 7300, 6)
		doer := newOpenSkipDoer(fake)
		doer.runs[7301], doer.runs[7302], doer.runs[7304] = bad, bad, bad
		walk := walkOpenSkip(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), doer, 0)
		if walk.err != nil || !walk.finalSeen {
			t.Fatalf("unit err=%v final=%v, want success: no three transient skips in a row", walk.err, walk.finalSeen)
		}
		if got := walk.result("open_runs_reread"); got != "3" {
			t.Fatalf("open_runs_reread = %s, want 3 (7300, 7303, 7305)", got)
		}
		if got := walk.result("open_runs_skipped"); got != "3" {
			t.Fatalf("open_runs_skipped = %s, want 3", got)
		}
		if want := openSkipCounterLine("transient", 3); !strings.Contains(walk.metrics, want) {
			t.Fatalf("missing %q in:\n%s", want, walk.metrics)
		}
	})
}

// The skip budget is the cap: a unit whose head holds more failing runs than the cap ends the phase as done with the
// marker, without failing the unit.
func TestOpenPhaseSkipBudgetEndsThePhase(t *testing.T) {
	repoID := listingRepoID(t)
	captureMembershipLogs(t)
	fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
	reader := openSkipStore(repoID, fake, 7500, 150)
	doer := newOpenSkipDoer(fake)
	for id := 7500; id < 7650; id++ {
		doer.runs[id] = []openSkipReply{{status: 410, body: openSkipPoisonBody}}
	}
	walk := walkOpenSkip(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), doer, 0)
	if walk.err != nil || !walk.finalSeen {
		t.Fatalf("unit err=%v final=%v, want success", walk.err, walk.finalSeen)
	}
	if got := walk.result("open_runs_skipped"); got != "100" {
		t.Fatalf("open_runs_skipped = %s, want the steady cap 100", got)
	}
	if got := walk.result("open_runs_reread"); got != "0" {
		t.Fatalf("open_runs_reread = %s, want 0", got)
	}
	if got := walk.result("open_runs_skip_limit"); got != "true" {
		t.Fatalf("open_runs_skip_limit = %s, want true", got)
	}
	total := 0
	for id := 7500; id < 7650; id++ {
		total += doer.hitsOf(id)
	}
	if total != 100 {
		t.Fatalf("%d by-id reads, want 100 (the skip budget)", total)
	}
}

// A resume after a skip (the attempt bound ends an invocation after each chunk) continues after the skipped run: it is
// read once in the unit.
func TestOpenPhaseResumeAfterASkipDoesNotReadTheRunAgain(t *testing.T) {
	repoID := listingRepoID(t)
	captureMembershipLogs(t)
	fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
	reader := openSkipStore(repoID, fake, 7400, 3)
	doer := newOpenSkipDoer(fake)
	doer.runs[7401] = []openSkipReply{{status: 410, body: openSkipPoisonBody}}
	walk := walkOpenSkip(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), doer, 1)
	if walk.err != nil || !walk.finalSeen {
		t.Fatalf("unit err=%v final=%v, want success", walk.err, walk.finalSeen)
	}
	if doer.hitsOf(7401) != 1 {
		t.Fatalf("the skipped run was read %d times across the resumes, want once", doer.hitsOf(7401))
	}
	if got := walk.result("open_runs_skipped"); got != "1" {
		t.Fatalf("open_runs_skipped = %s, want 1", got)
	}
	for _, id := range []string{"7400", "7402"} {
		if got := stringPtrValue(walk.rows()[id].Status); got != "success" {
			t.Fatalf("run %s status = %q, want success", id, got)
		}
	}
}

// 13966 cells C1, C3, C5 on the fake store: 1,500 old open rows, GitHub still reports the oldest open, and the next
// one fails with 410. A skip uses no cap slot, so the drain per unit equals the healthy drain (299/299/99), the runs
// are read in the same order, and the failing run is read again once per unit.
func TestOpenPhaseSkippedRunUsesNoCapSlotAcrossUnits(t *testing.T) {
	repoID := listingRepoID(t)
	captureMembershipLogs(t)
	const first, seeded, poison = 100000, 1500, 100001
	run := func(t *testing.T, poisoned bool) (drained []int, firstUnitReads []int, poisonHits int) {
		fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
		reader := openSkipStore(repoID, fake, first, seeded)
		stale := listingSince.Add(-40 * 24 * time.Hour)
		fake.byID[first] = listingRun{id: first, created: stale, updated: listingSince, status: "in_progress"}
		doer := newOpenSkipDoer(fake)
		if poisoned {
			doer.runs[poison] = []openSkipReply{{status: 410, body: openSkipPoisonBody}}
		}
		before := listingSince.Add(-githubTestsRefreshLookbackDefault)
		for unit := 1; unit <= 3; unit++ {
			backlog := len(reader.matching(repoID, before))
			reads := len(fake.runGets)
			walk := walkOpenSkip(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), doer, 0)
			if walk.err != nil {
				t.Fatalf("unit %d err=%v, want success", unit, walk.err)
			}
			drained = append(drained, backlog-len(reader.matching(repoID, before)))
			if unit == 1 {
				firstUnitReads = append([]int(nil), fake.runGets[reads:]...)
			}
		}
		return drained, firstUnitReads, doer.hitsOf(poison)
	}
	healthy, healthyReads, _ := run(t, false)
	poisoned, poisonedReads, poisonHits := run(t, true)
	if fmt.Sprint(healthy) != "[299 299 99]" {
		t.Fatalf("healthy drain per unit = %v, want [299 299 99]", healthy)
	}
	if fmt.Sprint(poisoned) != "[299 299 99]" {
		t.Fatalf("drain per unit with one 410 run = %v, want [299 299 99] like the healthy drain", poisoned)
	}
	if poisonHits != 3 {
		t.Fatalf("the failing run was read %d times over 3 units, want once a unit", poisonHits)
	}
	// The listing fake records only by-id reads it answers; the 410 is answered by the script, so the healthy order
	// with the failing run left out is the poisoned order.
	var want []int
	for _, id := range healthyReads {
		if id != poison {
			want = append(want, id)
		}
	}
	want = append(want, first+300)
	if fmt.Sprint(poisonedReads) != fmt.Sprint(want) {
		t.Fatalf("first-unit read order with the failing run differs from the healthy order:\n got %v\nwant %v", poisonedReads, want)
	}
}

func TestGitHubTestsChunkCursorRejectsNegativeSkipCounters(t *testing.T) {
	for _, raw := range []string{`{"phase":"open","osk":-1}`, `{"phase":"open","otr":-1}`} {
		if _, err := decodeGitHubTestsChunkCursor(raw); !errors.Is(err, ErrChunkCheckpointConflict) {
			t.Fatalf("decode %s err=%v, want ErrChunkCheckpointConflict", raw, err)
		}
	}
}

// openSkipContinuationShapes are the per-run skip classes of the by-id read and the jobs pages.
var openSkipContinuationShapes = []struct {
	name, reason string
	reply        openSkipReply
	jobsOnly     bool
}{
	{"gone", "gone", openSkipReply{status: 410, body: openSkipPoisonBody}, false},
	{"unprocessable", "unprocessable", openSkipReply{status: 422, body: openSkipPoisonBody}, false},
	{"conflict", "conflict", openSkipReply{status: 409, body: openSkipPoisonBody}, false},
	{"client_error", "client_error", openSkipReply{status: 451, body: openSkipPoisonBody}, false},
	{"invalid_payload", "invalid_payload", openSkipReply{status: 200, body: "poison-body-text not json"}, false},
	{"transient_5xx", "transient", openSkipReply{status: 502, body: openSkipPoisonBody}, false},
	{"transient_timeout", "transient", openSkipReply{err: context.DeadlineExceeded}, false},
	{"jobs_not_found", "jobs_not_found", openSkipReply{status: 404, body: openSkipPoisonBody}, true},
}

// 13966 r1 P1-a: a skip committed by the emission that ends the attempt (the chunk-count or wall-time bound returns a
// durable continuation after the commit) still logs its WARN line and steps the counter, and the resume does not
// record it again: exactly once over the unit, also when the resume fails at the repository read.
func TestOpenPhaseSkipTelemetrySurvivesAContinuation(t *testing.T) {
	repoID := listingRepoID(t)
	for _, resume := range []string{"healthy", "repo-404"} {
		for _, source := range []string{"run", "jobs"} {
			for _, bound := range []string{"count", "wall"} {
				for _, shape := range openSkipContinuationShapes {
					if shape.jobsOnly && source != "jobs" {
						continue
					}
					t.Run(resume+"/"+source+"/"+bound+"/"+shape.name, func(t *testing.T) {
						records := captureMembershipLogs(t)
						now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
						claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
						fake := newListingFake(t, nil)
						reader := openSkipStore(repoID, fake, 8100, 66)
						doer := newOpenSkipDoer(fake)
						poison := 8163 // the 64th open emission: the default chunk-count bound
						if bound == "wall" {
							poison = 8100
						}
						if source == "run" {
							doer.runs[poison] = []openSkipReply{shape.reply}
						} else {
							doer.jobs[poison] = []openSkipReply{shape.reply}
						}
						store := newChunkMemoryStore()
						executor := completeRouteExecutor(now, GitHubTestsRouteHandler{OpenRuns: reader}, store, &memoryEffectSink{})
						executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
						executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
						executor.Doer = fakehttp.Client(doer)
						executor.BudgetLimits = map[CostClass]int{claim.CostClass: 10}
						executor.Metrics = providerfoundation.NewMetrics()
						if bound == "wall" {
							executor.Now = func() time.Time {
								if doer.hitsOf(poison) > 0 {
									return now.Add(46 * time.Second)
								}
								return now
							}
						}
						descriptor, _ := Descriptor("github", "cicd")
						_, err := executor.Execute(context.Background(), session, descriptor)
						var stopped ChunkContinuationError
						if !errors.As(err, &stopped) {
							t.Fatalf("first attempt err=%v, want the continuation that ends the attempt", err)
						}
						checkpoint, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
						if err != nil {
							t.Fatal(err)
						}
						cursor, err := decodeGitHubTestsChunkCursor(checkpoint.NextCursor)
						if err != nil || cursor.OpenSkipped != 1 {
							t.Fatalf("premise: committed cursor skipped=%d err=%v, want the skip committed by the bounded emission", cursor.OpenSkipped, err)
						}
						if resume == "repo-404" {
							doer.setRepoStatus(http.StatusNotFound)
							if _, err = executor.Execute(context.Background(), session, descriptor); err == nil {
								t.Fatal("premise: the resume should fail at the repository read")
							}
						} else {
							for attempt := 0; attempt < 4; attempt++ {
								_, err = executor.Execute(context.Background(), session, descriptor)
								if !errors.Is(err, ErrChunkContinuation) {
									break
								}
							}
							if err != nil {
								t.Fatalf("resume err=%v", err)
							}
						}
						var rendered bytes.Buffer
						if err := executor.Metrics.WritePrometheus(&rendered); err != nil {
							t.Fatal(err)
						}
						lines := openSkipLines(*records)
						if len(lines) != 1 || lines[0]["run"] != strconv.Itoa(poison) || lines[0]["reason"] != shape.reason {
							t.Fatalf("stop=%s: skip lines %v, want exactly one for run %d reason %s", stopped.Reason, lines, poison, shape.reason)
						}
						want := fmt.Sprintf(`dev_health_provider_open_run_skipped_total{provider="github",dataset="cicd",reason=%q} 1`, shape.reason)
						if !strings.Contains(rendered.String(), want) {
							t.Fatalf("stop=%s: missing %q in:\n%s", stopped.Reason, want, rendered.String())
						}
						if total := strings.Count(rendered.String(), "dev_health_provider_open_run_skipped_total{"); total != 1 {
							t.Fatalf("stop=%s: %d skip counter series, want 1:\n%s", stopped.Reason, total, rendered.String())
						}
					})
				}
			}
		}
	}
}

// 13966 r1 P1-a: an emission of a skip that fails with anything but the durable continuation was not committed, so it
// records nothing: no WARN line, no counter step.
func TestOpenPhaseUncommittedSkipRecordsNothing(t *testing.T) {
	repoID := listingRepoID(t)
	records := captureMembershipLogs(t)
	fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
	reader := openSkipStore(repoID, fake, 7600, 3)
	doer := newOpenSkipDoer(fake)
	doer.runs[7601] = []openSkipReply{{status: 410, body: openSkipPoisonBody}}
	metrics := providerfoundation.NewMetrics()
	client := githubTestsClient(t, fakehttp.Client(doer))
	client.Metrics = metrics
	sinkDown := errors.New("sink refused the chunk")
	err := GitHubTestsRouteHandler{OpenRuns: reader}.CollectChunks(context.Background(), openWindowClaim(), providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "", func(emission ChunkRouteEmission) error {
			after, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if after.OpenSkipped > 0 {
				return sinkDown
			}
			return nil
		})
	if !errors.Is(err, sinkDown) {
		t.Fatalf("err=%v, want the sink error returned unchanged", err)
	}
	if lines := openSkipLines(*records); len(lines) != 0 {
		t.Fatalf("an uncommitted skip was logged: %v", lines)
	}
	var rendered bytes.Buffer
	if err := metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rendered.String(), "dev_health_provider_open_run_skipped_total{") {
		t.Fatalf("an uncommitted skip was counted:\n%s", rendered.String())
	}
}

// 13966 r1 P1-b: a run payload with data after the run object does not decode as one run: it is skipped as
// invalid_payload and nothing is written for it. Trailing whitespace is a valid payload.
func TestOpenPhaseSkipsARunPayloadWithTrailingData(t *testing.T) {
	repoID := listingRepoID(t)
	for _, dataset := range []string{"tests", "cicd"} {
		for _, suffix := range []string{"{}", "[]", `"extra"`, "123", "true", "null", "broken", " \n"} {
			t.Run(fmt.Sprintf("%s/suffix %q", dataset, suffix), func(t *testing.T) {
				records := captureMembershipLogs(t)
				fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
				reader := openSkipStore(repoID, fake, 8200, 3)
				doer := newOpenSkipDoer(fake)
				doer.runs[8201] = []openSkipReply{{status: 200, body: fake.byID[8201].json() + suffix}}
				claim := openWindowClaim()
				claim.Dataset = dataset
				capability, _ := Capability("github", dataset)
				claim.CostClass = capability.CostClass
				walk := walkOpenSkip(t, GitHubTestsRouteHandler{OpenRuns: reader}, claim, doer, 0)
				if walk.err != nil || !walk.finalSeen {
					t.Fatalf("unit err=%v final=%v, want success", walk.err, walk.finalSeen)
				}
				_, written := walk.rows()["8201"]
				if strings.TrimSpace(suffix) == "" {
					if !written || walk.result("open_runs_skipped") != "0" {
						t.Fatalf("whitespace suffix: written=%v skipped=%s, want the run re-read", written, walk.result("open_runs_skipped"))
					}
					return
				}
				if written {
					t.Fatalf("a payload with trailing data was written (status %q)", stringPtrValue(walk.rows()["8201"].Status))
				}
				lines := openSkipLines(*records)
				if walk.result("open_runs_skipped") != "1" || len(lines) != 1 || lines[0]["reason"] != "invalid_payload" {
					t.Fatalf("skipped=%s lines=%v, want one invalid_payload skip", walk.result("open_runs_skipped"), lines)
				}
			})
		}
	}
}

// 13966 r1 P1-b: a payload with trailing data between transient skips is a skip, not a re-read, so it does not reset
// the breaker: the third transient still fails the unit.
func TestOpenPhaseTrailingDataDoesNotResetTheBreaker(t *testing.T) {
	repoID := listingRepoID(t)
	captureMembershipLogs(t)
	fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
	reader := openSkipStore(repoID, fake, 8300, 5)
	doer := newOpenSkipDoer(fake)
	for _, id := range []int{8300, 8301, 8303} {
		doer.runs[id] = []openSkipReply{{status: 502, body: openSkipPoisonBody}}
	}
	doer.runs[8302] = []openSkipReply{{status: 200, body: fake.byID[8302].json() + "broken"}}
	walk := walkOpenSkip(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), doer, 0)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(walk.err, &providerErr) || providerErr.Class != providerfoundation.ErrorTransient {
		t.Fatalf("unit err=%v final=%v, want the third transient to fail the unit", walk.err, walk.finalSeen)
	}
	if doer.hitsOf(8304) != 0 {
		t.Fatal("the walk went past the breaker")
	}
}

// openSkipFaultStore fails the chunk store once, before or after the prepare, or before or after the commit mark. The
// "skip" target is the chunk whose cursor holds the first skip; the "next" target is the chunk prepared right after it.
type openSkipFaultStore struct {
	*chunkMemoryStore
	mode, target string
	fault        error
	seen         bool
	skipOrdinal  int
	failLoad     bool
}

// LoadChunkCheckpoint fails once after an "after-commit-unread" fault: the read back of the lost acknowledgement fails.
func (s *openSkipFaultStore) LoadChunkCheckpoint(ctx context.Context, claim Claim, now time.Time) (ChunkCheckpoint, error) {
	if s.failLoad {
		s.failLoad = false
		return ChunkCheckpoint{}, s.fault
	}
	return s.chunkMemoryStore.LoadChunkCheckpoint(ctx, claim, now)
}

func newOpenSkipFaultStore(mode, target string, fault error) *openSkipFaultStore {
	return &openSkipFaultStore{chunkMemoryStore: newChunkMemoryStore(), mode: mode, target: target, fault: fault, skipOrdinal: -1}
}

func openSkipCursorHasSkip(raw string) bool {
	cursor, err := decodeGitHubTestsChunkCursor(raw)
	return err == nil && cursor.OpenSkipped == 1
}

func (s *openSkipFaultStore) targets(chunk PreparedProviderChunk) bool {
	if s.seen {
		return false
	}
	if s.target == "next" {
		return s.skipOrdinal >= 0 && chunk.Ordinal == s.skipOrdinal+1
	}
	return openSkipCursorHasSkip(chunk.CursorAfter)
}

func (s *openSkipFaultStore) PrepareChunkGroup(ctx context.Context, claim Claim, chunks []PreparedProviderChunk, now time.Time) ([]PreparedProviderChunk, error) {
	fault := s.targets(chunks[0]) || s.targets(chunks[len(chunks)-1])
	if fault && s.mode == "before-prepare" {
		s.seen = true
		return nil, s.fault
	}
	group, err := s.chunkMemoryStore.PrepareChunkGroup(ctx, claim, chunks, now)
	if err == nil && s.skipOrdinal < 0 {
		for _, chunk := range group {
			if openSkipCursorHasSkip(chunk.CursorAfter) {
				s.skipOrdinal = chunk.Ordinal
				break
			}
		}
	}
	if fault && s.mode == "after-prepare" && err == nil {
		s.seen = true
		return nil, s.fault
	}
	return group, err
}

func (s *openSkipFaultStore) MarkChunkCommitted(ctx context.Context, claim Claim, ordinal int, digest string, now time.Time) error {
	chunk, err := s.LoadPreparedChunk(ctx, claim, ordinal, now)
	if err != nil {
		return err
	}
	fault := s.targets(chunk)
	if fault && s.mode == "before-commit" {
		s.seen = true
		return s.fault
	}
	err = s.chunkMemoryStore.MarkChunkCommitted(ctx, claim, ordinal, digest, now)
	if fault && (s.mode == "after-commit" || s.mode == "after-commit-unread") && err == nil {
		s.seen = true
		s.failLoad = s.mode == "after-commit-unread"
		return s.fault
	}
	return err
}

func openSkipUnconfirmedLines(records []slog.Record) []map[string]any {
	var out []map[string]any
	for _, record := range records {
		attrs := membershipLogAttrs(record)
		if record.Level == slog.LevelWarn && fmt.Sprint(attrs["outcome"]) == "unknown" {
			out = append(out, attrs)
		}
	}
	return out
}

// A skip chunk whose commit mark landed but failed, and whose checkpoint read back failed too, is not silent: a distinct
// WARN names the run and the chunk with outcome unknown. The skip itself is not recovered (no skip WARN, no counter step);
// the unit result stays exact.
func TestOpenPhaseSkipUnconfirmedCommitIsNotSilent(t *testing.T) {
	repoID := listingRepoID(t)
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for _, source := range []string{"run", "jobs"} {
		for _, shape := range openSkipContinuationShapes {
			if shape.jobsOnly && source != "jobs" {
				continue
			}
			t.Run(source+"/"+shape.name, func(t *testing.T) {
				records := captureMembershipLogs(t)
				fake := newListingFake(t, nil)
				reader := openSkipStore(repoID, fake, 8400, 3)
				doer := newOpenSkipDoer(fake)
				if source == "run" {
					doer.runs[8401] = []openSkipReply{shape.reply}
				} else {
					doer.jobs[8401] = []openSkipReply{shape.reply}
				}
				store := newOpenSkipFaultStore("after-commit-unread", "skip", io.ErrUnexpectedEOF)
				claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
				executor := completeRouteExecutor(now, GitHubTestsRouteHandler{OpenRuns: reader}, store, &memoryEffectSink{})
				executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
				executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
				executor.Doer = fakehttp.Client(doer)
				executor.BudgetLimits = map[CostClass]int{claim.CostClass: 10}
				executor.Metrics = providerfoundation.NewMetrics()
				descriptor, _ := Descriptor("github", "cicd")
				ctx := context.Background()
				if _, err := executor.Execute(ctx, session, descriptor); !store.seen || !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("premise: first attempt err=%v seen=%v, want the lost acknowledgement", err, store.seen)
				}
				var result CompleteRouteExecutionResult
				var err error
				for attempt := 0; attempt < 8; attempt++ {
					result, err = executor.Execute(ctx, session, descriptor)
					if !errors.Is(err, ErrChunkContinuation) {
						break
					}
				}
				if err != nil {
					t.Fatalf("retry err=%v, want the unit to finish", err)
				}
				if got := fmt.Sprint(result.Result[GitHubTestsResultKeyOpenRunsSkipped]); got != "1" {
					t.Fatalf("open_runs_skipped = %s, want 1", got)
				}
				unconfirmed := openSkipUnconfirmedLines(*records)
				t.Logf("unconfirmed=%v skip_lines=%d skip_chunk=%d", unconfirmed, len(openSkipLines(*records)), store.skipOrdinal)
				if len(unconfirmed) != 1 || unconfirmed[0]["run"] != "8401" || fmt.Sprint(unconfirmed[0]["chunk"]) != strconv.Itoa(store.skipOrdinal) ||
					fmt.Sprint(unconfirmed[0]["skip_reason"]) != shape.reason || fmt.Sprint(unconfirmed[0]["repository"]) != claim.SourceExternalID {
					t.Fatalf("unconfirmed lines %v, want one naming run 8401, chunk %d, reason %s", unconfirmed, store.skipOrdinal, shape.reason)
				}
				if lines := openSkipLines(*records); len(lines) != 0 {
					t.Fatalf("skip lines %v, want none: the unconfirmed skip is not recovered", lines)
				}
			})
		}
	}
}

// A store fault at the skip's chunk, or at the chunk after it, fails the attempt; the retry finishes the unit with
// open_runs_skipped exact. The skip's WARN line (naming the run and the claim's repository, never the provider's text)
// and its counter step happen exactly once at every fault point: a fault before the prepare re-runs the route; a fault
// after it is drained by recovery, which tells the route of the commit; a lost commit acknowledgement is settled by the
// checkpoint read back. A resume that then fails at the repository read loses nothing: the record does not wait for
// the route to run again.
func TestOpenPhaseSkipRecoveryKeepsTheResultExact(t *testing.T) {
	// The provider's full_name is arbitrary text: the WARN line must name the claim's repository instead.
	const providerName = "acme/body-fixture: arbitrary text\nsecond line"
	repoID, err := repositoryIdentity(providerName)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for _, resume := range []string{"healthy", "repo-404"} {
		for _, target := range []string{"skip", "next"} {
			for _, source := range []string{"run", "jobs"} {
				for _, mode := range []string{"before-prepare", "after-prepare", "before-commit", "after-commit"} {
					for _, shape := range openSkipContinuationShapes {
						if shape.jobsOnly && source != "jobs" {
							continue
						}
						t.Run(resume+"/"+target+"/"+source+"/"+mode+"/"+shape.name, func(t *testing.T) {
							records := captureMembershipLogs(t)
							fake := newListingFake(t, nil)
							reader := openSkipStore(repoID, fake, 8400, 3)
							doer := newOpenSkipDoer(fake)
							doer.repoName = providerName
							if source == "run" {
								doer.runs[8401] = []openSkipReply{shape.reply}
							} else {
								doer.jobs[8401] = []openSkipReply{shape.reply}
							}
							fault := NewChunkStoreError(io.ErrUnexpectedEOF)
							switch mode {
							case "after-prepare":
								fault = ErrInvalidConfiguration
							case "after-commit":
								fault = io.ErrUnexpectedEOF
							}
							store := newOpenSkipFaultStore(mode, target, fault)
							claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
							executor := completeRouteExecutor(now, GitHubTestsRouteHandler{OpenRuns: reader}, store, &memoryEffectSink{})
							executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
							executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
							executor.Doer = fakehttp.Client(doer)
							executor.BudgetLimits = map[CostClass]int{claim.CostClass: 10}
							executor.Metrics = providerfoundation.NewMetrics()
							descriptor, _ := Descriptor("github", "cicd")
							ctx := context.Background()
							if _, err := executor.Execute(ctx, session, descriptor); !store.seen || !errors.Is(err, fault) {
								t.Fatalf("premise: first attempt err=%v seen=%v, want the injected store fault", err, store.seen)
							}
							wantLines := 1
							var result CompleteRouteExecutionResult
							var err error
							if resume == "repo-404" {
								doer.setRepoStatus(http.StatusNotFound)
								_, err = executor.Execute(ctx, session, descriptor)
								if err == nil {
									t.Fatal("premise: the resume should fail at the repository read")
								}
								if target == "skip" && mode == "before-prepare" {
									wantLines = 0
								}
							} else {
								for attempt := 0; attempt < 8; attempt++ {
									result, err = executor.Execute(ctx, session, descriptor)
									if !errors.Is(err, ErrChunkContinuation) {
										break
									}
								}
								if err != nil {
									t.Fatalf("retry err=%v, want the unit to finish", err)
								}
								if got := fmt.Sprint(result.Result[GitHubTestsResultKeyOpenRunsSkipped]); got != "1" {
									t.Fatalf("open_runs_skipped = %s, want exactly 1 after recovery", got)
								}
								if doer.hitsOf(8402) != 1 && !(target == "next" && mode == "before-prepare") {
									t.Fatalf("the run after the skip was read %d times, want once", doer.hitsOf(8402))
								}
							}
							var rendered bytes.Buffer
							if err := executor.Metrics.WritePrometheus(&rendered); err != nil {
								t.Fatal(err)
							}
							lines := openSkipLines(*records)
							series := strings.Count(rendered.String(), "dev_health_provider_open_run_skipped_total{")
							t.Logf("WARN=%d series=%d hits(8401)=%d hits(8402)=%d resume_err=%v", len(lines), series, doer.hitsOf(8401), doer.hitsOf(8402), err)
							for _, line := range lines {
								if line["run"] != "8401" || line["reason"] != shape.reason || fmt.Sprint(line["repository"]) != claim.SourceExternalID {
									t.Fatalf("skip line %v, want run 8401 reason %s repository %s", line, shape.reason, claim.SourceExternalID)
								}
							}
							counter := fmt.Sprintf(`dev_health_provider_open_run_skipped_total{provider="github",dataset="cicd",reason=%q} 1`, shape.reason)
							if len(lines) != wantLines || series != wantLines || wantLines == 1 && !strings.Contains(rendered.String(), counter) {
								t.Fatalf("WARN=%d series=%d, want %d WARN line and counter step for the skip:\n%s", len(lines), series, wantLines, rendered.String())
							}
						})
					}
				}
			}
		}
	}
}
