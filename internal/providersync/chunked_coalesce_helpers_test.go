package providersync

// Shared fixtures of the chunk-coalescing cells. This file names no coalescing field, so it builds on the base too
// (the base-control cells run on it there).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/google/uuid"
)

const runsGroupFirstID = 9_000_001

// coalesceClock is the executor clock of the wall cells: GitHub reads and sink writes advance it by their measured cost.
type coalesceClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *coalesceClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *coalesceClock) advance(by time.Duration) {
	if clock == nil {
		return
	}
	clock.mu.Lock()
	clock.now = clock.now.Add(by)
	clock.mu.Unlock()
}

// runsGroupDoer lists `runs` completed runs newest first, perPage per listing page (Link to the next page), and answers
// each run's jobs read with jobsPerRun small jobs (bigJobs for bigRun, 100 per page). The artifacts listing is empty, so
// a walk is the runs phase and the final emission. failJobsOnce fails one run's first jobs read.
type runsGroupDoer struct {
	t             *testing.T
	runs          int
	perPage       int
	jobsPerRun    int
	bigRun        string
	bigJobs       int
	failJobsOnce  string
	clock         *coalesceClock
	readCost      time.Duration
	wallAt        time.Time
	readsPastWall int
	mu            sync.Mutex
	jobGets       map[string]int
}

func newRunsGroupDoer(t *testing.T, runs, jobsPerRun int) *runsGroupDoer {
	return &runsGroupDoer{t: t, runs: runs, perPage: 100, jobsPerRun: jobsPerRun, jobGets: map[string]int{}}
}

func runsGroupRunID(index int) string { return strconv.Itoa(runsGroupFirstID + index) }

func (doer *runsGroupDoer) Do(request *http.Request) (*http.Response, error) {
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	page := 1
	if raw := request.URL.Query().Get("page"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			doer.t.Errorf("page query %q", raw)
		}
		page = parsed
	}
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		if githubTestsIsArtifactRunListing(request.URL.Query()) {
			return githubTestsHTTPResponse(request, header, `{"workflow_runs":[]}`), nil
		}
		first := (page - 1) * doer.perPage
		last := min(first+doer.perPage, doer.runs)
		if last < doer.runs {
			next := *request.URL
			query := next.Query()
			query.Set("page", strconv.Itoa(page+1))
			next.RawQuery = query.Encode()
			header.Set("Link", "<"+next.String()+">; rel=\"next\"")
		}
		var body strings.Builder
		body.WriteString(`{"workflow_runs":[`)
		base := time.Date(2026, 7, 22, 15, 0, 0, 0, time.UTC)
		for index := first; index < last; index++ {
			if index > first {
				body.WriteByte(',')
			}
			id := runsGroupRunID(index)
			created := base.Add(-time.Duration(index) * 10 * time.Second)
			body.WriteString(`{"id":` + id + `,"name":"CI","status":"completed","conclusion":"success","created_at":"` +
				created.Format(time.RFC3339) + `","run_started_at":"` + created.Add(time.Minute).Format(time.RFC3339) +
				`","updated_at":"` + created.Add(5*time.Minute).Format(time.RFC3339) +
				`","run_attempt":1,"event":"push","head_sha":"abc","head_branch":"main","html_url":"https://github.com/acme/api/actions/runs/` +
				id + `","pull_requests":[]}`)
		}
		body.WriteString(`]}`)
		return githubTestsHTTPResponse(request, header, body.String()), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/jobs"):
		runID := strings.TrimSuffix(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/jobs")
		doer.mu.Lock()
		if doer.clock != nil && !doer.wallAt.IsZero() && !doer.clock.Now().Before(doer.wallAt) {
			doer.readsPastWall++
		}
		doer.mu.Unlock()
		doer.clock.advance(doer.readCost)
		doer.mu.Lock()
		doer.jobGets[runID]++
		fail := runID == doer.failJobsOnce
		if fail {
			doer.failJobsOnce = ""
		}
		doer.mu.Unlock()
		if fail {
			return &http.Response{StatusCode: http.StatusInternalServerError, Header: header,
				Body: http.NoBody, Request: request}, nil
		}
		total := doer.jobsPerRun
		if runID == doer.bigRun {
			total = doer.bigJobs
		}
		first := (page - 1) * githubTestsPerRunPerPage
		last := min(first+githubTestsPerRunPerPage, total)
		if last < total {
			next := *request.URL
			query := next.Query()
			query.Set("page", strconv.Itoa(page+1))
			next.RawQuery = query.Encode()
			header.Set("Link", "<"+next.String()+">; rel=\"next\"")
		}
		var body strings.Builder
		body.WriteString(`{"total_count":` + strconv.Itoa(total) + `,"jobs":[`)
		for job := first; job < last; job++ {
			if job > first {
				body.WriteByte(',')
			}
			body.WriteString(`{"id":` + runID + fmt.Sprintf("%04d", job) + `,"name":"job-` + strconv.Itoa(job) +
				`","status":"completed","conclusion":"success","started_at":"2026-07-22T13:01:00Z","completed_at":"2026-07-22T13:04:00Z","labels":["ubuntu-latest"]}`)
		}
		body.WriteString(`]}`)
		return githubTestsHTTPResponse(request, header, body.String()), nil
	default:
		doer.t.Errorf("unexpected request %s", request.URL.String())
		return &http.Response{StatusCode: http.StatusNotFound, Header: header, Body: http.NoBody, Request: request}, nil
	}
}

// runsGroupSink records every written row (a ReplacingMergeTree reader sees each physical row), counts the writes that
// carry pipeline rows (one per committed runs-phase chunk) and reads back exactly what it holds. failJobWrite fails that
// one ci_job_runs write (1-based) once.
type runsGroupSink struct {
	mu             sync.Mutex
	clock          *coalesceClock
	writeCost      time.Duration
	failJobWrite   int
	jobWrites      int
	pipelineWrites int
	pipelineRows   map[string]int
	jobRows        map[string]int
	rows           map[string]map[string]int
}

func newRunsGroupSink() *runsGroupSink {
	return &runsGroupSink{pipelineRows: map[string]int{}, jobRows: map[string]int{}, rows: map[string]map[string]int{}}
}

func (sink *runsGroupSink) WriteEffect(_ context.Context, _ Claim, batch EffectBatch) error {
	if len(batch.Rows) > 0 {
		sink.clock.advance(sink.writeCost)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if batch.Destination == "ci_job_runs" && len(batch.Rows) > 0 {
		sink.jobWrites++
		if sink.jobWrites == sink.failJobWrite {
			return errors.New("sink unavailable")
		}
	}
	if sink.rows[batch.Destination] == nil {
		sink.rows[batch.Destination] = map[string]int{}
	}
	for _, raw := range batch.Rows {
		sink.rows[batch.Destination][string(raw)]++
		var row struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			return err
		}
		switch batch.Destination {
		case "ci_pipeline_runs":
			sink.pipelineRows[row.RunID]++
		case "ci_job_runs":
			sink.jobRows[row.RunID]++
		}
	}
	if batch.Destination == "ci_pipeline_runs" && len(batch.Rows) > 0 {
		sink.pipelineWrites++
	}
	return nil
}

func (sink *runsGroupSink) InspectEffect(_ context.Context, _ Claim, batch EffectBatch) (EffectInspection, error) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	found := 0
	for _, raw := range batch.Rows {
		if sink.rows[batch.Destination][string(raw)] > 0 {
			found++
		}
	}
	switch found {
	case len(batch.Rows):
		return EffectExact, nil
	case 0:
		return EffectAbsent, nil
	default:
		return EffectConflict, nil
	}
}

// duplicates counts rows physically written more than once.
func (sink *runsGroupSink) duplicates() int {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	total := 0
	for _, rows := range sink.rows {
		for _, count := range rows {
			if count > 1 {
				total++
			}
		}
	}
	return total
}

// runsGroupSession is a github/cicd claim over 2026-07-22 12:00..20:00 (no UTC midnight, so no deep re-run range).
func runsGroupSession(t *testing.T, now time.Time) (Claim, *LeaseSession) {
	t.Helper()
	return runsGroupWindowSession(t, now, time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC), time.Date(2026, 7, 22, 20, 0, 0, 0, time.UTC))
}

func runsGroupWindowSession(t *testing.T, now, since, before time.Time) (Claim, *LeaseSession) {
	t.Helper()
	unit := nativeTestClaim("github", "cicd").Unit
	unit.SinceAt, unit.BeforeAt = &since, &before
	leases := newMemoryLeaseRepository(unit, "dispatching")
	claim, err := leases.Claim(context.Background(), ClaimRequest{
		UnitID: unit.ID, Owner: uuid.NewString(), Now: now, LeaseDuration: 15 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim, &LeaseSession{
		Repository: leases, Claim: claim, LeaseDuration: 15 * time.Minute,
		Deadline: now.Add(48 * time.Hour), Now: func() time.Time { return now },
	}
}

func runsGroupExecutor(
	now time.Time, handler CompleteRouteHandler, store EffectLedger, sink *runsGroupSink, doer fakehttp.Doer, clock *coalesceClock,
) CompleteRouteExecutor {
	executor := completeRouteExecutor(now, handler, store, sink)
	executor.Committer.Readback = sink
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	executor.Doer = fakehttp.Client(doer)
	executor.BudgetLimits = map[CostClass]int{CostMedium: 100_000, CostHeavy: 100_000, CostLight: 100_000}
	if clock != nil {
		executor.Now = clock.Now
	}
	return executor
}

// runUntilDone runs attempts until the unit completes and returns each attempt's continuation (nil for the last).
func runUntilDone(t *testing.T, executor CompleteRouteExecutor, session *LeaseSession, descriptor CompleteRouteDescriptor) []*ChunkContinuationError {
	t.Helper()
	var stops []*ChunkContinuationError
	for attempt := 1; ; attempt++ {
		if attempt > 200 {
			t.Fatal("the unit does not complete in 200 attempts")
		}
		_, err := executor.Execute(context.Background(), session, descriptor)
		if err == nil {
			return append(stops, nil)
		}
		var stopped ChunkContinuationError
		if !errors.As(err, &stopped) {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		stops = append(stops, &stopped)
	}
}

func cicdDescriptor(t *testing.T) CompleteRouteDescriptor {
	t.Helper()
	descriptor, ok := Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	return descriptor
}

func assertEachRunOnce(t *testing.T, sink *runsGroupSink, doer *runsGroupDoer, runs int) {
	t.Helper()
	if len(sink.pipelineRows) != runs {
		t.Fatalf("pipeline rows for %d runs, want %d", len(sink.pipelineRows), runs)
	}
	for index := 0; index < runs; index++ {
		id := runsGroupRunID(index)
		if sink.pipelineRows[id] != 1 {
			t.Fatalf("run %s pipeline row written %d times, want 1", id, sink.pipelineRows[id])
		}
		want := doer.jobsPerRun
		if id == doer.bigRun {
			want = doer.bigJobs
		}
		if sink.jobRows[id] != want {
			t.Fatalf("run %s job rows=%d, want %d", id, sink.jobRows[id], want)
		}
	}
	if duplicated := sink.duplicates(); duplicated != 0 {
		t.Fatalf("%d rows written more than once", duplicated)
	}
}

// storedRunGroups returns the run ids of each stored chunk that carries pipeline rows, in ordinal order, and checks
// that no destination of any stored chunk passes the policy's row cap.
func storedRunGroups(t *testing.T, store *chunkMemoryStore, policy ChunkPolicy) [][]string {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	groups := [][]string{}
	for ordinal := 0; ordinal < len(store.chunks); ordinal++ {
		chunk, ok := store.chunks[ordinal]
		if !ok {
			t.Fatalf("stored chunks have a gap at %d", ordinal)
		}
		var runs []string
		for _, effect := range chunk.Effects {
			if len(effect.Rows) > min(policy.MaxEffectRows, policy.MaxSourceItems) {
				t.Fatalf("chunk %d %s holds %d rows over the cap", ordinal, effect.Destination, len(effect.Rows))
			}
			if effect.Destination != "ci_pipeline_runs" {
				continue
			}
			for _, raw := range effect.Rows {
				var row struct {
					RunID string `json:"run_id"`
				}
				_ = json.Unmarshal(raw, &row)
				runs = append(runs, row.RunID)
			}
		}
		if len(runs) > 0 {
			groups = append(groups, runs)
		}
	}
	return groups
}

type coalesceDoerFunc func(*http.Request) (*http.Response, error)

func (do coalesceDoerFunc) Do(request *http.Request) (*http.Response, error) { return do(request) }
