package providersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// listingRun is one run the fake GitHub holds.
type listingRun struct {
	id         int
	created    time.Time
	updated    time.Time
	startedAt  time.Time // run_started_at; zero means the creation time
	status     string
	conclusion string
	branch     string
	attempt    int // run_attempt; zero means 1
}

// listingFake is a GitHub that honours created ranges, branch, paging, the documented cap of 1,000 results for a
// created search, by-id reads (404 for a run in gone) and the rate-limit headers.
type listingFake struct {
	t         *testing.T
	mu        sync.Mutex
	runs      []listingRun
	gone      map[int]bool
	byID      map[int]listingRun // what a by-id read answers, when different from the listing
	capSearch bool
	// remaining and limit are served as x-ratelimit headers unless noHeaders.
	remaining, limit int
	noHeaders        bool
	// requestCeiling fails the test when the fake is asked for more requests than a bounded cell allows.
	requestCeiling int
	requests       int
	listPages      int
	runGets        []int
	listRanges     []string
	jobGets        map[int]int
	artifactGets   map[int]int
	// perPage is the page size the fake serves (0: 100, GitHub's per_page maximum).
	perPage int
}

func newListingFake(t *testing.T, runs []listingRun) *listingFake {
	return &listingFake{t: t, runs: runs, gone: map[int]bool{}, byID: map[int]listingRun{}, capSearch: true,
		remaining: 4000, limit: 5000, jobGets: map[int]int{}, artifactGets: map[int]int{}}
}

func listingTime(raw string, upper bool) (time.Time, bool) {
	if raw == "*" || raw == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed, true
	}
	if parsed, err := time.Parse(time.DateOnly, raw); err == nil {
		if upper {
			return parsed.Add(24*time.Hour - time.Second), true
		}
		return parsed, true
	}
	return time.Time{}, false
}

func (run listingRun) json() string {
	started := run.startedAt
	if started.IsZero() {
		started = run.created
	}
	conclusion := "null"
	if run.conclusion != "" {
		conclusion = strconv.Quote(run.conclusion)
	}
	branch := run.branch
	if branch == "" {
		branch = "main"
	}
	return fmt.Sprintf(`{"id":%d,"name":"CI","status":%q,"conclusion":%s,"created_at":%q,"run_started_at":%q,"updated_at":%q,"run_attempt":%d,"event":"push","head_sha":"abc","head_branch":%q,"html_url":"https://github.com/acme/api/actions/runs/%d","pull_requests":[]}`,
		run.id, run.status, conclusion, run.created.UTC().Format(time.RFC3339), started.UTC().Format(time.RFC3339),
		run.updated.UTC().Format(time.RFC3339), max(1, run.attempt), branch, run.id)
}

func (fake *listingFake) respond(request *http.Request, header http.Header, body string, status int) *http.Response {
	if !fake.noHeaders {
		header.Set("X-RateLimit-Remaining", strconv.Itoa(fake.remaining))
		header.Set("X-RateLimit-Limit", strconv.Itoa(fake.limit))
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}
}

func (fake *listingFake) Do(request *http.Request) (*http.Response, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.requests++
	if fake.requestCeiling > 0 && fake.requests > fake.requestCeiling {
		fake.t.Fatalf("the fake was asked for more than %d requests: a bounded cell must end", fake.requestCeiling)
	}
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return fake.respond(request, header, gitHubRepositoryFixture, 200), nil
	case path == "/repos/acme/api/actions/runs":
		return fake.list(request, header), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/jobs"):
		id, _ := strconv.Atoi(strings.Split(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/")[0])
		fake.jobGets[id]++
		return fake.respond(request, header, `{"jobs":[]}`, 200), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/artifacts"):
		id, _ := strconv.Atoi(strings.Split(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/")[0])
		fake.artifactGets[id]++
		return fake.respond(request, header, `{"artifacts":[]}`, 200), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/"):
		id, err := strconv.Atoi(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"))
		if err != nil {
			fake.t.Fatalf("unexpected request %s", request.URL.String())
		}
		fake.runGets = append(fake.runGets, id)
		if fake.gone[id] {
			return fake.respond(request, header, `{"message":"Not Found"}`, 404), nil
		}
		if run, ok := fake.byID[id]; ok {
			return fake.respond(request, header, run.json(), 200), nil
		}
		for _, run := range fake.runs {
			if run.id == id {
				return fake.respond(request, header, run.json(), 200), nil
			}
		}
		return fake.respond(request, header, `{"message":"Not Found"}`, 404), nil
	}
	fake.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

func (fake *listingFake) list(request *http.Request, header http.Header) *http.Response {
	query := request.URL.Query()
	fake.listPages++
	created := query.Get("created")
	fake.listRanges = append(fake.listRanges, created)
	lo, hi := time.Time{}, time.Time{}
	var haveLo, haveHi bool
	switch {
	case strings.Contains(created, ".."):
		parts := strings.SplitN(created, "..", 2)
		lo, haveLo = listingTime(parts[0], false)
		hi, haveHi = listingTime(parts[1], true)
	case strings.HasPrefix(created, ">="):
		lo, haveLo = listingTime(strings.TrimPrefix(created, ">="), false)
	}
	var matched []listingRun
	for _, run := range fake.runs {
		if haveLo && run.created.Before(lo) || haveHi && run.created.After(hi) {
			continue
		}
		if branch := query.Get("branch"); branch != "" && run.branch != branch && !(run.branch == "" && branch == "main") {
			continue
		}
		matched = append(matched, run)
	}
	sort.SliceStable(matched, func(i, j int) bool {
		if !matched[i].created.Equal(matched[j].created) {
			return matched[i].created.After(matched[j].created)
		}
		return matched[i].id > matched[j].id
	})
	if fake.capSearch && created != "" && len(matched) > 1000 {
		matched = matched[:1000]
	}
	page := 1
	if raw := query.Get("page"); raw != "" {
		page, _ = strconv.Atoi(raw)
	}
	per := 100
	if fake.perPage > 0 {
		per = fake.perPage
	}
	from, to := min((page-1)*per, len(matched)), min(page*per, len(matched))
	if to < len(matched) {
		next := *request.URL
		forward := next.Query()
		forward.Set("page", strconv.Itoa(page+1))
		next.RawQuery = forward.Encode()
		header.Set("Link", "<"+next.String()+">; rel=\"next\"")
	}
	items := make([]string, 0, to-from)
	for _, run := range matched[from:to] {
		items = append(items, run.json())
	}
	return fake.respond(request, header, `{"workflow_runs":[`+strings.Join(items, ",")+`]}`, 200)
}

// fakeOpenRuns is the open-runs reader over an in-memory store, with the same keyset order as the ClickHouse reader.
type fakeOpenRuns struct {
	rows       []githubTestsPipelineRow // every stored row of every repo
	calls      int
	backlogs   int
	repoSeen   map[string]bool
	startedMax time.Time
}

func (reader *fakeOpenRuns) matching(repoID string, before time.Time) []githubTestsPipelineRow {
	var out []githubTestsPipelineRow
	for _, row := range reader.rows {
		if row.RepoID != repoID || row.Status == nil || (*row.Status != "running" && *row.Status != "queued") || !row.StartedAt.Before(before) {
			continue
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].StartedAt.Before(out[j].StartedAt)
		}
		return out[i].RunID < out[j].RunID
	})
	return out
}

func (reader *fakeOpenRuns) OpenRuns(_ context.Context, _ Claim, repoID string, before, afterStarted time.Time, afterRunID string, limit int) ([]githubTestsPipelineRow, error) {
	reader.calls++
	if reader.repoSeen == nil {
		reader.repoSeen = map[string]bool{}
	}
	reader.repoSeen[repoID] = true
	var out []githubTestsPipelineRow
	for _, row := range reader.matching(repoID, before) {
		if afterStarted.IsZero() || row.StartedAt.After(afterStarted) || row.StartedAt.Equal(afterStarted) && row.RunID > afterRunID {
			out = append(out, row)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (reader *fakeOpenRuns) OpenRunBacklog(_ context.Context, _ Claim, repoID string, before time.Time, limit int) (int, error) {
	reader.backlogs++
	return min(len(reader.matching(repoID, before)), limit), nil
}

// finalize applies what a unit wrote to the store, as ClickHouse would: the later row of a run replaces the stored one.
func (reader *fakeOpenRuns) finalize(rows []githubTestsPipelineRow) {
	for _, written := range rows {
		for index := range reader.rows {
			if reader.rows[index].RepoID == written.RepoID && reader.rows[index].RunID == written.RunID {
				reader.rows[index] = written
			}
		}
	}
}

type listingWalk struct {
	pipelines []githubTestsPipelineRow
	jobs      []githubTestsJobRow
	final     CompleteRouteBatch
	cursor    githubTestsChunkCursor
	chunks    int
	passes    int
}

// walkListing drives CollectChunks to its final emission and keeps every pipeline row the unit wrote.
func walkListing(t *testing.T, handler GitHubTestsRouteHandler, claim Claim, fake *listingFake, maxChunks int) listingWalk {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(fake))
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	walk := listingWalk{}
	resume := ""
	stop := errors.New("yield")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for pass := 1; ; pass++ {
		if pass > 100000 {
			t.Fatal("the walk never reached a final emission")
		}
		emitted, last, finalSeen := 0, resume, false
		err := handler.CollectChunks(ctx, claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				for _, effect := range emission.Batch.Effects {
					switch effect.Destination {
					case "ci_pipeline_runs":
						rows, decodeErr := decodeEffectRows[githubTestsPipelineRow](effect)
						if decodeErr != nil {
							t.Fatal(decodeErr)
						}
						walk.pipelines = append(walk.pipelines, rows...)
						// ClickHouse replaces a stored row by a later write: the store the reader sees follows the chunks.
						if store, ok := handler.OpenRuns.(*fakeOpenRuns); ok {
							store.finalize(rows)
						}
					case "ci_job_runs":
						rows, decodeErr := decodeEffectRows[githubTestsJobRow](effect)
						if decodeErr != nil {
							t.Fatal(decodeErr)
						}
						walk.jobs = append(walk.jobs, rows...)
					}
				}
				if emission.Final {
					walk.final, finalSeen = emission.Batch, true
					return nil
				}
				walk.chunks++
				emitted++
				if maxChunks > 0 && emitted >= maxChunks {
					return stop
				}
				return nil
			})
		if finalSeen {
			if err != nil {
				t.Fatalf("final emission returned err=%v", err)
			}
			cursor, decodeErr := decodeGitHubTestsChunkCursor(last)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			walk.cursor = cursor
			walk.passes = pass
			return walk
		}
		if !errors.Is(err, stop) {
			t.Fatalf("pass %d err=%v, want a continuation yield", pass, err)
		}
		resume = last
	}
}

func (walk listingWalk) pipelineIDs() map[string]githubTestsPipelineRow {
	out := map[string]githubTestsPipelineRow{}
	for _, row := range walk.pipelines {
		out[row.RunID] = row
	}
	return out
}

func (walk listingWalk) incompleteComponents() []string {
	var out []string
	for _, observation := range walk.cursor.Incomplete {
		out = append(out, observation.Component+"/"+observation.Cause)
	}
	return out
}

// listingClaim is a claim over [since, before] of the 2026-07-20 day.
func listingClaim(since, before time.Time) Claim {
	claim := nativeTestClaim("github", "tests")
	claim.SinceAt, claim.BeforeAt = &since, &before
	return claim
}

// makeListingRuns builds count completed runs, one every step, ids first..first+count-1 from start.
func makeListingRuns(first, count int, start time.Time, step time.Duration) []listingRun {
	runs := make([]listingRun, 0, count)
	for index := 0; index < count; index++ {
		created := start.Add(time.Duration(index) * step)
		runs = append(runs, listingRun{id: first + index, created: created, updated: created.Add(time.Minute), status: "completed", conclusion: "success"})
	}
	return runs
}

var listingSince = time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)

func mustURL(raw string) *url.URL {
	parsed, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return parsed
}

func TestListingFakeHonoursTheDocumentedCap(t *testing.T) {
	fake := newListingFake(t, makeListingRuns(1, 1500, listingSince, 10*time.Second))
	response := fake.list(&http.Request{URL: mustURL("https://api.github.com/repos/acme/api/actions/runs?created=2026-07-20T00:00:00Z..2026-07-21T00:00:00Z&per_page=100&page=10")}, http.Header{})
	if response.Header.Get("Link") != "" {
		t.Fatal("page 10 of a capped search must have no next link")
	}
}

// listingRepoID is the repository id the route resolves for the fixture repository.
func listingRepoID(t *testing.T) string {
	t.Helper()
	fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Second))
	since, before := listingSince, listingSince.Add(time.Hour)
	walk := walkListing(t, GitHubTestsRouteHandler{}, listingClaim(since, before), fake, 0)
	if len(walk.pipelines) == 0 {
		t.Fatal("the id probe wrote no pipeline row")
	}
	return walk.pipelines[0].RepoID
}

func openRow(repoID, runID string, started time.Time, status string) githubTestsPipelineRow {
	value := status
	finished := started.Add(time.Minute)
	return githubTestsPipelineRow{OrgID: "org-acme", RepoID: repoID, RunID: runID, Provider: "github_actions", Status: &value,
		StartedAt: started, FinishedAt: &finished, LastSynced: started}
}

// openWindowClaim is the hourly claim of 2026-07-20 00:00; the open phase reads rows started before 18:00 the day before.
func openWindowClaim() Claim {
	return listingClaim(listingSince, listingSince.Add(time.Hour))
}

func stringPtrValue(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// 13844: a run first stored open and finished long after is re-read by id and written with its final status. Without
// the open phase it stays open for good.
func TestOpenPhaseRereadsAStoredOpenRunByID(t *testing.T) {
	repoID := listingRepoID(t)
	stale := listingSince.Add(-30 * 24 * time.Hour)
	fake := newListingFake(t, makeListingRuns(1, 2, listingSince, time.Minute))
	fake.byID[7001] = listingRun{id: 7001, created: stale, updated: stale.Add(time.Hour), status: "completed", conclusion: "success"}
	reader := &fakeOpenRuns{rows: []githubTestsPipelineRow{openRow(repoID, "7001", stale, "running")}}

	without := walkListing(t, GitHubTestsRouteHandler{}, openWindowClaim(), fake, 0)
	if _, found := without.pipelineIDs()["7001"]; found {
		t.Fatal("without an open-runs reader the stale run must not be re-read (the defect)")
	}

	fake = newListingFake(t, makeListingRuns(1, 2, listingSince, time.Minute))
	fake.byID[7001] = listingRun{id: 7001, created: stale, updated: stale.Add(time.Hour), status: "completed", conclusion: "success"}
	walk := walkListing(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), fake, 0)
	row, found := walk.pipelineIDs()["7001"]
	if !found || stringPtrValue(row.Status) != "success" {
		t.Fatalf("run 7001 = %#v found=%v, want its final status success", row, found)
	}
	if got := walk.final.Result["open_runs_reread"]; got != 1 {
		t.Fatalf("open_runs_reread = %v, want 1", got)
	}
	if capReached, _ := walk.final.Result["open_runs_cap_reached"].(bool); capReached {
		t.Fatal("one open row is not a cap stop")
	}
	if complete, _ := walk.final.Result["reports_complete"].(bool); !complete {
		t.Fatal("a re-read run is not an incomplete observation")
	}
}

func TestOpenPhaseMarksARunTheProviderNoLongerHasAsDeleted(t *testing.T) {
	repoID := listingRepoID(t)
	stale := listingSince.Add(-40 * 24 * time.Hour)
	fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
	fake.gone[7002] = true
	fake.byID[7003] = listingRun{id: 7003, created: stale, updated: stale.Add(time.Hour), status: "completed", conclusion: "failure"}
	reader := &fakeOpenRuns{rows: []githubTestsPipelineRow{openRow(repoID, "7002", stale, "queued"), openRow(repoID, "7003", stale.Add(time.Minute), "running")}}
	walk := walkListing(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), fake, 0)
	rows := walk.pipelineIDs()
	if got := stringPtrValue(rows["7002"].Status); got != testops.StatusDeleted {
		t.Fatalf("run 7002 status = %q, want %q", got, testops.StatusDeleted)
	}
	if !rows["7002"].StartedAt.Equal(stale) || rows["7002"].PipelineName != nil && *rows["7002"].PipelineName != "" {
		t.Fatalf("a deleted row keeps the stored row: %#v", rows["7002"])
	}
	if got := stringPtrValue(rows["7003"].Status); got != "failure" {
		t.Fatalf("run 7003 status = %q, want failure", got)
	}
	if !rows["7002"].LastSynced.After(stale) {
		t.Fatal("the deleted row needs a later last_synced than the stored row so it replaces it")
	}
}

// The pace: 100 re-reads a unit, 300 while more than 1,000 open rows older than the lookback wait. Rows of another
// repository are never read; the oldest are read first.
func TestOpenPhasePaceFollowsTheBacklog(t *testing.T) {
	repoID := listingRepoID(t)
	fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
	var rows []githubTestsPipelineRow
	first := listingSince.Add(-60 * 24 * time.Hour)
	for index := 0; index < 1500; index++ {
		started := first.Add(time.Duration(index) * time.Minute)
		id := 100000 + index
		rows = append(rows, openRow(repoID, strconv.Itoa(id), started, "running"))
		fake.byID[id] = listingRun{id: id, created: started, updated: started.Add(time.Hour), status: "completed", conclusion: "success"}
	}
	for index := 0; index < 20; index++ {
		rows = append(rows, openRow("other-repo", strconv.Itoa(900000+index), first, "running"))
	}
	reader := &fakeOpenRuns{rows: rows}
	handler := GitHubTestsRouteHandler{OpenRuns: reader}
	var done []int
	for unit := 1; unit <= 4; unit++ {
		walk := walkListing(t, handler, openWindowClaim(), fake, 0)
		reread, _ := walk.final.Result["open_runs_reread"].(int)
		done = append(done, reread)
		if unit == 1 {
			oldest := ""
			for _, row := range walk.pipelines {
				if row.RunID == "100000" {
					oldest = row.RunID
				}
			}
			if oldest == "" {
				t.Fatal("the oldest open row must be read first")
			}
			for _, id := range fake.runGets {
				if id >= 900000 {
					t.Fatalf("read a row of another repository: %d", id)
				}
			}
		}
		reader.finalize(walk.pipelines)
	}
	// 1,500 open rows: units 1 and 2 see a backlog above 1,000 (1,500 then 1,200) and read 300; unit 3 sees 900 and
	// falls to the steady cap 100, as does unit 4 (800).
	if fmt.Sprint(done) != fmt.Sprint([]int{300, 300, 100, 100}) {
		t.Fatalf("re-reads per unit = %v, want [300 300 100 100]", done)
	}
	if reader.repoSeen["other-repo"] {
		t.Fatal("the reader was asked about another repository")
	}
}

func TestOpenPhaseReadsARowWithAnEpochStartTime(t *testing.T) {
	repoID := listingRepoID(t)
	epoch := time.Unix(0, 0).UTC()
	fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
	fake.byID[7004] = listingRun{id: 7004, created: listingSince.Add(-48 * time.Hour), updated: listingSince.Add(-47 * time.Hour), status: "completed", conclusion: "success"}
	reader := &fakeOpenRuns{rows: []githubTestsPipelineRow{openRow(repoID, "7004", epoch, "running")}}
	walk := walkListing(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), fake, 0)
	if got := stringPtrValue(walk.pipelineIDs()["7004"].Status); got != "success" {
		t.Fatalf("run 7004 status = %q, want success", got)
	}
}

func distinctRanges(fake *listingFake) int {
	seen := map[string]bool{}
	for _, created := range fake.listRanges {
		seen[created] = true
	}
	return len(seen)
}

// 13801: GitHub returns at most 1,000 results for a created search. A window of 8,000 runs is read whole by peeling
// the range; without it the unit reads 1,000 and still says it is complete.
func TestRunsListingPeelsPastTheSearchCap(t *testing.T) {
	runs := makeListingRuns(1, 8000, listingSince, 10*time.Second)
	fake := newListingFake(t, runs)
	fake.requestCeiling = 200000
	claim := listingClaim(listingSince, listingSince.Add(24*time.Hour))
	walk := walkListing(t, GitHubTestsRouteHandler{}, claim, fake, 0)
	if got := len(walk.pipelineIDs()); got != 8000 {
		t.Fatalf("pipeline rows = %d, want all 8,000 runs", got)
	}
	if complete, _ := walk.final.Result["reports_complete"].(bool); !complete {
		t.Fatalf("incomplete = %v, want a complete unit", walk.incompleteComponents())
	}
	if walk.final.Watermark == nil || !walk.final.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark = %v, want the window end", walk.final.Watermark)
	}
	if got := distinctRanges(fake); got < 8 {
		t.Fatalf("%d distinct created ranges listed, want at least 8 peels", got)
	}
}

// The artifacts phase peels the same way: every completed run is asked for its artifacts.
func TestArtifactsListingPeelsPastTheSearchCap(t *testing.T) {
	runs := makeListingRuns(1, 3000, listingSince, 10*time.Second)
	fake := newListingFake(t, runs)
	claim := listingClaim(listingSince, listingSince.Add(24*time.Hour))
	walkListing(t, GitHubTestsRouteHandler{}, claim, fake, 0)
	missing := 0
	for _, run := range runs {
		if fake.artifactGets[run.id] == 0 {
			missing++
		}
	}
	if missing != 0 {
		t.Fatalf("%d of 3,000 completed runs never had their artifacts listed", missing)
	}
}

// Fewer than 1,000 runs is one range per phase and no peel; 1,000 fills the cap, so each phase lists one more range;
// 1,001 loses nothing. The claim holds a UTC midnight, so the runs phase and the artifacts phase also list their deep range
// once each (no run there).
func TestPeelStartsOnlyWhenARangeFillsTheCap(t *testing.T) {
	for _, tc := range []struct {
		runs, wantRanges int
	}{{999, 4}, {1000, 6}, {1001, 6}} {
		fake := newListingFake(t, makeListingRuns(1, tc.runs, listingSince, 20*time.Second))
		claim := listingClaim(listingSince, listingSince.Add(24*time.Hour))
		walk := walkListing(t, GitHubTestsRouteHandler{}, claim, fake, 0)
		if got := len(walk.pipelineIDs()); got != tc.runs {
			t.Fatalf("%d runs: %d rows", tc.runs, got)
		}
		if got := distinctRanges(fake); got != tc.wantRanges {
			t.Fatalf("%d runs: %d distinct created ranges listed, want %d", tc.runs, got, tc.wantRanges)
		}
	}
}

// Runs that share the oldest second at the cut are listed again by the next range and kept once.
func TestPeelKeepsRunsThatShareTheCutSecond(t *testing.T) {
	runs := makeListingRuns(1, 998, listingSince.Add(time.Hour), 10*time.Second)
	for index := 0; index < 5; index++ {
		runs = append(runs, listingRun{id: 5000 + index, created: listingSince, updated: listingSince.Add(time.Minute), status: "completed", conclusion: "success"})
	}
	fake := newListingFake(t, runs)
	claim := listingClaim(listingSince, listingSince.Add(24*time.Hour))
	walk := walkListing(t, GitHubTestsRouteHandler{}, claim, fake, 0)
	rows := walk.pipelineIDs()
	for index := 0; index < 5; index++ {
		if _, found := rows[strconv.Itoa(5000+index)]; !found {
			t.Fatalf("run %d sharing the cut second was lost", 5000+index)
		}
	}
	if len(rows) != 1003 {
		t.Fatalf("rows = %d, want 1,003 distinct runs", len(rows))
	}
}

// More runs in one second than the cap allows cannot be walked past: the unit says so, withholds its watermark and
// ends. The fake fails the cell if it is asked for more than a bounded number of requests.
func TestPeelProgressGuardEndsAStuckRange(t *testing.T) {
	runs := make([]listingRun, 0, 1100)
	for index := 0; index < 1100; index++ {
		runs = append(runs, listingRun{id: 1 + index, created: listingSince.Add(time.Hour), updated: listingSince.Add(time.Hour + time.Minute), status: "completed", conclusion: "success"})
	}
	fake := newListingFake(t, runs)
	fake.requestCeiling = 6000
	claim := listingClaim(listingSince, listingSince.Add(24*time.Hour))
	walk := walkListing(t, GitHubTestsRouteHandler{}, claim, fake, 0)
	got := fmt.Sprint(walk.final.Result["incomplete"])
	for _, want := range []string{githubTestsRunInventorySaturatedComponent, githubTestsArtifactInventorySaturatedComponent} {
		if !strings.Contains(got, want) {
			t.Fatalf("incomplete = %s, want %s", got, want)
		}
	}
	if walk.final.Watermark != nil {
		t.Fatalf("watermark = %v, want withheld for a range that could not be walked", walk.final.Watermark)
	}
	if complete, _ := walk.final.Result["reports_complete"].(bool); complete {
		t.Fatal("a saturated range is not complete")
	}
}

// Beyond the 100-page phase budget the unit records the page budget marker and withholds the watermark.
func TestPageBudgetStillStopsAWindowOfMoreThanTenThousandRuns(t *testing.T) {
	fake := newListingFake(t, makeListingRuns(1, 11000, listingSince, 8*time.Second))
	claim := listingClaim(listingSince, listingSince.Add(25*time.Hour))
	walk := walkListing(t, GitHubTestsRouteHandler{}, claim, fake, 0)
	found := false
	for _, observation := range walk.cursor.Incomplete {
		if observation.Component == githubTestsRunInventoryComponent && observation.Cause == githubTestsPageBudgetCause {
			found = true
		}
	}
	if !found {
		t.Fatalf("incomplete = %v, want run_inventory/page_budget_exhausted", walk.incompleteComponents())
	}
	if walk.final.Watermark != nil {
		t.Fatalf("watermark = %v, want withheld", walk.final.Watermark)
	}
}

// 13821: a run created inside the window is kept whatever its latest attempt's start: a rerun that started after the
// window end and a run queued across it.
func TestRunCreatedInTheWindowIsKeptWhateverItsLatestAttemptStart(t *testing.T) {
	since, before := listingSince, listingSince.Add(time.Hour)
	runs := []listingRun{
		{id: 9000, created: since.Add(10 * time.Minute), updated: since.Add(20 * time.Minute), status: "completed", conclusion: "success"},
		{id: 9001, created: since.Add(40 * time.Minute), startedAt: since.Add(90 * time.Minute), updated: since.Add(95 * time.Minute), status: "completed", conclusion: "success"},
		{id: 9002, created: before.Add(-10 * time.Second), startedAt: before.Add(10 * time.Second), updated: before.Add(time.Minute), status: "queued"},
	}
	fake := newListingFake(t, runs)
	walk := walkListing(t, GitHubTestsRouteHandler{}, listingClaim(since, before), fake, 0)
	rows := walk.pipelineIDs()
	for _, id := range []string{"9000", "9001", "9002"} {
		if _, found := rows[id]; !found {
			t.Fatalf("run %s has no pipeline row; the window rule drops a run by the start of its latest attempt", id)
		}
	}
	for _, job := range []int{9000, 9001, 9002} {
		if fake.jobGets[job] == 0 {
			t.Fatalf("run %d had no jobs read", job)
		}
	}
}

// The non-chunked oracle follows the same rule and returns the same runs as the chunked route.
func TestOracleAndChunkedRouteAgreeOnTheRuns(t *testing.T) {
	since, before := listingSince, listingSince.Add(time.Hour)
	runs := []listingRun{
		{id: 9000, created: since.Add(10 * time.Minute), updated: since.Add(20 * time.Minute), status: "completed", conclusion: "success"},
		{id: 9001, created: since.Add(40 * time.Minute), startedAt: since.Add(90 * time.Minute), updated: since.Add(95 * time.Minute), status: "completed", conclusion: "failure"},
		{id: 9002, created: since.Add(-3 * time.Hour), updated: since.Add(30 * time.Minute), status: "completed", conclusion: "success"},
		{id: 9003, created: since.Add(-3 * time.Hour), updated: since.Add(-2 * time.Hour), status: "completed", conclusion: "success"},
	}
	chunked := walkListing(t, GitHubTestsRouteHandler{}, listingClaim(since, before), newListingFake(t, runs), 0)
	oracleFake := newListingFake(t, runs)
	batch, err := GitHubTestsRouteHandler{}.Collect(context.Background(), listingClaim(since, before), providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(oracleFake)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	oracle := map[string]bool{}
	for _, effect := range batch.Effects {
		if effect.Destination != "ci_pipeline_runs" {
			continue
		}
		rows, decodeErr := decodeEffectRows[githubTestsPipelineRow](effect)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		for _, row := range rows {
			oracle[row.RunID] = true
		}
	}
	got := chunked.pipelineIDs()
	for _, id := range []string{"9000", "9001", "9002"} {
		if !oracle[id] {
			t.Fatalf("the oracle dropped run %s", id)
		}
		if _, found := got[id]; !found {
			t.Fatalf("the chunked route dropped run %s", id)
		}
	}
	if oracle["9003"] {
		t.Fatal("the oracle kept run 9003: created before the window, terminal, not changed since the window start")
	}
	if _, found := got["9003"]; found {
		t.Fatal("the chunked route kept run 9003: created before the window, terminal, not changed since the window start")
	}
}

// The oracle peels a saturated range like the chunked route does.
func TestOraclePeelsPastTheSearchCap(t *testing.T) {
	fake := newListingFake(t, makeListingRuns(1, 2500, listingSince, 20*time.Second))
	claim := listingClaim(listingSince, listingSince.Add(24*time.Hour))
	batch, err := GitHubTestsRouteHandler{}.Collect(context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(fake)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for _, effect := range batch.Effects {
		if effect.Destination == "ci_pipeline_runs" {
			rows += len(effect.Rows)
		}
	}
	if rows != 2500 {
		t.Fatalf("oracle pipeline rows = %d, want 2,500", rows)
	}
}

// The refresh lookback: a run created before the window is refreshed only when it changed since the window start or is
// still open; one created before the lookback is not listed at all.
func TestRunsPhaseRefreshesOnlyChangedRunsOfTheLookback(t *testing.T) {
	since, before := listingSince.Add(12*time.Hour), listingSince.Add(13*time.Hour)
	runs := []listingRun{
		{id: 11, created: since.Add(-4 * time.Hour), updated: since.Add(-4*time.Hour + 5*time.Minute), status: "completed", conclusion: "success"},
		{id: 12, created: since.Add(-4 * time.Hour), updated: since.Add(30 * time.Minute), status: "completed", conclusion: "success"},
		{id: 13, created: since.Add(-3 * time.Hour), updated: since.Add(-3*time.Hour + 10*time.Minute), status: "in_progress"},
		{id: 14, created: since.Add(-8 * time.Hour), updated: since.Add(30 * time.Minute), status: "completed", conclusion: "success"},
		{id: 15, created: since.Add(30 * time.Minute), updated: since.Add(35 * time.Minute), status: "completed", conclusion: "success"},
	}
	fake := newListingFake(t, runs)
	walk := walkListing(t, GitHubTestsRouteHandler{}, listingClaim(since, before), fake, 0)
	rows := walk.pipelineIDs()
	for id, want := range map[string]bool{"11": false, "12": true, "13": true, "14": false, "15": true} {
		if _, found := rows[id]; found != want {
			t.Fatalf("run %s written = %v, want %v", id, found, want)
		}
	}
	if fake.jobGets[11] != 0 {
		t.Fatal("an unchanged terminal run of the lookback must not have its jobs read")
	}
}

// Governor: the open phase reads the rate-limit headers of the unit's own responses and stops under the floor, which
// is the larger of 500 and a fifth of the limit; missing headers count as the floor hit. The listing phases ignore it.
func TestOpenPhaseStopsUnderTheRateLimitFloor(t *testing.T) {
	repoID := listingRepoID(t)
	stale := listingSince.Add(-30 * 24 * time.Hour)
	for _, tc := range []struct {
		name             string
		remaining, limit int
		noHeaders        bool
		wantReread       int
		wantMarker       any
	}{
		{"under the floor of a fifth of 5,000", 999, 5000, false, 0, int64(999)},
		{"at the floor drains", 1000, 5000, false, 3, nil},
		{"under the 500 minimum of a small limit", 499, 1000, false, 0, int64(499)},
		{"above the 500 minimum of a small limit", 501, 1000, false, 3, nil},
		{"missing headers fail closed", 0, 0, true, 0, int64(-1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newListingFake(t, makeListingRuns(1, 2, listingSince, time.Minute))
			fake.remaining, fake.limit, fake.noHeaders = tc.remaining, tc.limit, tc.noHeaders
			var rows []githubTestsPipelineRow
			for index := 0; index < 3; index++ {
				id := 7100 + index
				started := stale.Add(time.Duration(index) * time.Minute)
				rows = append(rows, openRow(repoID, strconv.Itoa(id), started, "running"))
				fake.byID[id] = listingRun{id: id, created: started, updated: started.Add(time.Hour), status: "completed", conclusion: "success"}
			}
			walk := walkListing(t, GitHubTestsRouteHandler{OpenRuns: &fakeOpenRuns{rows: rows}}, openWindowClaim(), fake, 0)
			if got := walk.final.Result["open_runs_reread"]; got != tc.wantReread {
				t.Fatalf("open_runs_reread = %v, want %d", got, tc.wantReread)
			}
			marker, hit := walk.final.Result["open_phase_rate_floor"]
			if tc.wantMarker == nil && hit || tc.wantMarker != nil && (!hit || marker != tc.wantMarker) {
				t.Fatalf("open_phase_rate_floor = %v (present %v), want %v", marker, hit, tc.wantMarker)
			}
			if _, found := walk.pipelineIDs()["1"]; !found {
				t.Fatal("the listing phases must not be stopped by the open-phase floor")
			}
			if complete, _ := walk.final.Result["reports_complete"].(bool); !complete {
				t.Fatal("the floor marker must not make the unit incomplete or block the watermark")
			}
			if walk.final.Watermark == nil {
				t.Fatal("the floor marker must not withhold the watermark")
			}
		})
	}
}

// The cursor of each phase at its worst case stays well inside maxChunkCursorBytes: the base worst case of the budget
// cell (every closed-vocabulary pair, long URL, six-digit counters) plus the fields of that phase at their widest.
func TestChunkCursorWorstCaseOfEachPhaseKeepsTheMargin(t *testing.T) {
	maxID := strings.Repeat("9", githubTestsMaxArtifactIDBytes)
	for _, tc := range []struct {
		name  string
		floor int
		apply func(c *githubTestsChunkCursor)
	}{
		{"runs peel", 300, func(c *githubTestsChunkCursor) {
			c.Phase = githubTestsRunsPhase
			c.RangeHi, c.RangeOldest, c.RangeItems = "2026-07-20T23:59:59Z", "2026-07-20T00:00:00Z", githubTestsSearchResultCap
			c.SaturatedRuns, c.SaturatedArtifacts = true, true
			c.ActiveCutKeys, c.CarriedCutKeys = githubTestsWidestRunsCutKeys(t), githubTestsWidestRunsCutKeys(t)
			c.RangeOldestItems = githubTestsSearchResultCap
			c.AnchorID = githubTestsWidestAnchorID
			c.ReseekPages, c.PagesEntered, c.RecordCredit = githubTestsWidestReseekPages, githubTestsWidestReseekPages, githubTestsWidestRecordCredit
		}},
		// The deep range's floor is 250 (CHAOS-14385, acp ruling): its runs cut lists keep the 882 bytes main writes (a smaller
		// cap re-read a dense cut second and read fewer runs than main at the record bound), and its cursor holds the re-seek
		// state and its record count; it has no diagnostic-only sample to cut. Its 256 bytes are the tightest point of this
		// cursor: a later field pays for itself.
		{"runs deep range peel", 250, func(c *githubTestsChunkCursor) {
			c.Phase = githubTestsRunsPhase
			c.LateRuns, c.LateRunPages = true, githubTestsLateRerunMaxPages
			c.RangeHi, c.RangeOldest, c.RangeItems = "2026-07-20T23:59:59Z", "2026-07-20T00:00:00Z", githubTestsSearchResultCap
			c.SaturatedRuns, c.SaturatedArtifacts = true, true
			c.LateRunsBudget, c.LateRunsSaturated = true, true
			c.ActiveCutKeys, c.CarriedCutKeys = githubTestsWidestRunsCutKeys(t), githubTestsWidestRunsCutKeys(t)
			c.RangeOldestItems = githubTestsSearchResultCap
			c.AnchorID = githubTestsWidestAnchorID
			// The deep range's own record count rides its cursor too (CHAOS-14385: it was missing from this worst case). A deep
			// search is a created range under GitHub's 1,000-result cap: at most 10 pages entered, at most 10 free.
			deepPages := githubTestsSearchResultCap / nativePerPage
			c.ReseekPages, c.PagesEntered, c.RecordCredit = deepPages, deepPages, githubTestsWidestRecordCredit
			c.LateRecords = githubTestsMaxWalkRecords
		}},
		{"artifacts peel", 300, func(c *githubTestsChunkCursor) {
			c.Phase = githubTestsArtifactsPhase
			c.RangeHi, c.RangeOldest, c.RangeItems = "2026-07-20T23:59:59Z", "2026-07-20T00:00:00Z", githubTestsSearchResultCap
			c.SaturatedRuns, c.SaturatedArtifacts = true, true
			c.ActiveCutKeys, c.CarriedCutKeys = githubTestsWidestCutKeys(t), githubTestsWidestCutKeys(t)
			c.AnchorID = githubTestsWidestAnchorID
			c.ReseekPages, c.PagesEntered, c.RecordCredit = githubTestsWidestReseekPages, githubTestsWidestReseekPages, githubTestsWidestRecordCredit
			c.LateRecords = githubTestsMaxWalkRecords
		}},
		{"open phase", 300, func(c *githubTestsChunkCursor) {
			c.Phase = githubTestsOpenPhase
			c.NextURL, c.Index = "", 0
			c.OpenAfterStarted, c.OpenAfterRunID = "2026-07-20T23:59:59.999999999Z", maxID
			c.OpenCap, c.OpenDone, c.OpenCapReached = githubTestsOpenRunCapBacklog, githubTestsOpenRunCapBacklog, true
			c.OpenFloorHit, c.OpenFloorRemaining = true, 99999999
			c.OpenSkipped, c.OpenTransientRun, c.OpenSkipLimitHit = githubTestsOpenRunCapBacklog, githubTestsOpenTransientBreaker, true
			c.OpenSkipEvent = &githubTestsOpenSkipEvent{RunID: maxID, Reason: "jobs_not_found", Status: 599, Route: githubRunJobsRoute}
			c.SaturatedRuns, c.SaturatedArtifacts = true, true
		}},
	} {
		cursor := githubTestsRealisticCursorBase()
		cursor.SkippedArtifactsOverflow = 999999
		tc.apply(&cursor)
		encoded, err := encodeGitHubTestsChunkCursor(cursor)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		t.Logf("%s: worst-case cursor %d bytes", tc.name, len(encoded))
		if remaining := maxChunkCursorBytes - len(encoded); remaining < tc.floor {
			t.Fatalf("%s: worst-case cursor %d bytes leaves %d under maxChunkCursorBytes, want at least %d", tc.name, len(encoded), remaining, tc.floor)
		}
		decoded, decodeErr := decodeGitHubTestsChunkCursor(encoded)
		if decodeErr != nil || decoded.Phase != cursor.Phase {
			t.Fatalf("%s: decode = %v %v", tc.name, decoded.Phase, decodeErr)
		}
	}
}

// A cursor written before the peel and the open phase decodes and resumes.
func TestACursorFromBeforeThePeelStillDecodes(t *testing.T) {
	for _, raw := range []string{
		`{"phase":"artifacts","next_url":"https://api.github.com/x?page=2","index":3,"pipelines":1,"jobs":0,"acceptance":0,"suites":0,"cases":0,"coverage":0,"requests":9,"pages":4,"run_pages":2,"artifact_pages":2,"repo":"acme/api","archives_seen":0,"archives_unreadable":0}`,
		`{"phase":"runs","pipelines":0,"jobs":0,"acceptance":0,"suites":0,"cases":0,"coverage":0,"requests":1,"pages":0,"run_pages":0,"artifact_pages":0,"repo":"acme/api","archives_seen":0,"archives_unreadable":0}`,
		`{"phase":"open","pipelines":0,"jobs":0,"acceptance":0,"suites":0,"cases":0,"coverage":0,"requests":1,"pages":0,"run_pages":2,"artifact_pages":2,"repo":"acme/api","archives_seen":0,"archives_unreadable":0,"oc":100,"od":4}`,
	} {
		if _, err := decodeGitHubTestsChunkCursor(raw); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
	}
	if _, err := decodeGitHubTestsChunkCursor(`{"phase":"open","oc":-1,"repo":"x","archives_seen":0,"archives_unreadable":0}`); err == nil {
		t.Fatal("a negative open cap is a corrupt cursor")
	}
}

// The open phase over several invocations (the walk yields every 40 chunks): the backlog is read once, the cap is fixed
// at the phase start (300 for 1,100 rows although the backlog falls under 1,000 as rows are finalized), each row is
// re-read once and the floor marker never shows.
func TestOpenPhaseCapAndBacklogAreFixedAtThePhaseStartAcrossInvocations(t *testing.T) {
	repoID := listingRepoID(t)
	fake := newListingFake(t, makeListingRuns(1, 1, listingSince, time.Minute))
	first := listingSince.Add(-60 * 24 * time.Hour)
	var rows []githubTestsPipelineRow
	for index := 0; index < 1100; index++ {
		started := first.Add(time.Duration(index) * time.Minute)
		id := 200000 + index
		rows = append(rows, openRow(repoID, strconv.Itoa(id), started, "running"))
		fake.byID[id] = listingRun{id: id, created: started, updated: started.Add(time.Hour), status: "completed", conclusion: "success"}
	}
	reader := &fakeOpenRuns{rows: rows}
	walk := walkListing(t, GitHubTestsRouteHandler{OpenRuns: reader}, openWindowClaim(), fake, 40)
	if walk.passes < 3 {
		t.Fatalf("the walk took %d invocations, want at least 3 to cross the resumes", walk.passes)
	}
	if got := walk.final.Result["open_runs_reread"]; got != 300 {
		t.Fatalf("open_runs_reread = %v, want the cap 300 fixed at the phase start", got)
	}
	if reader.backlogs != 1 {
		t.Fatalf("the backlog was read %d times, want once", reader.backlogs)
	}
	seen := map[int]int{}
	for _, id := range fake.runGets {
		seen[id]++
	}
	if len(seen) != 300 || len(fake.runGets) != 300 {
		t.Fatalf("%d by-id reads over %d distinct runs, want 300 once each", len(fake.runGets), len(seen))
	}
	if _, hit := walk.final.Result["open_phase_rate_floor"]; hit {
		t.Fatal("no floor marker is expected")
	}
	if capReached, _ := walk.final.Result["open_runs_cap_reached"].(bool); !capReached {
		t.Fatal("the cap stopped the phase")
	}
	if walk.cursor.OpenCap != 300 {
		t.Fatalf("cursor open cap = %d, want 300", walk.cursor.OpenCap)
	}
}

// The 8,000-run split resumed in the middle of pages (the walk yields every 37 chunks) writes the same rows as the
// unbroken walk: no run is lost and a resume adds no duplicate.
func TestRunsListingPeelResumesMidPageWithoutLoss(t *testing.T) {
	runs := makeListingRuns(1, 8000, listingSince, 10*time.Second)
	claim := listingClaim(listingSince, listingSince.Add(24*time.Hour))
	unbrokenFake := newListingFake(t, runs)
	unbrokenFake.requestCeiling = 2000000
	unbroken := walkListing(t, GitHubTestsRouteHandler{}, claim, unbrokenFake, 0)
	fake := newListingFake(t, runs)
	fake.requestCeiling = 2000000
	walk := walkListing(t, GitHubTestsRouteHandler{}, claim, fake, 37)
	if walk.passes < 100 {
		t.Fatalf("the walk took %d invocations, want it split into many", walk.passes)
	}
	ids := walk.pipelineIDs()
	if len(ids) != 8000 {
		t.Fatalf("%d distinct runs, want 8,000", len(ids))
	}
	if len(walk.pipelines) != len(unbroken.pipelines) {
		t.Fatalf("resumed walk wrote %d rows, unbroken walk %d: a resume must add none", len(walk.pipelines), len(unbroken.pipelines))
	}
	if complete, _ := walk.final.Result["reports_complete"].(bool); !complete {
		t.Fatalf("incomplete = %v", walk.incompleteComponents())
	}
	if walk.final.Watermark == nil || !walk.final.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark = %v, want the window end", walk.final.Watermark)
	}
}
