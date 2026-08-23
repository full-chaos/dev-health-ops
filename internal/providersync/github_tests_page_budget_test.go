package providersync

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTestsPagedDoer serves a fixed number of Actions listing pages to BOTH
// inventory phases. The runs phase lists /actions/runs unfiltered; the
// artifacts phase lists the same path with ?branch=main, so the two are
// counted separately here exactly as they are budgeted separately in the
// route. rel="next" preserves the whole query, the way GitHub's own Link
// header does -- a resumed InitialURL that silently dropped ?branch would make
// the artifacts phase indistinguishable from the runs phase.
type githubTestsPagedDoer struct {
	t                    *testing.T
	pages                int
	perPage              int
	runListRequests      int
	artifactListRequests int
}

func (doer *githubTestsPagedDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		query := request.URL.Query()
		page := 1
		if raw := query.Get("page"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				doer.t.Fatalf("invalid page query %q: %v", raw, err)
			}
			page = parsed
		}
		if page < 1 || page > doer.pages {
			doer.t.Fatalf("unexpected workflow-runs page %d", page)
		}
		if query.Get("branch") != "" {
			doer.artifactListRequests++
		} else {
			doer.runListRequests++
		}
		if page < doer.pages {
			next := *request.URL
			forward := next.Query()
			forward.Set("page", strconv.Itoa(page+1))
			next.RawQuery = forward.Encode()
			header.Set("Link", "<"+next.String()+">; rel=\"next\"")
		}
		first := (page-1)*doer.perPage + 1
		return githubTestsHTTPResponse(
			request, header, githubTestsWorkflowRunsFixture(first, first+doer.perPage-1)), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/runs/") && strings.HasSuffix(path, "/artifacts"):
		return githubTestsHTTPResponse(request, header, `{"artifacts":[]}`), nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

var errGitHubTestsWalkContinuation = errors.New("test continuation yield")

type githubTestsWalk struct {
	cursor githubTestsChunkCursor
	final  CompleteRouteBatch
	passes int
	chunks int
}

// walkGitHubTestsChunks drives CollectChunks the way the chunked stream
// executor drives it in production: every pass stops after maxChunks
// non-final emissions and the next pass resumes from the CursorAfter of the
// last emission it committed. That is what makes a page get re-entered, and
// re-entry is the whole subject of CHAOS-4130.
func walkGitHubTestsChunks(
	t *testing.T,
	handler GitHubTestsRouteHandler,
	claim Claim,
	client *providerfoundation.HTTPClient,
	maxChunks int,
) githubTestsWalk {
	t.Helper()
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	walk := githubTestsWalk{}
	resume := ""
	for {
		walk.passes++
		if walk.passes > 500 {
			t.Fatal("continuation walk never reached a final emission")
		}
		emitted := 0
		last := resume
		finalSeen := false
		err := handler.CollectChunks(
			context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				if emission.Final {
					walk.final = emission.Batch
					finalSeen = true
					return nil
				}
				walk.chunks++
				emitted++
				if emitted >= maxChunks {
					return errGitHubTestsWalkContinuation
				}
				return nil
			},
		)
		if finalSeen {
			if err != nil {
				t.Fatalf("final emission returned err=%v", err)
			}
			cursor, decodeErr := decodeGitHubTestsChunkCursor(last)
			if decodeErr != nil {
				t.Fatalf("decode terminal cursor: %v", decodeErr)
			}
			walk.cursor = cursor
			return walk
		}
		if !errors.Is(err, errGitHubTestsWalkContinuation) {
			t.Fatalf("pass %d err=%v, want a continuation yield", walk.passes, err)
		}
		resume = last
	}
}

// A continuation re-GETs the page it stopped inside and discards the
// already-consumed prefix. Both cumulative page counters must charge a page
// exactly once, on FIRST entry -- counting visits shrank a 100-page budget to
// about 7.6 real pages and cancelled every busy repository (CHAOS-4130).
func TestGitHubTestsChunkRouteCountsEachInventoryPageOnce(t *testing.T) {
	doer := &githubTestsPagedDoer{t: t, pages: 2, perPage: 3}
	claim := nativeTestClaim("github", "tests")
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, claim, githubTestsClient(t, doer), 2)

	if walk.cursor.RunPages != 2 {
		t.Fatalf("RunPages=%d, want 2 (one charge per real page, not per visit)", walk.cursor.RunPages)
	}
	if walk.cursor.ArtifactPages != 2 {
		t.Fatalf("ArtifactPages=%d, want 2 (the artifacts twin counts the same way)", walk.cursor.ArtifactPages)
	}
	// Anti-vacuity: if the walk never re-entered a page there would be no
	// re-visit to miscount and the assertions above would hold under the bug
	// too. More listing requests than real pages is the proof it did.
	if doer.runListRequests <= 2 {
		t.Fatalf("runs phase made %d listing request(s); the walk never re-entered a page", doer.runListRequests)
	}
	if doer.artifactListRequests <= 2 {
		t.Fatalf("artifacts phase made %d listing request(s); the walk never re-entered a page", doer.artifactListRequests)
	}
	if walk.cursor.Pages < doer.runListRequests+doer.artifactListRequests {
		t.Fatalf(
			"cursor.Pages=%d understates %d fetched listing pages; evidence must stay per-visit",
			walk.cursor.Pages, doer.runListRequests+doer.artifactListRequests,
		)
	}
}

// A budget stop on a unit holding durable rows must finalize, not cancel.
// Before CHAOS-4130 it returned ErrPaginationCapExceeded, which providerunit
// maps to a deterministic-terminal category: the unit was cancelled, its
// checkpoint deleted, and the same window re-planned from page one forever.
func TestGitHubTestsChunkRouteFinalizesTruncatedInventoryInsteadOfCancelling(t *testing.T) {
	doer := &githubTestsPagedDoer{t: t, pages: 3, perPage: 2}
	claim := nativeTestClaim("github", "tests")
	// MaxRuns=100 is one page of budget per phase against three real pages.
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{MaxRuns: 100}, claim, githubTestsClient(t, doer), 2)

	if walk.chunks == 0 {
		t.Fatal("truncated unit committed nothing; the rows before the cap must still land")
	}
	if walk.cursor.Phase != "done" {
		t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
	}
	if doer.runListRequests == 0 || doer.artifactListRequests == 0 {
		t.Fatalf(
			"a truncated runs phase must not skip the artifacts phase (runs=%d artifacts=%d)",
			doer.runListRequests, doer.artifactListRequests,
		)
	}
	want := map[string]bool{
		githubTestsRunInventoryComponent:      false,
		githubTestsArtifactInventoryComponent: false,
	}
	for _, observation := range walk.cursor.Incomplete {
		if observation.Cause != githubTestsPageBudgetCause {
			continue
		}
		if _, tracked := want[observation.Component]; tracked {
			want[observation.Component] = true
		}
	}
	for component, seen := range want {
		if !seen {
			t.Fatalf("no %s page-budget observation in %+v", component, walk.cursor.Incomplete)
		}
	}
	// GitHub returns runs newest-first, so a truncated walk covers the NEW end
	// of the window and never reaches the old one. Advancing the watermark
	// there would turn that into a permanent hole (CHAOS-2587).
	if walk.final.Watermark != nil {
		t.Fatalf("truncated unit advanced its watermark to %v", walk.final.Watermark)
	}
	if complete, ok := walk.final.Result["reports_complete"].(bool); !ok || complete {
		t.Fatalf("reports_complete=%v, want false on a truncated unit", walk.final.Result["reports_complete"])
	}
	// The production comparator is the fail-closed gate the chunked executor
	// runs before any completion is durable. A truncated unit must pass it.
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, walk.final,
	); err != nil {
		t.Fatalf("production comparator rejected a truncated completion: %v", err)
	}
}

// The completion vocabulary is closed. A route cannot publish an observation
// downstream coverage readers have no meaning for.
func TestGitHubTestsIncompleteVocabularyIsClosed(t *testing.T) {
	cases := []struct {
		observation GitHubTestsIncomplete
		allowed     bool
	}{
		{GitHubTestsIncomplete{Component: githubTestsReportMemberComponent, Cause: "malformed", Count: 1}, true},
		{GitHubTestsIncomplete{Component: githubTestsReportMemberComponent, Cause: "unreadable", Count: 1}, true},
		{GitHubTestsIncomplete{Component: githubTestsRunInventoryComponent, Cause: githubTestsPageBudgetCause, Count: 1}, true},
		{GitHubTestsIncomplete{Component: githubTestsArtifactInventoryComponent, Cause: githubTestsPageBudgetCause, Count: 1}, true},
		{GitHubTestsIncomplete{Component: githubTestsRunInventoryComponent, Cause: "malformed", Count: 1}, false},
		{GitHubTestsIncomplete{Component: githubTestsReportMemberComponent, Cause: githubTestsPageBudgetCause, Count: 1}, false},
		{GitHubTestsIncomplete{Component: "run_inventories", Cause: githubTestsPageBudgetCause, Count: 1}, false},
		{GitHubTestsIncomplete{Component: "", Cause: "", Count: 1}, false},
	}
	for _, testCase := range cases {
		if got := githubTestsIncompleteInVocabulary(testCase.observation); got != testCase.allowed {
			t.Fatalf("%+v in vocabulary=%v, want %v", testCase.observation, got, testCase.allowed)
		}
		batch := CompleteRouteBatch{Result: map[string]any{
			"reports_complete": false, "reports_skipped": testCase.observation.Count,
			"incomplete": []GitHubTestsIncomplete{testCase.observation},
		}}
		err := validateGitHubTestsCompletion(nativeTestClaim("github", "tests"), batch)
		if testCase.allowed != (err == nil) {
			t.Fatalf("comparator accepted=%v for %+v, want %v", err == nil, testCase.observation, testCase.allowed)
		}
	}
}

// terminalChunkHandler refuses immediately, the way a route does when a
// deterministic fault is detected before it can emit anything.
type terminalChunkHandler struct{ err error }

func (handler terminalChunkHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler terminalChunkHandler) CollectChunks(
	context.Context, Claim, providerfoundation.Credential,
	*providerfoundation.HTTPClient, time.Time, string, func(ChunkRouteEmission) error,
) error {
	return handler.err
}

// The chunked executor must report the checkpoint's CUMULATIVE committed rows
// on its FAILURE path, not only on success. providerunit's
// terminalized-with-committed-rows alarm reads exactly this field, and the
// CHAOS-4130 attempts that died committed nothing of their own -- every row
// they held came from earlier attempts, so Effects.Written was zero on the
// very executions that most needed the alarm.
func TestChunkedExecutorReportsCommittedRowsOnTheFailurePath(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok {
		t.Fatal("descriptor")
	}
	descriptor.ChunkPolicy = ChunkPolicy{
		MaxSourceItems: 1, MaxEffectRows: 1, MaxPreparedBytes: 64 << 10,
		MaxChunksPerAttempt: 8, MaxWallTime: time.Minute,
	}
	store := newChunkMemoryStore()
	sink := &recoveryRowSink{failFrom: 7}
	first := completeRouteExecutor(
		now, &singleItemChunkHandler{batch: threeRowChunkBatch(t, claim)}, store, sink)
	first.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	first.Credentials.Decryptor = chunkedCredentialDecryptor{}
	if _, err := first.Execute(context.Background(), session, descriptor); err == nil {
		t.Fatal("premise broken: attempt 1 was supposed to fail at the sink")
	}
	checkpoint, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.CommittedRows <= 0 {
		t.Fatalf("premise broken: attempt 1 committed %d rows", checkpoint.CommittedRows)
	}

	sink.failFrom = 0
	terminal := completeRouteExecutor(
		now, terminalChunkHandler{err: ErrRepositoryIdentityAmbiguous}, store, sink)
	terminal.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	terminal.Credentials.Decryptor = chunkedCredentialDecryptor{}
	result, execErr := terminal.Execute(context.Background(), session, descriptor)
	if !errors.Is(execErr, ErrRepositoryIdentityAmbiguous) {
		t.Fatalf("attempt 2 err=%v, want the terminal route fault", execErr)
	}
	after, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.CommittedRows != after.CommittedRows {
		t.Fatalf(
			"CommittedRows=%d on the failure path, want the checkpoint's %d",
			result.CommittedRows, after.CommittedRows,
		)
	}
	// Anti-vacuity: the count must include rows this attempt did not write.
	// Sourced from Effects.Written it would report only the drain, which is
	// exactly the undercount that made the CHAOS-4130 cancels invisible.
	if result.CommittedRows <= int64(result.Effects.Written) {
		t.Fatalf(
			"CommittedRows=%d is not above this attempt's %d written rows; it is not cumulative",
			result.CommittedRows, result.Effects.Written,
		)
	}
}

// The paginator's own per-invocation cap (collection.PageBudgetExhausted) is a
// DIFFERENT branch from remainingPageBudget's refusal on entry: the first is
// reached by a pass that walks to the end of its allowance, the second only by
// a resume whose cumulative spend is already gone. Both must finalize, so both
// need a test -- a continuation-driven walk never reaches the first.
func TestGitHubTestsChunkRouteSinglePassCapReachedFinalizes(t *testing.T) {
	doer := &githubTestsPagedDoer{t: t, pages: 3, perPage: 2}
	claim := nativeTestClaim("github", "tests")
	var final CompleteRouteBatch
	terminal := ""
	// MaxRuns=100 is one page of budget per phase against three real pages,
	// and this emit never yields, so each phase walks its whole allowance and
	// stops at the paginator's cap rather than at a cumulative refusal.
	err := (GitHubTestsRouteHandler{MaxRuns: 100}).CollectChunks(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, doer), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(emission ChunkRouteEmission) error {
			terminal = emission.CursorAfter
			if emission.Final {
				final = emission.Batch
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("CollectChunks err=%v, want a finalized unit", err)
	}
	cursor, decodeErr := decodeGitHubTestsChunkCursor(terminal)
	if decodeErr != nil {
		t.Fatalf("decode terminal cursor: %v", decodeErr)
	}
	if cursor.RunPages != 1 || cursor.ArtifactPages != 1 {
		t.Fatalf(
			"premise broken: phases spent RunPages=%d ArtifactPages=%d, want one page each",
			cursor.RunPages, cursor.ArtifactPages,
		)
	}
	want := map[string]bool{
		githubTestsRunInventoryComponent:      false,
		githubTestsArtifactInventoryComponent: false,
	}
	for _, observation := range cursor.Incomplete {
		if observation.Cause == githubTestsPageBudgetCause {
			if _, tracked := want[observation.Component]; tracked {
				want[observation.Component] = true
			}
		}
	}
	for component, seen := range want {
		if !seen {
			t.Fatalf("no %s page-budget observation in %+v", component, cursor.Incomplete)
		}
	}
	if final.Watermark != nil {
		t.Fatalf("truncated unit advanced its watermark to %v", final.Watermark)
	}
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, final,
	); err != nil {
		t.Fatalf("production comparator rejected a truncated completion: %v", err)
	}
}

// The metric dataset label is bounded by an ALLOWLIST duplicated in
// providerfoundation, which cannot import this package to read the registry
// directly. A dataset added here and forgotten there would report as "other"
// forever -- a metric that quietly stops distinguishing the thing it exists to
// distinguish, which is the failure this whole ticket is about. This is the
// only place both sides are visible at once, so the drift check lives here.
func TestEveryRegisteredDatasetHasItsOwnMetricLabel(t *testing.T) {
	seen := 0
	for provider, datasets := range datasetCapabilities {
		for dataset := range datasets {
			seen++
			if label := providerfoundation.MetricDatasetLabel(dataset); label != dataset {
				t.Errorf(
					"%s/%s reports as metric dataset %q; add it to "+
						"providerfoundation.metricDatasetVocabulary",
					provider, dataset, label,
				)
			}
		}
	}
	if seen == 0 {
		t.Fatal("premise broken: the dataset capability registry is empty")
	}
}
