package providersync

import (
	"bytes"
	"context"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"log/slog"
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
		if githubTestsIsArtifactRunListing(query) {
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
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, claim, githubTestsClient(t, fakehttp.Client(doer)), 2)

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
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{MaxRuns: 100}, claim, githubTestsClient(t, fakehttp.Client(doer)), 2)

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
	sink := &recoveryRowSink{failFrom: 8}
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
		githubTestsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
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

// githubTestsStalledBackfillCursor is shaped like the stored cursor of a prod GitHub cicd backfill unit that ran for 13+ hours
// (route_version provider-chunk.v1, inventory_complete false): artifacts phase, both page budgets under 100, a peeled range end
// and about 8M records. next_url points at a page the fake doer serves, so a walk that lists again is seen.
func githubTestsStalledBackfillCursor(t *testing.T, phase, host string) string {
	t.Helper()
	nextURL := "https://" + host + "/repos/acme/api/actions/runs?created=2026-06-30..2026-07-31T23%3A59%3A59Z&page=2&per_page=100"
	if phase == githubTestsRunsPhase {
		nextURL = "https://" + host + "/repos/acme/api/actions/runs?created=2026-06-30T18%3A00%3A00Z..2026-07-31T23%3A59%3A59Z&page=2&per_page=100"
	}
	raw, err := encodeGitHubTestsChunkCursor(githubTestsChunkCursor{
		Phase: phase, NextURL: nextURL, Index: 40,
		Pipelines: 3090, Jobs: 30_000, Acceptance: 9_000, Suites: 120_000, Cases: 7_900_000, Coverage: 0,
		Requests: 17_192, Pages: 9054, RunPages: 45, ArtifactPages: 58,
		RangeHi: "2026-07-25T18:45:43Z", RangeItems: 236, Repo: "acme/api",
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func githubTestsHasPageBudgetObservation(cursor githubTestsChunkCursor, component string) bool {
	for _, observation := range cursor.Incomplete {
		if observation.Component == component && observation.Cause == githubTestsPageBudgetCause {
			return true
		}
	}
	return false
}

// A unit whose stored cursor already holds more records than one walk may write ends at the FIRST attempt after the change:
// no listing page is fetched, the inventory is recorded truncated, and the unit publishes its completion. Before the bound
// such a unit listed on for as long as its page budgets lasted, at up to 1,000 chunks per run.
func TestGitHubTestsChunkRouteStoredCursorPastWalkBoundEndsWithoutListing(t *testing.T) {
	for _, phase := range []string{githubTestsArtifactsPhase, githubTestsRunsPhase} {
		t.Run(phase, func(t *testing.T) {
			doer := &githubTestsPagedDoer{t: t, pages: 3, perPage: 50}
			claim := nativeTestClaim("github", "cicd")
			claim.Mode = "backfill"
			var logged bytes.Buffer
			swapDefaultLogger(t, slog.New(slog.NewJSONHandler(&logged, nil)))
			var final *CompleteRouteBatch
			terminal, nonFinal := "", 0
			err := (GitHubTestsRouteHandler{}).CollectChunks(
				context.Background(), claim, providerfoundation.Credential{},
				githubTestsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC),
				githubTestsStalledBackfillCursor(t, phase, "api.github.com"),
				func(emission ChunkRouteEmission) error {
					terminal = emission.CursorAfter
					if emission.Final {
						batch := emission.Batch
						final = &batch
						return nil
					}
					nonFinal++
					if nonFinal >= defaultMaxChunksPerAttempt {
						return errGitHubTestsWalkContinuation
					}
					return nil
				},
			)
			if err != nil {
				t.Fatalf("CollectChunks err=%v, want the unit to finalize on this attempt", err)
			}
			if final == nil {
				t.Fatal("no final emission: the walk did not end at the first attempt")
			}
			if doer.runListRequests != 0 || doer.artifactListRequests != 0 {
				t.Fatalf("listing requests runs=%d artifacts=%d, want none past the walk bound",
					doer.runListRequests, doer.artifactListRequests)
			}
			if nonFinal != 0 {
				t.Fatalf("%d run emissions past the walk bound, want 0", nonFinal)
			}
			cursor, decodeErr := decodeGitHubTestsChunkCursor(terminal)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if cursor.Phase != "done" {
				t.Fatalf("terminal phase=%q, want done", cursor.Phase)
			}
			if !githubTestsHasPageBudgetObservation(cursor, githubTestsArtifactInventoryComponent) {
				t.Fatalf("no artifact_inventory truncation in %+v", cursor.Incomplete)
			}
			if phase == githubTestsRunsPhase && !githubTestsHasPageBudgetObservation(cursor, githubTestsRunInventoryComponent) {
				t.Fatalf("no run_inventory truncation in %+v", cursor.Incomplete)
			}
			if final.Watermark != nil {
				t.Fatalf("truncated unit advanced its watermark to %v", final.Watermark)
			}
			if complete, ok := final.Result[GitHubTestsResultKeyReportsComplete].(bool); !ok || complete {
				t.Fatalf("reports_complete=%v, want false", final.Result[GitHubTestsResultKeyReportsComplete])
			}
			if _, err := (ProductionContractComparator{}).CompareCompleteRoute(context.Background(), claim, *final); err != nil {
				t.Fatalf("production comparator rejected the bounded completion: %v", err)
			}
			if !strings.Contains(logged.String(), `"msg":"provider walk record bound reached"`) ||
				!strings.Contains(logged.String(), `"mode":"backfill"`) {
				t.Fatalf("no WARN line for the walk bound with the mode in %s", logged.String())
			}
		})
	}
}

// A stored cursor EXACTLY at the bound ends too: the bound is the most a walk writes, not one past it.
func TestGitHubTestsChunkRouteStoredCursorAtWalkBoundEndsWithoutListing(t *testing.T) {
	for _, phase := range []string{githubTestsArtifactsPhase, githubTestsRunsPhase} {
		t.Run(phase, func(t *testing.T) {
			doer := &githubTestsPagedDoer{t: t, pages: 3, perPage: 50}
			stored, err := decodeGitHubTestsChunkCursor(githubTestsStalledBackfillCursor(t, phase, "api.github.com"))
			if err != nil {
				t.Fatal(err)
			}
			stored.Pipelines, stored.Jobs, stored.Acceptance, stored.Suites, stored.Cases, stored.Coverage = 10, 20, 30, 40, 50, 0
			raw, err := encodeGitHubTestsChunkCursor(stored)
			if err != nil {
				t.Fatal(err)
			}
			finalSeen := false
			err = (GitHubTestsRouteHandler{MaxWalkRecords: 150}).CollectChunks(
				context.Background(), nativeTestClaim("github", "cicd"), providerfoundation.Credential{},
				githubTestsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), raw,
				func(emission ChunkRouteEmission) error {
					if !emission.Final {
						return errGitHubTestsWalkContinuation
					}
					finalSeen = true
					return nil
				},
			)
			if err != nil || !finalSeen {
				t.Fatalf("err=%v final=%v, want the unit to finalize on this attempt", err, finalSeen)
			}
			if doer.runListRequests != 0 || doer.artifactListRequests != 0 {
				t.Fatalf("listing requests runs=%d artifacts=%d at the bound, want none", doer.runListRequests, doer.artifactListRequests)
			}
		})
	}
}

// githubTestsSeedCursorHandler emits one empty run chunk whose cursor is a stored cursor of an earlier release, the way a
// running unit's checkpoint holds it when the workers roll.
type githubTestsSeedCursorHandler struct{ cursor string }

func (handler githubTestsSeedCursorHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler githubTestsSeedCursorHandler) CollectChunks(
	_ context.Context, _ Claim, _ providerfoundation.Credential, _ *providerfoundation.HTTPClient, _ time.Time, _ string,
	emit func(ChunkRouteEmission) error,
) error {
	effects, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	if err := emit(ChunkRouteEmission{Batch: CompleteRouteBatch{Effects: effects}, CursorAfter: handler.cursor}); err != nil {
		return err
	}
	return errors.New("seed handler: the attempt was supposed to hand over after its one chunk")
}

// The executor path of the same unit: a checkpoint that holds the stored cursor past the bound reaches inventory end on the
// next attempt, and the published result is the truncated one.
func TestChunkedExecutorEndsGitHubTestsUnitPastWalkBoundAtNextAttempt(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok {
		t.Fatal("descriptor")
	}
	descriptor.ChunkPolicy = ChunkPolicy{
		MaxSourceItems: 100, MaxEffectRows: 500, MaxPreparedBytes: 2 << 20,
		MaxChunksPerAttempt: 1, MaxWallTime: time.Minute,
	}
	store := newChunkMemoryStore()
	sink := &recoveryRowSink{}
	// The executor client takes its base URL from the credential config (https://fixture.test).
	stored := githubTestsStalledBackfillCursor(t, githubTestsArtifactsPhase, "fixture.test")
	seed := completeRouteExecutor(now, githubTestsSeedCursorHandler{cursor: stored}, store, sink)
	seed.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	seed.Credentials.Decryptor = chunkedCredentialDecryptor{}
	if _, err := seed.Execute(context.Background(), session, descriptor); !errors.Is(err, ErrChunkContinuation) {
		t.Fatalf("seed attempt err=%v, want a continuation", err)
	}
	checkpoint, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.InventoryComplete || checkpoint.NextCursor != stored {
		t.Fatalf("premise broken: checkpoint complete=%v cursor matches=%v", checkpoint.InventoryComplete, checkpoint.NextCursor == stored)
	}

	doer := &githubTestsPagedDoer{t: t, pages: 3, perPage: 50}
	next := completeRouteExecutor(now, GitHubTestsRouteHandler{}, store, sink)
	next.Doer = fakehttp.Client(doer)
	next.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	next.Credentials.Decryptor = chunkedCredentialDecryptor{}
	result, err := next.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatalf("next attempt err=%v, want the unit to finalize", err)
	}
	if doer.runListRequests != 0 || doer.artifactListRequests != 0 {
		t.Fatalf("listing requests runs=%d artifacts=%d, want none", doer.runListRequests, doer.artifactListRequests)
	}
	after, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil {
		t.Fatal(err)
	}
	if !after.InventoryComplete || after.TotalChunks != 2 {
		t.Fatalf("checkpoint complete=%v total=%d, want inventory end after the seed chunk and the final chunk",
			after.InventoryComplete, after.TotalChunks)
	}
	if complete, ok := result.Result[GitHubTestsResultKeyReportsComplete].(bool); !ok || complete {
		t.Fatalf("reports_complete=%v, want false", result.Result[GitHubTestsResultKeyReportsComplete])
	}
	if result.Watermark != nil {
		t.Fatalf("watermark=%v, want none on a truncated unit", result.Watermark)
	}
}

// A fresh walk that crosses the bound in the runs phase keeps the runs before the bound, stops at the run where the bound is
// reached, and never lists the artifacts phase.
func TestGitHubTestsChunkRouteWalkBoundStopsInsideTheRunsPhase(t *testing.T) {
	doer := &githubTestsPagedDoer{t: t, pages: 3, perPage: 2}
	claim := nativeTestClaim("github", "tests")
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{MaxWalkRecords: 3}, claim, githubTestsClient(t, fakehttp.Client(doer)), 2)

	if walk.cursor.Pipelines != 3 || githubTestsWalkRecords(walk.cursor) != 3 {
		t.Fatalf("pipelines=%d records=%d, want the walk to stop at the bound of 3",
			walk.cursor.Pipelines, githubTestsWalkRecords(walk.cursor))
	}
	if walk.chunks != 3 {
		t.Fatalf("chunks=%d, want the 3 runs before the bound", walk.chunks)
	}
	if doer.artifactListRequests != 0 {
		t.Fatalf("artifacts phase listed %d page(s) after the bound", doer.artifactListRequests)
	}
	for _, component := range []string{githubTestsRunInventoryComponent, githubTestsArtifactInventoryComponent} {
		if !githubTestsHasPageBudgetObservation(walk.cursor, component) {
			t.Fatalf("no %s truncation in %+v", component, walk.cursor.Incomplete)
		}
	}
	if walk.cursor.Phase != "done" || walk.final.Watermark != nil {
		t.Fatalf("phase=%q watermark=%v, want done and no watermark", walk.cursor.Phase, walk.final.Watermark)
	}
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(context.Background(), claim, walk.final); err != nil {
		t.Fatalf("production comparator rejected the bounded completion: %v", err)
	}
}

// githubTestsWalkTwoRunReports walks the two-run fixture (both runs carry a JUnit report) to its end with no yield.
func githubTestsWalkTwoRunReports(t *testing.T, handler GitHubTestsRouteHandler) (githubTestsChunkCursor, CompleteRouteBatch, int) {
	t.Helper()
	doer := &githubTestsTwoRunDoer{t: t, archive: githubTestsZip(t, map[string]string{"junit.xml": githubTestsJUnitFixture})}
	var final CompleteRouteBatch
	terminal, chunks := "", 0
	err := handler.CollectChunks(
		context.Background(), nativeTestClaim("github", "tests"), providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(emission ChunkRouteEmission) error {
			terminal = emission.CursorAfter
			if emission.Final {
				final = emission.Batch
			} else {
				chunks++
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := decodeGitHubTestsChunkCursor(terminal)
	if err != nil {
		t.Fatal(err)
	}
	return cursor, final, chunks
}

// A walk under the bound is unchanged: every run of both phases, no truncation, the watermark advances. A walk that crosses the
// bound inside the artifacts phase reads the first run's reports and stops before the second run's.
func TestGitHubTestsChunkRouteWalkBoundInsideTheArtifactsPhase(t *testing.T) {
	full, fullFinal, fullChunks := githubTestsWalkTwoRunReports(t, GitHubTestsRouteHandler{})
	if len(full.Incomplete) != 0 || full.Phase != "done" || fullFinal.Watermark == nil {
		t.Fatalf("unbounded walk incomplete=%+v phase=%q watermark=%v, want a complete walk", full.Incomplete, full.Phase, fullFinal.Watermark)
	}
	if fullChunks != 4 || full.Pipelines != 2 || full.Cases == 0 || full.Cases%2 != 0 {
		t.Fatalf("premise broken: chunks=%d pipelines=%d cases=%d, want 2 runs in each phase and the same report on both",
			fullChunks, full.Pipelines, full.Cases)
	}
	// The bound is EXACTLY the records after the first artifacts-phase run: the stop is at the bound, not past it.
	runsPhase := full.Pipelines + full.Jobs + full.Acceptance
	firstRunReports := (githubTestsWalkRecords(full) - runsPhase) / 2
	bounded, boundedFinal, boundedChunks := githubTestsWalkTwoRunReports(t, GitHubTestsRouteHandler{MaxWalkRecords: runsPhase + firstRunReports})
	if bounded.Cases != full.Cases/2 || bounded.Suites != full.Suites/2 {
		t.Fatalf("cases=%d suites=%d, want the first run's reports only (%d, %d)", bounded.Cases, bounded.Suites, full.Cases/2, full.Suites/2)
	}
	if boundedChunks != 3 {
		t.Fatalf("chunks=%d, want 2 runs-phase runs and 1 artifacts-phase run", boundedChunks)
	}
	if githubTestsHasPageBudgetObservation(bounded, githubTestsRunInventoryComponent) ||
		!githubTestsHasPageBudgetObservation(bounded, githubTestsArtifactInventoryComponent) {
		t.Fatalf("incomplete=%+v, want only the artifact inventory truncated", bounded.Incomplete)
	}
	if bounded.Phase != "done" || boundedFinal.Watermark != nil {
		t.Fatalf("phase=%q watermark=%v, want done and no watermark", bounded.Phase, boundedFinal.Watermark)
	}
}

func TestGitHubTestsWalkRecordsCountsEveryRowCounter(t *testing.T) {
	for name, cursor := range map[string]githubTestsChunkCursor{
		"pipelines": {Pipelines: 1}, "jobs": {Jobs: 1}, "acceptance": {Acceptance: 1},
		"suites": {Suites: 1}, "cases": {Cases: 1}, "coverage": {Coverage: 1},
	} {
		if got := githubTestsWalkRecords(cursor); got != 1 {
			t.Fatalf("%s: records=%d, want 1", name, got)
		}
	}
	if got := githubTestsWalkRecords(githubTestsChunkCursor{Requests: 5, Pages: 5, RunPages: 5, ArtifactPages: 5}); got != 0 {
		t.Fatalf("fetch counters counted as records: %d", got)
	}
}

func TestGitHubTestsChunkRouteRefusesAWalkBoundOutsideItsRange(t *testing.T) {
	for _, bound := range []int{-1, githubTestsMaxWalkRecords + 1} {
		doer := &githubTestsPagedDoer{t: t, pages: 1, perPage: 1}
		err := (GitHubTestsRouteHandler{MaxWalkRecords: bound}).CollectChunks(
			context.Background(), nativeTestClaim("github", "tests"), providerfoundation.Credential{},
			githubTestsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
			func(ChunkRouteEmission) error { return nil },
		)
		if !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("MaxWalkRecords=%d err=%v, want ErrInvalidConfiguration", bound, err)
		}
	}
}
