package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// gitLabTestsOversizedRunDoer serves ONE pipeline whose job list is sized by
// the test. The pipelines listing is a single complete page so no INVENTORY
// budget can trip, which isolates the per-run cap as the only reason the
// watermark could be withheld.
type gitLabTestsOversizedRunDoer struct {
	t    *testing.T
	jobs int
	// jobsPerPage, when > 0, serves that many jobs per page and always sets
	// X-Next-Page, so the paginator stops on its PAGE budget rather than on the
	// item cap.
	jobsPerPage int
	jobRequests int
}

func (doer *gitLabTestsOversizedRunDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := make(http.Header)
	body := ""
	path := request.URL.Path
	switch {
	case path == "/api/v4/projects/123":
		body = `{"id":123,"path_with_namespace":"acme/api","default_branch":"main",` +
			`"only_allow_merge_if_pipeline_succeeds":true}`
	case path == "/api/v4/projects/123/pipelines":
		body = `[{"id":9000,"name":"CI","ref":"main","status":"success",` +
			`"created_at":"2026-07-22T10:00:00Z","started_at":"2026-07-22T10:01:00Z",` +
			`"finished_at":"2026-07-22T10:05:00Z","source":"push","sha":"abc",` +
			`"web_url":"https://gitlab.example/acme/api/-/pipelines/9000"}]`
	case strings.HasSuffix(path, "/jobs"):
		doer.jobRequests++
		count := doer.jobs
		if doer.jobsPerPage > 0 {
			count = doer.jobsPerPage
			header.Set("X-Next-Page", strconv.Itoa(doer.jobRequests+1))
		}
		items := make([]string, 0, count)
		for index := 1; index <= count; index++ {
			id := strconv.Itoa(index)
			items = append(items, `{"id":`+id+`,"name":"unit-`+id+`","stage":"test",`+
				`"status":"success","started_at":"2026-07-22T10:01:00Z",`+
				`"finished_at":"2026-07-22T10:04:00Z",`+
				`"runner":{"runner_type":"instance_type"},"artifacts_file":{},"artifacts":[]}`)
		}
		body = "[" + strings.Join(items, ",") + "]"
	case strings.HasSuffix(path, "/test_report"):
		body = `{"test_suites":[]}`
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{
		StatusCode: http.StatusOK, Header: header,
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

type gitLabTestsWalk struct {
	cursor gitLabTestsChunkCursor
	final  CompleteRouteBatch
	chunks int
}

// walkGitLabTestsChunks drives CollectChunks the way the chunked stream
// executor does: each pass stops after maxChunks non-final emissions and the
// next resumes from the last committed CursorAfter.
func walkGitLabTestsChunks(
	t *testing.T, claim Claim, client *providerfoundation.HTTPClient, maxChunks int,
) gitLabTestsWalk {
	t.Helper()
	return walkGitLabTestsChunksWith(t, GitLabTestsRouteHandler{}, claim, client, maxChunks)
}

// walkGitLabTestsChunksWith is the same walk with the handler supplied, so a
// test can vary the PAGE BUDGET. That is the only way to prove the committed
// prefix does not depend on it (CHAOS-4142, codex round 2, challenge 1).
func walkGitLabTestsChunksWith(
	t *testing.T, handler GitLabTestsRouteHandler, claim Claim,
	client *providerfoundation.HTTPClient, maxChunks int,
) gitLabTestsWalk {
	t.Helper()
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	stop := errors.New("test continuation yield")
	walk := gitLabTestsWalk{}
	resume := ""
	for pass := 1; ; pass++ {
		if pass > 500 {
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
					return stop
				}
				return nil
			},
		)
		if finalSeen {
			if err != nil {
				t.Fatalf("final emission returned err=%v", err)
			}
			decoded, decodeErr := decodeGitLabTestsChunkCursor(last)
			if decodeErr != nil {
				t.Fatalf("decode terminal cursor: %v", decodeErr)
			}
			walk.cursor = decoded
			return walk
		}
		if !errors.Is(err, stop) {
			t.Fatalf("pass %d err=%v, want a continuation yield", pass, err)
		}
		resume = last
	}
}

// The GitLab per-run job cap shares the github constant and, before
// CHAOS-4142, the same raw ErrPaginationCapExceeded. Fixing only github would
// have left GitLab cancelling busy projects forever the moment one grew past
// 500 jobs in a pipeline -- and a cap that finalizes on one provider while
// cancelling on the other is worse than either choice applied consistently.
func TestGitLabTestsPerRunJobsCapTruncatesAndAdvances(t *testing.T) {
	doer := &gitLabTestsOversizedRunDoer{t: t, jobs: githubTestsMaxJobsPerRun + 10}
	claim := nativeTestClaim("gitlab", "tests")
	walk := walkGitLabTestsChunks(t, claim, gitLabRepositoryClient(t, doer, "https://gitlab.example"), 4)

	// Anti-vacuity: the fixture must genuinely exceed the cap.
	if doer.jobs <= githubTestsMaxJobsPerRun {
		t.Fatalf("fixture serves %d jobs, which does not exceed the %d cap",
			doer.jobs, githubTestsMaxJobsPerRun)
	}
	found := false
	for _, component := range walk.cursor.Truncated {
		if component == gitLabRunJobsComponent {
			found = true
		}
	}
	if !found {
		t.Fatalf("no %s truncation in %+v", gitLabRunJobsComponent, walk.cursor.Truncated)
	}
	if walk.chunks == 0 {
		t.Fatal("truncated pipeline committed nothing; the jobs before the cap must still land")
	}
	if walk.cursor.Jobs != githubTestsMaxJobsPerRun {
		t.Fatalf("kept %d jobs, want exactly the cap %d", walk.cursor.Jobs, githubTestsMaxJobsPerRun)
	}
	if walk.cursor.Phase != "done" {
		t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
	}
	// The defect: a per-run cap that withholds the watermark pins since_at, so
	// every later window re-includes the same oversized pipeline forever.
	if walk.final.Watermark == nil {
		t.Fatal("per-run truncation withheld the watermark; since_at would never advance past the oversized pipeline")
	}
	if !sameOptionalTime(walk.final.Watermark, claim.BeforeAt) {
		t.Fatalf("watermark=%v, want the window end %v", walk.final.Watermark, claim.BeforeAt)
	}
	// Coverage honesty is a separate claim and must still report the loss.
	if complete, ok := walk.final.Result["coverage_complete"].(bool); !ok || complete {
		t.Fatalf("coverage_complete=%v, want false on a truncated pipeline",
			walk.final.Result["coverage_complete"])
	}
}

func TestGitLabTestsPerRunJobsUnderCapIsUntouched(t *testing.T) {
	doer := &gitLabTestsOversizedRunDoer{t: t, jobs: 2}
	claim := nativeTestClaim("gitlab", "tests")
	walk := walkGitLabTestsChunks(t, claim, gitLabRepositoryClient(t, doer, "https://gitlab.example"), 4)

	if len(walk.cursor.Truncated) != 0 {
		t.Fatalf("under-cap pipeline recorded truncation %+v", walk.cursor.Truncated)
	}
	if walk.cursor.Jobs != 2 {
		t.Fatalf("kept %d jobs, want all 2", walk.cursor.Jobs)
	}
	if complete, ok := walk.final.Result["coverage_complete"].(bool); !ok || !complete {
		t.Fatalf("coverage_complete=%v, want true on an untruncated unit",
			walk.final.Result["coverage_complete"])
	}
	if walk.final.Watermark == nil {
		t.Fatal("an untruncated unit must advance its watermark")
	}
}

// An INVENTORY truncation must still withhold the watermark on GitLab too.
// This pins the boundary from the other side: widening the predicate back to
// "any truncation" breaks the per-run test above, narrowing it to nothing
// breaks this one.
func TestGitLabTestsInventoryTruncationStillWithholdsTheWatermark(t *testing.T) {
	if !gitLabTestsBlocksWatermark([]string{gitLabPipelineInventoryComponent}) {
		t.Fatal("pipeline_inventory must block the watermark")
	}
	if !gitLabTestsBlocksWatermark([]string{gitLabReportInventoryComponent}) {
		t.Fatal("report_inventory must block the watermark")
	}
	if gitLabTestsBlocksWatermark([]string{gitLabRunJobsComponent}) {
		t.Fatal("run_jobs must NOT block the watermark")
	}
	if gitLabTestsBlocksWatermark([]string{gitLabRunArtifactsComponent}) {
		t.Fatal("run_artifacts must NOT block the watermark")
	}
	// A blocking component anywhere in the set blocks the whole unit.
	if !gitLabTestsBlocksWatermark(
		[]string{gitLabRunJobsComponent, gitLabPipelineInventoryComponent},
	) {
		t.Fatal("a mixed set containing an inventory truncation must block the watermark")
	}
}

// The cursor vocabulary is closed and fails closed on decode, so a checkpoint
// carrying an unknown component is a conflict rather than silently accepted
// evidence. Widening it for the per-run components must not have opened it.
func TestGitLabTestsTruncationVocabularyStaysClosed(t *testing.T) {
	for _, component := range []string{
		gitLabPipelineInventoryComponent, gitLabReportInventoryComponent,
		gitLabRunJobsComponent, gitLabRunArtifactsComponent,
	} {
		if !gitLabTestsTruncationKnown(component) {
			t.Fatalf("%s must be in the closed vocabulary", component)
		}
	}
	for _, component := range []string{"", "run_job", "pipeline_inventories", "per_run_cap"} {
		if gitLabTestsTruncationKnown(component) {
			t.Fatalf("%q must NOT be in the closed vocabulary", component)
		}
	}
	// A duplicate or unknown entry in a resumed checkpoint stays a conflict.
	if _, err := decodeGitLabTestsChunkCursor(
		`{"phase":"pipelines","truncated":["run_jobs","run_jobs"]}`,
	); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("duplicate truncation decoded err=%v, want a checkpoint conflict", err)
	}
	if _, err := decodeGitLabTestsChunkCursor(
		`{"phase":"pipelines","truncated":["run_job"]}`,
	); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("unknown truncation decoded err=%v, want a checkpoint conflict", err)
	}
}

// REGRESSION (codex round 1), GitLab side. CollectGitLabPageParamPages is
// passed no MaxItems at all, so its CapReached can ONLY ever mean page-budget
// exhaustion -- the item cap here is purely len-based. Treating that flag as
// the item cap advanced the watermark over jobs that were never fetched.
func TestGitLabTestsNestedJobPageBudgetWithholdsTheWatermark(t *testing.T) {
	doer := &gitLabTestsOversizedRunDoer{t: t, jobsPerPage: 2}
	claim := nativeTestClaim("gitlab", "tests")
	walk := walkGitLabTestsChunks(t, claim, gitLabRepositoryClient(t, doer, "https://gitlab.example"), 4)

	found := false
	for _, component := range walk.cursor.Truncated {
		if component == gitLabRunJobsPageBudgetComponent {
			found = true
		}
	}
	if !found {
		t.Fatalf("no %s truncation in %+v — a page-budget stop must not be reported as the item cap",
			gitLabRunJobsPageBudgetComponent, walk.cursor.Truncated)
	}
	// Anti-vacuity: stopped on PAGES while under the item cap.
	if walk.cursor.Jobs >= githubTestsMaxJobsPerRun {
		t.Fatalf("kept %d jobs, reaching the %d item cap; the fixture is not exercising the page budget",
			walk.cursor.Jobs, githubTestsMaxJobsPerRun)
	}
	if walk.cursor.Jobs == 0 {
		t.Fatal("kept no jobs; the pipeline committed nothing at all")
	}
	if walk.final.Watermark != nil {
		t.Fatalf("nested page-budget stop advanced the watermark to %v; unfetched jobs would be lost permanently",
			walk.final.Watermark)
	}
}

// REGRESSION (codex round 2, challenge 3). The per-run page budget must outrun
// the per-run item cap. If it does not, the page-budget branch fires, the
// watermark is withheld, and because the budget is the same on every future
// window it is withheld forever -- the CHAOS-4142 outage re-entered through the
// other branch.
//
// GitLab reaches that guarantee STRUCTURALLY rather than by refusing config,
// because handler.MaxPages also bounds the pipeline INVENTORY walk, where a
// small budget is a legitimate setting that CHAOS-4130's tests rely on. The
// per-run walk therefore has its own derived constant, and this pins the exact
// boundary it has to clear.
//
// The boundary is an inequality, not a rounding preference: the route tests
// `len(items) > githubTestsMaxJobsPerRun`, so a budget whose reach exactly
// EQUALS the cap is still insufficient.
func TestGitLabTestsPerRunJobPagesOutrunsTheItemCapAtTheExactBoundary(t *testing.T) {
	equality := githubTestsMaxJobsPerRun / nativePerPage // 5 pages = exactly 500 items

	// Anti-vacuity: this must really be the equality point.
	if equality*nativePerPage != githubTestsMaxJobsPerRun {
		t.Fatalf("not the equality point: %d x %d != %d",
			equality, nativePerPage, githubTestsMaxJobsPerRun)
	}
	// Equality is NOT enough, and the constant must be strictly past it.
	if equality*nativePerPage > githubTestsMaxJobsPerRun {
		t.Fatalf("a budget reaching exactly %d would satisfy a > %d test; the boundary is misstated",
			equality*nativePerPage, githubTestsMaxJobsPerRun)
	}
	if gitLabTestsPerRunJobPages*nativePerPage <= githubTestsMaxJobsPerRun {
		t.Fatalf("per-run job budget reaches %d, which does not exceed the %d-job cap; "+
			"the page-budget branch binds first and the source stalls permanently",
			gitLabTestsPerRunJobPages*nativePerPage, githubTestsMaxJobsPerRun)
	}
	// It must be the SMALLEST sufficient budget, so the constant cannot quietly
	// grow into fetching far more than the cap can ever use.
	if gitLabTestsPerRunJobPages != equality+1 {
		t.Fatalf("per-run job budget is %d pages, want the minimum sufficient %d",
			gitLabTestsPerRunJobPages, equality+1)
	}

	// And the inventory budget stays independently configurable: a small
	// MaxPages must still be accepted, because that is what bounds the
	// pipeline inventory walk (CHAOS-4130).
	claim := nativeTestClaim("gitlab", "tests")
	doer := &gitLabTestsOversizedRunDoer{t: t, jobs: githubTestsMaxJobsPerRun + 25}
	walk := walkGitLabTestsChunksWith(
		t, GitLabTestsRouteHandler{MaxPages: 1}, claim,
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), 4,
	)
	if walk.cursor.Jobs != githubTestsMaxJobsPerRun {
		t.Fatalf("MaxPages=1 kept %d jobs, want the cap %d; the per-run walk must not inherit the inventory budget",
			walk.cursor.Jobs, githubTestsMaxJobsPerRun)
	}
}

// REGRESSION (codex round 2, challenge 1). Codex read the item-cap branch
// winning over the page-budget branch as the original F1 defect surviving. It
// is not: when the len-based cap binds, the committed set is the first N items
// and is INDEPENDENT of the page budget, so a larger budget recovers nothing,
// the truncation is deterministic, and advancing is correct. Withholding there
// would pin since_at forever -- CHAOS-4142 from the opposite direction.
//
// This is that argument in executable form. Both stops fire together (short of
// nothing: the fixture always advertises a next page AND serves more than the
// cap), and two DIFFERENT page budgets must commit the identical prefix.
func TestGitLabTestsCombinedStopIsClassifiedAsTheItemCapAndAdvances(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	// Full pages that ALWAYS advertise another page: the walk exhausts its
	// per-run page budget AND holds more than the cap, so both stop conditions
	// are true at the same moment. This is exactly the state codex read as the
	// surviving defect.
	doer := &gitLabTestsOversizedRunDoer{t: t, jobsPerPage: nativePerPage}
	walk := walkGitLabTestsChunks(
		t, claim, gitLabRepositoryClient(t, doer, "https://gitlab.example"), 4,
	)

	// Anti-vacuity: both conditions must really hold. The per-run walk must
	// have spent its whole budget (so the page budget IS exhausted) while
	// holding more than the cap (so the item cap IS reached).
	//
	// The expected count is the budget PLUS ONE: the reports phase fetches the
	// same pipeline's jobs once more with SinglePage, which is a different
	// collection with its own documented disposition.
	const reportsPhaseJobFetches = 1
	if doer.jobRequests != gitLabTestsPerRunJobPages+reportsPhaseJobFetches {
		t.Fatalf("walk issued %d job requests, want the full %d-page budget plus %d reports-phase fetch; "+
			"the page-budget condition is not being exercised",
			doer.jobRequests, gitLabTestsPerRunJobPages, reportsPhaseJobFetches)
	}
	if gitLabTestsPerRunJobPages*nativePerPage <= githubTestsMaxJobsPerRun {
		t.Fatalf("walk held at most %d jobs, not more than the %d cap; "+
			"the item-cap condition is not being exercised",
			gitLabTestsPerRunJobPages*nativePerPage, githubTestsMaxJobsPerRun)
	}

	// The refutation: the item cap wins, and it is RIGHT to win. The committed
	// set is the first cap-worth either way, so a larger budget would recover
	// nothing -- which is why withholding here would stall forever for no gain.
	if walk.cursor.Jobs != githubTestsMaxJobsPerRun {
		t.Fatalf("kept %d jobs, want exactly the cap %d", walk.cursor.Jobs, githubTestsMaxJobsPerRun)
	}
	for _, component := range walk.cursor.Truncated {
		if component == gitLabRunJobsPageBudgetComponent {
			t.Fatalf("combined stop classified as a page budget: %+v; "+
				"that classification withholds the watermark on a deterministic truncation",
				walk.cursor.Truncated)
		}
	}
	if walk.final.Watermark == nil {
		t.Fatal("a combined item-cap + page-budget stop withheld the watermark; that is the stall CHAOS-4142 fixed")
	}
}

// assertNoSilentPerRunStall states the CHAOS-4142 defect class as an outcome
// rather than as a mechanism, so it holds whichever remedy a provider uses.
//
// A per-run page budget that cannot outrun the item cap must never produce a
// unit that FINALIZES SUCCESSFULLY while withholding its watermark. That
// combination is the permanent stall: the unit looks healthy, the run recurs in
// every future window, and since_at never moves. Refusing the configuration
// loudly is acceptable. Walking past it and advancing is acceptable. Quietly
// finalizing with a withheld watermark is not.
func assertNoSilentPerRunStall(t *testing.T, err error, final CompleteRouteBatch) {
	t.Helper()
	if err != nil {
		if !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("err=%v, want either a loud ErrInvalidConfiguration or a finalized advancing unit", err)
		}
		return // refused loudly: the operator finds out at startup.
	}
	if final.Watermark == nil {
		t.Fatal("unit finalized with a WITHHELD watermark on a per-run truncation that recurs " +
			"identically every window; since_at is pinned forever and the source stops advancing")
	}
}

// RED-FIRST ARTIFACT for codex round 2, challenge 3. This test was written to
// FAIL on the unfixed tree and does: with the per-run jobs walk taking
// handler.MaxPages, MaxPages=1 fetched 100 of the pipeline's 525 jobs, reported
// a page budget rather than an item cap, withheld the watermark, and finalized
// anyway -- a source that reports success forever while never advancing.
//
// It asserts the OUTCOME, not the remedy, so it stays honest under either fix.
func TestGitLabTestsLowPageBudgetDoesNotSilentlyStallTheSource(t *testing.T) {
	claim := nativeTestClaim("gitlab", "tests")
	// MULTI-PAGE on purpose. A fixture that serves the whole job list on ONE
	// page cannot starve any budget -- the walk simply ends, no page budget
	// binds, and the test passes on the broken tree too. The first draft of
	// this test did exactly that and was vacuous; it only went red once the
	// fixture served full pages that always advertise another.
	doer := &gitLabTestsOversizedRunDoer{t: t, jobsPerPage: nativePerPage}

	// Anti-vacuity: the budget under test must be genuinely unable to reach
	// the item cap, and the fixture must be able to keep feeding it pages.
	const starvedBudget = 1
	if doer.jobsPerPage == 0 {
		t.Fatal("fixture is single-page, so no page budget can bind and the stall cannot occur")
	}
	if starvedBudget*doer.jobsPerPage > githubTestsMaxJobsPerRun {
		t.Fatalf("budget of %d pages reaches %d, which already outruns the %d cap; "+
			"this fixture cannot exercise the stall",
			starvedBudget, starvedBudget*doer.jobsPerPage, githubTestsMaxJobsPerRun)
	}

	var final CompleteRouteBatch
	err := (GitLabTestsRouteHandler{MaxPages: starvedBudget}).CollectChunks(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"),
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(emission ChunkRouteEmission) error {
			if emission.Final {
				final = emission.Batch
			}
			return nil
		},
	)
	assertNoSilentPerRunStall(t, err, final)
}
