package providersync

import (
	"bytes"
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

// githubTestsOversizedRunDoer serves ONE workflow run whose per-run item lists
// are sized by the test. The runs listing itself is a single complete page, so
// nothing here can trip an INVENTORY page budget -- that isolates the per-run
// caps as the only thing under test and keeps the watermark assertions
// meaningful (an inventory truncation would withhold the watermark for an
// unrelated reason).
type githubTestsOversizedRunDoer struct {
	t         *testing.T
	jobs      int
	artifacts int
	// reportSuitesPerArtifact makes each artifact contribute that many suite
	// rows, so the suites+cases+coverage cap can be crossed independently of
	// the artifact-count cap.
	reportSuitesPerArtifact int

	// jobsPerPage, when > 0, overrides `jobs` and serves that many jobs on
	// every page with a next link, so the page budget is what stops the walk.
	jobsPerPage      int
	jobRequests      int
	artifactRequests int
	archiveRequests  int
}

func (doer *githubTestsOversizedRunDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, 1)), nil
	case strings.HasSuffix(path, "/jobs"):
		doer.jobRequests++
		// jobsPerPage>0 serves SHORT pages that always advertise a next link,
		// which is how the nested paginator runs out of PAGE budget long before
		// the 500-item cap is reached.
		if doer.jobsPerPage > 0 {
			next := *request.URL
			forward := next.Query()
			forward.Set("page", strconv.Itoa(doer.jobRequests+1))
			next.RawQuery = forward.Encode()
			header.Set("Link", "<"+next.String()+">; rel=\"next\"")
			return githubTestsHTTPResponse(request, header, githubTestsJobsFixture(doer.jobsPerPage)), nil
		}
		return githubTestsHTTPResponse(request, header, githubTestsJobsFixture(doer.jobs)), nil
	case strings.HasSuffix(path, "/artifacts"):
		doer.artifactRequests++
		return githubTestsHTTPResponse(request, header, githubTestsArtifactsFixture(doer.artifacts)), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		doer.archiveRequests++
		archive := githubTestsZip(doer.t, map[string]string{
			"junit.xml": githubTestsMultiSuiteJUnit(doer.reportSuitesPerArtifact),
		})
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/zip"}},
			Body:       io.NopCloser(bytes.NewReader(archive)), Request: request,
		}, nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

// githubTestsMultiSuiteJUnit builds a report holding `suites` suites, each with
// one case, so a test can cross the suites+cases+coverage cap by choosing how
// many suites an artifact contributes.
func githubTestsMultiSuiteJUnit(suites int) string {
	var body strings.Builder
	body.WriteString(`<testsuites>`)
	for index := 1; index <= suites; index++ {
		name := strconv.Itoa(index)
		body.WriteString(`<testsuite name="suite-` + name + `" time="1.0">`)
		body.WriteString(`<testcase name="case-` + name + `" classname="pkg" time="1.0"/>`)
		body.WriteString(`</testsuite>`)
	}
	body.WriteString(`</testsuites>`)
	return body.String()
}

func githubTestsJobsFixture(count int) string {
	var body strings.Builder
	body.WriteString(`{"jobs":[`)
	for index := 1; index <= count; index++ {
		if index > 1 {
			body.WriteByte(',')
		}
		body.WriteString(`{"id":`)
		body.WriteString(strconv.Itoa(index))
		body.WriteString(`,"name":"job-`)
		body.WriteString(strconv.Itoa(index))
		body.WriteString(`","status":"completed","conclusion":"success",` +
			`"started_at":"2026-07-22T10:01:00Z","completed_at":"2026-07-22T10:04:00Z"}`)
	}
	body.WriteString(`]}`)
	return body.String()
}

func githubTestsArtifactsFixture(count int) string {
	var body strings.Builder
	body.WriteString(`{"artifacts":[`)
	for index := 1; index <= count; index++ {
		if index > 1 {
			body.WriteByte(',')
		}
		body.WriteString(`{"id":`)
		body.WriteString(strconv.Itoa(index))
		body.WriteString(`,"name":"test-results-`)
		body.WriteString(strconv.Itoa(index))
		body.WriteString(`","expired":false}`)
	}
	body.WriteString(`]}`)
	return body.String()
}

// perRunWalk drives one oversized run through the chunked route and returns the
// terminal cursor and final batch.
func perRunWalk(t *testing.T, doer *githubTestsOversizedRunDoer) githubTestsWalk {
	t.Helper()
	claim := nativeTestClaim("github", "cicd")
	return walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, claim, githubTestsClient(t, doer), 4)
}

func perRunObservation(
	t *testing.T, walk githubTestsWalk, component string,
) GitHubTestsIncomplete {
	t.Helper()
	for _, observation := range walk.cursor.Incomplete {
		if observation.Component == component {
			return observation
		}
	}
	t.Fatalf("no %s observation in %+v", component, walk.cursor.Incomplete)
	return GitHubTestsIncomplete{}
}

// assertFinalizedAndAdvanced is the failure mode CHAOS-4142 pins: a unit whose
// window contains an oversized run must FINALIZE with committed rows and an
// ADVANCED watermark. Before the fix each of these caps returned a raw
// ErrPaginationCapExceeded, which providerunit maps to a deterministic-terminal
// category -- the unit was cancelled, since_at never moved past the oversized
// run, and every subsequent hourly window re-included it forever.
func assertFinalizedAndAdvanced(t *testing.T, walk githubTestsWalk) {
	t.Helper()
	if walk.chunks == 0 {
		t.Fatal("truncated run committed nothing; the items before the cap must still land")
	}
	if walk.cursor.Phase != "done" {
		t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
	}
	if walk.final.Watermark == nil {
		t.Fatal("per-run truncation withheld the watermark; since_at would never advance past the oversized run")
	}
	claim := nativeTestClaim("github", "cicd")
	if !sameOptionalTime(walk.final.Watermark, claim.BeforeAt) {
		t.Fatalf("watermark=%v, want the window end %v", walk.final.Watermark, claim.BeforeAt)
	}
	// Coverage honesty is a SEPARATE claim from watermark advancement: the run
	// really did lose items, and the unit must say so.
	if complete, ok := walk.final.Result["reports_complete"].(bool); !ok || complete {
		t.Fatalf("reports_complete=%v, want false on a truncated run", walk.final.Result["reports_complete"])
	}
	// The production comparator is the fail-closed gate the chunked executor
	// runs before any completion becomes durable.
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, walk.final,
	); err != nil {
		t.Fatalf("production comparator rejected a per-run-truncated completion: %v", err)
	}
}

func TestGitHubTestsPerRunJobsCapTruncatesAndAdvances(t *testing.T) {
	doer := &githubTestsOversizedRunDoer{t: t, jobs: githubTestsMaxJobsPerRun + 25, artifacts: 0}
	walk := perRunWalk(t, doer)

	// Anti-vacuity: the fixture must genuinely exceed the cap, or an
	// unconditional "truncates" assertion would pass on a route that never
	// truncated anything.
	if doer.jobs <= githubTestsMaxJobsPerRun {
		t.Fatalf("fixture serves %d jobs, which does not exceed the %d cap", doer.jobs, githubTestsMaxJobsPerRun)
	}
	observation := perRunObservation(t, walk, githubTestsRunJobsComponent)
	if observation.Cause != githubTestsPerRunCapCause || observation.Count != 1 {
		t.Fatalf("jobs observation=%+v, want cause=%s count=1", observation, githubTestsPerRunCapCause)
	}
	// Exactly the first cap-worth is kept -- not the oversized set, and not zero.
	if walk.cursor.Jobs != githubTestsMaxJobsPerRun {
		t.Fatalf("kept %d jobs, want exactly the cap %d", walk.cursor.Jobs, githubTestsMaxJobsPerRun)
	}
	assertFinalizedAndAdvanced(t, walk)
}

func TestGitHubTestsPerRunJobsUnderCapIsUntouched(t *testing.T) {
	doer := &githubTestsOversizedRunDoer{t: t, jobs: 3, artifacts: 0}
	walk := perRunWalk(t, doer)

	for _, observation := range walk.cursor.Incomplete {
		if observation.Cause == githubTestsPerRunCapCause {
			t.Fatalf("under-cap run recorded a truncation: %+v", observation)
		}
	}
	if walk.cursor.Jobs != 3 {
		t.Fatalf("kept %d jobs, want all 3", walk.cursor.Jobs)
	}
	if complete, ok := walk.final.Result["reports_complete"].(bool); !ok || !complete {
		t.Fatalf("reports_complete=%v, want true on an untruncated unit", walk.final.Result["reports_complete"])
	}
	if walk.final.Watermark == nil {
		t.Fatal("an untruncated unit must advance its watermark")
	}
}

// A per-run truncation must NOT suppress the watermark, but an INVENTORY
// truncation still must. This pins the boundary between the two classes: if
// the predicate were widened back to "any observation", the jobs test above
// would fail; if it were narrowed to nothing, this one would.
func TestGitHubTestsInventoryTruncationStillWithholdsTheWatermark(t *testing.T) {
	doer := &githubTestsPagedDoer{t: t, pages: 3, perPage: 2}
	claim := nativeTestClaim("github", "tests")
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{MaxRuns: 100}, claim, githubTestsClient(t, doer), 2)

	if !githubTestsBlocksWatermark(walk.cursor.Incomplete) {
		t.Fatalf("inventory observations %+v must block the watermark", walk.cursor.Incomplete)
	}
	if walk.final.Watermark != nil {
		t.Fatalf("inventory-truncated unit advanced its watermark to %v", walk.final.Watermark)
	}
}

// The watermark predicate is a CLASSIFICATION over (component, cause) PAIRS,
// not over components. The same component carries both a positively observed
// item cap (advances) and a nested page-budget stop (withholds), so a
// component-only rule would have advanced over unobserved data -- the
// regression codex round 1 caught.
func TestGitHubTestsWatermarkBlockingClassification(t *testing.T) {
	cases := []struct {
		component, cause string
		blocking         bool
	}{
		{githubTestsRunInventoryComponent, githubTestsPageBudgetCause, true},
		{githubTestsArtifactInventoryComponent, githubTestsPageBudgetCause, true},
		// Preserved from before CHAOS-4142 rather than reclassified; the
		// reclassification is CHAOS-4153.
		{githubTestsReportMemberComponent, "malformed", true},
		{githubTestsReportMemberComponent, "unreadable", true},
		// Positively observed item caps advance.
		{githubTestsRunJobsComponent, githubTestsPerRunCapCause, false},
		{githubTestsRunArtifactsComponent, githubTestsPerRunCapCause, false},
		{githubTestsRunReportsComponent, githubTestsPerRunCapCause, false},
		// The SAME components withhold when the nested page budget was what
		// stopped the walk, because the remainder was never observed.
		{githubTestsRunJobsComponent, githubTestsPerRunPageBudgetCause, true},
		{githubTestsRunArtifactsComponent, githubTestsPerRunPageBudgetCause, true},
		// Fail-safe: anything not explicitly on the advancing allowlist
		// withholds, so a future vocabulary entry forgotten there stalls
		// loudly instead of silently advancing.
		{githubTestsRunJobsComponent, "", true},
		{githubTestsRunJobsComponent, "invented_cause", true},
		{"invented_component", githubTestsPerRunCapCause, true},
	}
	for _, testCase := range cases {
		got := githubTestsBlocksWatermark([]GitHubTestsIncomplete{
			{Component: testCase.component, Cause: testCase.cause, Count: 1},
		})
		if got != testCase.blocking {
			t.Fatalf("%s/%s blocks watermark=%v, want %v",
				testCase.component, testCase.cause, got, testCase.blocking)
		}
	}
	// A withholding pair anywhere in the slice withholds for the whole unit.
	mixed := []GitHubTestsIncomplete{
		{Component: githubTestsRunJobsComponent, Cause: githubTestsPerRunCapCause, Count: 1},
		{Component: githubTestsRunInventoryComponent, Cause: githubTestsPageBudgetCause, Count: 1},
	}
	if !githubTestsBlocksWatermark(mixed) {
		t.Fatal("a mixed slice containing an inventory observation must withhold the watermark")
	}
	// And the per-run page budget dominates its own component's item cap.
	sameComponent := []GitHubTestsIncomplete{
		{Component: githubTestsRunJobsComponent, Cause: githubTestsPerRunCapCause, Count: 1},
		{Component: githubTestsRunJobsComponent, Cause: githubTestsPerRunPageBudgetCause, Count: 1},
	}
	if !githubTestsBlocksWatermark(sameComponent) {
		t.Fatal("a page-budget observation must withhold even alongside an item-cap one")
	}
}

// The per-run pairs must be in the closed vocabulary, and only in their own
// cause -- the comparator fails closed against this, so a route cannot publish
// an observation downstream coverage readers have no meaning for.
func TestGitHubTestsPerRunVocabularyIsClosed(t *testing.T) {
	cases := []struct {
		observation GitHubTestsIncomplete
		allowed     bool
	}{
		{GitHubTestsIncomplete{Component: githubTestsRunJobsComponent, Cause: githubTestsPerRunCapCause, Count: 1}, true},
		{GitHubTestsIncomplete{Component: githubTestsRunArtifactsComponent, Cause: githubTestsPerRunCapCause, Count: 1}, true},
		{GitHubTestsIncomplete{Component: githubTestsRunReportsComponent, Cause: githubTestsPerRunCapCause, Count: 1}, true},
		{GitHubTestsIncomplete{Component: githubTestsRunJobsComponent, Cause: githubTestsPageBudgetCause, Count: 1}, false},
		{GitHubTestsIncomplete{Component: githubTestsRunInventoryComponent, Cause: githubTestsPerRunCapCause, Count: 1}, false},
		{GitHubTestsIncomplete{Component: "run_job", Cause: githubTestsPerRunCapCause, Count: 1}, false},
	}
	for _, testCase := range cases {
		if got := githubTestsIncompleteInVocabulary(testCase.observation); got != testCase.allowed {
			t.Fatalf("%+v in vocabulary=%v, want %v", testCase.observation, got, testCase.allowed)
		}
	}
}

func TestGitHubTestsPerRunArtifactsCapTruncatesAndAdvances(t *testing.T) {
	doer := &githubTestsOversizedRunDoer{
		t: t, jobs: 1, artifacts: githubTestsMaxArtifacts + 5, reportSuitesPerArtifact: 1,
	}
	walk := perRunWalk(t, doer)

	// Anti-vacuity: the fixture must genuinely exceed the artifact cap.
	if doer.artifacts <= githubTestsMaxArtifacts {
		t.Fatalf("fixture serves %d artifacts, which does not exceed the %d cap",
			doer.artifacts, githubTestsMaxArtifacts)
	}
	observation := perRunObservation(t, walk, githubTestsRunArtifactsComponent)
	if observation.Cause != githubTestsPerRunCapCause || observation.Count != 1 {
		t.Fatalf("artifacts observation=%+v, want cause=%s count=1", observation, githubTestsPerRunCapCause)
	}
	// Only the first cap-worth of artifacts is downloaded; the rest are dropped
	// rather than fetched and discarded.
	if doer.archiveRequests != githubTestsMaxArtifacts {
		t.Fatalf("downloaded %d archives, want exactly the cap %d",
			doer.archiveRequests, githubTestsMaxArtifacts)
	}
	assertFinalizedAndAdvanced(t, walk)
}

func TestGitHubTestsPerRunArtifactsUnderCapIsUntouched(t *testing.T) {
	doer := &githubTestsOversizedRunDoer{t: t, jobs: 1, artifacts: 2, reportSuitesPerArtifact: 1}
	walk := perRunWalk(t, doer)

	for _, observation := range walk.cursor.Incomplete {
		if observation.Cause == githubTestsPerRunCapCause {
			t.Fatalf("under-cap run recorded a truncation: %+v", observation)
		}
	}
	if doer.archiveRequests != 2 {
		t.Fatalf("downloaded %d archives, want all 2", doer.archiveRequests)
	}
	if walk.final.Watermark == nil {
		t.Fatal("an untruncated unit must advance its watermark")
	}
}

// The report-rows cap counts suites+cases+coverage across a run's artifacts, so
// it is crossed by artifact CONTENT rather than artifact count. Two artifacts,
// each carrying more than half the cap, stay under the artifact cap and over
// the report cap -- which is what makes this a distinct site rather than a
// second spelling of the artifacts cap.
func TestGitHubTestsPerRunReportRowsCapTruncatesAndAdvances(t *testing.T) {
	suitesPer := githubTestsMaxJobsPerRun // each artifact: 500 suites + 500 cases
	doer := &githubTestsOversizedRunDoer{
		t: t, jobs: 1, artifacts: 2, reportSuitesPerArtifact: suitesPer,
	}
	walk := perRunWalk(t, doer)

	// Anti-vacuity: prove the fixture crosses the REPORT cap while staying
	// under the ARTIFACT cap, so the observation below can only come from the
	// report-rows site.
	if doer.artifacts > githubTestsMaxArtifacts {
		t.Fatalf("fixture serves %d artifacts, which also trips the artifact cap %d",
			doer.artifacts, githubTestsMaxArtifacts)
	}
	if suitesPer*2 <= githubTestsMaxJobsPerRun {
		t.Fatalf("fixture yields %d suites, which does not exceed the %d report cap",
			suitesPer*2, githubTestsMaxJobsPerRun)
	}
	observation := perRunObservation(t, walk, githubTestsRunReportsComponent)
	if observation.Cause != githubTestsPerRunCapCause || observation.Count != 1 {
		t.Fatalf("reports observation=%+v, want cause=%s count=1", observation, githubTestsPerRunCapCause)
	}
	// The run kept the rows it had accumulated when the cap tripped, and the
	// walk stopped consuming FURTHER artifacts for that run.
	if walk.cursor.Suites == 0 {
		t.Fatal("report-capped run committed no suites; the rows before the cap must still land")
	}
	if doer.archiveRequests != 1 {
		t.Fatalf("downloaded %d archives, want 1 (the cap must stop further artifacts for the run)",
			doer.archiveRequests)
	}
	assertFinalizedAndAdvanced(t, walk)
}

// The comparator invariant is BIDIRECTIONAL: the watermark is nil IFF a
// window-blocking observation is present. Testing only one direction leaves a
// regressed producer failing open on the other -- a unit that withholds its
// watermark for a merely per-run truncation would be accepted and would pin
// since_at exactly the way CHAOS-4142 describes, while still reporting success.
func TestGitHubTestsCompletionWatermarkInvariantIsBidirectional(t *testing.T) {
	claim := nativeTestClaim("github", "cicd")
	perRun := GitHubTestsIncomplete{
		Component: githubTestsRunJobsComponent, Cause: githubTestsPerRunCapCause, Count: 1,
	}
	blocking := GitHubTestsIncomplete{
		Component: githubTestsRunInventoryComponent, Cause: githubTestsPageBudgetCause, Count: 1,
	}
	// The durable form is always a non-nil slice -- the comparator refuses a
	// JSON null, so the one writer normalizes it (see githubTestsFinalMetadataBatch).
	batchFor := func(observations []GitHubTestsIncomplete, watermark *time.Time) CompleteRouteBatch {
		observations = append(make([]GitHubTestsIncomplete, 0, len(observations)), observations...)
		skipped := 0
		for _, observation := range observations {
			skipped += observation.Count
		}
		return CompleteRouteBatch{
			Watermark: watermark,
			Result: map[string]any{
				"reports_complete": len(observations) == 0,
				"reports_skipped":  skipped,
				"incomplete":       observations,
			},
		}
	}

	cases := []struct {
		name        string
		observation []GitHubTestsIncomplete
		watermark   *time.Time
		valid       bool
	}{
		{"per-run truncation advances", []GitHubTestsIncomplete{perRun}, claim.BeforeAt, true},
		// Direction 1: withholding without a window-blocking reason.
		{"per-run truncation withholding is invalid", []GitHubTestsIncomplete{perRun}, nil, false},
		{"inventory truncation withholds", []GitHubTestsIncomplete{blocking}, nil, true},
		// Direction 2: advancing despite a window-blocking reason.
		{"inventory truncation advancing is invalid", []GitHubTestsIncomplete{blocking}, claim.BeforeAt, false},
		// A blocking observation dominates a mixed set in both directions.
		{"mixed set withholds", []GitHubTestsIncomplete{perRun, blocking}, nil, true},
		{"mixed set advancing is invalid", []GitHubTestsIncomplete{perRun, blocking}, claim.BeforeAt, false},
		{"complete unit advances", nil, claim.BeforeAt, true},
		{"complete unit withholding is invalid", nil, nil, false},
	}
	for _, testCase := range cases {
		err := validateGitHubTestsCompletion(claim, batchFor(testCase.observation, testCase.watermark))
		if (err == nil) != testCase.valid {
			t.Fatalf("%s: comparator accepted=%v, want %v (err=%v)",
				testCase.name, err == nil, testCase.valid, err)
		}
	}
}

// REGRESSION (codex round 1). CapReached is written by providerfoundation for
// TWO different reasons -- MaxPages exhausted and MaxItems reached -- and the
// first version of this change treated both as the per-run item cap. With a
// provider serving short pages, the nested jobs paginator runs out of PAGE
// budget at far fewer than 500 items; committing those and advancing the
// watermark silently discarded every remaining job. Measured directly against
// the real paginator: MaxPages=100 with 3 items/page yields CapReached=true at
// 300 items.
//
// The remainder there was never observed, so it must withhold the watermark,
// and it must say per_run_page_budget so an operator can tell which of the two
// caps fired without reading route code.
func TestGitHubTestsNestedJobPageBudgetWithholdsTheWatermark(t *testing.T) {
	doer := &githubTestsOversizedRunDoer{t: t, jobsPerPage: 3, artifacts: 0}
	claim := nativeTestClaim("github", "cicd")
	// The budget is the smallest one validatePerRunPageBudget ACCEPTS, not an
	// arbitrary small number: after codex round 2 a budget that cannot outrun
	// the item cap is refused at configuration time, so this branch can no
	// longer be reached that way. It is still reachable exactly as here -- a
	// provider advertising a further page while serving SHORT ones -- which is
	// why the branch is kept and still withholds. This test is the evidence
	// for that "kept because still reachable" claim.
	handler := GitHubTestsRouteHandler{MaxJobPages: githubTestsMaxJobsPerRun/githubTestsPerRunPerPage + 1}
	walk := walkGitHubTestsChunks(t, handler, claim, githubTestsClient(t, doer), 4)

	observation := perRunObservation(t, walk, githubTestsRunJobsComponent)
	if observation.Cause != githubTestsPerRunPageBudgetCause {
		t.Fatalf("cause=%q, want %q — a page-budget stop must not be reported as the item cap",
			observation.Cause, githubTestsPerRunPageBudgetCause)
	}
	// Anti-vacuity: the fixture must stop on PAGES while staying UNDER the item
	// cap, or this would just be re-testing the item cap under another name.
	if walk.cursor.Jobs >= githubTestsMaxJobsPerRun {
		t.Fatalf("kept %d jobs, which reaches the %d item cap; the fixture is not exercising the page budget",
			walk.cursor.Jobs, githubTestsMaxJobsPerRun)
	}
	if walk.cursor.Jobs == 0 {
		t.Fatal("kept no jobs; the run committed nothing at all")
	}
	// The defect: advancing here loses every job past the page budget forever.
	if walk.final.Watermark != nil {
		t.Fatalf("nested page-budget stop advanced the watermark to %v; the unfetched jobs would be lost permanently",
			walk.final.Watermark)
	}
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(
		context.Background(), claim, walk.final,
	); err != nil {
		t.Fatalf("production comparator rejected a page-budget completion: %v", err)
	}
}

// REGRESSION (codex round 1). Report rows were appended a whole artifact at a
// time and only THEN measured, so the committed set could creep arbitrarily far
// past the cap -- the original fixture committed 1000 rows against a 500 cap.
// The bound is deliberately soft, max(cap, first artifact), because splitting
// an artifact would separate a suite from its cases; but it must be a bound.
func TestGitHubTestsReportRowsStayWithinTheDocumentedBound(t *testing.T) {
	// Each artifact carries 400 suites + 400 cases = 800 rows, so the FIRST
	// alone exceeds the 500 cap and any second artifact must be refused.
	suitesPer := 400
	doer := &githubTestsOversizedRunDoer{
		t: t, jobs: 1, artifacts: 3, reportSuitesPerArtifact: suitesPer,
	}
	walk := perRunWalk(t, doer)

	firstArtifactRows := suitesPer * 2
	// Anti-vacuity: the first artifact must genuinely exceed the cap on its own.
	if firstArtifactRows <= githubTestsMaxJobsPerRun {
		t.Fatalf("first artifact carries %d rows, which does not exceed the %d cap",
			firstArtifactRows, githubTestsMaxJobsPerRun)
	}
	committed := walk.cursor.Suites + walk.cursor.Cases + walk.cursor.Coverage
	bound := githubTestsMaxJobsPerRun
	if firstArtifactRows > bound {
		bound = firstArtifactRows
	}
	// THE INVARIANT: durable rows never exceed max(cap, first artifact's rows).
	if committed > bound {
		t.Fatalf("committed %d report rows, exceeding the documented bound max(cap=%d, firstArtifact=%d)=%d",
			committed, githubTestsMaxJobsPerRun, firstArtifactRows, bound)
	}
	// A run with reports must never commit zero, which is why the first
	// oversized artifact is kept whole rather than refused.
	if committed == 0 {
		t.Fatal("committed no report rows; an oversized first artifact must still land")
	}
	// Only the first artifact is downloaded; the rest are refused before fetch.
	if doer.archiveRequests != 1 {
		t.Fatalf("downloaded %d archives, want 1 — later artifacts must be refused, not fetched and discarded",
			doer.archiveRequests)
	}
	perRunObservation(t, walk, githubTestsRunReportsComponent)
	assertFinalizedAndAdvanced(t, walk)
}

// The discriminating case for the row bound: artifacts that each fit UNDER the
// cap but together exceed it. The oversized-first-artifact test above cannot
// see the defect, because max(cap, firstArtifact) absorbs the overshoot and the
// fixed and broken code commit the same rows — a mutation that restored
// append-then-check survived it. Here the bound is the flat cap, so
// appending before measuring is visible: 2x300 rows lands 600 against a 500
// bound.
func TestGitHubTestsReportRowsDoNotCreepPastTheCapAcrossArtifacts(t *testing.T) {
	// 150 suites + 150 cases = 300 rows per artifact; three artifacts.
	suitesPer := 150
	doer := &githubTestsOversizedRunDoer{
		t: t, jobs: 1, artifacts: 3, reportSuitesPerArtifact: suitesPer,
	}
	walk := perRunWalk(t, doer)

	rowsPerArtifact := suitesPer * 2
	// Anti-vacuity, both directions: each artifact must FIT on its own, and two
	// together must NOT. Otherwise this collapses into one of the other tests.
	if rowsPerArtifact > githubTestsMaxJobsPerRun {
		t.Fatalf("artifact carries %d rows and already exceeds the %d cap alone; "+
			"this fixture is the oversized-first case, not the creep case",
			rowsPerArtifact, githubTestsMaxJobsPerRun)
	}
	if rowsPerArtifact*2 <= githubTestsMaxJobsPerRun {
		t.Fatalf("two artifacts carry %d rows, which does not exceed the %d cap; "+
			"nothing would truncate", rowsPerArtifact*2, githubTestsMaxJobsPerRun)
	}
	committed := walk.cursor.Suites + walk.cursor.Cases + walk.cursor.Coverage
	// Every artifact fits individually, so the bound here is the flat cap.
	if committed > githubTestsMaxJobsPerRun {
		t.Fatalf("committed %d report rows against a %d cap; rows were appended before "+
			"being measured, so the aggregate crept past the bound one artifact at a time",
			committed, githubTestsMaxJobsPerRun)
	}
	if committed != rowsPerArtifact {
		t.Fatalf("committed %d rows, want exactly the one artifact that fit (%d)",
			committed, rowsPerArtifact)
	}
	perRunObservation(t, walk, githubTestsRunReportsComponent)
	assertFinalizedAndAdvanced(t, walk)
}

// githubTestsRefusingDoer fails the test if it is ever called: a configuration
// refusal must precede all provider traffic, which is what makes it a loud
// startup error instead of a source that quietly stops advancing.
type githubTestsRefusingDoer struct{ t *testing.T }

func (doer githubTestsRefusingDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.t.Fatalf("config refusal issued a provider request: %s", request.URL.String())
	return nil, nil
}

// REGRESSION (codex round 2, challenge 3), github twin of the gitlab boundary
// test. MaxJobPages is fixed config; if its reach cannot exceed the per-run item
// cap, the page-budget branch fires on every window and withholds the watermark
// forever. The boundary is EQUALITY, not "too small": the item cap needs
// strictly more than the cap in hand (MaxItems is cap+1 against a >= check), so
// a budget reaching exactly the cap is still insufficient.
func TestGitHubTestsPerRunPageBudgetIsRefusedAtTheEqualityBoundary(t *testing.T) {
	claim := nativeTestClaim("github", "cicd")
	equality := githubTestsMaxJobsPerRun / githubTestsPerRunPerPage // 5 pages = exactly 500 items

	if equality*githubTestsPerRunPerPage != githubTestsMaxJobsPerRun {
		t.Fatalf("fixture is not at the equality point: %d x %d != %d",
			equality, githubTestsPerRunPerPage, githubTestsMaxJobsPerRun)
	}

	err := (GitHubTestsRouteHandler{MaxJobPages: equality}).CollectChunks(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, githubTestsRefusingDoer{t: t}),
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(ChunkRouteEmission) error {
			t.Fatal("a refused configuration emitted a chunk")
			return nil
		},
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("MaxJobPages=%d (reach exactly %d, cap %d) err=%v, want ErrInvalidConfiguration",
			equality, equality*githubTestsPerRunPerPage, githubTestsMaxJobsPerRun, err)
	}
	for _, want := range []string{"MaxJobPages", strconv.Itoa(equality), strconv.Itoa(githubTestsMaxJobsPerRun)} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q does not name %q", err.Error(), want)
		}
	}

	// One above the boundary is accepted and does NOT stall: the guard removes
	// the stall rather than relocating it into a refusal of everything.
	doer := &githubTestsOversizedRunDoer{t: t, jobs: githubTestsMaxJobsPerRun + 25, artifacts: 0}
	walk := walkGitHubTestsChunks(
		t, GitHubTestsRouteHandler{MaxJobPages: equality + 1}, claim, githubTestsClient(t, doer), 4,
	)
	if walk.cursor.Jobs != githubTestsMaxJobsPerRun {
		t.Fatalf("MaxJobPages=%d kept %d jobs, want the cap %d",
			equality+1, walk.cursor.Jobs, githubTestsMaxJobsPerRun)
	}
	if walk.final.Watermark == nil {
		t.Fatal("the first ACCEPTED budget withheld the watermark; the guard must remove the stall, not move it")
	}
}

// The per-run page budget on the ARTIFACTS collection is satisfied structurally
// rather than by validation: MaxPages is 1 and per_page is 100, against a cap
// that the existing range check already holds at or below githubTestsMaxArtifacts.
// Pinning it means a future widening of that cap cannot silently reopen the
// stall on a path that has no configuration knob to refuse.
func TestGitHubTestsArtifactPageBudgetStructurallyOutrunsItsCap(t *testing.T) {
	const artifactPageBudget = 1 // hardcoded at the artifacts collection site
	if artifactPageBudget*githubTestsPerRunPerPage <= githubTestsMaxArtifacts {
		t.Fatalf("artifact page budget reaches %d, which does not exceed the %d-artifact cap; "+
			"the per-run page-budget branch there is reachable again",
			artifactPageBudget*githubTestsPerRunPerPage, githubTestsMaxArtifacts)
	}
}
