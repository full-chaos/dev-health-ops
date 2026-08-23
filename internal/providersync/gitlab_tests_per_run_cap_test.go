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
		err := (GitLabTestsRouteHandler{}).CollectChunks(
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
