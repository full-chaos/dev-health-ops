package providersync

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-13794: the report rows of ONE run are admitted artifact by artifact, whole or not at all, smallest declared size first,
// under githubTestsMaxReportRowsPerRun rows and githubTestsMaxReportPayloadBytesPerDestination bytes per destination.

// rowsArtifact is one artifact of a run: its id, its declared size (what the listing says, and what orders the admission), and
// the number of report rows (suites + cases, rows/2 suites with one case each) its JUnit file holds.
type rowsArtifact struct {
	id    string
	size  int64
	rows  int
	names string            // name prefix; default "a<id>-" so artifacts do not repeat each other's cases
	body  string            // the JUnit file itself, when the test builds the cases (it replaces rows and names)
	extra map[string]string // more archive members (such as a coverage report) beside junit.xml, by member name
}

type githubTestsRowsDoer struct {
	t          *testing.T
	runs       int
	artifacts  []rowsArtifact
	downloaded []string
}

func (doer *githubTestsRowsDoer) Do(request *http.Request) (*http.Response, error) {
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, doer.runs)), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, githubTestsJobsFixture(1)), nil
	case strings.HasSuffix(path, "/artifacts"):
		var body strings.Builder
		body.WriteString(`{"artifacts":[`)
		for index, artifact := range doer.artifacts {
			if index > 0 {
				body.WriteByte(',')
			}
			body.WriteString(`{"id":` + artifact.id + `,"name":"test-results-` + artifact.id + `","expired":false,"size_in_bytes":` +
				strconv.FormatInt(artifact.size, 10) + `}`)
		}
		body.WriteString(`]}`)
		return githubTestsHTTPResponse(request, header, body.String()), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/repos/acme/api/actions/artifacts/"), "/zip")
		doer.downloaded = append(doer.downloaded, id)
		for _, artifact := range doer.artifacts {
			if artifact.id == id {
				prefix := artifact.names
				if prefix == "" {
					prefix = "a" + artifact.id + "-"
				}
				junit := githubTestsNamedMultiSuiteJUnit(prefix, artifact.rows/2)
				if artifact.body != "" {
					junit = artifact.body
				}
				members := map[string]string{"junit.xml": junit}
				for name, member := range artifact.extra {
					members[name] = member
				}
				archive := githubTestsZip(doer.t, members)
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}},
					Body: io.NopCloser(bytes.NewReader(archive)), Request: request}, nil
			}
		}
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

// rowsWalk drives one unit through the chunked route and returns the walk, the doer and the rendered metrics.
func rowsWalk(t *testing.T, runs int, artifacts ...rowsArtifact) (githubTestsWalk, *githubTestsRowsDoer, string) {
	t.Helper()
	doer := &githubTestsRowsDoer{t: t, runs: runs, artifacts: artifacts}
	client := githubTestsClient(t, fakehttp.Client(doer))
	client.Metrics = providerfoundation.NewMetrics()
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, nativeTestClaim("github", "cicd"), client, 8)
	var rendered bytes.Buffer
	if err := client.Metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	return walk, doer, rendered.String()
}

func committedReportRows(walk githubTestsWalk) int {
	return walk.cursor.Suites + walk.cursor.Cases + walk.cursor.Coverage
}

func skippedArtifactIDs(walk githubTestsWalk) []string {
	ids := []string{}
	for _, marker := range walk.cursor.SkippedArtifacts {
		ids = append(ids, marker.ArtifactID)
	}
	slices.Sort(ids)
	return ids
}

// (1) Artifacts that fit together are all admitted: three of 300 rows were cut to one by the old 500 bound.
func TestGitHubTestsRunReportRowsAdmitEveryArtifactThatFits(t *testing.T) {
	walk, doer, rendered := rowsWalk(t, 1, rowsArtifact{id: "1", size: 10, rows: 300}, rowsArtifact{id: "2", size: 20, rows: 300}, rowsArtifact{id: "3", size: 30, rows: 300})
	if got := committedReportRows(walk); got != 900 {
		t.Fatalf("committed %d rows, want all 900", got)
	}
	if len(doer.downloaded) != 3 || len(walk.cursor.SkippedArtifacts) != 0 {
		t.Fatalf("downloaded %v, markers %+v, want 3 downloads and no marker", doer.downloaded, walk.cursor.SkippedArtifacts)
	}
	if strings.Contains(rendered, `component="run_reports"`) {
		t.Fatalf("a run under the bound recorded a run_reports truncation:\n%s", rendered)
	}
	if walk.cursor.Phase != "done" || walk.final.Watermark == nil {
		t.Fatalf("phase %q, watermark %v: a complete run must finalize and advance", walk.cursor.Phase, walk.final.Watermark)
	}
}

// The route honours the plan end to end, in ONE walk of about 200,000 rows (the bound is fixed, so a walk that reaches it costs
// that many parsed rows; the exact bound and the order rules are checked at plan level in github_tests_run_report_plan_test.go).
// Listed largest declared size first: artifact 1 (199,954 rows, size 10) is admitted, artifacts 3, 4 and 5 (22 rows each) are
// skipped whole, and artifact 6 (4 rows, size 50) after them is admitted: no break at the overflow. The run records ONE
// per_run_cap and each skipped artifact has ONE marker.
func TestGitHubTestsRunReportRowsRouteHonoursThePlan(t *testing.T) {
	walk, doer, rendered := rowsWalk(t, 1,
		rowsArtifact{id: "6", size: 50, rows: 4}, rowsArtifact{id: "5", size: 40, rows: 22}, rowsArtifact{id: "4", size: 30, rows: 22},
		rowsArtifact{id: "3", size: 20, rows: 22}, rowsArtifact{id: "1", size: 10, rows: githubTestsMaxReportRowsPerRun - 21})
	if got := committedReportRows(walk); got != githubTestsMaxReportRowsPerRun-17 {
		t.Fatalf("committed %d rows, want 199,958 (artifacts 1 and 6)", got)
	}
	if ids := skippedArtifactIDs(walk); strings.Join(ids, ",") != "3,4,5" {
		t.Fatalf("skipped markers %v, want one each for 3, 4 and 5", ids)
	}
	for _, marker := range walk.cursor.SkippedArtifacts {
		if marker.Cause != githubTestsPerRunCapCause || marker.RunID == "" || marker.Name == "" {
			t.Fatalf("marker %+v lacks cause, run id or name", marker)
		}
	}
	if len(doer.downloaded) != 5 {
		t.Fatalf("downloaded %v, want all 5: no break at the overflow", doer.downloaded)
	}
	observation := perRunObservation(t, walk, githubTestsRunReportsComponent)
	if observation.Cause != githubTestsPerRunCapCause || observation.Count != 1 {
		t.Fatalf("observation %+v, want per_run_cap count 1 for the run", observation)
	}
	want := `dev_health_provider_per_run_truncation_total{provider="github",dataset="cicd",component="run_reports",cause="per_run_cap"} 1`
	if !strings.Contains(rendered, want) {
		t.Fatalf("metric not exactly once for the run; want %q in:\n%s", want, rendered)
	}
	assertFinalizedAndAdvanced(t, walk)
}

// (6) The payload bound is judged on json bytes per destination, below the 128 MiB effect ceiling.
func TestGitHubTestsReportBudgetPayloadBound(t *testing.T) {
	var budget githubTestsReportBudget
	if !budget.fits(1, 1, githubTestsMaxReportPayloadBytesPerDestination, 0) {
		t.Fatal("a destination exactly at the payload bound was refused")
	}
	if budget.fits(1, 1, githubTestsMaxReportPayloadBytesPerDestination+1, 0) {
		t.Fatal("a destination one byte over the payload bound was admitted")
	}
	budget.admit(10, 0, githubTestsMaxReportPayloadBytesPerDestination-5, 0)
	if budget.fits(1, 0, 6, 0) || !budget.fits(1, 0, 5, 0) {
		t.Fatal("the payload bound is not judged on the sum of the admitted artifacts")
	}
	if budget.fits(githubTestsMaxReportRowsPerRun-9, 0, 0, 0) || !budget.fits(githubTestsMaxReportRowsPerRun-10, 0, 0, 0) {
		t.Fatal("the row bound is not judged on the sum of the admitted artifacts")
	}
}

// K2: the estimate is never below what the effect batch counts, on rows with the longest stack trace a case row can carry, and a
// run admitted at the payload bound builds its effect batch without a bound error.
func TestGitHubTestsReportPayloadEstimateIsAnUpperBound(t *testing.T) {
	trace := strings.Repeat("x", 4096)
	rows := make([]testCaseResultRow, 1000)
	for index := range rows {
		rows[index] = testCaseResultRow{CaseName: "case-" + strconv.Itoa(index), Status: "failed", StackTrace: &trace}
	}
	estimate, err := githubTestsRowsJSONBytes(rows)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := effectBatchFromValues("test_case_results", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	if estimate < batch.PayloadBytes {
		t.Fatalf("estimate %d is below the effect payload %d", estimate, batch.PayloadBytes)
	}
	perRow := estimate / len(rows)
	atBound := githubTestsMaxReportPayloadBytesPerDestination / perRow
	if _, err := effectBatchFromValues("test_case_results", EffectReadbackRequired, make([]testCaseResultRow, 0)); err != nil {
		t.Fatal(err)
	}
	filled := make([]testCaseResultRow, atBound)
	for index := range filled {
		filled[index] = rows[index%len(rows)]
	}
	if _, err := testOpsEffects(nil, nil, nil, nil, filled, nil); err != nil {
		t.Fatalf("%d failed cases (%d bytes by estimate) are at the payload bound but the effect batch refused them: %v",
			atBound, atBound*perRow, err)
	}
	// And the bound is under the ceiling by construction.
	if githubTestsMaxReportRowsPerRun > maxEffectRows || githubTestsMaxReportPayloadBytesPerDestination >= maxEffectPayloadBytes {
		t.Fatalf("report bounds (%d rows, %d bytes) are not under the effect contract (%d rows, %d bytes)",
			githubTestsMaxReportRowsPerRun, githubTestsMaxReportPayloadBytesPerDestination, maxEffectRows, maxEffectPayloadBytes)
	}
	var bound *EffectBoundExceededError
	if _, err := testOpsEffects(nil, nil, nil, nil, make([]testCaseResultRow, maxEffectRows+1), nil); !errors.As(err, &bound) {
		t.Fatalf("one row over the effect ceiling gave %v, want EffectBoundExceededError", err)
	}
}
