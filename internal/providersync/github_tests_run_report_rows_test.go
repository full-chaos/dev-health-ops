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
	id   string
	size int64
	rows int
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
				archive := githubTestsZip(doer.t, map[string]string{"junit.xml": githubTestsMultiSuiteJUnit(artifact.rows / 2)})
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
	walk, doer, rendered := rowsWalk(t, 1, rowsArtifact{"1", 10, 300}, rowsArtifact{"2", 20, 300}, rowsArtifact{"3", 30, 300})
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

// (2) The listing order does not decide the survivor: the same artifacts, listed two ways, admit the same set.
func TestGitHubTestsRunReportRowsAdmissionDoesNotDependOnListingOrder(t *testing.T) {
	big := rowsArtifact{"101", 3000, 60_000}
	small := rowsArtifact{"102", 1000, 60_000}
	middle := rowsArtifact{"103", 2000, 30_000}
	first, _, _ := rowsWalk(t, 1, big, small, middle)
	second, _, _ := rowsWalk(t, 1, middle, small, big)
	// Smallest declared size first: small (60,000) and middle (30,000) fit, big would reach 150,000.
	for name, walk := range map[string]githubTestsWalk{"big,small,middle": first, "middle,small,big": second} {
		if got := committedReportRows(walk); got != 90_000 {
			t.Fatalf("%s: committed %d rows, want 90,000", name, got)
		}
		if ids := skippedArtifactIDs(walk); len(ids) != 1 || ids[0] != "101" {
			t.Fatalf("%s: skipped %v, want only the largest declared artifact 101", name, ids)
		}
	}
}

// (3) An artifact that alone exceeds the bound is skipped whole, and the artifacts after it are still admitted.
func TestGitHubTestsRunReportRowsSkipAnArtifactThatAloneExceedsTheBound(t *testing.T) {
	walk, doer, _ := rowsWalk(t, 1, rowsArtifact{"1", 9000, githubTestsMaxReportRowsPerRun + 2}, rowsArtifact{"2", 10, 400}, rowsArtifact{"3", 20, 400})
	if got := committedReportRows(walk); got != 800 {
		t.Fatalf("committed %d rows, want the two small artifacts (800)", got)
	}
	if ids := skippedArtifactIDs(walk); len(ids) != 1 || ids[0] != "1" {
		t.Fatalf("skipped %v, want only the oversized artifact 1", ids)
	}
	if len(doer.downloaded) != 3 {
		t.Fatalf("downloaded %v, want all 3", doer.downloaded)
	}
	assertFinalizedAndAdvanced(t, walk)
}

// (4) + K4: three artifacts skipped in one run leave ONE per_run_cap record (count and metric) and ONE marker per skipped artifact.
func TestGitHubTestsRunReportRowsRecordOnceForTheRunAndOneMarkerPerSkippedArtifact(t *testing.T) {
	walk, _, rendered := rowsWalk(t, 1,
		rowsArtifact{"1", 10, 80_000}, rowsArtifact{"2", 20, 60_000}, rowsArtifact{"3", 30, 60_000}, rowsArtifact{"4", 40, 60_000})
	if got := committedReportRows(walk); got != 80_000 || got > githubTestsMaxReportRowsPerRun {
		t.Fatalf("committed %d rows, want 80,000 and never above the bound", got)
	}
	observation := perRunObservation(t, walk, githubTestsRunReportsComponent)
	if observation.Cause != githubTestsPerRunCapCause || observation.Count != 1 {
		t.Fatalf("observation %+v, want per_run_cap count 1 for the run", observation)
	}
	if ids := skippedArtifactIDs(walk); strings.Join(ids, ",") != "2,3,4" {
		t.Fatalf("skipped markers %v, want one each for 2, 3 and 4", ids)
	}
	for _, marker := range walk.cursor.SkippedArtifacts {
		if marker.Cause != githubTestsPerRunCapCause || marker.RunID == "" || marker.Name == "" {
			t.Fatalf("marker %+v lacks cause, run id or name", marker)
		}
	}
	want := `dev_health_provider_per_run_truncation_total{provider="github",dataset="cicd",component="run_reports",cause="per_run_cap"} 1`
	if !strings.Contains(rendered, want) {
		t.Fatalf("metric not exactly once for the run; want %q in:\n%s", want, rendered)
	}
	assertFinalizedAndAdvanced(t, walk)
}

// (5) The bound is exact: 100,000 rows are admitted, 100,002 are not.
func TestGitHubTestsRunReportRowsBoundaryIsExact(t *testing.T) {
	atBound, _, _ := rowsWalk(t, 1, rowsArtifact{"1", 10, 60_000}, rowsArtifact{"2", 20, 40_000})
	if got := committedReportRows(atBound); got != githubTestsMaxReportRowsPerRun || len(atBound.cursor.SkippedArtifacts) != 0 {
		t.Fatalf("at the bound: committed %d, markers %d, want %d and none", got, len(atBound.cursor.SkippedArtifacts), githubTestsMaxReportRowsPerRun)
	}
	over, _, _ := rowsWalk(t, 1, rowsArtifact{"1", 10, 60_000}, rowsArtifact{"2", 20, 40_002})
	if got := committedReportRows(over); got != 60_000 || len(over.cursor.SkippedArtifacts) != 1 {
		t.Fatalf("one pair over the bound: committed %d, markers %d, want 60,000 and one skip", got, len(over.cursor.SkippedArtifacts))
	}
}

// (6) The payload bound is judged on json bytes per destination, below the 64 MiB effect ceiling.
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

// (7) The budget is per run: two runs of one unit each admit what fits.
func TestGitHubTestsRunReportRowsBudgetsAreIndependentPerRun(t *testing.T) {
	walk, _, rendered := rowsWalk(t, 2, rowsArtifact{"1", 10, 60_000}, rowsArtifact{"2", 20, 60_000})
	if got := committedReportRows(walk); got != 120_000 {
		t.Fatalf("committed %d rows over two runs, want 60,000 each (120,000)", got)
	}
	want := `component="run_reports",cause="per_run_cap"} 2`
	if !strings.Contains(rendered, want) {
		t.Fatalf("want the metric once per affected run (%q) in:\n%s", want, rendered)
	}
}

// (8) After an overflow the walk goes on: a later artifact that fits is admitted.
func TestGitHubTestsRunReportRowsWalkContinuesAfterAnOverflow(t *testing.T) {
	walk, doer, _ := rowsWalk(t, 1, rowsArtifact{"1", 10, 99_000}, rowsArtifact{"2", 20, 2_000}, rowsArtifact{"3", 30, 500})
	if got := committedReportRows(walk); got != 99_500 {
		t.Fatalf("committed %d rows, want 99,500 (the first and the third)", got)
	}
	if ids := skippedArtifactIDs(walk); len(ids) != 1 || ids[0] != "2" {
		t.Fatalf("skipped %v, want only artifact 2", ids)
	}
	if len(doer.downloaded) != 3 {
		t.Fatalf("downloaded %v, want all 3: no break at the overflow", doer.downloaded)
	}
}
