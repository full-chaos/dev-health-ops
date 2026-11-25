package providersync

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// githubTestsExpiredClassDoer serves the runs listing and artifact listing the cell gives; every artifact that is not listed as
// expired downloads the healthy archive.
type githubTestsExpiredClassDoer struct {
	t         *testing.T
	runs      string
	artifacts string
	archive   []byte
}

func (doer *githubTestsExpiredClassDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, doer.runs), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		return githubTestsHTTPResponse(request, header, doer.artifacts), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}},
			Body: io.NopCloser(bytes.NewReader(doer.archive)), Request: request,
		}, nil
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

type githubTestsExpiredCell struct {
	name        string
	runs        string
	artifacts   string
	wantExpired int // report_member/artifact_expired count; 0 = the unit must read complete
	wantNoCap   bool
}

// githubTestsDuplicateRunFixture lists run 1 twice, as an overlapping search range lists a boundary run again.
func githubTestsDuplicateRunFixture() string {
	inner := strings.TrimSuffix(strings.TrimPrefix(githubTestsWorkflowRunsFixture(1, 1), `{"workflow_runs":[`), `]}`)
	return `{"workflow_runs":[` + inner + `,` + inner + `]}`
}

func githubTestsExpiredArtifactsJSON(entries ...string) string {
	return `{"artifacts":[` + strings.Join(entries, ",") + `]}`
}

func githubTestsExpiredEntry(id int, name string, expired bool) string {
	return fmt.Sprintf(`{"id":%d,"name":%q,"expired":%v}`, id, name, expired)
}

func githubTestsExpiredCells() []githubTestsExpiredCell {
	many := []string{}
	for id := 1; id <= 25; id++ {
		many = append(many, githubTestsExpiredEntry(id, fmt.Sprintf("test-results-%d", id), false))
	}
	return []githubTestsExpiredCell{
		{name: "older_expired_newer_readable_is_superseded", runs: githubTestsWorkflowRunsFixture(1, 1), wantExpired: 0,
			artifacts: githubTestsExpiredArtifactsJSON(githubTestsExpiredEntry(1, "test-results", true), githubTestsExpiredEntry(2, "test-results", false))},
		{name: "newest_expired_older_readable_is_a_loss", runs: githubTestsWorkflowRunsFixture(1, 1), wantExpired: 1,
			artifacts: githubTestsExpiredArtifactsJSON(githubTestsExpiredEntry(1, "test-results", false), githubTestsExpiredEntry(2, "test-results", true))},
		{name: "both_attempts_expired_counts_once", runs: githubTestsWorkflowRunsFixture(1, 1), wantExpired: 1,
			artifacts: githubTestsExpiredArtifactsJSON(githubTestsExpiredEntry(1, "test-results", true), githubTestsExpiredEntry(2, "test-results", true))},
		{name: "expired_name_passing_the_filter_is_fail_closed", runs: githubTestsWorkflowRunsFixture(1, 1), wantExpired: 2,
			artifacts: githubTestsExpiredArtifactsJSON(githubTestsExpiredEntry(1, "release-binary", true), githubTestsExpiredEntry(2, "screenshots-index", true))},
		{name: "expired_playwright_results_counts", runs: githubTestsWorkflowRunsFixture(1, 1), wantExpired: 1,
			artifacts: githubTestsExpiredArtifactsJSON(githubTestsExpiredEntry(44176848287, "playwright-results-default-1-44176848287-1", true))},
		{name: "expired_name_the_filter_excludes_never_counts", runs: githubTestsWorkflowRunsFixture(1, 1), wantExpired: 0,
			artifacts: githubTestsExpiredArtifactsJSON(githubTestsExpiredEntry(1, "go-images-api", true), githubTestsExpiredEntry(2, "base.dockerbuild", true), githubTestsExpiredEntry(3, "sbom.spdx.json", true))},
		{name: "run_listed_twice_counts_once", runs: githubTestsDuplicateRunFixture(), wantExpired: 1,
			artifacts: githubTestsExpiredArtifactsJSON(githubTestsExpiredEntry(1, "test-results", true))},
		{name: "expired_listed_first_keeps_all_25_readable", runs: githubTestsWorkflowRunsFixture(1, 1), wantExpired: 1, wantNoCap: true,
			artifacts: githubTestsExpiredArtifactsJSON(append([]string{githubTestsExpiredEntry(99, "junit-extra", true)}, many...)...)},
		{name: "expired_listed_last_keeps_all_25_readable", runs: githubTestsWorkflowRunsFixture(1, 1), wantExpired: 1, wantNoCap: true,
			artifacts: githubTestsExpiredArtifactsJSON(append(append([]string{}, many...), githubTestsExpiredEntry(99, "junit-extra", true))...)},
	}
}

func githubTestsCheckExpiredCell(t *testing.T, cell githubTestsExpiredCell, claim Claim, batch CompleteRouteBatch, metrics *providerfoundation.Metrics) {
	t.Helper()
	incomplete, _ := batch.Result["incomplete"].([]GitHubTestsIncomplete)
	expired, other := 0, 0
	for _, observation := range incomplete {
		if observation.Component == githubTestsReportMemberComponent && observation.Cause == githubTestsArtifactExpiredCause {
			expired += observation.Count
			continue
		}
		other++
		if cell.wantNoCap && observation.Component == githubTestsRunArtifactsComponent {
			t.Errorf("unexpected observation %+v: an expired artifact must not take a download slot", observation)
		}
	}
	if expired != cell.wantExpired {
		t.Errorf("artifact_expired count=%d, want %d (incomplete=%+v)", expired, cell.wantExpired, incomplete)
	}
	markers, _ := batch.Result["skipped_artifacts"].([]GitHubTestsSkippedArtifact)
	expiredMarkers := 0
	for _, marker := range markers {
		if marker.Cause == githubTestsArtifactExpiredCause {
			expiredMarkers++
		}
	}
	if expiredMarkers != cell.wantExpired {
		t.Errorf("markers=%+v, want %d expired markers (one per run and artifact)", markers, cell.wantExpired)
	}
	if cell.wantExpired == 0 && other == 0 && !cell.wantNoCap && batch.Result["reports_complete"] != true {
		t.Errorf("reports_complete=%v, want true", batch.Result["reports_complete"])
	}
	if batch.Watermark == nil || !sameOptionalTime(batch.Watermark, claim.BeforeAt) {
		t.Errorf("watermark=%v, want the window end %v", batch.Watermark, claim.BeforeAt)
	}
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(context.Background(), claim, batch); err != nil {
		t.Errorf("production comparator rejected the completion: %v", err)
	}
	// The route never counts the expired skip itself (providerunit counts the committed observation once).
	var buffer bytes.Buffer
	if err := metrics.WritePrometheus(&buffer); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	if strings.Contains(buffer.String(), `reason="artifact_expired"`) {
		t.Errorf("route-level metric must not carry artifact_expired, got:\n%s", buffer.String())
	}
}

// CHAOS-13980 (codex round 1): the chunked and the non-chunked route agree on every expired-artifact cell: supersession, one
// record per (run, artifact), no cap slot taken, report-shaped names only, and the window-end watermark with a durable marker.
func TestGitHubTestsExpiredArtifactClassesBothRoutes(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	for _, dataset := range []string{"tests", "cicd"} {
		for _, cell := range githubTestsExpiredCells() {
			t.Run("chunked/"+dataset+"/"+cell.name, func(t *testing.T) {
				doer := &githubTestsExpiredClassDoer{t: t, runs: cell.runs, artifacts: cell.artifacts, archive: githubTestsHealthyArchive(t)}
				client := githubTestsClient(t, fakehttp.Client(doer))
				client.Metrics = providerfoundation.NewMetrics()
				claim := githubTestsExpiredClaim(dataset)
				walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, claim, client, 4)
				githubTestsCheckExpiredCell(t, cell, claim, walk.final, client.Metrics)
			})
			t.Run("nonchunked/"+dataset+"/"+cell.name, func(t *testing.T) {
				doer := &githubTestsExpiredClassDoer{t: t, runs: cell.runs, artifacts: cell.artifacts, archive: githubTestsHealthyArchive(t)}
				client := githubTestsClient(t, fakehttp.Client(doer))
				client.Metrics = providerfoundation.NewMetrics()
				claim := githubTestsExpiredClaim(dataset)
				batch, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt)
				if err != nil {
					t.Fatalf("Collect: %v", err)
				}
				githubTestsCheckExpiredCell(t, cell, claim, batch, client.Metrics)
			})
		}
	}
}

// CHAOS-13980: the named residual of the once-per-(run, artifact) rule. The marker sample is bounded; once it is full, a revisit
// of an expired artifact whose marker was dropped counts again. The count is inflated, but the close decision does not change: the
// unit reads reports_complete=false with only report_member/artifact_expired causes and the watermark still advances.
func TestGitHubTestsExpiredRevisitWithFullSampleOvercounts(t *testing.T) {
	entries := []string{}
	for id := 1; id <= githubTestsMaxSkippedArtifactRecords+1; id++ {
		entries = append(entries, githubTestsExpiredEntry(id, fmt.Sprintf("build-%d", id), true))
	}
	distinct := len(entries)
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	check := func(t *testing.T, claim Claim, batch CompleteRouteBatch, wantCount int) {
		t.Helper()
		incomplete, _ := batch.Result["incomplete"].([]GitHubTestsIncomplete)
		if len(incomplete) != 1 || incomplete[0].Component != githubTestsReportMemberComponent || incomplete[0].Cause != githubTestsArtifactExpiredCause {
			t.Fatalf("incomplete=%+v, want only report_member/artifact_expired", incomplete)
		}
		if incomplete[0].Count != wantCount {
			t.Errorf("count=%d, want %d", incomplete[0].Count, wantCount)
		}
		if batch.Result["reports_complete"] != false {
			t.Errorf("reports_complete=%v, want false", batch.Result["reports_complete"])
		}
		if batch.Watermark == nil || !sameOptionalTime(batch.Watermark, claim.BeforeAt) {
			t.Errorf("watermark=%v, want the window end", batch.Watermark)
		}
		if _, err := (ProductionContractComparator{}).CompareCompleteRoute(context.Background(), claim, batch); err != nil {
			t.Errorf("production comparator rejected the completion: %v", err)
		}
	}
	for _, route := range []string{"chunked", "nonchunked"} {
		t.Run(route, func(t *testing.T) {
			doer := &githubTestsExpiredClassDoer{t: t, runs: githubTestsDuplicateRunFixture(), artifacts: githubTestsExpiredArtifactsJSON(entries...), archive: githubTestsHealthyArchive(t)}
			client := githubTestsClient(t, fakehttp.Client(doer))
			client.Metrics = providerfoundation.NewMetrics()
			claim := githubTestsExpiredClaim("cicd")
			if route == "chunked" {
				check(t, claim, walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, claim, client, 4).final,
					// the first visit counts every artifact; the revisit dedups those with a marker and recounts the one whose marker was dropped
					distinct+1)
				return
			}
			batch, err := (GitHubTestsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt)
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			// this route lists each run id once, so it has no revisit to recount
			check(t, claim, batch, distinct)
		})
	}
}

// githubTestsExpiredClaim is a unit of the integration that carries this repository's own CI artifact names as its exclude list.
func githubTestsExpiredClaim(dataset string) Claim {
	claim := nativeTestClaim("github", dataset)
	claim.IntegrationConfig = map[string]any{"ci_artifact_exclude_prefixes": []any{"go-images-", "go-test-timings-", "go-junit-part-"}}
	return claim
}
