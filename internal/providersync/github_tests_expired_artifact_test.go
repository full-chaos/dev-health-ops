package providersync

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// githubTestsExpiredDoer serves one completed run with two report artifacts: 1 readable, 2 listed as expired (never downloaded).
type githubTestsExpiredDoer struct {
	t       *testing.T
	archive []byte
}

func (doer *githubTestsExpiredDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, 1)), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		return githubTestsHTTPResponse(request, header,
			`{"artifacts":[{"id":1,"name":"test-results-1","expired":false},{"id":2,"name":"test-results-2","expired":true}]}`), nil
	case path == "/repos/acme/api/actions/artifacts/1/zip":
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}},
			Body: io.NopCloser(bytes.NewReader(doer.archive)), Request: request,
		}, nil
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

// CHAOS-13980: a report artifact GitHub lists as expired is a lost report, like an unavailable or oversized one: the unit result
// says reports_complete=false with a report_member/artifact_expired observation and a durable marker, the skip is counted, and the
// watermark still advances (the loss is permanent provider data).
func TestGitHubTestsExpiredReportArtifactIsALoss(t *testing.T) {
	doer := &githubTestsExpiredDoer{t: t, archive: githubTestsHealthyArchive(t)}
	client := githubTestsClient(t, fakehttp.Client(doer))
	client.Metrics = providerfoundation.NewMetrics()
	claim := nativeTestClaim("github", "cicd")
	walk := walkGitHubTestsChunks(t, GitHubTestsRouteHandler{}, claim, client, 4)
	t.Logf("unit result: reports_complete=%v incomplete=%v skipped_artifacts=%v",
		walk.final.Result["reports_complete"], walk.final.Result["incomplete"], walk.final.Result["skipped_artifacts"])

	if complete, ok := walk.final.Result["reports_complete"].(bool); !ok || complete {
		t.Errorf("reports_complete=%v, want false: the expired artifact's report was never read", walk.final.Result["reports_complete"])
	}
	found := false
	for _, observation := range walk.cursor.Incomplete {
		if observation.Component == githubTestsReportMemberComponent && observation.Cause == "artifact_expired" && observation.Count == 1 {
			found = true
		}
	}
	if !found {
		t.Errorf("incomplete=%+v, want one report_member/artifact_expired observation", walk.cursor.Incomplete)
	}
	marked := false
	markers, _ := walk.final.Result["skipped_artifacts"].([]GitHubTestsSkippedArtifact)
	for _, marker := range markers {
		if marker.RunID == "1" && marker.ArtifactID == "2" && marker.Cause == "artifact_expired" {
			marked = true
		}
	}
	if !marked {
		t.Errorf("skipped_artifacts=%#v, want a marker run 1 artifact 2 cause artifact_expired", walk.final.Result["skipped_artifacts"])
	}
	var buffer bytes.Buffer
	if err := client.Metrics.WritePrometheus(&buffer); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	if want := `dev_health_provider_artifact_skipped_total{provider="github",dataset="cicd",reason="artifact_expired"} 1`; !strings.Contains(buffer.String(), want) {
		t.Errorf("metrics did not carry the expired skip: want line %s", want)
	}
	if walk.final.Watermark == nil || !sameOptionalTime(walk.final.Watermark, claim.BeforeAt) {
		t.Errorf("watermark=%v, want the window end %v (an expired report is permanent provider data)", walk.final.Watermark, claim.BeforeAt)
	}
	if _, err := (ProductionContractComparator{}).CompareCompleteRoute(context.Background(), claim, walk.final); err != nil {
		t.Errorf("production comparator rejected the completion: %v", err)
	}
}
