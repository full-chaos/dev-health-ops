package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type gitLabGapSignalDoer struct {
	t      *testing.T
	report string
}

func (d *gitLabGapSignalDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	body := ""
	switch {
	case path == "/api/v4/projects/123":
		body = `{"id":123,"path_with_namespace":"acme/api","default_branch":"main","only_allow_merge_if_pipeline_succeeds":true}`
	case path == "/api/v4/projects/123/pipelines":
		body = `[{"id":9000,"name":"CI","ref":"main","status":"success","created_at":"2026-07-22T10:00:00Z","started_at":"2026-07-22T10:01:00Z","finished_at":"2026-07-22T10:05:00Z","source":"push","sha":"abc","web_url":"https://gitlab.example/acme/api/-/pipelines/9000"}]`
	case strings.HasSuffix(path, "/jobs"):
		body = `[]`
	case strings.HasSuffix(path, "/test_report"):
		body = d.report
	default:
		d.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func gitLabGapSignalReport(cases int) string {
	one := `{"status":"success","name":"TestSomething/with_a_subcase","classname":"pkg/service","execution_time":0.012}`
	return `{"test_suites":[{"name":"s","total_time":1,"test_cases":[` + strings.TrimSuffix(strings.Repeat(one+",", cases), ",") + `]}]}`
}

func gitLabGapSignalOversize() string {
	one := `{"status":"success","name":"TestSomething/with_a_subcase","classname":"pkg/service","execution_time":0.012}`
	return gitLabGapSignalReport(nativeMaxObjectBytes/(len(one)+1) + 200)
}

func gitLabGapSignalClient(t *testing.T, report string) *providerfoundation.HTTPClient {
	t.Helper()
	return gitLabRepositoryClient(t, fakehttp.Client(&gitLabGapSignalDoer{t: t, report: report}), "https://gitlab.example")
}

func gitLabGapSignalFinal(t *testing.T, report string) CompleteRouteBatch {
	t.Helper()
	var final CompleteRouteBatch
	seen := false
	err := GitLabTestsRouteHandler{}.CollectChunks(context.Background(), nativeTestClaim("gitlab", "tests"), providerfoundation.Credential{},
		gitLabGapSignalClient(t, report), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(e ChunkRouteEmission) error {
			if e.Final {
				final, seen = e.Batch, true
			}
			return nil
		})
	if err != nil || !seen {
		t.Fatalf("CollectChunks err=%v final=%v", err, seen)
	}
	return final
}

// The synccoverage reader marks a successful tests unit as a gap when its result carries reports_complete=false
// (load.go, the 13797 rule). A GitLab report over the size cap is skipped with the unit still a success, so the
// unit must say so under that same key.
func TestGitLabTestsSkippedReportSetsTheGapSignal(t *testing.T) {
	final := gitLabGapSignalFinal(t, gitLabGapSignalOversize())
	if got, ok := final.Result["reports_complete"].(bool); !ok || got {
		t.Fatalf("reports_complete = %v (%T), want false for a skipped report", final.Result["reports_complete"], final.Result["reports_complete"])
	}
	if got, ok := final.Result["coverage_complete"].(bool); !ok || got {
		t.Fatalf("coverage_complete = %v, want false", final.Result["coverage_complete"])
	}
	truncated, _ := final.Result["inventory_truncated"].([]string)
	if !slices.Equal(truncated, []string{gitLabReportTooLargeComponent}) {
		t.Fatalf("inventory_truncated = %v, want [%s]", truncated, gitLabReportTooLargeComponent)
	}
}

func TestGitLabTestsCompleteUnitKeepsTheGapSignalTrue(t *testing.T) {
	final := gitLabGapSignalFinal(t, gitLabGapSignalReport(3))
	if got, ok := final.Result["reports_complete"].(bool); !ok || !got {
		t.Fatalf("reports_complete = %v, want true for a complete unit", final.Result["reports_complete"])
	}
	if got, ok := final.Result["coverage_complete"].(bool); !ok || !got {
		t.Fatalf("coverage_complete = %v, want true", final.Result["coverage_complete"])
	}
}

// The non-chunked route has no skip: a report over the cap fails the unit, so there is no success-with-skip result to
// mark (named limit; gitlab_tests_route.go fetchGitLabTestsReport and the artifact download return the error).
func TestGitLabTestsCollectFailsTheUnitOnAnOversizedReport(t *testing.T) {
	batch, err := GitLabTestsRouteHandler{}.Collect(context.Background(), nativeTestClaim("gitlab", "tests"), providerfoundation.Credential{},
		gitLabGapSignalClient(t, gitLabGapSignalOversize()), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))
	if !errors.Is(err, errGitLabTestsReportTooLarge) {
		t.Fatalf("err = %v, want errGitLabTestsReportTooLarge", err)
	}
	if batch.Result != nil || len(batch.Effects) != 0 {
		t.Fatalf("a failed unit returned a result: %+v", batch.Result)
	}
}
