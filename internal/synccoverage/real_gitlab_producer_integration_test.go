//go:build integration

package synccoverage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type realGitLabDoer struct {
	t      *testing.T
	report string
}

func (d *realGitLabDoer) Do(request *http.Request) (*http.Response, error) {
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

// realGitLabResult runs the real GitLab chunked tests producer on a fake server and returns the unit result it writes, as
// the JSON the unit row stores. report is the test_report body the fake serves.
func realGitLabResult(t *testing.T, report string) string {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient("gitlab", "https://gitlab.example", fakehttp.Client(&realGitLabDoer{t: t, report: report}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	capability, _ := providersync.Capability("gitlab", "tests")
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC)
	claim := providersync.Claim{
		Unit: providersync.Unit{
			ID: "11111111-1111-4111-8111-111111111111", SyncRunID: "22222222-2222-4222-8222-222222222222", OrgID: "org-acme",
			IntegrationID: "33333333-3333-4333-8333-333333333333", SourceID: "44444444-4444-4444-8444-444444444444",
			SourceExternalID: "123", SourceName: "acme/api", Provider: "gitlab", Dataset: "tests", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before,
			CredentialID: "55555555-5555-4555-8555-555555555555", AuthSource: "integration_credential",
		},
		Owner: "66666666-6666-4666-8666-666666666666", Attempt: 1,
		LeaseExpiresAt: time.Date(2026, 7, 23, 13, 0, 0, 0, time.UTC),
	}
	var result map[string]any
	err = providersync.GitLabTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(e providersync.ChunkRouteEmission) error {
			if e.Final {
				result = e.Batch.Result
			}
			return nil
		})
	if err != nil || result == nil {
		t.Fatalf("real GitLab producer: err=%v result=%v", err, result)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func realGitLabReport(cases int) string {
	one := `{"status":"success","name":"TestSomething/with_a_subcase","classname":"pkg/service","execution_time":0.012}`
	return `{"test_suites":[{"name":"s","total_time":1,"test_cases":[` + strings.TrimSuffix(strings.Repeat(one+",", cases), ",") + `]}]}`
}

// realGitLabSkippedBody is a test_report over the 2 MiB body cap, so the producer skips it and the unit still succeeds.
func realGitLabSkippedBody() string {
	one := `{"status":"success","name":"TestSomething/with_a_subcase","classname":"pkg/service","execution_time":0.012}`
	return realGitLabReport(2*1024*1024/(len(one)+1) + 200)
}
