package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabDeploymentsResponse struct {
	status  int
	body    string
	headers http.Header
}

type gitLabDeploymentsDoer struct {
	t         *testing.T
	responses []gitLabDeploymentsResponse
	requests  []*http.Request
}

func (doer *gitLabDeploymentsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request)
	if len(doer.responses) == 0 {
		doer.t.Fatalf("unexpected request %s", request.URL.RequestURI())
	}
	response := doer.responses[0]
	doer.responses = doer.responses[1:]
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     response.headers,
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}

func TestGitLabDeploymentsRouteMirrorsPythonReleaseMRWindowSinglePageAndEvidence(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 9, 12, 0, 0, 987654321, time.UTC)
	doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[{"tag_name":"v1.2.3"}]`, headers: http.Header{"X-Next-Page": []string{"2"}}},
		{body: `[
			{"id":501,"iid":7,"status":"success","environment":{"name":"future"},"created_at":"2026-08-01T10:00:00Z","finished_at":"2026-08-01T10:05:00Z","sha":"future","ref":"future"},
			{"id":502,"iid":8,"status":"success","environment":{"name":"production"},"created_at":"2026-07-22T10:00:00Z","finished_at":"2026-07-22T10:05:00Z","sha":"main","ref":"v1.2.3"},
			{"id":503,"iid":9,"status":"success","environment":{"name":"old"},"created_at":"2026-06-30T10:00:00Z","finished_at":"2026-06-30T10:05:00Z","sha":"old","ref":"old"}
		]`, headers: http.Header{"X-Next-Page": []string{"2"}}},
		{body: `[{"iid":11,"state":"merged","merged_at":"2026-08-01T09:00:00Z"}]`},
		{body: `[{"iid":44,"state":"opened","merged_at":""},{"iid":45,"state":"merged","merged_at":"2026-07-21T10:00:00Z"}]`},
		{body: `[{"iid":99,"state":"merged","merged_at":"2026-06-30T09:00:00Z"}]`},
	}}
	claim := nativeTestClaim("gitlab", "deployments")
	batch, err := (GitLabDeploymentsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "deployments" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	if batch.Watermark == nil || claim.BeforeAt == nil || !batch.Watermark.Equal(*claim.BeforeAt) ||
		batch.Evidence.Requests != 6 || batch.Evidence.Pages != 5 ||
		batch.Evidence.Records != 1 || batch.Evidence.CapReached {
		t.Fatalf("batch=%+v", batch)
	}
	var row deploymentRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.RepoID != "c7198fbc-1945-3717-05d8-eb78866b4e79" || row.DeploymentID != "502" ||
		row.Status == nil || *row.Status != "success" || row.Environment == nil || *row.Environment != "production" ||
		row.StartedAt == nil || row.FinishedAt == nil || row.DeployedAt == nil ||
		row.PullRequestNumber == nil || *row.PullRequestNumber != 45 || row.MergedAt == nil ||
		row.ReleaseRef != "v1.2.3" || row.ReleaseRefConfidence != 1 ||
		row.OrgID != claim.OrgID ||
		!row.LastSynced.Equal(normalizedAt.UTC().Truncate(time.Millisecond)) {
		t.Fatalf("row=%+v", row)
	}
	wantRequests := []string{
		"/api/v4/projects/123",
		"/api/v4/projects/123/releases?page=1&per_page=100",
		"/api/v4/projects/123/deployments?order_by=created_at&page=1&per_page=100&sort=desc",
		"/api/v4/projects/123/repository/commits/future/merge_requests?page=1&per_page=100",
		"/api/v4/projects/123/repository/commits/main/merge_requests?page=1&per_page=100",
		"/api/v4/projects/123/repository/commits/old/merge_requests?page=1&per_page=100",
	}
	if len(doer.requests) != len(wantRequests) {
		t.Fatalf("requests=%d want=%d", len(doer.requests), len(wantRequests))
	}
	for index, want := range wantRequests {
		if got := doer.requests[index].URL.RequestURI(); got != want {
			t.Fatalf("request[%d]=%q want=%q", index, got, want)
		}
	}
}

func TestGitLabDeploymentsRouteMirrorsPythonCoreFailureAsEmptySuccess(t *testing.T) {
	t.Parallel()
	doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[]`},
		{status: http.StatusServiceUnavailable, body: `{"message":"temporary"}`},
	}}
	claim := nativeTestClaim("gitlab", "deployments")
	batch, err := (GitLabDeploymentsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"),
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 0 ||
		batch.Watermark == nil || claim.BeforeAt == nil || !batch.Watermark.Equal(*claim.BeforeAt) ||
		batch.Evidence.Requests != 3 || batch.Evidence.Pages != 1 || batch.Evidence.Records != 0 {
		t.Fatalf("batch=%+v", batch)
	}
}

func TestGitLabDeploymentsRouteMirrorsPythonSkipsNonObjectCorePayloadAndContinues(t *testing.T) {
	t.Parallel()
	doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[]`},
		{body: `[{"id":501,"iid":7,"status":"success","environment":{"name":"production"},"created_at":"2026-07-22T10:00:00Z","sha":"abc"},"not-an-object",{"id":502,"iid":8,"status":"success","environment":{"name":"staging"},"created_at":"2026-07-21T10:00:00Z","sha":"def"}]`},
		{body: `[{"iid":45,"state":"merged","merged_at":"2026-07-21T10:00:00Z"}]`},
		{body: `[{"iid":46,"state":"merged","merged_at":"2026-07-20T10:00:00Z"}]`},
	}}
	claim := nativeTestClaim("gitlab", "deployments")
	batch, err := (GitLabDeploymentsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"),
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 2 ||
		batch.Watermark == nil || claim.BeforeAt == nil || !batch.Watermark.Equal(*claim.BeforeAt) ||
		batch.Evidence.Requests != 5 || batch.Evidence.Pages != 4 || batch.Evidence.Records != 2 ||
		len(doer.requests) != 5 {
		t.Fatalf("batch=%+v requests=%d", batch, len(doer.requests))
	}
	var first, second deploymentRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(batch.Effects[0].Rows[1], &second); err != nil {
		t.Fatal(err)
	}
	if first.DeploymentID != "501" || first.PullRequestNumber == nil || *first.PullRequestNumber != 45 ||
		second.DeploymentID != "502" || second.PullRequestNumber == nil || *second.PullRequestNumber != 46 {
		t.Fatalf("rows=%+v %+v", first, second)
	}
}

func TestGitLabDeploymentsRouteMirrorsPythonBestEffortReleaseAndMRErrors(t *testing.T) {
	t.Parallel()
	doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
		{body: gitLabRepositoryFixture},
		{status: http.StatusServiceUnavailable, body: `{"message":"temporary"}`},
		{body: `[{"id":501,"iid":7,"status":"success","environment":{"name":"production"},"created_at":"2026-07-22T10:00:00Z","sha":"abc"}]`},
		{status: http.StatusServiceUnavailable, body: `{"message":"temporary"}`},
	}}
	claim := nativeTestClaim("gitlab", "deployments")
	batch, err := (GitLabDeploymentsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"),
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 ||
		batch.Evidence.Requests != 4 || batch.Evidence.Pages != 1 || batch.Evidence.Records != 1 {
		t.Fatalf("batch=%+v", batch)
	}
	var row deploymentRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.ReleaseRef != "7" || row.ReleaseRefConfidence != 0.3 ||
		row.PullRequestNumber != nil || row.MergedAt != nil {
		t.Fatalf("row=%+v", row)
	}
}

func TestGitLabDeploymentsRouteRejectsInvalidClaimClientAndMaxBeforeRequests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		claim            Claim
		handler          GitLabDeploymentsRouteHandler
		clientNil        bool
		zeroNormalizedAt bool
		clientMut        func(*providerfoundation.HTTPClient)
	}{
		{
			name: "wrong claim provider",
			claim: func() Claim {
				claim := nativeTestClaim("gitlab", "deployments")
				claim.Provider = "github"
				return claim
			}(),
			handler: GitLabDeploymentsRouteHandler{},
		},
		{
			name:    "wrong claim dataset",
			claim:   nativeTestClaim("gitlab", "commits"),
			handler: GitLabDeploymentsRouteHandler{},
		},
		{
			name:    "wrong client provider",
			claim:   nativeTestClaim("gitlab", "deployments"),
			handler: GitLabDeploymentsRouteHandler{},
			clientMut: func(client *providerfoundation.HTTPClient) {
				client.Provider = "github"
			},
		},
		{
			name:    "negative max deployments",
			claim:   nativeTestClaim("gitlab", "deployments"),
			handler: GitLabDeploymentsRouteHandler{MaxDeployments: -1},
		},
		{
			name:      "nil client",
			claim:     nativeTestClaim("gitlab", "deployments"),
			handler:   GitLabDeploymentsRouteHandler{},
			clientNil: true,
		},
		{
			name:    "nil client base URL",
			claim:   nativeTestClaim("gitlab", "deployments"),
			handler: GitLabDeploymentsRouteHandler{},
			clientMut: func(client *providerfoundation.HTTPClient) {
				client.BaseURL = nil
			},
		},
		{
			name:             "zero normalized at",
			claim:            nativeTestClaim("gitlab", "deployments"),
			handler:          GitLabDeploymentsRouteHandler{},
			zeroNormalizedAt: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			doer := &gitLabDeploymentsDoer{t: t}
			client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
			if test.clientMut != nil {
				test.clientMut(client)
			}
			if test.clientNil {
				client = nil
			}
			normalizedAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
			if test.zeroNormalizedAt {
				normalizedAt = time.Time{}
			}
			batch, err := test.handler.Collect(
				context.Background(), test.claim, providerfoundation.Credential{}, client,
				normalizedAt,
			)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v", err)
			}
			if len(doer.requests) != 0 || len(batch.Effects) != 0 || batch.Result != nil ||
				batch.Watermark != nil || batch.Evidence != (FetchEvidence{}) {
				t.Fatalf("requests=%d batch=%+v", len(doer.requests), batch)
			}
		})
	}
}

func TestGitLabDeploymentsRouteRejectsProjectIDMismatchBeforeListRequests(t *testing.T) {
	t.Parallel()
	doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
		{body: `{"id":124,"name":"api","path_with_namespace":"Acme/API"}`},
	}}
	batch, err := (GitLabDeploymentsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "deployments"), providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"),
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		t.Fatalf("error=%v", err)
	}
	if len(doer.requests) != 1 || doer.requests[0].URL.Path != "/api/v4/projects/123" ||
		len(batch.Effects) != 0 || batch.Result != nil || batch.Watermark != nil ||
		batch.Evidence != (FetchEvidence{}) {
		t.Fatalf("requests=%d batch=%+v", len(doer.requests), batch)
	}
}
