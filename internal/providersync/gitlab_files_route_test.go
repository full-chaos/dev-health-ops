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

type gitLabFilesResponse struct {
	status  int
	body    string
	headers http.Header
}

type gitLabFilesDoer struct {
	t             *testing.T
	requests      []*http.Request
	treeResponses map[string]gitLabFilesResponse
	graphQLSize   gitLabFilesResponse
	graphQLText   gitLabFilesResponse
	project       gitLabFilesResponse
	commits       gitLabFilesResponse
	contentCalls  int
	treeCalls     int
}

func (doer *gitLabFilesDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request)
	response := gitLabFilesResponse{body: `{}`}
	switch {
	case request.URL.Path == "/api/v4/projects/123":
		response = doer.project
	case request.URL.Path == "/api/v4/projects/123/repository/tree":
		doer.treeCalls++
		response = doer.treeResponses[request.URL.Query().Get("page")]
	case request.URL.Path == "/api/v4/projects/123/repository/commits":
		response = doer.commits
	case request.URL.Path == "/api/graphql":
		doer.contentCalls++
		body, err := io.ReadAll(request.Body)
		if err != nil {
			doer.t.Fatal(err)
		}
		if strings.Contains(string(body), "rawSize") {
			response = doer.graphQLSize
		} else if strings.Contains(string(body), "rawTextBlob") {
			response = doer.graphQLText
		} else {
			doer.t.Fatalf("unexpected GitLab GraphQL query=%s", body)
		}
	default:
		doer.t.Fatalf("unexpected GitLab files request %s", request.URL)
	}
	status := response.status
	if status == 0 {
		status = http.StatusOK
	}
	headers := response.headers
	if headers == nil {
		headers = http.Header{"Content-Type": []string{"application/json"}}
	}
	return &http.Response{
		StatusCode: status,
		Header:     headers,
		Body:       io.NopCloser(strings.NewReader(response.body)),
		Request:    request,
	}, nil
}

func gitLabFilesFixtureDoer(t *testing.T) *gitLabFilesDoer {
	t.Helper()
	return &gitLabFilesDoer{
		t:       t,
		project: gitLabFilesResponse{body: gitLabRepositoryFixture},
		treeResponses: map[string]gitLabFilesResponse{
			"1": {
				body: `[{
					"path":"README.md","type":"blob","size":20
				},{"path":"src/main.go","type":"blob","size":15}]`,
				headers: http.Header{"X-Next-Page": []string{"2"}},
			},
			"2": {
				body: `[{
					"path":"tests/example.go","type":"blob","size":20
				},{"path":"src/types.d.ts","type":"blob","size":20}]`,
				headers: http.Header{"X-Next-Page": []string{"3"}},
			},
			"3": {
				body: `[{"path":"src/data.bin","type":"blob","size":8}]`,
			},
		},
		graphQLSize: gitLabFilesResponse{
			body: `{"data":{"project":{"repository":{"blobs":{"nodes":[{"path":"src/main.go","rawSize":15}]}}}}}`,
		},
		graphQLText: gitLabFilesResponse{
			body: `{"data":{"project":{"repository":{"blobs":{"nodes":[{"path":"src/main.go","rawTextBlob":"package main\n"}]}}}}}`,
		},
	}
}

func TestGitLabFilesRouteTraversesTreeAndWritesNonEmptyInventory(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	normalizedAt := time.Date(2026, 8, 10, 12, 0, 0, 987654321, time.UTC)
	batch, err := (GitLabFilesRouteHandler{PerPage: 2, MaxPages: 4}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 6 || doer.treeCalls != 3 || doer.contentCalls != 2 {
		t.Fatalf("requests=%d tree=%d content=%d", len(doer.requests), doer.treeCalls, doer.contentCalls)
	}
	if got := doer.requests[1].URL.Query(); got.Get("ref") != "main" || got.Get("recursive") != "true" ||
		got.Get("page") != "1" || got.Get("per_page") != "2" {
		t.Fatalf("tree query=%s", got.Encode())
	}
	if got := doer.requests[2].URL.Query().Get("page"); got != "2" {
		t.Fatalf("second tree page=%q", got)
	}
	if got := doer.requests[3].URL.Query().Get("page"); got != "3" {
		t.Fatalf("third tree page=%q", got)
	}
	if batch.Evidence.Provider != "gitlab" || batch.Evidence.Dataset != "files" ||
		batch.Evidence.Requests != 6 || batch.Evidence.Pages != 3 || batch.Evidence.Records != 5 ||
		batch.Evidence.CapReached || batch.Watermark != nil {
		t.Fatalf("evidence=%+v watermark=%v", batch.Evidence, batch.Watermark)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "git_files" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || len(batch.Effects[0].Rows) != 5 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	rows := make(map[string]gitFileRow)
	for _, raw := range batch.Effects[0].Rows {
		var row gitFileRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		rows[row.Path] = row
	}
	if len(rows) != 5 || rows["src/main.go"].Contents == nil ||
		*rows["src/main.go"].Contents != "package main\n" || rows["README.md"].Contents != nil ||
		rows["tests/example.go"].Contents != nil || rows["src/types.d.ts"].Contents != nil ||
		rows["src/data.bin"].Contents != nil {
		t.Fatalf("rows=%+v", rows)
	}
	for _, row := range rows {
		if row.OrgID != claim.OrgID || !row.LastSynced.Equal(normalizedAt.UTC().Truncate(time.Millisecond)) {
			t.Fatalf("row scope/timestamp=%+v", row)
		}
	}
	if batch.Result["inventory_status"] != "complete" || batch.Result["files_synced"] != 5 {
		t.Fatalf("result=%+v", batch.Result)
	}
}

func TestGitLabFilesRouteUsesPythonBoundRefAndDistinguishesNoCommit(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.commits = gitLabFilesResponse{body: `[]`}
	claim := nativeTestClaim("gitlab", "files")
	normalizedAt := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 2 || doer.requests[1].URL.Query().Get("ref_name") != "main" ||
		doer.requests[1].URL.Query().Get("until") != "2026-07-31T23:59:59Z" {
		t.Fatalf("bound requests=%v", doer.requests)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 0 || batch.Watermark == nil ||
		!batch.Watermark.Equal(*claim.BeforeAt) || batch.Evidence.Pages != 0 ||
		batch.Result["inventory_status"] != "no_commit_at_bound" {
		t.Fatalf("batch=%+v", batch)
	}
}

func TestGitLabFilesRouteReportsLegitimateEmptyTree(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.treeResponses = map[string]gitLabFilesResponse{"1": {body: `[]`}}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 0 || batch.Evidence.Pages != 1 ||
		batch.Result["inventory_status"] != "empty" {
		t.Fatalf("batch=%+v", batch)
	}
}

func TestGitLabFilesRouteFailsClosedOnTreePaginationCap(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{PerPage: 2, MaxPages: 1}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("cap error=%v", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("capped traversal returned effects/watermark: %+v", batch)
	}
	if len(doer.requests) != 2 || doer.contentCalls != 0 {
		t.Fatalf("requests=%d content=%d", len(doer.requests), doer.contentCalls)
	}
}

func TestGitLabFilesRouteRejectsMalformedTreeItem(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.treeResponses = map[string]gitLabFilesResponse{"1": {body: `[null]`}}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		t.Fatalf("malformed tree error=%v", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("malformed tree returned effects/watermark: %+v", batch)
	}
}

func TestGitLabFilesRouteDegradesGraphQLPayloadErrorToPathsOnly(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.graphQLSize = gitLabFilesResponse{
		body: `{"errors":[{"message":"repository unavailable"}]}`,
	}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 5 || batch.Watermark != nil {
		t.Fatalf("GraphQL payload batch=%+v", batch)
	}
	incomplete, ok := batch.Result[gitLabFilesIncompleteResultKey].([]GitLabFilesIncomplete)
	if !ok || len(incomplete) != 1 || incomplete[0].Cause != "content_size_fetch" {
		t.Fatalf("GraphQL payload incomplete=%#v", batch.Result[gitLabFilesIncompleteResultKey])
	}
}

func TestGitLabFilesRouteDegradesMissingGraphQLRepositoryToPathsOnly(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.graphQLSize = gitLabFilesResponse{body: `{"data":{"project":null}}`}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 5 || batch.Watermark != nil {
		t.Fatalf("missing GraphQL repository batch=%+v", batch)
	}
	incomplete, ok := batch.Result[gitLabFilesIncompleteResultKey].([]GitLabFilesIncomplete)
	if !ok || len(incomplete) != 1 || incomplete[0].Cause != "content_size_fetch" {
		t.Fatalf("missing GraphQL repository incomplete=%#v", batch.Result[gitLabFilesIncompleteResultKey])
	}
}

func TestGitLabFilesRoutePreservesEmptyTextAsPresentContent(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.treeResponses = map[string]gitLabFilesResponse{
		"1": {body: `[{"path":"src/empty.go","type":"blob","size":0}]`},
	}
	doer.graphQLSize = gitLabFilesResponse{
		body: `{"data":{"project":{"repository":{"blobs":{"nodes":[{"path":"src/empty.go","rawSize":0}]}}}}}`,
	}
	doer.graphQLText = gitLabFilesResponse{
		body: `{"data":{"project":{"repository":{"blobs":{"nodes":[{"path":"src/empty.go","rawTextBlob":""}]}}}}}`,
	}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	var row gitFileRow
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 ||
		json.Unmarshal(batch.Effects[0].Rows[0], &row) != nil || row.Contents == nil || *row.Contents != "" {
		t.Fatalf("empty content row=%+v", row)
	}
}

func TestGitLabFilesRouteDegradesGraphQLContentErrorToPathsOnly(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.graphQLSize = gitLabFilesResponse{status: http.StatusInternalServerError, body: `{}`}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 5 || batch.Watermark != nil {
		t.Fatalf("content failure batch=%+v", batch)
	}
	incomplete, ok := batch.Result[gitLabFilesIncompleteResultKey].([]GitLabFilesIncomplete)
	if !ok || len(incomplete) != 1 || incomplete[0].Cause != "content_size_fetch" || incomplete[0].Subject != "gitlab/files" {
		t.Fatalf("content failure incomplete=%#v", batch.Result[gitLabFilesIncompleteResultKey])
	}
}

func TestGitLabFilesRoutePreservesRateLimitPropagation(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.graphQLSize = gitLabFilesResponse{status: http.StatusTooManyRequests, body: `{}`}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("rate error=%v", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("rate failure returned effects/watermark: %+v", batch)
	}
}

func TestGitLabFilesRoutePreservesQualifiedForbiddenRateLimitPropagation(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.graphQLSize = gitLabFilesResponse{
		status:  http.StatusForbidden,
		body:    `{}`,
		headers: http.Header{"RateLimit-Remaining": []string{"0"}},
	}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("qualified forbidden error=%v", err)
	}
	if len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("qualified forbidden returned effects/watermark: %+v", batch)
	}
}

func TestGitLabFilesRouteDegradesUnqualifiedForbiddenToPathsOnly(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.graphQLSize = gitLabFilesResponse{status: http.StatusForbidden, body: `{}`}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 5 || batch.Watermark != nil {
		t.Fatalf("plain forbidden batch=%+v", batch)
	}
	incomplete, ok := batch.Result[gitLabFilesIncompleteResultKey].([]GitLabFilesIncomplete)
	if !ok || len(incomplete) != 1 || incomplete[0].Cause != "content_size_fetch" {
		t.Fatalf("plain forbidden incomplete=%#v", batch.Result[gitLabFilesIncompleteResultKey])
	}
}

func TestGitLabFilesRouteDegradesGraphQLTextFailureToPathsOnly(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.graphQLText = gitLabFilesResponse{status: http.StatusInternalServerError, body: `{}`}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 5 || batch.Watermark != nil {
		t.Fatalf("text failure batch=%+v", batch)
	}
	incomplete, ok := batch.Result[gitLabFilesIncompleteResultKey].([]GitLabFilesIncomplete)
	if !ok || len(incomplete) != 1 || incomplete[0].Cause != "content_fetch" {
		t.Fatalf("text failure incomplete=%#v", batch.Result[gitLabFilesIncompleteResultKey])
	}
}

func TestGitLabFilesRouteMarksContentCapIncompleteWithoutWatermark(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.treeResponses = map[string]gitLabFilesResponse{
		"1": {body: `[{"path":"src/a.go","type":"blob"},{"path":"src/b.go","type":"blob"},{"path":"src/c.go","type":"blob"}]`},
	}
	doer.commits = gitLabFilesResponse{body: `[{"id":"commit-cap"}]`}
	doer.graphQLSize = gitLabFilesResponse{
		body: `{"data":{"project":{"repository":{"blobs":{"nodes":[{"path":"src/a.go","rawSize":10},{"path":"src/b.go","rawSize":10}]}}}}}`,
	}
	doer.graphQLText = gitLabFilesResponse{
		body: `{"data":{"project":{"repository":{"blobs":{"nodes":[{"path":"src/a.go","rawTextBlob":"a"},{"path":"src/b.go","rawTextBlob":"b"}]}}}}}`,
	}
	claim := nativeTestClaim("gitlab", "files")
	bound := time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC)
	claim.BeforeAt = &bound
	batch, err := (GitLabFilesRouteHandler{MaxFiles: 2}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 3 || batch.Watermark != nil || !batch.Evidence.CapReached {
		t.Fatalf("cap batch=%+v", batch)
	}
	incomplete, ok := batch.Result[gitLabFilesIncompleteResultKey].([]GitLabFilesIncomplete)
	if !ok || len(incomplete) != 1 || incomplete[0].Cause != "content_cap" || incomplete[0].Limit != 2 || incomplete[0].Observed != 3 {
		t.Fatalf("cap incomplete=%#v", batch.Result[gitLabFilesIncompleteResultKey])
	}
}

func TestGitLabFilesRouteReraisesGraphQLRateLimit(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.graphQLSize = gitLabFilesResponse{status: http.StatusTooManyRequests, body: `{}`}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	_, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("rate error=%v", err)
	}
}

func TestGitLabFilesRouteCountsPhysicalRetryAttempts(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	client, err := providerfoundation.NewHTTPClient(
		"gitlab", "https://gitlab.example", &retryingGitLabFilesDoer{delegate: doer},
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{PerPage: 2, MaxPages: 4}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Evidence.Requests != 7 || len(doer.requests) != 6 {
		t.Fatalf("evidence=%+v requests=%d", batch.Evidence, len(doer.requests))
	}
}

type retryingGitLabFilesDoer struct {
	delegate *gitLabFilesDoer
	used     bool
}

func (doer *retryingGitLabFilesDoer) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Path == "/api/v4/projects/123/repository/tree" && !doer.used {
		doer.used = true
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{}`)),
			Request:    request,
		}, nil
	}
	return doer.delegate.Do(request)
}

func TestGitLabFilesRouteRejectsBlameClaim(t *testing.T) {
	claim := nativeTestClaim("gitlab", "blame")
	_, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, gitLabFilesFixtureDoer(t), "https://gitlab.example"), time.Now().UTC(),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("wrong dataset error=%v", err)
	}
}

type deniedGitLabFilesBudget struct{}

func (deniedGitLabFilesBudget) Acquire(context.Context, providerfoundation.BudgetKey) (providerfoundation.Reservation, error) {
	return nil, providerfoundation.ErrBudgetUnavailable
}

func TestGitLabFilesRoutePropagatesBudgetDenialBeforeProviderWork(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")
	client.Budget = deniedGitLabFilesBudget{}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	_, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Now().UTC(),
	)
	if !errors.Is(err, providerfoundation.ErrBudgetUnavailable) {
		t.Fatalf("budget error=%v", err)
	}
	if len(doer.requests) != 0 {
		t.Fatalf("budget denial still reached provider: requests=%d", len(doer.requests))
	}
}

func TestGitLabFileContentEligibilityMatchesComplexityScannerConfig(t *testing.T) {
	for _, test := range []struct {
		path string
		want bool
	}{
		{path: "src/main.go", want: true},
		{path: "src/Main.GO", want: false},
		{path: "src/mixed.Go", want: false},
		{path: "README.md", want: false},
		{path: "tests/example.go", want: false},
		{path: "migrations/001.go", want: false},
		{path: "venv/lib.go", want: false},
		{path: ".venv/lib.go", want: false},
		{path: "node_modules/pkg/index.js", want: false},
		{path: "dist/bundle.js", want: false},
		{path: "build/generated.go", want: false},
		{path: ".next/server.js", want: false},
		{path: "vendor/module.go", want: false},
		{path: "src/__init__.py", want: false},
		{path: "src/types.d.ts", want: false},
		{path: "src/app.min.js", want: false},
		{path: "src/app.config.js", want: false},
		{path: "src/app.config.ts", want: false},
		{path: "src/app.config.mjs", want: false},
		{path: "src/data.bin", want: false},
	} {
		if got := gitLabFileContentEligible(test.path); got != test.want {
			t.Fatalf("eligible(%q)=%v want %v", test.path, got, test.want)
		}
	}
}

func TestGitLabFilesRouteSkipsTreeEntriesAndRejectsProjectMismatch(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.treeResponses = map[string]gitLabFilesResponse{"1": {body: `[{"path":"src","type":"tree"}]`}}
	claim := nativeTestClaim("gitlab", "files")
	claim.BeforeAt = nil
	batch, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example"), time.Now().UTC(),
	)
	if err != nil || len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 0 {
		t.Fatalf("tree-only batch=%+v error=%v", batch, err)
	}
	mismatch := gitLabFilesFixtureDoer(t)
	mismatch.project = gitLabFilesResponse{body: `{"id":999,"path_with_namespace":"Acme/API"}`}
	_, err = (GitLabFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, mismatch, "https://gitlab.example"), time.Now().UTC(),
	)
	if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		t.Fatalf("project mismatch error=%v", err)
	}
}
