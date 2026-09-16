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

type gitHubFilesDoer struct {
	t             *testing.T
	contentStatus int
}

func (doer gitHubFilesDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	body := `{"full_name":"acme/api","default_branch":"main"}`
	switch request.URL.Path {
	case "/repos/acme/api":
	case "/repos/acme/api/commits":
		body = `[{"sha":"tree-sha"}]`
	case "/repos/acme/api/git/trees/tree-sha":
		body = `{"tree":[{"path":"README.md","type":"blob","size":20},{"path":"src/main.go","type":"blob","size":12},{"path":"dir","type":"tree"}]}`
	case "/graphql":
		if doer.contentStatus != 0 {
			return &http.Response{
				StatusCode: doer.contentStatus,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"message":"content fetch failed"}`)),
				Request:    request,
			}, nil
		}
		var requestBody struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
			doer.t.Fatal(err)
		}
		if !strings.Contains(requestBody.Query, "tree-sha:src/main.go") {
			doer.t.Fatalf("graphql query=%q", requestBody.Query)
		}
		body = `{"data":{"repository":{"f0":{"text":"package main\n","isBinary":false,"isTruncated":false}}}}`
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func TestGitHubFilesRouteTraversesTreeAndWritesNonEmptyInventory(t *testing.T) {
	claim := nativeTestClaim("github", "files")
	client := gitHubRepositoryClient(t, gitHubFilesDoer{t: t}, "https://api.github.com")
	batch, err := (GitHubFilesRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "git_files" || len(batch.Effects[0].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var withContent gitFileRow
	if err := json.Unmarshal(batch.Effects[0].Rows[1], &withContent); err != nil {
		t.Fatal(err)
	}
	if withContent.OrgID != claim.OrgID || withContent.Path != "src/main.go" || withContent.Contents == nil || *withContent.Contents != "package main\n" {
		t.Fatalf("file row=%+v", withContent)
	}
}

func TestGitHubFilesRouteReturnsTraversalFailureWhenContentFetchFails(t *testing.T) {
	claim := nativeTestClaim("github", "files")
	client := gitHubRepositoryClient(t, gitHubFilesDoer{
		t: t, contentStatus: http.StatusInternalServerError,
	}, "https://api.github.com")

	_, err := (GitHubFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitHubFilesTraversalFailed) {
		t.Fatalf("content fetch error=%v, want ErrGitHubFilesTraversalFailed", err)
	}
}

func TestGitHubFilesRouteReraisesContentRateLimits(t *testing.T) {
	claim := nativeTestClaim("github", "files")
	client := gitHubRepositoryClient(t, gitHubFilesDoer{
		t: t, contentStatus: http.StatusTooManyRequests,
	}, "https://api.github.com")

	_, err := (GitHubFilesRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("rate limit error=%v, want ProviderError{Class: ErrorRateLimited}", err)
	}
}

func TestGitHubFilesTraversalPropagatesContextCancellation(t *testing.T) {
	err := continueGitHubFilesTraversal(context.Canceled, "acme/api")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v, want context.Canceled", err)
	}
}

func TestGitHubFileContentEligibilityMatchesScannerConfig(t *testing.T) {
	large := gitHubFileContentMaxBytes + 1
	for _, test := range []struct {
		path string
		size *int
		want bool
	}{
		{path: "src/main.go", want: true},
		{path: "tests/main.go", want: false},
		{path: "migrations/001.go", want: false},
		{path: "src/app.min.js", want: false},
		{path: "src/types.d.ts", want: false},
		{path: "README.md", want: false},
		{path: "src/oversized.go", size: &large, want: false},
	} {
		if got := gitHubFileContentEligible(test.path, test.size); got != test.want {
			t.Fatalf("eligible(%q)=%v want %v", test.path, got, test.want)
		}
	}
}

// gitHubFilesTreeWalkDoer serves the tree-truncation recovery scenarios: a
// truncated recursive listing at the root, a non-recursive re-fetch of that
// same ref, and a per-sha non-recursive fetch for every subtree the walk
// descends into.
type gitHubFilesTreeWalkDoer struct {
	t *testing.T

	rootMissing      bool
	subtreeTruncated bool
	subtreeMissing   bool
}

func (doer gitHubFilesTreeWalkDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	status := http.StatusOK
	body := `{"full_name":"acme/api","default_branch":"main"}`
	recursive := request.URL.Query().Get("recursive") == "true"
	switch {
	case request.URL.Path == "/repos/acme/api":
	case request.URL.Path == "/repos/acme/api/commits":
		body = `[{"sha":"tree-sha"}]`
	case request.URL.Path == "/repos/acme/api/git/trees/tree-sha" && recursive:
		if doer.rootMissing {
			status = http.StatusNotFound
			body = `{"message":"Not Found"}`
			break
		}
		body = `{"truncated":true,"tree":[{"path":"README.md","type":"blob","size":5}]}`
	case request.URL.Path == "/repos/acme/api/git/trees/tree-sha" && !recursive:
		body = `{"truncated":false,"tree":[` +
			`{"path":"README.md","type":"blob","size":5},` +
			`{"path":"src","type":"tree","sha":"src-sha"}` +
			`]}`
	case request.URL.Path == "/repos/acme/api/git/trees/src-sha":
		if doer.subtreeMissing {
			status = http.StatusNotFound
			body = `{"message":"Not Found"}`
			break
		}
		if doer.subtreeTruncated {
			body = `{"truncated":true,"tree":[{"path":"main.go","type":"blob","size":12}]}`
			break
		}
		body = `{"truncated":false,"tree":[{"path":"main.go","type":"blob","size":12}]}`
	case request.URL.Path == "/graphql":
		var requestBody struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
			doer.t.Fatal(err)
		}
		if !strings.Contains(requestBody.Query, "tree-sha:src/main.go") {
			doer.t.Fatalf("graphql query=%q", requestBody.Query)
		}
		body = `{"data":{"repository":{"f0":{"text":"package main\n","isBinary":false,"isTruncated":false}}}}`
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func TestGitHubFilesRouteWalksTruncatedTreeToACompleteInventory(t *testing.T) {
	claim := nativeTestClaim("github", "files")
	client := gitHubRepositoryClient(t, gitHubFilesTreeWalkDoer{t: t}, "https://api.github.com")
	batch, err := (GitHubFilesRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 2 {
		t.Fatalf("effects=%+v, want 2 rows (README.md + src/main.go) recovered via the subtree walk", batch.Effects)
	}
	var paths []string
	for _, raw := range batch.Effects[0].Rows {
		var row gitFileRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, row.Path)
	}
	if !strings.Contains(strings.Join(paths, ","), "src/main.go") {
		t.Fatalf("paths=%v, want src/main.go recovered from the non-recursive subtree walk", paths)
	}
}

func TestGitHubFilesRouteFailsClosedWhenATruncatedSubtreeCannotBeEnumerated(t *testing.T) {
	claim := nativeTestClaim("github", "files")
	client := gitHubRepositoryClient(t, gitHubFilesTreeWalkDoer{t: t, subtreeTruncated: true}, "https://api.github.com")
	batch, err := (GitHubFilesRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if !errors.Is(err, ErrGitHubFilesTraversalFailed) {
		t.Fatalf("error=%v, want ErrGitHubFilesTraversalFailed", err)
	}
	if !strings.Contains(err.Error(), "src") {
		t.Fatalf("error=%v, want the truncated subtree path named in the cause", err)
	}
	if len(batch.Effects) != 0 {
		t.Fatalf("effects=%+v, want no rows written on a failed traversal", batch.Effects)
	}
}

func TestGitHubFilesRouteFailsClosedWhenASubtreeFetchIs404(t *testing.T) {
	claim := nativeTestClaim("github", "files")
	client := gitHubRepositoryClient(t, gitHubFilesTreeWalkDoer{t: t, subtreeMissing: true}, "https://api.github.com")
	batch, err := (GitHubFilesRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if !errors.Is(err, ErrGitHubFilesTraversalFailed) {
		t.Fatalf("error=%v, want ErrGitHubFilesTraversalFailed", err)
	}
	if len(batch.Effects) != 0 {
		t.Fatalf("effects=%+v, want no rows written on a failed traversal", batch.Effects)
	}
}

func TestGitHubFilesRouteFailsClosedOnA404RecursiveTree(t *testing.T) {
	claim := nativeTestClaim("github", "files")
	client := gitHubRepositoryClient(t, gitHubFilesTreeWalkDoer{t: t, rootMissing: true}, "https://api.github.com")
	batch, err := (GitHubFilesRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if !errors.Is(err, ErrGitHubFilesTraversalFailed) {
		t.Fatalf("error=%v, want ErrGitHubFilesTraversalFailed", err)
	}
	if len(batch.Effects) != 0 {
		t.Fatalf("effects=%+v, want no rows written on a missing tree", batch.Effects)
	}
}

func TestGitHubFilesRouteAcceptsAGenuinelyEmptyNonTruncatedTree(t *testing.T) {
	claim := nativeTestClaim("github", "files")
	client := gitHubRepositoryClient(t, gitHubFilesDoerReturningEmptyTree{t: t}, "https://api.github.com")
	batch, err := (GitHubFilesRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 0 {
		t.Fatalf("effects=%+v, want a zero-row success for a genuinely empty tree", batch.Effects)
	}
	if batch.Result["inventory_status"] != "empty" {
		t.Fatalf("result=%+v, want inventory_status=empty", batch.Result)
	}
}

type gitHubFilesDoerReturningEmptyTree struct{ t *testing.T }

func (doer gitHubFilesDoerReturningEmptyTree) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	body := `{"full_name":"acme/api","default_branch":"main"}`
	switch request.URL.Path {
	case "/repos/acme/api":
	case "/repos/acme/api/commits":
		body = `[{"sha":"tree-sha"}]`
	case "/repos/acme/api/git/trees/tree-sha":
		body = `{"truncated":false,"tree":[]}`
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}
