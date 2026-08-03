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
