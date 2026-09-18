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

type githubCommitsResponse struct {
	status  int
	body    string
	headers http.Header
}

type githubCommitsDoer struct {
	t          *testing.T
	responses  []githubCommitsResponse
	failPath   string
	failedOnce bool
	requests   []*http.Request
}

func (doer *githubCommitsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request)
	if request.URL.Path == doer.failPath && !doer.failedOnce {
		doer.failedOnce = true
		return nil, errors.New("simulated transient transport failure")
	}
	if len(doer.responses) == 0 {
		doer.t.Fatalf("unexpected request %s", request.URL)
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

func TestGitHubCommitsRouteEmitsCompleteEffect(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	doer := &githubCommitsDoer{t: t, responses: []githubCommitsResponse{
		{body: gitHubRepositoryFixture},
		{body: `[{"sha":"sha-1","commit":{"message":"ship it","author":{"name":"Ada","date":"2026-07-20T09:00:00Z"},"committer":{"name":"Grace","date":"2026-07-20T10:00:00Z"}},"parents":[]}]`},
	}}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "commits")
	batch, err := (GitHubCommitsRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 2 {
		t.Fatalf("requests=%d", len(doer.requests))
	}
	if batch.Evidence.Requests != 2 || batch.Evidence.Pages != 1 || batch.Evidence.Records != 1 ||
		batch.Evidence.CapReached {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "git_commits" ||
		len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row gitCommitRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.Hash != "sha-1" || row.AuthorName != "Ada" || row.CommitterName != "Grace" {
		t.Fatalf("row=%+v", row)
	}
}

// TestGitHubCommitsRouteCountsFailedAndRetriedAttempts verifies that a repo
// fetch that fails its first wire attempt and succeeds on retry counts that
// extra attempt, not just the decoded page total.
func TestGitHubCommitsRouteCountsFailedAndRetriedAttempts(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	doer := &githubCommitsDoer{
		t: t, failPath: "/repos/acme/api",
		responses: []githubCommitsResponse{
			{body: gitHubRepositoryFixture},
			{body: `[]`},
		},
	}
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://api.github.com", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 2, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := (GitHubCommitsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("github", "commits"),
		providerfoundation.Credential{}, client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 3 {
		t.Fatalf("requests=%d want 3 (failed repo attempt, retried repo attempt, one commits page)", len(doer.requests))
	}
	if batch.Evidence.Requests != 3 {
		t.Fatalf("evidence=%+v want Requests=3 (every physical attempt)", batch.Evidence)
	}
}
