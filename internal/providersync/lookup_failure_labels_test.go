package providersync

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/egressguard"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// egressRefusedOnPathDoer refuses, as the dial-time egress guard does, every request whose path ends with suffix; the
// other requests go to inner. The refusal is a request that got no response.
type egressRefusedOnPathDoer struct {
	inner  providerfoundation.HTTPDoer
	suffix string
}

func (doer egressRefusedOnPathDoer) Do(request *http.Request) (*http.Response, error) {
	if strings.HasSuffix(request.URL.Path, doer.suffix) {
		return nil, fmt.Errorf("dial tcp: %w", egressguard.ErrRefused)
	}
	return doer.inner.Do(request)
}

// collectDeploymentsWithDoer is collectDeploymentsLookup with the caller's doer.
func collectDeploymentsWithDoer(t *testing.T, provider string, doer providerfoundation.HTTPDoer) (CompleteRouteBatch, error) {
	t.Helper()
	base := map[string]string{"github": "https://api.github.com", "gitlab": "https://gitlab.example"}[provider]
	client, err := providerfoundation.NewHTTPClient(provider, base, fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 3, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	defaultGitHubTreeWalkCache.clear()
	claim := nativeTestClaim(provider, "deployments")
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	if provider == "github" {
		return (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	}
	return (GitLabDeploymentsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
}

// An egress refusal of only the enrichment or lookup call (the list reads passed) is a request that got no response,
// not an answered 4xx: the route must return the refusal and hold the watermark, so the unit fails with its
// egress_refused category instead of acknowledging rows whose enrichment was never read. RED at main: the
// status-0 ErrorPermanent is read as an answered 4xx ("other_4xx"), the read keeps best effort and the window advances.
func TestEgressRefusedEnrichmentReadFailsTheUnit(t *testing.T) {
	for name, collect := range map[string]func(t *testing.T) (CompleteRouteBatch, error){
		"github/pr-reviews": func(t *testing.T) (CompleteRouteBatch, error) {
			batch, err, _ := collectPRReviews(t, egressRefusedOnPathDoer{suffix: "/graphql",
				inner: &gitHubPullRequestReviewRouteDoer{t: t, restBodies: defaultGitHubPullRequestFixtures()}})
			return batch, err
		},
		"gitlab/prs approvals": func(t *testing.T) (CompleteRouteBatch, error) {
			responses := gitLabOneMergeRequestWithApprovals(gitLabPullRequestResponse{})
			responses = append(responses[:2], responses[3:]...)
			batch, err, _ := collectGitLabPRsForApprovals(t, egressRefusedOnPathDoer{suffix: "/approvals",
				inner: &gitLabPullRequestDoer{t: t, responses: responses}})
			return batch, err
		},
		"gitlab/prs commits listing": func(t *testing.T) (CompleteRouteBatch, error) {
			batch, err, _ := collectGitLabPRsWithCommits(t, egressRefusedOnPathDoer{suffix: "/commits",
				inner: &gitLabPullRequestDoer{t: t, responses: gitLabOneMergeRequestResponses()}})
			return batch, err
		},
		"github/deployments pull request lookup": func(t *testing.T) (CompleteRouteBatch, error) {
			return collectDeploymentsWithDoer(t, "github", egressRefusedOnPathDoer{suffix: "/commits/abc/pulls",
				inner: deploymentsLookupDoer{provider: "github", lookup: deploymentsLookupHappy}})
		},
		"gitlab/deployments merge request lookup": func(t *testing.T) (CompleteRouteBatch, error) {
			return collectDeploymentsWithDoer(t, "gitlab", egressRefusedOnPathDoer{suffix: "/repository/commits/abc/merge_requests",
				inner: deploymentsLookupDoer{provider: "gitlab", lookup: deploymentsLookupHappy}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			batch, err := collect(t)
			if !providerfoundation.IsEgressRefused(err) || batch.Watermark != nil {
				t.Fatalf("err = %v, watermark = %v, result = %v: want the egress refusal and no watermark", err, batch.Watermark, batch.Result)
			}
		})
	}
}

// The permanent arm keeps an answered 4xx served; only a refusal with no response fails the lookup.
func TestClassifyLookupFailurePermanentNeedsAnAnswered4xx(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		verdict lookupVerdict
		reason  string
	}{
		{"answered 400", &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, StatusCode: http.StatusBadRequest}, lookupRecorded, "bad_request"},
		{"answered 410", &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, StatusCode: http.StatusGone}, lookupRecorded, "other_4xx"},
		{"no response", &providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, Reason: providerfoundation.TransportReasonEgressRefused}, lookupFails, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if verdict, reason := classifyLookupFailure(test.err, nil); verdict != test.verdict || reason != test.reason {
				t.Fatalf("verdict = %v, reason = %q; want %v, %q", verdict, reason, test.verdict, test.reason)
			}
		})
	}
}

// gitHubAuthFailsOnGraphQLDoer records whether a /graphql request reached the wire; REST goes to inner.
type gitHubAuthFailsOnGraphQLDoer struct {
	inner   *gitHubPullRequestReviewRouteDoer
	graphQL *int
}

func (doer gitHubAuthFailsOnGraphQLDoer) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Path == "/graphql" {
		*doer.graphQL++
	}
	return doer.inner.Do(request)
}

// A token refresh that fails before the reviews read is sent (an AuthStepError) fails the unit, and its Warn names no
// HTTP status class: the reviews request was never sent, so status_class is none, not the token exchange's class.
// RED at main: lookupStatusClass unwraps the AuthStepError and logs the exchange's 5xx.
func TestAuthStepFailureAtTheReviewsReadLogsNoStatusClass(t *testing.T) {
	var logs strings.Builder
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	graphQL := 0
	client := gitHubPullRequestClient(t, fakehttp.Client(gitHubAuthFailsOnGraphQLDoer{
		inner: &gitHubPullRequestReviewRouteDoer{t: t, restBodies: defaultGitHubPullRequestFixtures()}, graphQL: &graphQL,
	}), "https://api.github.com")
	client.Auth = func(request *http.Request) error {
		if request.URL.Path == "/graphql" {
			return &providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient, StatusCode: http.StatusBadGateway}
		}
		return nil
	}
	batch, err := (GitHubPullRequestReviewRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("github", "pr-reviews"), providerfoundation.Credential{}, client,
		time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
	)
	if err == nil || batch.Watermark != nil || graphQL != 0 {
		t.Fatalf("err = %v, watermark = %v, graphql requests = %d: want the auth-step error, no watermark, /graphql never sent", err, batch.Watermark, graphQL)
	}
	var warn string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "github_pr_reviews.review_read_failed") {
			warn = line
		}
	}
	if !strings.Contains(warn, "status_class=none") {
		t.Fatalf("warn = %q: want status_class=none", warn)
	}
}
