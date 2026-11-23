package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// statusLookupDoer answers one path with a fixed status and passes every other
// request to the route's normal fixture. Status 0 passes everything.
type statusLookupDoer struct {
	inner  providerfoundation.HTTPDoer
	path   string
	status int
}

func (doer statusLookupDoer) Do(request *http.Request) (*http.Response, error) {
	if doer.status != 0 && request.URL.Path == doer.path {
		return &http.Response{
			StatusCode: doer.status, Header: http.Header{},
			Body: io.NopCloser(strings.NewReader(`{"message":"refused"}`)), Request: request,
		}, nil
	}
	return doer.inner.Do(request)
}

type lookupRoute struct {
	name    string
	collect func(t *testing.T, status int) (CompleteRouteBatch, error)
	claim   func() Claim
}

func storedRows(batch CompleteRouteBatch) int {
	total := 0
	for _, effect := range batch.Effects {
		total += len(effect.Rows)
	}
	return total
}

func lookupRoutes() []lookupRoute {
	return []lookupRoute{
		{
			name:  "github deployments",
			claim: func() Claim { return nativeTestClaim("github", "deployments") },
			collect: func(t *testing.T, status int) (CompleteRouteBatch, error) {
				doer := statusLookupDoer{inner: &gitHubDeploymentsDoer{}, path: "/repos/acme/api/deployments", status: status}
				client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
				return (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "deployments"),
					providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
			},
		},
		{
			name:  "github security",
			claim: func() Claim { return nativeTestClaim("github", "security") },
			collect: func(t *testing.T, status int) (CompleteRouteBatch, error) {
				doer := statusLookupDoer{inner: &gitHubSecurityDoer{}, path: "/repos/acme/api/dependabot/alerts", status: status}
				client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
				return (GitHubSecurityRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "security"),
					providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
			},
		},
		{
			name:  "gitlab deployments",
			claim: func() Claim { return nativeTestClaim("gitlab", "deployments") },
			collect: func(t *testing.T, status int) (CompleteRouteBatch, error) {
				deployments := gitLabDeploymentsResponse{body: `[{"id":501,"iid":7,"status":"success","environment":{"name":"production"},"created_at":"2026-07-22T10:00:00Z","sha":"abc"}]`}
				if status != 0 {
					deployments = gitLabDeploymentsResponse{status: status, body: `{"message":"refused"}`}
				}
				doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
					{body: gitLabRepositoryFixture}, {body: `[]`}, deployments, {body: `[]`},
				}}
				return (GitLabDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("gitlab", "deployments"),
					providerfoundation.Credential{}, gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"),
					time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
			},
		},
		{
			name:  "launchdarkly code references",
			claim: func() Claim { return nativeTestClaim("launchdarkly", "feature-flags") },
			collect: func(t *testing.T, status int) (CompleteRouteBatch, error) {
				codeRefs := launchDarklyRouteResponse{status: http.StatusOK, body: `{"items":[]}`}
				if status != 0 {
					codeRefs = launchDarklyRouteResponse{status: status, body: `{"message":"refused"}`}
				}
				doer := &launchDarklyRouteDoer{responses: []launchDarklyRouteResponse{
					{status: http.StatusOK, body: `{"items":[{"key":"checkout","_projectKey":"payments","creationDate":1725000000123}],"totalCount":1}`},
					{status: http.StatusOK, body: `{"items":[]}`},
					codeRefs,
				}}
				client, err := providerfoundation.NewHTTPClient(
					"launchdarkly", "https://app.launchdarkly.com", fakehttp.Client(doer),
					func(*http.Request) error { return nil },
					providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
					providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
				)
				if err != nil {
					t.Fatal(err)
				}
				claim := nativeTestClaim("launchdarkly", "feature-flags")
				claim.DatasetOptions = map[string]any{"project_key": "payments"}
				return (LaunchDarklyRouteHandler{CodeReferences: staticLaunchDarklyReferenceResolver{}}).Collect(
					context.Background(), claim, providerfoundation.Credential{Provider: "launchdarkly"}, client,
					time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC))
			},
		},
	}
}

// A rate-limited lookup returns the rate_limited error with no batch, so the
// unit defers and the watermark is held; the next run reads the same window and
// stores the rows.
func TestRateLimitedLookupHoldsTheWatermarkAndTheNextRunStoresTheRows(t *testing.T) {
	t.Parallel()
	for _, route := range lookupRoutes() {
		route := route
		t.Run(route.name, func(t *testing.T) {
			t.Parallel()
			batch, err := route.collect(t, http.StatusTooManyRequests)
			if !isRateLimited(err) || batch.Watermark != nil || len(batch.Effects) != 0 {
				t.Fatalf("limited run: err=%v watermark=%v effects=%d, want the rate_limited error and no batch", err, batch.Watermark, len(batch.Effects))
			}
			next, err := route.collect(t, 0)
			if err != nil {
				t.Fatalf("next run: %v", err)
			}
			claim := route.claim()
			if next.Watermark == nil || claim.BeforeAt == nil || !next.Watermark.Equal(*claim.BeforeAt) || storedRows(next) == 0 {
				t.Fatalf("next run: watermark=%v rows=%d, want the claim bound and the rows of the window", next.Watermark, storedRows(next))
			}
		})
	}
}

// Per class: an answer that says the source is absent is served empty; a
// 5xx that outlasted the retries and a 401 return their own class.
func TestLookupFailureIsServedEmptyOnlyWhenTheSourceIsAbsent(t *testing.T) {
	t.Parallel()
	for _, route := range lookupRoutes() {
		route := route
		t.Run(route.name, func(t *testing.T) {
			t.Parallel()
			for status, want := range map[int]providerfoundation.ErrorClass{
				http.StatusServiceUnavailable: providerfoundation.ErrorTransient,
				http.StatusUnauthorized:       providerfoundation.ErrorAuthentication,
			} {
				batch, err := route.collect(t, status)
				var providerErr *providerfoundation.ProviderError
				if !errors.As(err, &providerErr) || providerErr.Class != want || batch.Watermark != nil {
					t.Fatalf("status %d: err=%v watermark=%v, want class %s and no batch", status, err, batch.Watermark, want)
				}
			}
			for _, status := range []int{http.StatusNotFound, http.StatusForbidden} {
				batch, err := route.collect(t, status)
				if err != nil || batch.Watermark == nil {
					t.Fatalf("status %d: err=%v watermark=%v, want the absent source served as an empty success", status, err, batch.Watermark)
				}
			}
		})
	}
}

func TestLookupSourceAbsentByClass(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"404":                 {&providerfoundation.ProviderError{Class: providerfoundation.ErrorNotFound, StatusCode: 404}, true},
		"409":                 {&providerfoundation.ProviderError{Class: providerfoundation.ErrorConflict, StatusCode: 409}, true},
		"410":                 {&providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, StatusCode: 410}, true},
		"403 not rate limit":  {&providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication, StatusCode: 403}, true},
		"401":                 {&providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication, StatusCode: 401}, false},
		"429":                 {&providerfoundation.ProviderError{Class: providerfoundation.ErrorRateLimited, StatusCode: 429}, false},
		"403 rate limit":      {&providerfoundation.ProviderError{Class: providerfoundation.ErrorRateLimited, StatusCode: 403}, false},
		"503":                 {&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient, StatusCode: 503}, false},
		"transport failure":   {&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient}, false},
		"deadline":            {context.DeadlineExceeded, false},
		"lease lost":          {providerfoundation.ErrLeaseLost, false},
		"body failed to read": {providerfoundation.RouteFailure("/x", providerfoundation.ReasonBodyDecode, errors.New("bad body")), false},
	} {
		if got := lookupSourceAbsent(tc.err); got != tc.want {
			t.Errorf("%s: lookupSourceAbsent = %v, want %v", name, got, tc.want)
		}
	}
}

// The routes that already return a rate-limited lookup stay that way.
func TestGitHubCommitStatsDetailRateLimitStillReturnsRateLimited(t *testing.T) {
	t.Parallel()
	doer := statusLookupDoer{inner: &gitHubCommitStatsDoer{t: t}, path: "/repos/acme/api/commits/first", status: http.StatusTooManyRequests}
	client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
	batch, err := (GitHubCommitStatsRouteHandler{MaxCommits: 3}).Collect(context.Background(), nativeTestClaim("github", "commit-stats"),
		providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if !isRateLimited(err) || batch.Watermark != nil {
		t.Fatalf("err=%v watermark=%v, want rate_limited and no batch", err, batch.Watermark)
	}
}

func TestGitLabPullRequestApprovalsRateLimitStillReturnsRateLimited(t *testing.T) {
	t.Parallel()
	responses := gitLabPullRequestFixtureResponses()
	responses[2] = gitLabPullRequestResponse{status: http.StatusTooManyRequests, body: `{"message":"refused"}`}
	doer := &gitLabPullRequestDoer{t: t, responses: responses}
	client := gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example")
	batch, err := (GitLabPullRequestRouteHandler{PerPage: 2}).Collect(context.Background(), nativeTestClaim("gitlab", "prs"),
		providerfoundation.Credential{}, client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
	if !isRateLimited(err) || batch.Watermark != nil {
		t.Fatalf("err=%v watermark=%v, want rate_limited and no batch", err, batch.Watermark)
	}
}
