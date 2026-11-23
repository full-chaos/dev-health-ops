package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// A provider that answers 429 on a lookup (retries exhausted, context alive)
// must not be acknowledged as a finished window: the route returns the
// rate_limited error, or at least holds the watermark.
func assertRateLimitedLookupNotAcknowledged(t *testing.T, route string, batch CompleteRouteBatch, err error) {
	t.Helper()
	if err != nil {
		if !isRateLimited(err) {
			t.Fatalf("%s: error = %v, want rate_limited or a held watermark", route, err)
		}
		return
	}
	if batch.Watermark != nil {
		t.Fatalf("%s: a 429 on the lookup was swallowed and the batch advanced the watermark (with %d effect(s)); want rate_limited returned or the watermark held",
			route, len(batch.Effects))
	}
}

type pathLimitDoer struct {
	inner providerfoundation.HTTPDoer
	path  string
}

func (doer pathLimitDoer) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Path == doer.path {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests, Header: http.Header{},
			Body: io.NopCloser(strings.NewReader(`{"message":"rate limit"}`)), Request: request,
		}, nil
	}
	return doer.inner.Do(request)
}

func TestReproGitHubDeploymentsListRateLimitIsNotAcknowledged(t *testing.T) {
	t.Parallel()
	doer := pathLimitDoer{inner: &gitHubDeploymentsDoer{}, path: "/repos/acme/api/deployments"}
	client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("github", "deployments"), providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	assertRateLimitedLookupNotAcknowledged(t, "github deployments", batch, err)
}

func TestReproGitHubSecurityAlertsRateLimitIsNotAcknowledged(t *testing.T) {
	t.Parallel()
	doer := pathLimitDoer{inner: &gitHubSecurityDoer{}, path: "/repos/acme/api/dependabot/alerts"}
	client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
	batch, err := (GitHubSecurityRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("github", "security"), providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	assertRateLimitedLookupNotAcknowledged(t, "github security", batch, err)
}

func TestReproGitLabDeploymentsListRateLimitIsNotAcknowledged(t *testing.T) {
	t.Parallel()
	doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[]`},
		{status: http.StatusTooManyRequests, body: `{"message":"rate limit"}`},
	}}
	batch, err := (GitLabDeploymentsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "deployments"), providerfoundation.Credential{},
		gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"),
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
	assertRateLimitedLookupNotAcknowledged(t, "gitlab deployments", batch, err)
}

func TestReproLaunchDarklyCodeReferencesRateLimitIsNotAcknowledged(t *testing.T) {
	t.Parallel()
	doer := &launchDarklyRouteDoer{responses: []launchDarklyRouteResponse{
		{status: http.StatusOK, body: `{"items":[],"totalCount":0}`},
		{status: http.StatusOK, body: `{"items":[]}`},
		{status: http.StatusTooManyRequests, body: `{"message":"rate limit"}`},
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
	batch, collectErr := (LaunchDarklyRouteHandler{CodeReferences: staticLaunchDarklyReferenceResolver{}}).Collect(
		context.Background(), claim, providerfoundation.Credential{Provider: "launchdarkly"}, client,
		time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC))
	assertRateLimitedLookupNotAcknowledged(t, "launchdarkly", batch, collectErr)
}
