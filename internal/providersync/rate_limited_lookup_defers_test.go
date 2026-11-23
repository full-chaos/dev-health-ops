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
	body   string
}

func (doer statusLookupDoer) Do(request *http.Request) (*http.Response, error) {
	if doer.status != 0 && request.URL.Path == doer.path {
		return &http.Response{
			StatusCode: doer.status, Header: http.Header{},
			Body: io.NopCloser(strings.NewReader(doer.responseBody())), Request: request,
		}, nil
	}
	return doer.inner.Do(request)
}

func (doer statusLookupDoer) responseBody() string {
	if doer.body != "" {
		return doer.body
	}
	return `{"message":"refused"}`
}

type lookupRoute struct {
	name    string
	collect func(t *testing.T, status int, metrics *providerfoundation.Metrics) (CompleteRouteBatch, error)
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
			collect: func(t *testing.T, status int, metrics *providerfoundation.Metrics) (CompleteRouteBatch, error) {
				doer := statusLookupDoer{inner: &gitHubDeploymentsDoer{}, path: "/repos/acme/api/deployments", status: status}
				client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
				client.Metrics = metrics
				return (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "deployments"),
					providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
			},
		},
		{
			name:  "github security",
			claim: func() Claim { return nativeTestClaim("github", "security") },
			collect: func(t *testing.T, status int, metrics *providerfoundation.Metrics) (CompleteRouteBatch, error) {
				doer := statusLookupDoer{inner: &gitHubSecurityDoer{}, path: "/repos/acme/api/dependabot/alerts", status: status}
				client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
				client.Metrics = metrics
				return (GitHubSecurityRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "security"),
					providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
			},
		},
		{
			name:  "gitlab deployments",
			claim: func() Claim { return nativeTestClaim("gitlab", "deployments") },
			collect: func(t *testing.T, status int, metrics *providerfoundation.Metrics) (CompleteRouteBatch, error) {
				deployments := gitLabDeploymentsResponse{body: `[{"id":501,"iid":7,"status":"success","environment":{"name":"production"},"created_at":"2026-07-22T10:00:00Z","sha":"abc"}]`}
				if status != 0 {
					deployments = gitLabDeploymentsResponse{status: status, body: `{"message":"refused"}`}
				}
				doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
					{body: gitLabRepositoryFixture}, {body: `[]`}, deployments, {body: `[]`},
				}}
				client := gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example")
				client.Metrics = metrics
				return (GitLabDeploymentsRouteHandler{}).Collect(context.Background(), nativeTestClaim("gitlab", "deployments"),
					providerfoundation.Credential{}, client, time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
			},
		},
		{
			name:  "launchdarkly code references",
			claim: func() Claim { return nativeTestClaim("launchdarkly", "feature-flags") },
			collect: func(t *testing.T, status int, metrics *providerfoundation.Metrics) (CompleteRouteBatch, error) {
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
				client.Metrics = metrics
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
			batch, err := route.collect(t, http.StatusTooManyRequests, nil)
			if !isRateLimited(err) || batch.Watermark != nil || len(batch.Effects) != 0 {
				t.Fatalf("limited run: err=%v watermark=%v effects=%d, want the rate_limited error and no batch", err, batch.Watermark, len(batch.Effects))
			}
			next, err := route.collect(t, 0, nil)
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

func lookupCounter(t *testing.T, metrics *providerfoundation.Metrics) string {
	t.Helper()
	var exposition strings.Builder
	if err := metrics.WritePrometheus(&exposition); err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, line := range strings.Split(exposition.String(), "\n") {
		if strings.HasPrefix(line, "dev_health_provider_lookup_unavailable_total{") {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

// Per class: a 5xx that outlasted the retries and a 401 fail the unit with
// their own class and no batch. A 404 is an absent source, served silently. Any
// other 4xx is served empty but counted and logged with its reason.
func TestLookupFailureClassesPerRoute(t *testing.T) {
	t.Parallel()
	for _, route := range lookupRoutes() {
		route := route
		t.Run(route.name, func(t *testing.T) {
			t.Parallel()
			for status, want := range map[int]providerfoundation.ErrorClass{
				http.StatusServiceUnavailable: providerfoundation.ErrorTransient,
				http.StatusUnauthorized:       providerfoundation.ErrorAuthentication,
			} {
				batch, err := route.collect(t, status, providerfoundation.NewMetrics())
				var providerErr *providerfoundation.ProviderError
				if !errors.As(err, &providerErr) || providerErr.Class != want || batch.Watermark != nil {
					t.Fatalf("status %d: err=%v watermark=%v, want class %s and no batch", status, err, batch.Watermark, want)
				}
			}
			metrics := providerfoundation.NewMetrics()
			batch, err := route.collect(t, http.StatusNotFound, metrics)
			if err != nil || batch.Watermark == nil || lookupCounter(t, metrics) != "" {
				t.Fatalf("404: err=%v watermark=%v counter=%q, want a silent empty success", err, batch.Watermark, lookupCounter(t, metrics))
			}
			for status, reason := range map[int]string{
				http.StatusForbidden:           "forbidden",
				http.StatusBadRequest:          "bad_request",
				http.StatusUnprocessableEntity: "bad_request",
				http.StatusConflict:            "other_4xx",
				http.StatusGone:                "other_4xx",
			} {
				metrics := providerfoundation.NewMetrics()
				batch, err := route.collect(t, status, metrics)
				if err != nil || batch.Watermark == nil {
					t.Fatalf("status %d: err=%v watermark=%v, want an empty success", status, err, batch.Watermark)
				}
				if want := `reason="` + reason + `"} 1`; !strings.Contains(lookupCounter(t, metrics), want) {
					t.Fatalf("status %d: counter %q lacks %s: the swallow is silent", status, lookupCounter(t, metrics), want)
				}
			}
		})
	}
}

// A GitHub 403 whose body is the documented feature-off message skips that one
// source silently; the same 403 with another body is served empty and counted.
func TestGitHubSecurityFeatureOffForbiddenIsSilentAndOtherForbiddenIsCounted(t *testing.T) {
	t.Parallel()
	collect := func(path, body string, metrics *providerfoundation.Metrics) (CompleteRouteBatch, error) {
		doer := statusLookupDoer{inner: &gitHubSecurityDoer{}, path: path, status: http.StatusForbidden, body: body}
		client := gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com")
		client.Metrics = metrics
		return (GitHubSecurityRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "security"),
			providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	}
	for _, c := range []struct{ path, body string }{
		{"/repos/acme/api/code-scanning/alerts", `{"message":"Advanced Security must be enabled for this repository to use code scanning."}`},
		{"/repos/acme/api/dependabot/alerts", `{"message":"Dependabot alerts are disabled for this repository."}`},
	} {
		metrics := providerfoundation.NewMetrics()
		batch, err := collect(c.path, c.body, metrics)
		if err != nil || batch.Watermark == nil || storedRows(batch) != 2 || lookupCounter(t, metrics) != "" {
			t.Fatalf("%s: err=%v rows=%d counter=%q, want 2 rows from the other sources and no counter", c.path, err, storedRows(batch), lookupCounter(t, metrics))
		}
	}
	metrics := providerfoundation.NewMetrics()
	batch, err := collect("/repos/acme/api/code-scanning/alerts", `{"message":"Resource not accessible by integration"}`, metrics)
	if err != nil || batch.Watermark == nil || storedRows(batch) != 2 || !strings.Contains(lookupCounter(t, metrics), `reason="forbidden"} 1`) {
		t.Fatalf("other 403 body: err=%v rows=%d counter=%q, want served empty and counted as forbidden", err, storedRows(batch), lookupCounter(t, metrics))
	}
}

func TestClassifyLookupFailureByClass(t *testing.T) {
	t.Parallel()
	off := []string{"feature x is disabled"}
	for name, tc := range map[string]struct {
		err     error
		verdict lookupVerdict
		reason  string
	}{
		"404":               {&providerfoundation.ProviderError{Class: providerfoundation.ErrorNotFound, StatusCode: 404}, lookupAbsent, ""},
		"403 feature off":   {&providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication, StatusCode: 403, Body: `{"message":"Feature X is DISABLED"}`}, lookupAbsent, ""},
		"403 permission":    {&providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication, StatusCode: 403, Body: "forbidden"}, lookupRecorded, "forbidden"},
		"400":               {&providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, StatusCode: 400}, lookupRecorded, "bad_request"},
		"422":               {&providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, StatusCode: 422}, lookupRecorded, "bad_request"},
		"410":               {&providerfoundation.ProviderError{Class: providerfoundation.ErrorPermanent, StatusCode: 410}, lookupRecorded, "other_4xx"},
		"409":               {&providerfoundation.ProviderError{Class: providerfoundation.ErrorConflict, StatusCode: 409}, lookupRecorded, "other_4xx"},
		"401":               {&providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication, StatusCode: 401, Body: "feature x is disabled"}, lookupFails, ""},
		"429":               {&providerfoundation.ProviderError{Class: providerfoundation.ErrorRateLimited, StatusCode: 429}, lookupFails, ""},
		"403 rate limit":    {&providerfoundation.ProviderError{Class: providerfoundation.ErrorRateLimited, StatusCode: 403, Body: "feature x is disabled"}, lookupFails, ""},
		"503":               {&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient, StatusCode: 503}, lookupFails, ""},
		"transport failure": {&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient}, lookupFails, ""},
		"deadline":          {context.DeadlineExceeded, lookupFails, ""},
		"lease lost":        {providerfoundation.ErrLeaseLost, lookupFails, ""},
		"body failed":       {providerfoundation.RouteFailure("/x", providerfoundation.ReasonBodyDecode, errors.New("bad body")), lookupFails, ""},
	} {
		verdict, reason := classifyLookupFailure(tc.err, off)
		if verdict != tc.verdict || reason != tc.reason {
			t.Errorf("%s: verdict=%d reason=%q, want %d %q", name, verdict, reason, tc.verdict, tc.reason)
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
