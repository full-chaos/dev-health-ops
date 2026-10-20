package providersync

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type routeBodyFaultDoer struct{ mode string }

func (d routeBodyFaultDoer) Do(request *http.Request) (*http.Response, error) {
	var body io.Reader = strings.NewReader("not json{")
	if d.mode == "read" {
		body = io.MultiReader(strings.NewReader(`{"data":`), iotest.ErrReader(syscall.ECONNRESET))
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(body), Request: request}, nil
}

func routeBodyFaultClient(t *testing.T, provider, mode string) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(provider, "https://api.example.test", fakehttp.Client(routeBodyFaultDoer{mode: mode}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func requireRouteFailure(t *testing.T, err error, route, reason string) {
	t.Helper()
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Route != route || providerErr.Reason != reason {
		t.Fatalf("error=%v, want ProviderError route=%q reason=%q", err, route, reason)
	}
	if !strings.Contains(err.Error(), "route="+route) || strings.Contains(err.Error(), "acme-secret") {
		t.Fatalf("error text %q must name route=%s and no raw path", err.Error(), route)
	}
	if providerErr.Class != providerfoundation.ErrorPermanent || providerErr.Retryable() {
		t.Fatalf("class=%s: a 2xx body failure stays permanent", providerErr.Class)
	}
}

type routeReadCase struct {
	route      string
	readOnly   bool
	run        func(context.Context, *providerfoundation.HTTPClient) error
	wantReadIs error
}

func TestRouteLocalReadersCarryRouteOnBodyReadAndDecodeFailure(t *testing.T) {
	jiraRoute := "/rest/api/3/issue/{key}"
	cases := map[string]routeReadCase{
		"fetchObjectRoute": {route: "/repos/{owner}/{repo}", run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			var out map[string]any
			return fetchObjectRoute(ctx, c, "/repos/{owner}/{repo}", "/repos/acme-secret/api", &out)
		}},
		"jira object": {route: jiraRoute, run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			var out map[string]any
			return jiraFetchObject(ctx, c, http.MethodGet, jiraRoute, "/rest/api/3/issue/ACME-SECRET-1", nil, &out)
		}},
		"jira incident object": {route: jiraRoute, run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			var out map[string]any
			return jiraIncidentFetchObject(ctx, c, http.MethodGet, jiraRoute, "/rest/api/3/issue/ACME-SECRET-1", nil, &out)
		}},
		"jira dev status": {route: "/rest/dev-status/1.0/issue/detail", run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			_, _, err := fetchJiraDevStatusPullRequests(ctx, c, "10001", "GitHub")
			return err
		}},
		"launchdarkly code references": {route: "/api/v2/code-refs/repositories", run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			_, err := fetchLaunchDarklyCodeReferences(ctx, c, "acme-secret")
			return err
		}},
		"gitlab feature flag project": {route: gitlabProjectRoute, run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			_, err := fetchGitLabFeatureFlagProjectName(ctx, c, "/projects/acme-secret", "fallback")
			return err
		}},
		"gitlab blame": {route: gitlabBlameRoute, run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			_, err := fetchGitLabBlame(ctx, c, "/projects/acme-secret", "a.go", "main")
			return err
		}},
		"github blame": {route: graphQLRoute, run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			_, err := fetchGitHubBlame(ctx, c, "acme-secret", "api", "main", "a.go")
			return err
		}},
		"gitlab files graphql": {route: graphQLRoute, run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			_, err := fetchGitLabGraphQLBlobs(ctx, c, "acme-secret/api", "main", []string{"a.go"}, "rawBlob")
			return err
		}},
		"github files graphql": {route: graphQLRoute, run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			_, err := fetchGitHubFileContents(ctx, c, "acme-secret", "api", "main", []string{"src/a.go"}, map[string]*int{})
			return err
		}},
		"gitlab security": {route: "/projects/{project}/vulnerability_findings", run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			n := 0
			_, _, err := fetchGitLabSecurityObjects(ctx, c, "/projects/{project}/vulnerability_findings", "/projects/acme-secret/vulnerability_findings", &gitLabSecurityResponseObserver{attempts: &n})
			return err
		}},
		"gitlab tests report": {route: gitlabPipelinesRoute + "/{pipeline}/test_report", run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			_, _, err := fetchGitLabTestsReport(ctx, c, "/projects/acme-secret", "7")
			return err
		}},
		"github projects v2 graphql": {route: graphQLRoute, run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			var out map[string]any
			return fetchGitHubProjectV2GraphQL(ctx, c, "query", map[string]any{"login": "acme-secret"}, &out, &FetchEvidence{})
		}},
		"github team catalog email": {route: "/users/{login}", run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			_, err := (GitHubTeamCatalogRouteHandler{ResolveEmail: true, Client: c}).resolveEmail(ctx, map[string]*string{}, "acme-secret")
			return err
		}},
		"gitlab pull request page": {route: "/projects/{project}/merge_requests", run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			response, err := c.DoRoute(ctx, http.MethodGet, "/projects/{project}/merge_requests", "/projects/acme-secret/merge_requests", nil)
			if err != nil {
				return err
			}
			_, err = decodeGitLabPullRequestPage(response)
			return err
		}},
		"gitlab incident page": {route: "/projects/{project}/issues", run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			response, err := c.DoRoute(ctx, http.MethodGet, "/projects/{project}/issues", "/projects/acme-secret/issues", nil)
			if err != nil {
				return err
			}
			_, err = decodeGitLabIncidentPage(response)
			return err
		}},
		"gitlab job artifact download": {route: gitlabJobArtifactsRoute, readOnly: true, run: func(ctx context.Context, c *providerfoundation.HTTPClient) error {
			_, err := downloadGitLabTestsArtifact(ctx, c, "/projects/acme-secret", "9")
			return err
		}},
	}
	for name, tc := range cases {
		t.Run(name+"/read", func(t *testing.T) {
			provider := "github"
			if strings.Contains(name, "gitlab") {
				provider = "gitlab"
			}
			err := tc.run(context.Background(), routeBodyFaultClient(t, provider, "read"))
			requireRouteFailure(t, err, tc.route, providerfoundation.ReasonBodyRead)
			if !errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, providerfoundation.ErrNormalizationInvalid) &&
				!errors.Is(err, providerfoundation.ErrPaginationInvalid) {
				t.Fatalf("error=%v lost its sentinel", err)
			}
		})
		if tc.readOnly {
			continue
		}
		t.Run(name+"/decode", func(t *testing.T) {
			provider := "github"
			if strings.Contains(name, "gitlab") {
				provider = "gitlab"
			}
			err := tc.run(context.Background(), routeBodyFaultClient(t, provider, "decode"))
			requireRouteFailure(t, err, tc.route, providerfoundation.ReasonBodyDecode)
		})
	}
}

func TestFetchObjectRouteKeepsCapSignalAndSentinelsOnRouteFailures(t *testing.T) {
	var out map[string]any
	err := fetchObjectRoute(context.Background(), routeBodyFaultClient(t, "github", "decode"), "/repos/{owner}/{repo}", "/repos/acme-secret/api", &out)
	if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		t.Fatalf("error=%v lost ErrNormalizationInvalid", err)
	}
	if errors.As(err, new(*providerfoundation.ObjectTooLargeError)) {
		t.Fatalf("a decode failure must not read as too large: %v", err)
	}
}
