package providersync

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type statusDoer int

func (status statusDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: int(status), Header: http.Header{}, Body: http.NoBody, Request: request}, nil
}

func statusClient(t *testing.T, provider, base string) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(provider, base, fakehttp.Client(statusDoer(http.StatusBadRequest)),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// CHAOS-9801: the Jira, repository listing and PagerDuty-style helpers name
// their static template on a status failure and never the issue, org or
// project name in the request path.
func TestRemainingRoutesNameTheTemplateNotThePath(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		err   func() error
		route string
		leak  string
	}{
		"jira comments": {func() error {
			var page map[string]any
			return jiraFetchObject(ctx, statusClient(t, "jira", "https://jira.example"), http.MethodGet,
				"/rest/api/3/issue/{issue}/comment", "/rest/api/3/issue/SECRET-1/comment", nil, &page)
		}, "/rest/api/3/issue/{issue}/comment", "SECRET-1"},
		"github org repos": {func() error {
			_, err := ListGitHubRepositories(ctx, statusClient(t, "github", "https://api.github.com"), GitHubListing{Org: "secret-org", Pattern: "*"})
			return err
		}, "/orgs/{org}/repos", "secret-org"},
		"github files subtree walk": {func() error {
			_, _, err := gitHubWalkTree(ctx, statusClient(t, "github", "https://api.github.com"), "/repos/secret-org/secret-repo", "abc123", "src")
			return err
		}, "/repos/{owner}/{repo}/git/trees/{ref}", "secret"},
		"pagerduty users walk": {func() error {
			_, err := providerfoundation.CollectPagerDutyOffsetPages(ctx, statusClient(t, "pagerduty", "https://api.pagerduty.com"),
				providerfoundation.PagerDutyOffsetOptions{Path: "/users", Route: "/users", DataKey: "users", PerPage: 25, MaxPages: 1})
			return err
		}, "/users", "offset="},
		"github artifact download": {func() error {
			_, _, _, err := downloadGitHubTestsArtifact(ctx, statusClient(t, "github", "https://api.github.com"), "/repos/secret-org/secret-repo", "77", 1<<20)
			return err
		}, "/repos/{owner}/{repo}/actions/artifacts/{artifact}/zip", "secret"},
		"gitlab group projects": {func() error {
			_, err := ListGitLabProjects(ctx, statusClient(t, "gitlab", "https://gitlab.example"), GitLabListing{Group: "secret-group", Pattern: "*"})
			return err
		}, "/groups/{group}/projects", "secret-group"},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.err()
			if err == nil || !strings.Contains(err.Error(), "route="+tc.route) || strings.Contains(err.Error(), tc.leak) {
				t.Fatalf("err = %v, want route=%s and no %q", err, tc.route, tc.leak)
			}
		})
	}
}

// routeArtifactRedirectDoer answers the provider's artifact request with a
// redirect, and the redirect target as the failure under test.
type routeArtifactRedirectDoer struct{ failure string }

type routeFailingBody struct{}

func (routeFailingBody) Read([]byte) (int, error) { return 0, errors.New("connection reset") }
func (routeFailingBody) Close() error             { return nil }

func (doer routeArtifactRedirectDoer) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Host != "blob.example" {
		header := http.Header{}
		if doer.failure != "no location" {
			header.Set("Location", "https://blob.example/secret-org/secret-repo/artifact.zip")
		}
		return &http.Response{StatusCode: http.StatusFound, Header: header, Body: http.NoBody, Request: request}, nil
	}
	switch doer.failure {
	case "transport":
		return nil, errors.New("dial tcp: connection reset")
	case "status read":
		return &http.Response{StatusCode: http.StatusInternalServerError, Header: http.Header{}, Body: routeFailingBody{}, Request: request}, nil
	default:
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: routeFailingBody{}, Request: request}, nil
	}
}

// CHAOS-9801: every failure of the redirected artifact download (no target,
// a transport failure, a body that fails to read) names the artifact route,
// never the artifact store URL or the repository.
func TestArtifactRedirectFailuresNameTheRoute(t *testing.T) {
	ctx := context.Background()
	for _, failure := range []string{"no location", "transport", "status read", "body read"} {
		for provider, download := range map[string]struct {
			route string
			call  func(*providerfoundation.HTTPClient) error
		}{
			"github": {"/repos/{owner}/{repo}/actions/artifacts/{artifact}/zip", func(client *providerfoundation.HTTPClient) error {
				_, _, _, err := downloadGitHubTestsArtifact(ctx, client, "/repos/secret-org/secret-repo", "77", 1<<20)
				return err
			}},
			"gitlab": {"/projects/{project}/jobs/{job}/artifacts", func(client *providerfoundation.HTTPClient) error {
				_, err := downloadGitLabTestsArtifact(ctx, client, "/projects/secret-org%2Fsecret-repo", "77")
				return err
			}},
		} {
			client, err := providerfoundation.NewHTTPClient(provider, "https://"+provider+".example", fakehttp.Client(routeArtifactRedirectDoer{failure: failure}),
				func(*http.Request) error { return nil },
				providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
				providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
			if err != nil {
				t.Fatal(err)
			}
			err = download.call(client)
			if err == nil || !strings.Contains(err.Error(), "route="+download.route) || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "blob.example") {
				t.Errorf("%s %s: err = %v, want route=%s and no repository or store URL", provider, failure, err, download.route)
			}
		}
	}
}
