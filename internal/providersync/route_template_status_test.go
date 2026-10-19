package providersync

import (
	"context"
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
