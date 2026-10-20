package providerfoundation

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type fixedPageDoer struct{ body string }

func (d fixedPageDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Request: request, Body: io.NopCloser(strings.NewReader(d.body))}, nil
}

func TestWalkerPageShapeFailuresCarryRouteAsBodyShape(t *testing.T) {
	ctx := context.Background()
	rows := map[string]struct {
		body, route string
		run         func(*HTTPClient) error
	}{
		"launchdarkly audit links not an object": {`{"items":[{"x":1}],"_links":"x"}`, "/api/v2/auditlog", func(c *HTTPClient) error {
			_, err := CollectLaunchDarklyAuditPages(ctx, c, LaunchDarklyAuditOptions{MaxItems: 100})
			return err
		}},
		"launchdarkly audit repeated cursor": {`{"items":[{"x":1}],"_links":{"next":{"href":"/api/v2/auditlog?limit=20"}}}`, "/api/v2/auditlog", func(c *HTTPClient) error {
			_, err := CollectLaunchDarklyAuditPages(ctx, c, LaunchDarklyAuditOptions{MaxItems: 100})
			return err
		}},
		"linear empty end cursor": {`{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":""}}}}`, "/graphql", func(c *HTTPClient) error {
			_, err := CollectLinearGraphQLPages(ctx, c, LinearPageOptions{Query: "q", ConnectionPath: []string{"issues"}, PerPage: 10, MaxPages: 2})
			return err
		}},
		"linear connection missing": {`{"data":{}}`, "/graphql", func(c *HTTPClient) error {
			_, err := CollectLinearGraphQLPages(ctx, c, LinearPageOptions{Query: "q", ConnectionPath: []string{"issues"}, PerPage: 10, MaxPages: 2})
			return err
		}},
		"pagerduty more missing": {`{"incidents":[]}`, "/incidents", func(c *HTTPClient) error {
			_, err := CollectPagerDutyOffsetPages(ctx, c, PagerDutyOffsetOptions{Path: "/incidents", Route: "/incidents", DataKey: "incidents", PerPage: 10, MaxPages: 2})
			return err
		}},
		"pagerduty more with an empty page": {`{"incidents":[],"more":true}`, "/incidents", func(c *HTTPClient) error {
			_, err := CollectPagerDutyOffsetPages(ctx, c, PagerDutyOffsetOptions{Path: "/incidents", Route: "/incidents", DataKey: "incidents", PerPage: 10, MaxPages: 2})
			return err
		}},
		"jira repeated page token": {`{"issues":[{"id":1}],"nextPageToken":"t"}`, "/rest/api/3/search/jql", func(c *HTTPClient) error {
			_, err := CollectJiraTokenOffsetPages(ctx, c, JiraPageOptions{Path: "/rest/api/3/search/jql", Route: "/rest/api/3/search/jql", DataKey: "issues", MaxResults: 10, MaxPages: 3})
			return err
		}},
		"top level is not an array": {`{"a":1}`, "/repos/{owner}/{repo}/pulls", func(c *HTTPClient) error {
			_, err := CollectGitHubLinkPages(ctx, c, GitHubPageOptions{Path: "/repos/acme-secret/api/pulls", Route: "/repos/{owner}/{repo}/pulls", MaxPages: 1})
			return err
		}},
	}
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			client := paginationClient(t, "github", "https://api.example.test", fixedPageDoer{body: row.body})
			assertRouteFailure(t, row.run(client), row.route, ReasonBodyShape, ErrPaginationInvalid)
		})
	}
}
