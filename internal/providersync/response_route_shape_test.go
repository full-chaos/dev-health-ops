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

type fixedBodyDoer struct{ body string }

func (d fixedBodyDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Request: request, Body: io.NopCloser(strings.NewReader(d.body))}, nil
}

func fixedBodyClient(t *testing.T, provider, body string) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(provider, "https://api.example.test", fakehttp.Client(fixedBodyDoer{body: body}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestPostDecodeShapeFailuresOnRoutedBodiesCarryRoute(t *testing.T) {
	ctx := context.Background()
	t.Run("pagerduty endpoint decode", func(t *testing.T) {
		endpoint := pagerDutyIncidentFamilyEndpoint{path: "/incidents/acme-secret/notes", route: "/incidents/{incident}/notes", decode: decodePagerDutyNotes}
		_, _, err := requestPagerDutyIncidentFamilyPage(ctx, fixedBodyClient(t, "pagerduty", `{"unexpected":[]}`), endpoint, nil, 0, 10)
		requireRouteFailure(t, err, "/incidents/{incident}/notes", providerfoundation.ReasonBodyDecode)
		if !errors.Is(err, providerfoundation.ErrPaginationInvalid) {
			t.Fatalf("error=%v lost ErrPaginationInvalid", err)
		}
	})
	t.Run("gitlab security item decode", func(t *testing.T) {
		n := 0
		_, _, err := fetchGitLabSecurityObjects(ctx, fixedBodyClient(t, "gitlab", `[1]`), "/projects/{project}/vulnerability_findings", "/projects/acme-secret/vulnerability_findings", &gitLabSecurityResponseObserver{attempts: &n})
		requireRouteFailure(t, err, "/projects/{project}/vulnerability_findings", providerfoundation.ReasonBodyDecode)
		if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
			t.Fatalf("error=%v lost ErrNormalizationInvalid", err)
		}
	})
	t.Run("gitlab security dependency vulnerability shape", func(t *testing.T) {
		n := 0
		_, _, err := fetchGitLabSecurityObjects(ctx, fixedBodyClient(t, "gitlab", `[{"name":"x","vulnerabilities":[1]}]`), "/projects/{project}/dependencies", "/projects/acme-secret/dependencies", &gitLabSecurityResponseObserver{attempts: &n})
		requireRouteFailure(t, err, "/projects/{project}/dependencies", providerfoundation.ReasonBodyShape)
	})
}

func TestShapeWrappedSitesCarryRouteAndKeepTheirSentinel(t *testing.T) {
	ctx := context.Background()
	rows := map[string]struct {
		provider, body string
		sentinel       error
		run            func(*providerfoundation.HTTPClient) error
	}{
		"jira graphql worklog node": {"jira", `{"data":{"issue":{"worklogs":{"edges":[{"node":null}],"pageInfo":{"hasNextPage":false}}}}}`, providerfoundation.ErrNormalizationInvalid, func(c *providerfoundation.HTTPClient) error {
			_, _, err := collectJiraGraphQLWorklogs(ctx, c, "ACME-SECRET-1", 1, "cloud")
			return err
		}},
		"github pr reviews pull missing": {"github", `{"data":{"repository":{}}}`, providerfoundation.ErrNormalizationInvalid, func(c *providerfoundation.HTTPClient) error {
			_, err := (GitHubPullRequestReviewFetcher{}).fetchPage(ctx, c, "acme-secret", "api", []int{1}, nil, &GitHubPullRequestReviewFetchResult{})
			return err
		}},
		"github work item social pull missing": {"github", `{"data":{"repository":{}}}`, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			_, err := (GitHubWorkItemPRSocialFetcher{}).fetchPage(ctx, c, "acme-secret", "api", []int{1}, 10, nil, 10, nil, &GitHubWorkItemPRSocialFetchResult{})
			return err
		}},
		"github pr reviews repository null": {"github", `{"data":{"repository":null}}`, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			_, err := (GitHubPullRequestReviewFetcher{}).fetchPage(ctx, c, "acme-secret", "api", []int{1}, nil, &GitHubPullRequestReviewFetchResult{})
			return err
		}},
		"github work item social repository null": {"github", `{"data":{"repository":null}}`, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			_, err := (GitHubWorkItemPRSocialFetcher{}).fetchPage(ctx, c, "acme-secret", "api", []int{1}, 10, nil, 10, nil, &GitHubWorkItemPRSocialFetchResult{})
			return err
		}},
		"gitlab files blobs missing": {"gitlab", `{"data":{"project":null}}`, providerfoundation.ErrGraphQLResponse, func(c *providerfoundation.HTTPClient) error {
			_, err := fetchGitLabGraphQLBlobs(ctx, c, "acme-secret/api", "main", []string{"a.go"}, "rawBlob")
			return err
		}},
		"linear reference connection missing": {"linear", `{"data":{}}`, providerfoundation.ErrPaginationInvalid, func(c *providerfoundation.HTTPClient) error {
			_, _, _, err := collectLinearReferenceConnection(ctx, c, "query", linearReferenceConnectionTeams, linearReferenceConnectionVariables{}, 10, 1)
			return err
		}},
	}
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			err := row.run(fixedBodyClient(t, row.provider, row.body))
			requireRouteFailure(t, err, graphQLRoute, providerfoundation.ReasonBodyShape)
			if !errors.Is(err, row.sentinel) {
				t.Fatalf("error=%v lost %v", err, row.sentinel)
			}
		})
	}
}
