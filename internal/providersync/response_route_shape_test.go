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
	t.Run("jira graphql worklog node", func(t *testing.T) {
		body := `{"data":{"issue":{"worklogs":{"edges":[{"node":null}],"pageInfo":{"hasNextPage":false}}}}}`
		_, _, err := collectJiraGraphQLWorklogs(ctx, fixedBodyClient(t, "jira", body), "ACME-SECRET-1", 1, "cloud")
		requireRouteFailure(t, err, graphQLRoute, providerfoundation.ReasonBodyShape)
		if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
			t.Fatalf("error=%v lost ErrNormalizationInvalid", err)
		}
	})
}
