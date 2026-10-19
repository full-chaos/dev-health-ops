package sync

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

type discoveryStatusDoer int

func (status discoveryStatusDoer) Do(request *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: int(status), Header: http.Header{}, Body: http.NoBody, Request: request}, nil
}

func discoveryStatusClient(t *testing.T, provider, base string) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(provider, base, fakehttp.Client(discoveryStatusDoer(http.StatusInternalServerError)),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// CHAOS-9801: a failed source discovery request names its static route
// template and never the owner in the request path.
func TestSourceDiscoveryFailuresNameTheRouteTemplate(t *testing.T) {
	ctx := context.Background()
	_, err := collectGitHubOwnerRepos(ctx, discoveryStatusClient(t, "github", "https://api.github.com"), "secret-owner")
	if err == nil || !strings.Contains(err.Error(), "route=/orgs/{org}/repos") || strings.Contains(err.Error(), "secret-owner") {
		t.Errorf("github owner repos: err = %v, want route=/orgs/{org}/repos and no owner", err)
	}
	_, err = discoverJiraLegacyProjects(ctx, discoveryStatusClient(t, "jira", "https://jira.example"))
	if err == nil || !strings.Contains(err.Error(), "route=/rest/api/3/project") {
		t.Errorf("jira legacy projects: err = %v, want route=/rest/api/3/project", err)
	}
}
