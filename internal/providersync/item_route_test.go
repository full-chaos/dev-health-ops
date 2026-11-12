package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// An item that fails to decode after its page was read is still a 2xx body decode failure of the route that served the
// page: the error names that route, keeps ErrNormalizationInvalid and stays permanent. One cell per site class.
func requireItemRouteFailure(t *testing.T, err error, route string) {
	t.Helper()
	requireRouteFailure(t, err, route, providerfoundation.ReasonBodyDecode)
	if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		t.Fatalf("error=%v lost ErrNormalizationInvalid", err)
	}
}

func TestPagerDutyIncidentItemFailureNamesTheIncidentsRoute(t *testing.T) {
	t.Parallel()
	doer := &pagerDutyIncidentFamilyDoer{t: t, responses: []pagerDutyIncidentFamilyResponse{
		{body: `{"incidents":[{"created_at":"2026-07-11T12:00:00Z"}],"more":false}`},
	}}
	_, err := (PagerDutyIncidentFamilyRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), nativeTestClaim("pagerduty", "incidents"),
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		pagerDutyIncidentFamilyTestClient(t, fakehttp.Client(doer)), time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC),
	)
	requireItemRouteFailure(t, err, "/incidents")
}

func TestPagerDutyChildItemFailureNamesTheChildRoute(t *testing.T) {
	t.Parallel()
	doer := &pagerDutyIncidentFamilyDoer{t: t, responses: []pagerDutyIncidentFamilyResponse{
		{body: `{"incidents":[{"id":"PI-1","created_at":"2026-07-11T12:00:00Z","updated_at":"2026-07-11T12:00:00Z"}],"more":false}`},
		{body: `{"alerts":[{"status":"triggered","created_at":"2026-07-11T12:00:00Z"}],"more":false}`},
	}}
	claim := nativeTestClaim("pagerduty", "incident-alerts")
	claim.DatasetOptions = map[string]any{"enrichment_cap": 1}
	_, err := (PagerDutyIncidentFamilyRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}},
		pagerDutyIncidentFamilyTestClient(t, fakehttp.Client(doer)), time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC),
	)
	requireItemRouteFailure(t, err, "/incidents/{incident}/alerts")
}

func TestGitLabListItemFailureNamesTheListRoute(t *testing.T) {
	t.Parallel()
	doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{
		{body: gitLabRepositoryFixture},
		{body: `[{"id":1,"iid":1,"issue_type":"incident","title":5,"created_at":"2026-07-20T10:00:00Z"}]`},
	}}
	_, err := (GitLabIncidentsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "incidents"), providerfoundation.Credential{},
		gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	requireItemRouteFailure(t, err, gitlabIssuesRoute)
}

func TestGitHubPullDetailFailureNamesThePullRoute(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	client := gitHubPullRequestClient(t, fakehttp.Client(&gitHubPullRequestDoer{t: t, bodies: map[string]string{
		"/repos/acme/api":          gitHubPullRequestRepoFixture,
		"/repos/acme/api/pulls":    `[{"number": 42, "updated_at": "2026-07-21T15:30:00Z"}]`,
		"/repos/acme/api/pulls/42": `{"id": 4200, "number": 42, "title": 5}`,
	}}), "https://api.github.com")
	_, err := (GitHubPullRequestRouteHandler{Now: func() time.Time { return now }}).Collect(
		context.Background(), nativeTestClaim("github", "prs"), providerfoundation.Credential{}, client, now,
	)
	requireItemRouteFailure(t, err, githubPullRoute)
}
