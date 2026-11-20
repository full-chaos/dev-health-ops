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

// The github_ and gitlab_ item decode sites of CHAOS-14279: each names the route that served its item. One cell per site class.

type itemRoutePathDoer map[string]string

func (doer itemRoutePathDoer) Do(request *http.Request) (*http.Response, error) {
	body, ok := doer[request.URL.Path]
	if !ok {
		return nil, errors.New("unexpected request " + request.URL.Path)
	}
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

func TestGitHubRESTListItemFailureNamesTheListRoute(t *testing.T) {
	t.Parallel()
	doer := &githubCommitsDoer{t: t, responses: []githubCommitsResponse{{body: gitHubRepositoryFixture}, {body: `[[]]`}}}
	_, err := (GitHubCommitsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("github", "commits"), providerfoundation.Credential{},
		gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com"), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	requireItemRouteFailure(t, err, githubCommitsRoute)
}

func TestGitHubTestsRunListItemFailureNamesTheRunsRoute(t *testing.T) {
	t.Parallel()
	doer := itemRoutePathDoer{"/repos/acme/api": gitHubRepositoryFixture, "/repos/acme/api/actions/runs": `{"workflow_runs":[[]]}`}
	_, err := (GitHubTestsRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("github", "tests"), providerfoundation.Credential{},
		githubTestsClient(t, fakehttp.Client(doer)), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	requireItemRouteFailure(t, err, githubRunsRoute)
}

func TestGitHubTeamCatalogItemFailureNamesTheTeamsRoute(t *testing.T) {
	t.Parallel()
	doer := &githubTeamCatalogFixtureDoer{t: t, byPath: map[string]string{"/orgs/acme/teams": `[[]]`}}
	collector := GitHubTeamCatalogRouteHandler{
		Client: githubTeamCatalogTestClient(t, fakehttp.Client(doer)), OrgName: "acme",
		Now: func() time.Time { return time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC) },
	}
	_, _, err := collector.Collect(context.Background(), "org-1", true, false)
	requireItemRouteFailure(t, err, "/orgs/{org}/teams")
}

func TestGitLabListItemFailureNamesTheIssuesRoute(t *testing.T) {
	responses := gitLabWorkItemResponses()
	responses["/api/v4/projects/123/issues?page=1"] = []string{`[[]]`}
	requireItemRouteFailure(t, collectGitLabWorkItemsForItemRoute(t, responses), gitlabIssuesRoute)
}

func TestGitLabNestedItemFailureNamesTheChildRoute(t *testing.T) {
	responses := gitLabWorkItemResponses()
	responses["/api/v4/projects/123/issues/42/resource_label_events?page=1"] = []string{`[[]]`}
	requireItemRouteFailure(t, collectGitLabWorkItemsForItemRoute(t, responses), gitlabIssuesRoute+"/{iid}/resource_label_events")
}

func collectGitLabWorkItemsForItemRoute(t *testing.T, responses map[string][]string) error {
	t.Helper()
	claim := nativeTestClaim("gitlab", "work-items")
	_, err := (GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10}).Collect(
		context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(&gitLabWorkItemsDoer{responses: responses})), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	return err
}

func TestGitLabTreeRefItemFailureNamesTheCommitsRoute(t *testing.T) {
	t.Parallel()
	doer := gitLabFilesFixtureDoer(t)
	doer.commits = gitLabFilesResponse{body: `[[]]`}
	_, err := (GitLabFilesRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "files"), providerfoundation.Credential{},
		gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"), time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	requireItemRouteFailure(t, err, gitlabCommitsRoute)
}

// The two identity compares of the bare sentinel keep their feeding sites bare (CHAOS-14279): the cells pin that the bare sentinel
// still matches there, so a later wrap or compare change is caught.
func TestTheBareSentinelStillSkipsAnOpenRunAtTheIdentityCompare(t *testing.T) {
	t.Parallel()
	reason, _, _, _, skip := githubTestsOpenRunSkip(context.Background(), providerfoundation.ErrNormalizationInvalid, false)
	if !skip || reason != "invalid_payload" {
		t.Fatalf("githubTestsOpenRunSkip(bare) = reason %q skip %v; want invalid_payload skip", reason, skip)
	}
}

func TestTheBareSentinelStillSoftensACommitStatsDetailAtTheIdentityCompare(t *testing.T) {
	t.Parallel()
	if err := gitLabCommitStatsFatalDetailError(context.Background(), providerfoundation.ErrNormalizationInvalid); err != nil {
		t.Fatalf("gitLabCommitStatsFatalDetailError(bare) = %v; want nil (soft degradation)", err)
	}
}

func TestAMalformedRunBodyStaysTheBareSentinelForTheOpenRunSkip(t *testing.T) {
	t.Parallel()
	client := gitHubRepositoryClient(t, itemRoutePathDoer{"/repos/o/r/actions/runs/7": `[]`}, "https://api.github.com")
	_, _, err := fetchGitHubTestsRun(context.Background(), client, "/repos/o/r", "7")
	if err != providerfoundation.ErrNormalizationInvalid {
		t.Fatalf("fetchGitHubTestsRun(malformed) = %v; want the bare ErrNormalizationInvalid", err)
	}
}
