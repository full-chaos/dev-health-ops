package providersync

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-9776: a status failure of a sync route names the static template and
// never the owner, repository or project id of the request path. The census
// test covers every route; these rows prove the behaviour on the commits
// routes, for the repository/project fetch and for the page walk.
func TestSyncRouteStatusErrorsNameTheTemplateNotThePath(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	bad := githubCommitsResponse{status: http.StatusBadRequest, body: `{}`}
	for name, tc := range map[string]struct {
		run   func() error
		route string
		leak  string
	}{
		"github repo fetch": {func() error {
			doer := &githubCommitsDoer{t: t, responses: []githubCommitsResponse{bad}}
			_, err := (GitHubCommitsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "commits"),
				providerfoundation.Credential{}, gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com"), normalizedAt)
			return err
		}, githubRepoRoute, "acme"},
		"github commits page": {func() error {
			doer := &githubCommitsDoer{t: t, responses: []githubCommitsResponse{{body: gitHubRepositoryFixture}, bad}}
			_, err := (GitHubCommitsRouteHandler{}).Collect(context.Background(), nativeTestClaim("github", "commits"),
				providerfoundation.Credential{}, gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com"), normalizedAt)
			return err
		}, githubCommitsRoute, "acme"},
		"gitlab project fetch": {func() error {
			doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{{status: http.StatusBadRequest, body: `{}`}}}
			_, err := (GitLabCommitsRouteHandler{PerPage: 2, MaxPages: 2}).Collect(context.Background(), nativeTestClaim("gitlab", "commits"),
				providerfoundation.Credential{}, gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"), normalizedAt)
			return err
		}, gitlabProjectRoute, "/projects/123"},
		"gitlab commits page": {func() error {
			doer := &gitLabCommitsDoer{t: t, responses: []gitLabCommitsResponse{{body: gitLabRepositoryFixture}, {status: http.StatusBadRequest, body: `{}`}}}
			_, err := (GitLabCommitsRouteHandler{PerPage: 2, MaxPages: 2}).Collect(context.Background(), nativeTestClaim("gitlab", "commits"),
				providerfoundation.Credential{}, gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"), normalizedAt)
			return err
		}, gitlabCommitsRoute, "/projects/123"},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.run()
			if err == nil || !strings.Contains(err.Error(), "route="+tc.route) || strings.Contains(err.Error(), tc.leak) {
				t.Fatalf("err = %v, want route=%s and no %q", err, tc.route, tc.leak)
			}
		})
	}
}

// A template is a static shape: literal lowercase segments and {placeholder}
// segments only, so no request value can ever be one.
func TestRouteTemplatesHaveAStaticShape(t *testing.T) {
	shape := regexp.MustCompile(`^(/([a-z_]+|\{[a-z_]+\}))+$`)
	templates := []string{
		githubRepoRoute, githubPullsRoute, githubPullRoute, githubCommitsRoute, githubCommitPullsRoute,
		githubIssuesRoute, githubIssueEventsRoute, githubIssueCommentRoute, githubLabelsRoute, githubMilestonesRoute,
		githubDeploymentsRoute, githubStatusesRoute, githubReleasesRoute, githubRunsRoute, githubRunJobsRoute,
		githubRunArtifactsRoute, gitlabProjectRoute, gitlabCommitsRoute, gitlabMergeRequestsRoute, gitlabPipelinesRoute,
		gitlabPipelineJobsRoute, gitlabIssuesRoute, gitlabDeploymentsRoute, gitlabReleasesRoute, gitlabLabelsRoute,
		gitlabMilestonesRoute,
	}
	for _, template := range templates {
		if !shape.MatchString(template) {
			t.Errorf("template %q is not a static shape", template)
		}
	}
}
