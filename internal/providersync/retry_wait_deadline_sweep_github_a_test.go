package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// sweepG1Reply answers one request with status 200 and a JSON body.
func sweepG1Reply(request *http.Request, body string) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK, Request: request,
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Body:   io.NopCloser(strings.NewReader(body)),
	}, nil
}

func sweepG1NotFound(request *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusNotFound, Request: request,
		Header: http.Header{"Content-Type": []string{"application/json"}},
		Body:   io.NopCloser(strings.NewReader(`{}`)),
	}, nil
}

// sweepG1PathDoer serves a body per URL path and 404 for the rest.
func sweepG1PathDoer(bodies map[string]string) HTTPDoerForSweep {
	return jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		if body, ok := bodies[request.URL.Path]; ok {
			return sweepG1Reply(request, body)
		}
		return sweepG1NotFound(request)
	})
}

func sweepG1Now() time.Time { return time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC) }

const sweepG1CommitStatsList = `[
  {"sha":"first","commit":{"committer":{"date":"2026-07-22T10:00:00Z"}}},
  {"sha":"second","commit":{"author":{"date":"2026-07-22T09:00:00Z"}}}
]`

func sweepG1TreeDoer(withBlame bool) HTTPDoerForSweep {
	return jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/repos/acme/api":
			return sweepG1Reply(request, `{"full_name":"acme/api","default_branch":"main"}`)
		case "/repos/acme/api/commits":
			return sweepG1Reply(request, `[{"sha":"tree-sha"}]`)
		case "/repos/acme/api/git/trees/tree-sha":
			return sweepG1Reply(request, `{"tree":[{"path":"README.md","type":"blob","size":20},{"path":"src/main.go","type":"blob","size":12},{"path":"src/util.go","type":"blob","size":12},{"path":"dir","type":"tree"}]}`)
		case "/graphql":
			if withBlame {
				return sweepG1Reply(request, `{"data":{"repository":{"object":{"blame":{"ranges":[{"startingLine":1,"endingLine":2,"commit":{"oid":"abc123","author":{"name":"Ada","email":"ada@example.com"}}}]}}}}}`)
			}
			return sweepG1Reply(request, `{"data":{"repository":{"f0":{"text":"package main\n","isBinary":false,"isTruncated":false},"f1":{"text":"package main\n","isBinary":false,"isTruncated":false}}}}`)
		}
		return sweepG1NotFound(request)
	})
}

func sweepG1DeploymentsDoer() HTTPDoerForSweep {
	return sweepG1PathDoer(map[string]string{
		"/repos/acme/api":                          gitHubRepositoryFixture,
		"/repos/acme/api/releases":                 `[{"tag_name":"v1.2.3"}]`,
		"/repos/acme/api/deployments":              `[{"id":101,"state":"success","environment":"production","created_at":"2026-07-22T10:00:00Z","ref":"v1.2.3","sha":"abc"},{"id":102,"status":"pending","created_at":"2026-06-20T10:00:00Z"}]`,
		"/repos/acme/api/commits/abc/pulls":        `[{"number":42,"merge_commit_sha":"abc","merged_at":"2026-07-21T10:00:00Z"}]`,
		"/repos/acme/api/deployments/101/statuses": `[{"id":1,"state":"queued","created_at":"2026-07-22T09:58:00Z"},{"id":2,"state":"in_progress","created_at":"2026-07-22T09:59:00Z"},{"id":3,"state":"success","created_at":"2026-07-22T10:01:00Z"}]`,
	})
}

func init() {
	now := sweepG1Now()
	registerRetrySweepRoute(retrySweepRoute{
		name: "github/commits",
		newDoer: func() HTTPDoerForSweep {
			return sweepG1PathDoer(map[string]string{
				"/repos/acme/api":         gitHubRepositoryFixture,
				"/repos/acme/api/commits": `[{"sha":"sha-1","commit":{"message":"ship it","author":{"name":"Ada","date":"2026-07-20T09:00:00Z"},"committer":{"name":"Grace","date":"2026-07-20T10:00:00Z"}},"parents":[]}]`,
			})
		},
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return (GitHubCommitsRouteHandler{}).Collect(ctx, nativeTestClaim("github", "commits"), providerfoundation.Credential{},
				sweepClient(t, "github", "https://api.github.com", doer, wait), now)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name: "github/commit-stats",
		newDoer: func() HTTPDoerForSweep {
			return sweepG1PathDoer(map[string]string{
				"/repos/acme/api":                gitHubRepositoryFixture,
				"/repos/acme/api/commits":        sweepG1CommitStatsList,
				"/repos/acme/api/commits/first":  `{"files":[{"filename":"src/main.go","additions":4,"deletions":2},{"filename":"","additions":8,"deletions":3}]}`,
				"/repos/acme/api/commits/second": `{"files":[{"filename":"README.md","additions":1,"deletions":0}]}`,
			})
		},
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return (GitHubCommitStatsRouteHandler{MaxCommits: 3}).Collect(ctx, nativeTestClaim("github", "commit-stats"), providerfoundation.Credential{},
				sweepClient(t, "github", "https://api.github.com", doer, wait), now)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name: "github/repo-metadata",
		newDoer: func() HTTPDoerForSweep {
			return sweepG1PathDoer(map[string]string{"/repos/acme/api": gitHubRepositoryFixture})
		},
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return (GitHubRepositoryRouteHandler{Now: func() time.Time { return now }}).Collect(ctx, nativeTestClaim("github", "repo-metadata"), providerfoundation.Credential{},
				sweepClient(t, "github", "https://api.github.com", doer, wait), now)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name:    "github/deployments",
		newDoer: sweepG1DeploymentsDoer,
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return (GitHubDeploymentsRouteHandler{}).Collect(ctx, nativeTestClaim("github", "deployments"), providerfoundation.Credential{},
				sweepClient(t, "github", "https://api.github.com", doer, wait), now)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name:    "github/files",
		newDoer: func() HTTPDoerForSweep { return sweepG1TreeDoer(false) },
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return (GitHubFilesRouteHandler{}).Collect(ctx, nativeTestClaim("github", "files"), providerfoundation.Credential{},
				sweepClient(t, "github", "https://api.github.com", doer, wait), now)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name:    "github/blame",
		newDoer: func() HTTPDoerForSweep { return sweepG1TreeDoer(true) },
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return (GitHubBlameRouteHandler{Coverage: staticGitHubBlameCoverage{}, MaxFiles: 3}).Collect(ctx, nativeTestClaim("github", "blame"), providerfoundation.Credential{},
				sweepClient(t, "github", "https://api.github.com", doer, wait), now)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name:    "github/prs",
		newDoer: func() HTTPDoerForSweep { return sweepG1PathDoer(defaultGitHubPullRequestFixtures()) },
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return (GitHubPullRequestRouteHandler{Now: func() time.Time { return now }}).Collect(ctx, nativeTestClaim("github", "prs"), providerfoundation.Credential{},
				sweepClient(t, "github", "https://api.github.com", doer, wait), now)
		},
	})
	registerRetrySweepRoute(retrySweepRoute{
		name: "github/pr-reviews",
		newDoer: func() HTTPDoerForSweep {
			bodies := defaultGitHubPullRequestFixtures()
			bodies["/graphql"] = `{"data":{"repository":{"pr0":{"number":42,"reviews":{"nodes":[` +
				`{"databaseId":9007199254740993,"state":"APPROVED","submittedAt":"2026-07-11T10:30:00Z","author":{"login":"octocat"}},` +
				`{"id":"R_changes","state":"CHANGES_REQUESTED","submittedAt":"2026-07-10T11:00:00Z","author":{"login":"hubot"}}` +
				`],"pageInfo":{"hasNextPage":false,"endCursor":""}}}}}}`
			return sweepG1PathDoer(bodies)
		},
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			return (GitHubPullRequestSocialRouteHandler{}).Collect(ctx, nativeTestClaim("github", "pr-reviews"), providerfoundation.Credential{},
				sweepClient(t, "github", "https://api.github.com", doer, wait), time.Date(2026, 8, 4, 12, 0, 0, 123456000, time.UTC))
		},
	})
}
