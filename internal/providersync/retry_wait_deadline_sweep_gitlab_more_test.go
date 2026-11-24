package providersync

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type sweepG3Resp struct {
	status int
	body   string
	header http.Header
}

func (resp sweepG3Resp) response(request *http.Request) *http.Response {
	status := resp.status
	if status == 0 {
		status = http.StatusOK
	}
	header := resp.header
	if header == nil {
		header = http.Header{"Content-Type": []string{"application/json"}}
	}
	return &http.Response{
		StatusCode: status, Status: http.StatusText(status), Header: header,
		Body: io.NopCloser(strings.NewReader(resp.body)), Request: request,
	}
}

// sweepG3Queue answers its responses in order, like the route tests' queues.
type sweepG3Queue struct {
	mu        sync.Mutex
	responses []sweepG3Resp
}

func (doer *sweepG3Queue) Do(request *http.Request) (*http.Response, error) {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	if len(doer.responses) == 0 {
		return sweepG3Resp{status: http.StatusNotFound, body: `{"message":"no fixture"}`}.response(request), nil
	}
	resp := doer.responses[0]
	doer.responses = doer.responses[1:]
	return resp.response(request), nil
}

type sweepG3Func func(request *http.Request) sweepG3Resp

func (fn sweepG3Func) Do(request *http.Request) (*http.Response, error) {
	return fn(request).response(request), nil
}

func sweepG3Next(page string) http.Header { return http.Header{"X-Next-Page": []string{page}} }

func sweepG3Now() time.Time { return time.Date(2026, 8, 9, 12, 0, 0, 987654321, time.UTC) }

// sweepG3Collect registers one gitlab route whose handler runs through collect.
func sweepG3Register(
	dataset string,
	newDoer func() HTTPDoerForSweep,
	run func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error),
) {
	registerRetrySweepRoute(retrySweepRoute{
		name:    "gitlab/" + dataset,
		newDoer: newDoer,
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			claim := nativeTestClaim("gitlab", sweepG3Claim(dataset))
			return run(ctx, claim, sweepClient(t, "gitlab", "https://gitlab.example", doer, wait))
		},
	})
}

func sweepG3Claim(name string) string {
	switch name {
	case "repository":
		return "repo-metadata"
	case "commit_stats":
		return "commit-stats"
	case "feature_flags":
		return "feature-flags"
	case "work_items":
		return "work-items"
	case "tests/chunked":
		return "tests"
	}
	return name
}

func sweepG3Queued(responses func() []sweepG3Resp) func() HTTPDoerForSweep {
	return func() HTTPDoerForSweep { return &sweepG3Queue{responses: responses()} }
}

func sweepG3Zip(members map[string]string) []byte {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for name, body := range members {
		member, err := writer.Create(name)
		if err != nil {
			panic(err)
		}
		if _, err := member.Write([]byte(body)); err != nil {
			panic(err)
		}
	}
	if err := writer.Close(); err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

func sweepG3FilesDoer() HTTPDoerForSweep {
	trees := map[string]sweepG3Resp{
		"1": {body: `[{"path":"README.md","type":"blob","size":20},{"path":"src/main.go","type":"blob","size":15}]`, header: sweepG3Next("2")},
		"2": {body: `[{"path":"tests/example.go","type":"blob","size":20},{"path":"src/types.d.ts","type":"blob","size":20}]`, header: sweepG3Next("3")},
		"3": {body: `[{"path":"src/data.bin","type":"blob","size":8}]`},
	}
	return sweepG3Func(func(request *http.Request) sweepG3Resp {
		switch request.URL.Path {
		case "/api/v4/projects/123":
			return sweepG3Resp{body: gitLabRepositoryFixture}
		case "/api/v4/projects/123/repository/tree":
			return trees[request.URL.Query().Get("page")]
		case "/api/v4/projects/123/repository/commits":
			return sweepG3Resp{body: `{}`}
		case "/api/graphql":
			body, _ := io.ReadAll(request.Body)
			if strings.Contains(string(body), "rawSize") {
				return sweepG3Resp{body: `{"data":{"project":{"repository":{"blobs":{"nodes":[{"path":"src/main.go","rawSize":15}]}}}}}`}
			}
			return sweepG3Resp{body: `{"data":{"project":{"repository":{"blobs":{"nodes":[{"path":"src/main.go","rawTextBlob":"package main\n"}]}}}}}`}
		}
		return sweepG3Resp{status: http.StatusNotFound, body: `{"message":"no fixture"}`}
	})
}

func sweepG3BlameDoer() HTTPDoerForSweep {
	const prefix = "/api/v4/projects/123/repository/files/"
	return sweepG3Func(func(request *http.Request) sweepG3Resp {
		path := request.URL.Path
		switch {
		case path == "/api/v4/projects/123":
			return sweepG3Resp{body: `{"id":123,"name":"api","path_with_namespace":"acme/api","default_branch":"main"}`}
		case path == "/api/v4/projects/123/repository/commits":
			return sweepG3Resp{body: `[{"id":"tree-sha"}]`}
		case path == "/api/v4/projects/123/repository/tree":
			entries := make([]string, 0, 3)
			for index := range 3 {
				entries = append(entries, fmt.Sprintf(`{"path":"src/file-%03d.go","type":"blob"}`, index))
			}
			return sweepG3Resp{body: `[` + strings.Join(entries, ",") + `]`}
		case strings.HasPrefix(path, prefix) && strings.HasSuffix(path, "/blame"):
			return sweepG3Resp{body: `[{"lines":["package main","func main() {}"],"commit":{"id":"abc123","author_name":"Ada","author_email":"ada@example.com"}}]`}
		}
		return sweepG3Resp{status: http.StatusNotFound, body: `{"message":"no fixture"}`}
	})
}

func sweepG3PullRequestResponses() []sweepG3Resp {
	fixture := gitLabPullRequestFixtureResponses()
	out := make([]sweepG3Resp, 0, len(fixture))
	for _, resp := range fixture {
		out = append(out, sweepG3Resp{status: resp.status, body: resp.body, header: resp.headers})
	}
	return out
}

func sweepG3TestsDoer() HTTPDoerForSweep {
	return &gitLabTestsRouteDoer{archive: sweepG3Zip(map[string]string{"coverage.info": githubTestsLCOVFixture})}
}

func init() {
	sweepG3Register("blame", sweepG3BlameDoer, func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error) {
		return (GitLabBlameRouteHandler{Coverage: staticGitLabBlameCoverage{}}).Collect(
			ctx, claim, providerfoundation.Credential{}, client, sweepG3Now())
	})
	sweepG3Register("commit_stats", sweepG3Queued(func() []sweepG3Resp {
		return []sweepG3Resp{
			{body: gitLabRepositoryFixture},
			{body: `[{"id":"sha-1","committed_date":"2026-07-22T10:00:00Z"}]`, header: sweepG3Next("2")},
			{body: `[{"id":"sha-2","committed_date":"2026-07-21T10:00:00Z"}]`},
			{body: `{"id":"sha-1","stats":{"additions":"4","deletions":2}}`},
			{body: `{"id":"sha-2","stats":{"additions":1,"deletions":"3"}}`},
		}
	}), func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error) {
		return (GitLabCommitStatsRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{}, client, sweepG3Now())
	})
	sweepG3Register("commits", sweepG3Queued(func() []sweepG3Resp {
		return []sweepG3Resp{
			{body: gitLabRepositoryFixture},
			{body: `[
			{"id":"after-window","message":"future","author_name":"A","authored_date":"2026-08-01T00:00:00Z","committer_name":"C","committed_date":"2026-08-04T00:00:00Z","parent_ids":[]},
			{"id":"sha-1","message":null,"author_name":null,"authored_date":null,"committer_name":"Grace","committed_date":"2026-07-20T10:00:00Z","parent_ids":["p1","p2"]}
		]`, header: sweepG3Next("4")},
			{body: `[
			{"id":"sha-2","message":"ship it","author_name":"Ada","authored_date":"2026-07-10T09:00:00Z","committer_name":null,"committed_date":null,"parent_ids":[]},
			{"id":"before-window","message":"old","author_name":"A","authored_date":"2026-06-01T00:00:00Z","committer_name":"C","committed_date":"2026-06-01T00:00:00Z","parent_ids":[]}
		]`},
			{body: `[]`},
		}
	}), func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error) {
		return (GitLabCommitsRouteHandler{PerPage: 2, MaxPages: 10}).Collect(ctx, claim, providerfoundation.Credential{}, client, sweepG3Now())
	})
	sweepG3Register("deployments", sweepG3Queued(func() []sweepG3Resp {
		return []sweepG3Resp{
			{body: gitLabRepositoryFixture},
			{body: `[{"tag_name":"v1.2.3"}]`, header: sweepG3Next("2")},
			{body: `[
			{"id":501,"iid":7,"status":"success","environment":{"name":"future"},"created_at":"2026-08-01T10:00:00Z","deployable":{"started_at":"2026-08-01T09:58:00Z","finished_at":"2026-08-01T10:05:00Z","status":"success"},"sha":"future","ref":"future"},
			{"id":502,"iid":8,"status":"success","environment":{"name":"production"},"created_at":"2026-07-22T10:00:00Z","deployable":{"started_at":"2026-07-22T09:58:00Z","finished_at":"2026-07-22T10:05:00Z","status":"success"},"sha":"main","ref":"v1.2.3"},
			{"id":503,"iid":9,"status":"success","environment":{"name":"old"},"created_at":"2026-06-30T10:00:00Z","deployable":{"started_at":"2026-06-30T09:58:00Z","finished_at":"2026-06-30T10:05:00Z","status":"success"},"sha":"old","ref":"old"}
		]`, header: sweepG3Next("2")},
			{body: `[{"iid":11,"state":"merged","merged_at":"2026-08-01T09:00:00Z"}]`},
			{body: `[{"iid":44,"state":"opened","merged_at":""},{"iid":45,"state":"merged","merged_at":"2026-07-21T10:00:00Z"}]`},
			{body: `[{"iid":99,"state":"merged","merged_at":"2026-06-30T09:00:00Z"}]`},
		}
	}), func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error) {
		return (GitLabDeploymentsRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{}, client, sweepG3Now())
	})
	sweepG3Register("files", sweepG3FilesDoer, func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error) {
		claim.BeforeAt = nil
		return (GitLabFilesRouteHandler{PerPage: 2, MaxPages: 4}).Collect(ctx, claim, providerfoundation.Credential{}, client, sweepG3Now())
	})
	sweepG3Register("incidents", sweepG3Queued(func() []sweepG3Resp {
		return []sweepG3Resp{
			{body: gitLabRepositoryFixture},
			{body: `[
			{"id":9001,"iid":7,"issue_type":"incident","state":"opened","title":"API unavailable","description":"edge <down>","created_at":"2026-07-20T10:00:00.123456Z","updated_at":"2026-07-21T11:00:00.654321Z","web_url":"https://gitlab.example/Acme/API/-/issues/7","severity":"sev-1"},
			{"id":"9002","iid":"8","issue_type":"INCIDENT","state":"closed","title":"Queue lag","created_at":"2026-07-22T10:00:00Z","updated_at":"2026-07-23T10:00:00Z","closed_at":"2026-07-23T11:00:00Z","url":"https://gitlab.example/fallback","severity":"medium"}
		]`},
		}
	}), func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error) {
		return (GitLabIncidentsRouteHandler{PerPage: 5, MaxIssues: 10}).Collect(ctx, claim, providerfoundation.Credential{}, client, sweepG3Now())
	})
	sweepG3Register("repository", sweepG3Queued(func() []sweepG3Resp {
		return []sweepG3Resp{{body: gitLabRepositoryFixture}}
	}), func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error) {
		return (GitLabRepositoryRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{}, client, sweepG3Now())
	})
	sweepG3Register("feature_flags", sweepG3Queued(func() []sweepG3Resp {
		return []sweepG3Resp{
			{body: `[
			{"name":"checkout","active":true,"version":"new_version_flag","created_at":"2026-08-01T10:00:00Z","updated_at":"2026-08-09T10:00:00Z","strategies":[{"scopes":[{"environment_scope":"production"},{"environment_scope":"staging"}]}]},
			{"name":"search","active":false,"created_at":"2026-08-02T10:00:00Z","updated_at":"2026-08-09T11:00:00Z","strategies":[{"scopes":[{"environment_scope":"*"}]}]}
		]`, header: sweepG3Next("2")},
			{body: `[{"key":"billing","active":true,"updated_at":"2026-08-09T12:00:00Z","strategies":[]}]`},
			{body: `{"path_with_namespace":"","path":"acme/api"}`},
		}
	}), func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error) {
		claim.SourceExternalID = "group/project"
		claim.SourceName = "group/project"
		return (GitLabFeatureFlagsRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{}, client, sweepG3Now())
	})
	sweepG3Register("prs", sweepG3Queued(sweepG3PullRequestResponses),
		func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error) {
			return (GitLabPullRequestRouteHandler{PerPage: 2}).Collect(ctx, claim, providerfoundation.Credential{}, client, sweepG3Now())
		})
	registerRetrySweepRoute(retrySweepRoute{
		name:    "gitlab/work_items",
		newDoer: func() HTTPDoerForSweep { return &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()} },
		collect: func(t *testing.T, ctx context.Context, doer HTTPDoerForSweep, wait time.Duration) (CompleteRouteBatch, error) {
			claim := nativeTestClaim("gitlab", "work-items")
			return (GitLabWorkItemsRouteHandler{StatusMapping: sweepStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10}).Collect(
				ctx, claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
				sweepClient(t, "gitlab", "https://gitlab.example", doer, wait), sweepG3Now())
		},
	})
	sweepG3Register("tests", sweepG3TestsDoer, func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error) {
		return (GitLabTestsRouteHandler{}).Collect(ctx, claim, providerfoundation.Credential{}, client, sweepG3Now())
	})
	sweepG3Register("tests/chunked", sweepG3TestsDoer, func(ctx context.Context, claim Claim, client *providerfoundation.HTTPClient) (CompleteRouteBatch, error) {
		var final CompleteRouteBatch
		err := (GitLabTestsRouteHandler{}).CollectChunks(ctx, claim, providerfoundation.Credential{}, client, sweepG3Now(), "",
			func(emission ChunkRouteEmission) error {
				if emission.Final {
					final = emission.Batch
				}
				return nil
			})
		return final, err
	})
}
