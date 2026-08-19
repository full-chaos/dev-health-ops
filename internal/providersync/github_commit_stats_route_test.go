package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const gitHubCommitStatsListFixture = `[
  {"sha":"first","commit":{"committer":{"date":"2026-07-22T10:00:00Z"}}},
  {"sha":"second","commit":{"author":{"date":"2026-07-22T09:00:00Z"}}}
]`

type gitHubCommitStatsDoer struct {
	t        *testing.T
	requests []string
}

func (doer *gitHubCommitStatsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request.URL.RequestURI())
	body := ""
	switch request.URL.Path {
	case "/repos/acme/api":
		body = gitHubRepositoryFixture
	case "/repos/acme/api/commits":
		body = gitHubCommitStatsListFixture
	case "/repos/acme/api/commits/first":
		body = `{"files":[{"filename":"src/main.go","additions":4,"deletions":2},{"filename":"","additions":8,"deletions":3}]}`
	case "/repos/acme/api/commits/second":
		body = `{"files":[{"filename":"README.md","additions":1,"deletions":0}]}`
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.RequestURI())
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

func TestGitHubCommitStatsRouteListsThenFetchesEachCommitDetail(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitHubCommitStatsDoer{t: t}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "commit-stats")

	batch, err := (GitHubCommitStatsRouteHandler{MaxCommits: 3}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "git_commit_stats" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || len(batch.Effects[0].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	if batch.Evidence.Requests != 4 || batch.Evidence.Pages != 1 || batch.Evidence.Records != 2 {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
	wantRequests := []string{
		"/repos/acme/api",
		"/repos/acme/api/commits?per_page=100&since=2026-07-01T00%3A00%3A00Z&until=2026-07-31T23%3A59%3A59Z",
		"/repos/acme/api/commits/first",
		"/repos/acme/api/commits/second",
	}
	if len(doer.requests) != len(wantRequests) {
		t.Fatalf("requests=%v want=%v", doer.requests, wantRequests)
	}
	for index := range wantRequests {
		if doer.requests[index] != wantRequests[index] {
			t.Fatalf("request[%d]=%q want %q", index, doer.requests[index], wantRequests[index])
		}
	}
	var first commitStatsRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &first); err != nil {
		t.Fatal(err)
	}
	if first.OrgID != claim.OrgID || first.CommitHash != "first" || first.FilePath != "src/main.go" ||
		first.Additions != 4 || first.Deletions != 2 || first.OldFileMode != "unknown" ||
		first.NewFileMode != "unknown" || !first.LastSynced.Equal(now) {
		t.Fatalf("row=%+v", first)
	}
}

func TestGitHubCommitStatsRouteSkipsIncrementalWindowThatExceedsCap(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitHubCommitStatsDoer{t: t}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "commit-stats")
	since := now.Add(-24 * time.Hour)
	claim.SinceAt = &since

	batch, err := (GitHubCommitStatsRouteHandler{MaxCommits: 1}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 0 || len(doer.requests) != 2 {
		t.Fatalf("effects=%+v requests=%v", batch.Effects, doer.requests)
	}
}

func TestCommitStatsRowRejectsTenantMismatch(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "commit-stats")
	row := commitStatsRow{
		OrgID: "other-org", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
		CommitHash: "abc123", FilePath: "src/main.go", OldFileMode: "unknown",
		NewFileMode: "unknown", LastSynced: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
	}
	if err := row.validate(claim); err == nil {
		t.Fatal("tenant-mismatched row passed validation")
	}
}
