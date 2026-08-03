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

const gitLabRepositoryFixture = `{
  "id": 123,
  "name": "api",
  "path_with_namespace": "Acme/API",
  "web_url": "https://gitlab.example/Acme/API",
  "default_branch": "main",
  "archived": false,
  "last_activity_at": "2026-07-20T10:00:00Z"
}`

type gitLabRepositoryDoer struct {
	t        *testing.T
	body     string
	requests []*http.Request
}

func (doer *gitLabRepositoryDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(doer.body)),
		Request:    request,
	}, nil
}

func gitLabRepositoryClient(
	t *testing.T,
	doer providerfoundation.HTTPDoer,
	base string,
) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"gitlab", base, doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{
			MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
		},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestGitLabRepositoryRouteEmitsCompleteReposEffect(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 987654321, time.UTC)
	doer := &gitLabRepositoryDoer{t: t, body: gitLabRepositoryFixture}
	claim := nativeTestClaim("gitlab", "repo-metadata")
	batch, err := (GitLabRepositoryRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://GITLAB.example:443"), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 1 || doer.requests[0].URL.Path != "/api/v4/projects/123" {
		t.Fatalf("requests=%d path=%q", len(doer.requests), doer.requests[0].URL.Path)
	}
	descriptor, ok := (CompleteRouteSwitches{}).Descriptor("gitlab", "repo-metadata")
	if !ok {
		t.Fatal("gitlab/repo-metadata has no descriptor")
	}
	if err := batch.validate(descriptor); err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil || batch.Evidence.Requests != 1 ||
		batch.Evidence.Pages != 1 || batch.Evidence.Records != 1 ||
		batch.Evidence.CapReached {
		t.Fatalf("watermark=%v evidence=%+v", batch.Watermark, batch.Evidence)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "repos" ||
		batch.Effects[0].Recovery != EffectReadbackRequired ||
		len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row repositoryRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	wantAt := now.UTC().Truncate(time.Millisecond)
	if row.ID != "c7198fbc-1945-3717-05d8-eb78866b4e79" ||
		row.OrgID != claim.OrgID || row.Repo != "Acme/API" ||
		row.Ref != nil || row.Provider != "gitlab" ||
		!row.CreatedAt.Equal(wantAt) || !row.LastSynced.Equal(wantAt) {
		t.Fatalf("row=%+v", row)
	}
	if row.Settings != `{"source":"gitlab","project_id":123,"url":"https://gitlab.example/Acme/API","default_branch":"main","gitlab_instance_url":"https://gitlab.example"}` {
		t.Fatalf("settings=%s", row.Settings)
	}
	if row.Tags != `["gitlab"]` {
		t.Fatalf("tags=%s", row.Tags)
	}
}

func TestGitLabRepositoryRoutePreservesSelfManagedInstancePort(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitLabRepositoryDoer{t: t, body: gitLabRepositoryFixture}
	batch, err := (GitLabRepositoryRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "repo-metadata"),
		providerfoundation.Credential{},
		gitLabRepositoryClient(t, doer, "https://gitlab.example:8443"), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	var row repositoryRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(row.Settings, `"gitlab_instance_url":"https://gitlab.example:8443"`) {
		t.Fatalf("settings=%s", row.Settings)
	}
}

func TestGitLabRepositoryRouteFailsClosedOnInvalidScopeAndPayload(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	client := gitLabRepositoryClient(t, &gitLabRepositoryDoer{
		t: t, body: gitLabRepositoryFixture,
	}, "https://gitlab.example")
	for name, claim := range map[string]Claim{
		"wrong provider": nativeTestClaim("github", "repo-metadata"),
		"wrong dataset":  nativeTestClaim("gitlab", "commits"),
		"non numeric id": func() Claim {
			claim := nativeTestClaim("gitlab", "repo-metadata")
			claim.SourceExternalID = "acme/api"
			return claim
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := (GitLabRepositoryRouteHandler{}).Collect(
				context.Background(), claim, providerfoundation.Credential{}, client, now,
			); err == nil {
				t.Fatal("invalid route input was accepted")
			}
		})
	}
	malformed := gitLabRepositoryClient(t, &gitLabRepositoryDoer{
		t: t, body: `{"id":"not-a-number","path_with_namespace":"Acme/API"}`,
	}, "https://gitlab.example")
	if _, err := (GitLabRepositoryRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "repo-metadata"),
		providerfoundation.Credential{}, malformed, now,
	); err == nil {
		t.Fatal("malformed project payload was accepted")
	}
	mismatched := gitLabRepositoryClient(t, &gitLabRepositoryDoer{
		t: t, body: `{"id":124,"path_with_namespace":"Acme/API"}`,
	}, "https://gitlab.example")
	if _, err := (GitLabRepositoryRouteHandler{}).Collect(
		context.Background(), nativeTestClaim("gitlab", "repo-metadata"),
		providerfoundation.Credential{}, mismatched, now,
	); err == nil {
		t.Fatal("project payload for a different claim id was accepted")
	}
}
