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

type gitHubDeploymentsDoer struct{ requests []string }

func (doer *gitHubDeploymentsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.requests = append(doer.requests, request.URL.Path)
	body := `[]`
	switch request.URL.Path {
	case "/repos/acme/api":
		body = gitHubRepositoryFixture
	case "/repos/acme/api/releases":
		body = `[{"tag_name":"v1.2.3"}]`
	case "/repos/acme/api/deployments":
		body = `[{"id":101,"state":"success","environment":"production","created_at":"2026-07-22T10:00:00Z","ref":"v1.2.3","sha":"abc"},{"id":102,"status":"pending","created_at":"2026-06-20T10:00:00Z"}]`
	case "/repos/acme/api/commits/abc/pulls":
		body = `[{"number":42,"merge_commit_sha":"abc","merged_at":"2026-07-21T10:00:00Z"}]`
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func TestGitHubDeploymentsRouteMirrorsPythonEnrichmentAndWindow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	doer := &gitHubDeploymentsDoer{}
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "deployments")
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := (CompleteRouteSwitches{}).Descriptor("github", "deployments")
	if !ok {
		t.Fatal("github/deployments has no canonical descriptor")
	}
	if err := batch.validate(descriptor); err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v", batch.Watermark)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "deployments" || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row deploymentRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.OrgID != claim.OrgID || row.DeploymentID != "101" || row.ReleaseRef != "v1.2.3" || row.ReleaseRefConfidence != 1 || row.PullRequestNumber == nil || *row.PullRequestNumber != 42 || row.MergedAt == nil || row.DeployedAt == nil {
		t.Fatalf("row=%+v", row)
	}
	want := []string{"/repos/acme/api", "/repos/acme/api/releases", "/repos/acme/api/deployments", "/repos/acme/api/commits/abc/pulls"}
	if len(doer.requests) != len(want) {
		t.Fatalf("requests=%v want=%v", doer.requests, want)
	}
	for index := range want {
		if doer.requests[index] != want[index] {
			t.Fatalf("requests=%v want=%v", doer.requests, want)
		}
	}
}

func TestDeploymentReadbackRejectsEachPersistedFieldMismatch(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	expected := deploymentRow{OrgID: "org-1", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", DeploymentID: "101", DeployedAt: deploymentTimePointer(now), ReleaseRef: "v1", ReleaseRefConfidence: 1, LastSynced: now}
	actual := expected
	actual.ReleaseRef = "other"
	if got := compareDeploymentVersion(expected, actual, true); got != EffectConflict {
		t.Fatalf("inspection=%s", got)
	}
}

func TestDeploymentRowRejectsCrossTenantClaim(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "deployments")
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	row := deploymentRow{OrgID: "other-org", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", DeploymentID: "101", DeployedAt: deploymentTimePointer(now), LastSynced: now}
	if err := row.validate(claim); err == nil {
		t.Fatal("cross-tenant row passed validation")
	}
}

func deploymentTimePointer(value time.Time) *time.Time { return &value }
