package providersync

import (
	"context"
	"encoding/json"
	"io"
	"math"
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
	now := time.Date(2026, 7, 23, 12, 30, 0, 123000000, time.UTC)
	base := deploymentComparatorRow(now)
	tests := []struct {
		name     string
		expected func() deploymentRow
		mutate   func(*deploymentRow)
		found    bool
		want     EffectInspection
	}{
		{name: "exact", mutate: func(*deploymentRow) {}, found: true, want: EffectExact},
		{name: "repo id", mutate: func(row *deploymentRow) { row.RepoID = "c7198fbc-1945-3717-05d8-eb78866b4e78" }, found: true, want: EffectConflict},
		{name: "deployment id", mutate: func(row *deploymentRow) { row.DeploymentID = "102" }, found: true, want: EffectConflict},
		{name: "org id", mutate: func(row *deploymentRow) { row.OrgID = "org-2" }, found: true, want: EffectConflict},
		{name: "status", mutate: func(row *deploymentRow) { row.Status = deploymentStringPointer("failed") }, found: true, want: EffectConflict},
		{name: "environment", mutate: func(row *deploymentRow) { row.Environment = deploymentStringPointer("staging") }, found: true, want: EffectConflict},
		{name: "started at", mutate: func(row *deploymentRow) { row.StartedAt = deploymentTimePointer(now.Add(-9 * time.Minute)) }, found: true, want: EffectConflict},
		{name: "finished at", mutate: func(row *deploymentRow) { row.FinishedAt = deploymentTimePointer(now.Add(-4 * time.Minute)) }, found: true, want: EffectConflict},
		{name: "deployed at", mutate: func(row *deploymentRow) { row.DeployedAt = deploymentTimePointer(now.Add(-3 * time.Minute)) }, found: true, want: EffectConflict},
		{name: "merged at", mutate: func(row *deploymentRow) { row.MergedAt = deploymentTimePointer(now.Add(-14 * time.Minute)) }, found: true, want: EffectConflict},
		{name: "pull request number", mutate: func(row *deploymentRow) { row.PullRequestNumber = deploymentIntPointer(43) }, found: true, want: EffectConflict},
		{name: "pull request number nil", mutate: func(row *deploymentRow) { row.PullRequestNumber = nil }, found: true, want: EffectConflict},
		{
			name: "pull request nil versus zero",
			expected: func() deploymentRow {
				row := base
				row.PullRequestNumber = nil
				return row
			},
			mutate: func(row *deploymentRow) { row.PullRequestNumber = deploymentIntPointer(0) },
			found:  true,
			want:   EffectConflict,
		},
		{
			name: "pull request zero versus nil",
			expected: func() deploymentRow {
				row := base
				row.PullRequestNumber = deploymentIntPointer(0)
				return row
			},
			mutate: func(row *deploymentRow) { row.PullRequestNumber = nil },
			found:  true,
			want:   EffectConflict,
		},
		{name: "release ref", mutate: func(row *deploymentRow) { row.ReleaseRef = "other" }, found: true, want: EffectConflict},
		{name: "release confidence", mutate: func(row *deploymentRow) { row.ReleaseRefConfidence = 0.5 }, found: true, want: EffectConflict},
		{name: "last synced zero", mutate: func(row *deploymentRow) { row.LastSynced = time.Time{} }, found: true, want: EffectAbsent},
		{name: "last synced older", mutate: func(row *deploymentRow) { row.LastSynced = now.Add(-time.Millisecond) }, found: true, want: EffectAbsent},
		{name: "last synced newer", mutate: func(row *deploymentRow) { row.LastSynced = now.Add(time.Millisecond) }, found: true, want: EffectConflict},
		{name: "not found", mutate: func(*deploymentRow) {}, found: false, want: EffectAbsent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := base
			if test.expected != nil {
				expected = test.expected()
			}
			actual := expected
			test.mutate(&actual)
			if got := compareDeploymentVersion(expected, actual, test.found); got != test.want {
				t.Fatalf("inspection=%s want=%s expected=%+v actual=%+v", got, test.want, expected, actual)
			}
		})
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

func TestDeploymentRowRejectsPullRequestNumbersOutsideUInt32(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "deployments")
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	values := []struct {
		name   string
		number int
	}{{name: "negative", number: -1}}
	tooLarge := uint64(math.MaxUint32) + 1
	if uint64(math.MaxInt) >= tooLarge {
		values = append(values, struct {
			name   string
			number int
		}{name: "above UInt32", number: int(tooLarge)})
	}
	for _, value := range values {
		t.Run(value.name, func(t *testing.T) {
			number := value.number
			row := deploymentRow{
				OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
				DeploymentID: "101", DeployedAt: deploymentTimePointer(now),
				PullRequestNumber: &number, LastSynced: now,
			}
			if err := row.validate(claim); err == nil {
				t.Fatalf("pull request number %d passed validation", value.number)
			}
		})
	}
}

func deploymentTimePointer(value time.Time) *time.Time { return &value }

func deploymentStringPointer(value string) *string { return &value }

func deploymentIntPointer(value int) *int { return &value }

func deploymentComparatorRow(now time.Time) deploymentRow {
	return deploymentRow{
		OrgID: "org-1", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", DeploymentID: "101",
		Status: deploymentStringPointer("success"), Environment: deploymentStringPointer("production"),
		StartedAt:         deploymentTimePointer(now.Add(-10 * time.Minute)),
		FinishedAt:        deploymentTimePointer(now.Add(-5 * time.Minute)),
		DeployedAt:        deploymentTimePointer(now.Add(-4 * time.Minute)),
		MergedAt:          deploymentTimePointer(now.Add(-15 * time.Minute)),
		PullRequestNumber: deploymentIntPointer(42), ReleaseRef: "v1.2.3",
		ReleaseRefConfidence: 0.875, LastSynced: now,
	}
}
