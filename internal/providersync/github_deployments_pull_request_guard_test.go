package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type githubDeploymentPullLookupDoer struct {
	pullStatus int
	pullBody   string
}

func (doer *githubDeploymentPullLookupDoer) Do(request *http.Request) (*http.Response, error) {
	header := http.Header{"Content-Type": {"application/json"}}
	respond := func(status int, body string) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	switch request.URL.Path {
	case "/repos/acme/api":
		return respond(http.StatusOK, gitHubRepositoryFixture)
	case "/repos/acme/api/deployments":
		return respond(http.StatusOK, `[{"id":821,"sha":"abc123","created_at":"2026-07-22T10:00:00Z"}]`)
	case "/repos/acme/api/commits/abc123/pulls":
		return respond(doer.pullStatus, doer.pullBody)
	case "/repos/acme/api/deployments/821/statuses":
		return respond(http.StatusOK, `[{"state":"success","created_at":"2026-07-22T10:01:00Z"}]`)
	}
	return respond(http.StatusOK, `[]`)
}

func collectGitHubDeploymentPullLookupRow(t *testing.T, doer *githubDeploymentPullLookupDoer) deploymentRow {
	t.Helper()
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")
	claim := nativeTestClaim("github", "deployments")
	batch, err := (GitHubDeploymentsRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row deploymentRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	return row
}

func TestGitHubDeploymentsRoutePullRequestLookupFailureMarksRow(t *testing.T) {
	log := captureSlog(t)
	row := collectGitHubDeploymentPullLookupRow(t, &githubDeploymentPullLookupDoer{pullStatus: http.StatusNotFound, pullBody: `{"message":"Not Found"}`})
	if !row.PullRequestLookupFailed || row.MergedAt != nil || row.PullRequestNumber != nil {
		t.Fatalf("row=%+v want PullRequestLookupFailed with nil pair", row)
	}
	if row.LifecycleLookupFailed || row.Status == nil {
		t.Fatalf("row=%+v: the statuses lookup succeeded and must stay unmarked", row)
	}
	if !strings.Contains(log.String(), "github_deployments.pull_request_lookup_failed") || !strings.Contains(log.String(), "deployment_id=821") {
		t.Fatalf("expected a pull_request_lookup_failed log line for deployment_id=821, got: %s", log.String())
	}
}

func TestGitHubDeploymentsRoutePullRequestLookupEmptyStaysUnmarked(t *testing.T) {
	row := collectGitHubDeploymentPullLookupRow(t, &githubDeploymentPullLookupDoer{pullStatus: http.StatusOK, pullBody: `[]`})
	if row.PullRequestLookupFailed || row.MergedAt != nil || row.PullRequestNumber != nil {
		t.Fatalf("row=%+v want an unmarked honest nil pair", row)
	}
}

func TestGitHubDeploymentsRoutePullRequestLookupSuccessStaysUnmarked(t *testing.T) {
	row := collectGitHubDeploymentPullLookupRow(t, &githubDeploymentPullLookupDoer{pullStatus: http.StatusOK, pullBody: `[{"number":5,"merged_at":"2026-07-22T09:00:00Z","merge_commit_sha":"abc123"}]`})
	if row.PullRequestLookupFailed || row.PullRequestNumber == nil || *row.PullRequestNumber != 5 || row.MergedAt == nil {
		t.Fatalf("row=%+v want pull request 5, unmarked", row)
	}
}

var errDeploymentGuardReadFailed = errors.New("deployment guard read failed")

// deploymentGuardReadFailsConn fails every Query and counts PrepareBatch, so
// a test can prove the write stops before the insert.
type deploymentGuardReadFailsConn struct {
	driver.Conn
	queries  int
	prepares int
}

func (conn *deploymentGuardReadFailsConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	conn.queries++
	return nil, errDeploymentGuardReadFailed
}

func (conn *deploymentGuardReadFailsConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn.prepares++
	return &deploymentEffectsRecordingBatch{}, nil
}

func TestDeploymentsEffectsGuardReadFailureStopsWriteAndInspect(t *testing.T) {
	for _, flag := range []string{"lifecycle", "pull_request"} {
		for _, provider := range []string{"github", "gitlab"} {
			claim := nativeTestClaim(provider, "deployments")
			row := deploymentEffectsUnitRow(claim, "831")
			row.LifecycleLookupFailed = flag == "lifecycle"
			row.PullRequestLookupFailed = flag == "pull_request"
			effect := deploymentEffectsUnitEffect(t, []deploymentRow{row})
			lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
			conn := &deploymentGuardReadFailsConn{}
			var sink deploymentEffectsIntegrationSinkForUnit = GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: lease}
			if provider == "gitlab" {
				sink = GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: lease}
			}
			if err := sink.WriteEffect(context.Background(), claim, effect); !errors.Is(err, errDeploymentGuardReadFailed) || conn.prepares != 0 {
				t.Fatalf("%s/%s write err=%v prepares=%d want the guard read error and no insert", provider, flag, err, conn.prepares)
			}
			inspection, err := sink.InspectEffect(context.Background(), claim, effect)
			if !errors.Is(err, errDeploymentGuardReadFailed) || inspection != EffectConflict || conn.queries != 2 {
				t.Fatalf("%s/%s inspection=%s err=%v queries=%d want the guard read error, %s, 2 queries", provider, flag, inspection, err, conn.queries, EffectConflict)
			}
		}
	}
}

type deploymentEffectsIntegrationSinkForUnit interface {
	WriteEffect(context.Context, Claim, EffectBatch) error
	InspectEffect(context.Context, Claim, EffectBatch) (EffectInspection, error)
}
