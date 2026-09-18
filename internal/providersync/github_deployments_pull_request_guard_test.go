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

func deploymentPullRequestGuardStored(row deploymentRow, mergedAt *time.Time, number *int) map[deploymentGuardKey]storedDeploymentGuardValues {
	return map[deploymentGuardKey]storedDeploymentGuardValues{
		{RepoID: row.RepoID, DeploymentID: row.DeploymentID}: {MergedAt: mergedAt, PullRequestNumber: number},
	}
}

func TestGuardDeploymentPullRequestRegressionsCarriesPairForwardOnFailure(t *testing.T) {
	claim := nativeTestClaim("github", "deployments")
	priorMerged := time.Date(2026, 7, 22, 9, 0, 0, 0, time.UTC)
	priorNumber := 42
	row := deploymentEffectsUnitRow(claim, "801")
	row.PullRequestLookupFailed = true
	rows := []deploymentRow{row}

	carried := guardDeploymentPullRequestRegressions(rows, deploymentPullRequestGuardStored(row, &priorMerged, &priorNumber))

	if rows[0].MergedAt == nil || !rows[0].MergedAt.Equal(priorMerged) {
		t.Fatalf("merged_at=%v want=%v", rows[0].MergedAt, priorMerged)
	}
	if rows[0].PullRequestNumber == nil || *rows[0].PullRequestNumber != priorNumber {
		t.Fatalf("pull_request_number=%v want=%d", rows[0].PullRequestNumber, priorNumber)
	}
	if len(carried) != 1 || carried[0].RepoID != row.RepoID || carried[0].DeploymentID != "801" {
		t.Fatalf("carried=%+v want exactly one event for deployment 801", carried)
	}
}

func TestGuardDeploymentPullRequestRegressionsCarriesNumberAloneOnFailure(t *testing.T) {
	claim := nativeTestClaim("github", "deployments")
	priorNumber := 7
	row := deploymentEffectsUnitRow(claim, "802")
	row.PullRequestLookupFailed = true
	rows := []deploymentRow{row}

	carried := guardDeploymentPullRequestRegressions(rows, deploymentPullRequestGuardStored(row, nil, &priorNumber))

	if rows[0].PullRequestNumber == nil || *rows[0].PullRequestNumber != priorNumber || rows[0].MergedAt != nil {
		t.Fatalf("row=%+v want number %d and nil merged_at", rows[0], priorNumber)
	}
	if len(carried) != 1 {
		t.Fatalf("carried=%d want 1", len(carried))
	}
}

func TestGuardDeploymentPullRequestRegressionsCarriesMergedAtAloneOnFailure(t *testing.T) {
	claim := nativeTestClaim("github", "deployments")
	priorMerged := time.Date(2026, 7, 22, 9, 0, 0, 0, time.UTC)
	row := deploymentEffectsUnitRow(claim, "807")
	row.PullRequestLookupFailed = true
	rows := []deploymentRow{row}

	carried := guardDeploymentPullRequestRegressions(rows, deploymentPullRequestGuardStored(row, &priorMerged, nil))

	if rows[0].MergedAt == nil || !rows[0].MergedAt.Equal(priorMerged) || rows[0].PullRequestNumber != nil {
		t.Fatalf("row=%+v want merged_at %v and nil number", rows[0], priorMerged)
	}
	if len(carried) != 1 {
		t.Fatalf("carried=%d want 1", len(carried))
	}
}

func TestGuardDeploymentPullRequestRegressionsKeepsHonestEmptyLookup(t *testing.T) {
	claim := nativeTestClaim("github", "deployments")
	priorMerged := time.Date(2026, 7, 22, 9, 0, 0, 0, time.UTC)
	priorNumber := 42
	row := deploymentEffectsUnitRow(claim, "803")
	rows := []deploymentRow{row}

	carried := guardDeploymentPullRequestRegressions(rows, deploymentPullRequestGuardStored(row, &priorMerged, &priorNumber))

	if rows[0].MergedAt != nil || rows[0].PullRequestNumber != nil || len(carried) != 0 {
		t.Fatalf("row=%+v carried=%d: a successful lookup's nil must stay nil", rows[0], len(carried))
	}
}

func TestGuardDeploymentPullRequestRegressionsSkipsBrandNewAndEmptyStored(t *testing.T) {
	claim := nativeTestClaim("github", "deployments")
	row := deploymentEffectsUnitRow(claim, "804")
	row.PullRequestLookupFailed = true

	rows := []deploymentRow{row}
	if carried := guardDeploymentPullRequestRegressions(rows, map[deploymentGuardKey]storedDeploymentGuardValues{}); len(carried) != 0 || rows[0].MergedAt != nil || rows[0].PullRequestNumber != nil {
		t.Fatalf("brand-new key: row=%+v carried=%d", rows[0], len(carried))
	}
	rows = []deploymentRow{row}
	if carried := guardDeploymentPullRequestRegressions(rows, deploymentPullRequestGuardStored(row, nil, nil)); len(carried) != 0 {
		t.Fatalf("empty stored pair: carried=%d want 0", len(carried))
	}
}

func TestGuardDeploymentPullRequestRegressionsLeavesLifecycleAndOtherKeysUntouched(t *testing.T) {
	claim := nativeTestClaim("github", "deployments")
	priorMerged := time.Date(2026, 7, 22, 9, 0, 0, 0, time.UTC)
	priorNumber := 42
	priorStarted := time.Date(2026, 7, 22, 8, 0, 0, 0, time.UTC)
	failed := deploymentEffectsUnitRow(claim, "805")
	failed.PullRequestLookupFailed = true
	other := deploymentEffectsUnitRow(claim, "806")
	other.PullRequestLookupFailed = true
	freshStatus := "success"
	failed.Status = &freshStatus
	rows := []deploymentRow{failed, other}
	stored := map[deploymentGuardKey]storedDeploymentGuardValues{
		{RepoID: failed.RepoID, DeploymentID: "805"}: {MergedAt: &priorMerged, PullRequestNumber: &priorNumber, StartedAt: &priorStarted},
	}

	carried := guardDeploymentPullRequestRegressions(rows, stored)

	if rows[0].StartedAt != nil || rows[0].Status == nil || *rows[0].Status != freshStatus {
		t.Fatalf("row=%+v: the pull request guard must not touch lifecycle columns", rows[0])
	}
	if rows[1].MergedAt != nil || rows[1].PullRequestNumber != nil {
		t.Fatalf("row=%+v: a key absent from stored must not borrow another key's values", rows[1])
	}
	if len(carried) != 1 {
		t.Fatalf("carried=%d want 1", len(carried))
	}
}

func TestGitHubDeploymentsEffectsWriteCarriesPullRequestForwardOnFailure(t *testing.T) {
	log := captureSlog(t)
	claim := nativeTestClaim("github", "deployments")
	priorMerged := time.Date(2026, 7, 22, 9, 0, 0, 0, time.UTC)
	priorNumber := uint32(42)
	row := deploymentEffectsUnitRow(claim, "811")
	row.PullRequestLookupFailed = true
	effect := deploymentEffectsUnitEffect(t, []deploymentRow{row})

	conn := &deploymentLifecycleGuardConn{queryRows: &deploymentLifecycleGuardRows{
		repoID: row.RepoID, deploymentID: row.DeploymentID,
		mergedAt: &priorMerged, pullRequestNumber: &priorNumber,
	}}
	sink := GitHubDeploymentsClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
		t.Fatalf("write error=%v", err)
	}
	if conn.queries != 1 {
		t.Fatalf("queries=%d want exactly 1 (the carry-forward read)", conn.queries)
	}
	if conn.batch == nil || len(conn.batch.values) != 1 {
		t.Fatalf("batch=%+v", conn.batch)
	}
	gotMerged, ok := conn.batch.values[0][7].(*time.Time)
	if !ok || gotMerged == nil || !gotMerged.Equal(priorMerged) {
		t.Fatalf("appended merged_at=%v want=%v", conn.batch.values[0][7], priorMerged)
	}
	gotNumber, ok := conn.batch.values[0][8].(*uint32)
	if !ok || gotNumber == nil || *gotNumber != priorNumber {
		t.Fatalf("appended pull_request_number=%v want=%d", conn.batch.values[0][8], priorNumber)
	}
	if got := strings.Count(log.String(), deploymentPullRequestRegressionGuardedEvent); got != 1 {
		t.Fatalf("guard events=%d want 1: %s", got, log.String())
	}
	if gotStarted, ok := conn.batch.values[0][4].(*time.Time); !ok || gotStarted != nil {
		t.Fatalf("appended started_at=%v want nil: lifecycle was not marked failed", conn.batch.values[0][4])
	}
}

func TestGitLabDeploymentsEffectsWriteCarriesPullRequestForwardOnFailure(t *testing.T) {
	claim := nativeTestClaim("gitlab", "deployments")
	priorNumber := uint32(9)
	row := deploymentEffectsUnitRow(claim, "812")
	row.PullRequestLookupFailed = true
	effect := deploymentEffectsUnitEffect(t, []deploymentRow{row})

	conn := &deploymentLifecycleGuardConn{queryRows: &deploymentLifecycleGuardRows{
		repoID: row.RepoID, deploymentID: row.DeploymentID, pullRequestNumber: &priorNumber,
	}}
	sink := GitLabDeploymentsClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
		t.Fatalf("write error=%v", err)
	}
	gotNumber, ok := conn.batch.values[0][8].(*uint32)
	if !ok || gotNumber == nil || *gotNumber != priorNumber {
		t.Fatalf("appended pull_request_number=%v want=%d", conn.batch.values[0][8], priorNumber)
	}
}

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
