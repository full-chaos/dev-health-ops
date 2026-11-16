//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func TestDeploymentsNonFiniteConfidenceStoresUnreadableValueRuleAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newDeploymentsIntegrationConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	glClaim := nativeTestClaim("gitlab", "deployments")
	doer := &gitLabDeploymentsDoer{t: t, responses: []gitLabDeploymentsResponse{
		{body: gitLabRepositoryFixture}, {body: `[]`},
		{body: gitLabConfidenceBody(`"release_ref":"v1","release_ref_confidence":"NaN"`)},
	}}
	glBatch, err := (GitLabDeploymentsRouteHandler{}).Collect(ctx, glClaim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"), time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	ghClaim := nativeTestClaim("github", "deployments")
	ghBatch, err := (GitHubDeploymentsRouteHandler{}).Collect(ctx, ghClaim, providerfoundation.Credential{},
		gitHubRepositoryClient(t, fakehttp.Client(gitHubConfidenceDoer{bad: `"NaN"`}), "https://api.github.com"), time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	sinks := []struct {
		claim Claim
		batch CompleteRouteBatch
		write func(Claim, EffectBatch) error
	}{
		{glClaim, glBatch, func(c Claim, e EffectBatch) error {
			return GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: lease}.WriteEffect(ctx, c, e)
		}},
		{ghClaim, ghBatch, func(c Claim, e EffectBatch) error {
			return GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: lease}.WriteEffect(ctx, c, e)
		}},
	}
	for _, sink := range sinks {
		if err := sink.write(sink.claim, sink.batch.Effects[0]); err != nil {
			t.Fatalf("%s: %v", sink.claim.Provider, err)
		}
		var rows, bad uint64
		if err := conn.QueryRow(ctx, `SELECT count(), countIf(deployment_id = '602' AND release_ref_confidence = 1) FROM deployments FINAL WHERE org_id = ? AND repo_id = ?`,
			sink.claim.OrgID, confidenceRows0RepoID(t, sink.batch)).Scan(&rows, &bad); err != nil {
			t.Fatal(err)
		}
		if rows != 3 || bad != 1 {
			t.Fatalf("%s: stored rows=%d NaN-confidence row with value 1=%d, want 3 and 1", sink.claim.Provider, rows, bad)
		}
	}
}

func confidenceRows0RepoID(t *testing.T, batch CompleteRouteBatch) string {
	t.Helper()
	var row deploymentRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	return row.RepoID
}
