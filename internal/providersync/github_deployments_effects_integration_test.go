//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const deploymentsDDL = `
CREATE TABLE deployments (
  repo_id UUID, deployment_id String, status Nullable(String), environment Nullable(String),
  started_at Nullable(DateTime64(3, 'UTC')), finished_at Nullable(DateTime64(3, 'UTC')),
  deployed_at Nullable(DateTime64(3, 'UTC')), merged_at Nullable(DateTime64(3, 'UTC')),
  pull_request_number Nullable(Int32), release_ref String, release_ref_confidence Float64,
  org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, deployment_id)`

func TestGitHubDeploymentsReadbackResolvesWinningReplacingMergeTreeVersion(t *testing.T) {
	ctx, sink := newGitHubDeploymentsIntegrationSink(t)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "deployments")
	current := deploymentRow{OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", DeploymentID: "101", DeployedAt: pointerTo(now.Add(-time.Minute)), ReleaseRef: "v1", ReleaseRefConfidence: 1, LastSynced: now}
	previous := current
	previous.LastSynced = now.Add(-time.Hour)
	previous.ReleaseRef = "old"
	if err := sink.WriteEffect(ctx, claim, deploymentEffect(t, previous)); err != nil {
		t.Fatal(err)
	}
	inspection, err := sink.InspectEffect(ctx, claim, deploymentEffect(t, current))
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("stale inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, deploymentEffect(t, current)); err != nil {
		t.Fatal(err)
	}
	inspection, err = sink.InspectEffect(ctx, claim, deploymentEffect(t, current))
	if err != nil || inspection != EffectExact {
		t.Fatalf("winning inspection=%s error=%v", inspection, err)
	}
}

func TestGitHubDeploymentsReadbackExcludesSameNaturalKeyFromOtherTenant(t *testing.T) {
	ctx, sink := newGitHubDeploymentsIntegrationSink(t)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "deployments")
	row := deploymentRow{OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", DeploymentID: "101", DeployedAt: pointerTo(now), ReleaseRef: "v1", ReleaseRefConfidence: 1, LastSynced: now}
	otherClaim := claim
	otherClaim.OrgID = "other-org"
	otherRow := row
	otherRow.OrgID = otherClaim.OrgID
	otherRow.ReleaseRef = "other"
	if err := sink.WriteEffect(ctx, otherClaim, deploymentEffect(t, otherRow)); err != nil {
		t.Fatal(err)
	}
	inspection, err := sink.InspectEffect(ctx, claim, deploymentEffect(t, row))
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign-only inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, deploymentEffect(t, row)); err != nil {
		t.Fatal(err)
	}
	inspection, err = sink.InspectEffect(ctx, claim, deploymentEffect(t, row))
	if err != nil || inspection != EffectExact {
		t.Fatalf("tenant-scoped inspection=%s error=%v", inspection, err)
	}
}

func newGitHubDeploymentsIntegrationSink(t *testing.T) (context.Context, GitHubDeploymentsClickHouseEffects) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, deploymentsDDL); err != nil {
		t.Fatal(err)
	}
	return ctx, GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
}

func deploymentEffect(t *testing.T, row deploymentRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("deployments", EffectReadbackRequired, []deploymentRow{row})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
func pointerTo(value time.Time) *time.Time { return &value }
