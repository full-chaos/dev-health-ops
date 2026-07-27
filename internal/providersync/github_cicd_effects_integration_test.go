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

const ciPipelineRunsDDL = `
CREATE TABLE ci_pipeline_runs (
  org_id String,
  repo_id UUID,
  run_id String,
  status Nullable(String),
  queued_at Nullable(DateTime64(3, 'UTC')),
  started_at DateTime64(3, 'UTC'),
  finished_at Nullable(DateTime64(3, 'UTC')),
  retry_count UInt32 DEFAULT 0,
  last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, repo_id, run_id)`

func TestGitHubCICDReadbackResolvesWinningReplacingMergeTreeVersion(t *testing.T) {
	ctx, sink := newGitHubCICDIntegrationSink(t)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "cicd")
	current := ciPipelineRunRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "101",
		StartedAt: now.Add(-time.Minute), RetryCount: 2, LastSynced: now,
	}
	effect := ciPipelineRunEffect(t, current)

	inspection, err := sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("empty table inspection=%s error=%v", inspection, err)
	}
	previous := current
	previous.LastSynced = now.Add(-time.Hour)
	previous.RetryCount = 1
	if err := sink.WriteEffect(ctx, claim, ciPipelineRunEffect(t, previous)); err != nil {
		t.Fatal(err)
	}
	inspection, err = sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("stale-only inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	inspection, err = sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("winning inspection=%s error=%v", inspection, err)
	}
}

func TestGitHubCICDReadbackExcludesSameRunFromOtherTenant(t *testing.T) {
	ctx, sink := newGitHubCICDIntegrationSink(t)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "cicd")
	row := ciPipelineRunRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "101",
		StartedAt: now.Add(-time.Minute), RetryCount: 2, LastSynced: now,
	}
	otherClaim := claim
	otherClaim.OrgID = "other-org"
	otherRow := row
	otherRow.OrgID = otherClaim.OrgID
	otherRow.RetryCount = 3
	if err := sink.WriteEffect(ctx, otherClaim, ciPipelineRunEffect(t, otherRow)); err != nil {
		t.Fatal(err)
	}

	effect := ciPipelineRunEffect(t, row)
	inspection, err := sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign-only inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	inspection, err = sink.InspectEffect(ctx, claim, effect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("tenant-scoped inspection=%s error=%v", inspection, err)
	}
}

func newGitHubCICDIntegrationSink(
	t *testing.T,
) (context.Context, GitHubCICDClickHouseEffects) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, ciPipelineRunsDDL); err != nil {
		t.Fatal(err)
	}
	return ctx, GitHubCICDClickHouseEffects{
		Conn: conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
			return nil
		}),
	}
}

func ciPipelineRunEffect(t *testing.T, row ciPipelineRunRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues(
		"ci_pipeline_runs", EffectReadbackRequired, []ciPipelineRunRow{row},
	)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
