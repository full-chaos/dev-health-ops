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

const gitCommitStatsDDL = `
CREATE TABLE git_commit_stats (
  repo_id UUID,
  commit_hash String,
  file_path String,
  additions Int32,
  deletions Int32,
  old_file_mode String,
  new_file_mode String,
  last_synced DateTime64(3, 'UTC'),
  org_id String
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, repo_id, commit_hash, file_path)`

func TestGitHubCommitStatsReadbackResolvesWinningReplacingMergeTreeVersion(t *testing.T) {
	ctx, sink := newGitHubCommitStatsIntegrationSink(t)
	claim := nativeTestClaim("github", "commit-stats")
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	current := commitStatsRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
		CommitHash: "abc123", FilePath: "src/main.go", Additions: 4, Deletions: 2,
		OldFileMode: "unknown", NewFileMode: "unknown", LastSynced: now,
	}
	effect := commitStatsEffect(t, current)
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("empty inspection=%s error=%v", inspection, err)
	}
	previous := current
	previous.LastSynced = now.Add(-time.Hour)
	previous.Additions = 1
	if err := sink.WriteEffect(ctx, claim, commitStatsEffect(t, previous)); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("stale-only inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
		t.Fatalf("winning inspection=%s error=%v", inspection, err)
	}
}

func TestGitHubCommitStatsReadbackSelectsOwningTenantFromNaturalKeyCollision(t *testing.T) {
	ctx, sink := newGitHubCommitStatsIntegrationSink(t)
	claim := nativeTestClaim("github", "commit-stats")
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	row := commitStatsRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
		CommitHash: "abc123", FilePath: "src/main.go", Additions: 4, Deletions: 2,
		OldFileMode: "unknown", NewFileMode: "unknown", LastSynced: now,
	}
	otherClaim := claim
	otherClaim.OrgID = claim.OrgID + "-foreign"
	otherRow := row
	otherRow.OrgID = otherClaim.OrgID
	otherRow.Additions = 9
	if err := sink.WriteEffect(ctx, claim, commitStatsEffect(t, row)); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, otherClaim, commitStatsEffect(t, otherRow)); err != nil {
		t.Fatal(err)
	}
	var count uint64
	var minOrgID, maxOrgID string
	var minAdditions, maxAdditions int32
	if err := sink.Conn.QueryRow(ctx, `
SELECT count(), min(org_id), max(org_id), min(additions), max(additions)
FROM git_commit_stats FINAL
WHERE org_id = ? AND repo_id = ? AND commit_hash = ? AND file_path = ?`,
		row.OrgID, row.RepoID, row.CommitHash, row.FilePath,
	).Scan(&count, &minOrgID, &maxOrgID, &minAdditions, &maxAdditions); err != nil {
		t.Fatal(err)
	}
	if count != 1 || minOrgID != row.OrgID || maxOrgID != row.OrgID ||
		minAdditions != row.Additions || maxAdditions != row.Additions {
		t.Fatalf(
			"tenant-scoped readback returned count=%d orgs=(%q,%q) additions=(%d,%d), want only %+v",
			count, minOrgID, maxOrgID, minAdditions, maxAdditions, row,
		)
	}
	if inspection, err := sink.InspectEffect(ctx, claim, commitStatsEffect(t, row)); err != nil || inspection != EffectExact {
		t.Fatalf("tenant-scoped inspection=%s error=%v", inspection, err)
	}
	if inspection, err := sink.InspectEffect(ctx, otherClaim, commitStatsEffect(t, otherRow)); err != nil || inspection != EffectExact {
		t.Fatalf("foreign tenant inspection=%s error=%v", inspection, err)
	}
}

func newGitHubCommitStatsIntegrationSink(
	t *testing.T,
) (context.Context, GitHubCommitStatsClickHouseEffects) {
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
	if err := conn.Exec(ctx, gitCommitStatsDDL); err != nil {
		t.Fatal(err)
	}
	return ctx, GitHubCommitStatsClickHouseEffects{
		Conn: conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
			return nil
		}),
	}
}

func commitStatsEffect(t *testing.T, row commitStatsRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues(
		"git_commit_stats", EffectReadbackRequired, []commitStatsRow{row},
	)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
