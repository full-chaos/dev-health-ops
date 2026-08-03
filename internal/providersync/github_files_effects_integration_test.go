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

const gitFilesDDL = `
CREATE TABLE git_files (
  repo_id UUID,
  path String,
  executable Bool,
  contents Nullable(String),
  last_synced DateTime64(3, 'UTC'),
  org_id String
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, repo_id, path)`

func TestGitHubFilesReadbackResolvesWinningReplacingMergeTreeVersion(t *testing.T) {
	ctx, sink := newGitHubFilesIntegrationSink(t)
	claim := nativeTestClaim("github", "files")
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	current := gitFileRow{RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go", LastSynced: now, OrgID: claim.OrgID}
	currentContent := "package main\n"
	current.Contents = &currentContent
	previous := current
	previous.LastSynced = now.Add(-time.Hour)
	previousContent := "package old"
	previous.Contents = &previousContent
	if err := sink.WriteEffect(ctx, claim, gitFileEffect(t, previous)); err != nil {
		t.Fatal(err)
	}
	inspection, err := sink.InspectEffect(ctx, claim, gitFileEffect(t, current))
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("stale-only inspection=%s error=%v", inspection, err)
	}
	if err := sink.WriteEffect(ctx, claim, gitFileEffect(t, current)); err != nil {
		t.Fatal(err)
	}
	inspection, err = sink.InspectEffect(ctx, claim, gitFileEffect(t, current))
	if err != nil || inspection != EffectExact {
		t.Fatalf("winning inspection=%s error=%v", inspection, err)
	}
}

func TestGitHubFilesReadbackReturnsClaimTenantForSameNaturalKey(t *testing.T) {
	ctx, sink := newGitHubFilesIntegrationSink(t)
	claim := nativeTestClaim("github", "files")
	claim.OrgID = "org-a"
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	row := gitFileRow{RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go", LastSynced: now, OrgID: claim.OrgID}
	claimContent := "package orga\n"
	row.Contents = &claimContent
	otherClaim := claim
	otherClaim.OrgID = "org-b"
	otherRow := row
	otherRow.OrgID = otherClaim.OrgID
	otherContent := "package orgb\n"
	otherRow.Contents = &otherContent
	if err := sink.WriteEffect(ctx, otherClaim, gitFileEffect(t, otherRow)); err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, claim, gitFileEffect(t, row)); err != nil {
		t.Fatal(err)
	}
	var count uint64
	var minOrgID, maxOrgID, minContents, maxContents string
	if err := sink.Conn.QueryRow(ctx, `
SELECT count(), min(org_id), max(org_id), min(ifNull(contents, '')), max(ifNull(contents, ''))
FROM git_files FINAL
WHERE org_id = ? AND repo_id = ? AND path = ?`,
		row.OrgID, row.RepoID, row.Path,
	).Scan(&count, &minOrgID, &maxOrgID, &minContents, &maxContents); err != nil {
		t.Fatal(err)
	}
	if count != 1 || minOrgID != row.OrgID || maxOrgID != row.OrgID ||
		minContents != claimContent || maxContents != claimContent {
		t.Fatalf(
			"tenant-scoped readback returned count=%d orgs=(%q,%q) contents=(%q,%q), want only %+v",
			count, minOrgID, maxOrgID, minContents, maxContents, row,
		)
	}
	inspection, err := sink.InspectEffect(ctx, claim, gitFileEffect(t, row))
	if err != nil || inspection != EffectExact {
		t.Fatalf("claim tenant must win same-key readback, inspection=%s error=%v", inspection, err)
	}
	inspection, err = sink.InspectEffect(ctx, otherClaim, gitFileEffect(t, otherRow))
	if err != nil || inspection != EffectExact {
		t.Fatalf("other tenant must read its own same-key row, inspection=%s error=%v", inspection, err)
	}
}

func newGitHubFilesIntegrationSink(t *testing.T) (context.Context, GitHubFilesClickHouseEffects) {
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
	if err := conn.Exec(ctx, gitFilesDDL); err != nil {
		t.Fatal(err)
	}
	return ctx, GitHubFilesClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
}

func gitFileEffect(t *testing.T, row gitFileRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("git_files", EffectReadbackRequired, []gitFileRow{row})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
