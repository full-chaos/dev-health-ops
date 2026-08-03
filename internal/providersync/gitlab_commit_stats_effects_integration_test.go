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

func TestGitLabCommitStatsReadbackSeparatesTenantsAndConvergesAfterRetry(t *testing.T) {
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
	sink := GitLabCommitStatsClickHouseEffects{
		Conn: conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
			return nil
		}),
	}
	claim := nativeTestClaim("gitlab", "commit-stats")
	otherClaim := claim
	otherClaim.OrgID += "-foreign"
	now := time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)
	row := commitStatsRow{
		OrgID: claim.OrgID, RepoID: "a6a5cafb-6680-a10a-9e41-a5ef763ca016",
		CommitHash: "abc123", FilePath: gitLabAggregateStatsMarker,
		Additions: 4, Deletions: 2, OldFileMode: "unknown", NewFileMode: "unknown",
		LastSynced: now,
	}
	otherRow := row
	otherRow.OrgID = otherClaim.OrgID
	otherRow.Additions = 9
	for _, write := range []struct {
		claim Claim
		row   commitStatsRow
	}{{claim, row}, {otherClaim, otherRow}, {claim, row}} {
		if err := sink.WriteEffect(ctx, write.claim, commitStatsEffect(t, write.row)); err != nil {
			t.Fatal(err)
		}
	}
	for _, inspect := range []struct {
		claim Claim
		row   commitStatsRow
	}{{claim, row}, {otherClaim, otherRow}} {
		got, err := sink.InspectEffect(ctx, inspect.claim, commitStatsEffect(t, inspect.row))
		if err != nil || got != EffectExact {
			t.Fatalf("tenant %s inspection=%s error=%v", inspect.claim.OrgID, got, err)
		}
	}
	var count uint64
	var additions int32
	if err := conn.QueryRow(ctx, `
SELECT count(), any(additions)
FROM git_commit_stats FINAL
WHERE org_id = ? AND repo_id = ? AND commit_hash = ? AND file_path = ?`,
		row.OrgID, row.RepoID, row.CommitHash, row.FilePath,
	).Scan(&count, &additions); err != nil {
		t.Fatal(err)
	}
	if count != 1 || additions != row.Additions {
		t.Fatalf("owning tenant count=%d additions=%d want one row %+v", count, additions, row)
	}
}
