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

const gitLabBlameIntegrationDDL = `
CREATE TABLE git_blame (
  repo_id UUID, path String, line_no UInt32,
  author_email Nullable(String), author_name Nullable(String),
  author_when Nullable(DateTime64(3, 'UTC')), commit_hash Nullable(String),
  line Nullable(String), last_synced DateTime64(3, 'UTC'), org_id String
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, repo_id, path, line_no)`

func TestGitLabBlameClickHouseReadbackAndCoverageAreTenantScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, gitLabBlameIntegrationDDL); err != nil {
		t.Fatal(err)
	}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitLabBlameClickHouseEffects{Conn: conn, Lease: lease}
	claimA := nativeTestClaim("gitlab", "blame")
	claimA.OrgID = "org-a"
	claimB := claimA
	claimB.OrgID = "org-b"
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	row := func(orgID, name, path string) gitBlameRow {
		email, hash, line := name+"@example.com", "abc123", "line"
		return gitBlameRow{
			RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: path, LineNo: 1,
			AuthorEmail: &email, AuthorName: &name, CommitHash: &hash, Line: &line,
			LastSynced: now, OrgID: orgID,
		}
	}
	rowA, rowB := row(claimA.OrgID, "Tenant A", "src/shared.go"), row(claimB.OrgID, "Tenant B", "src/shared.go")
	for _, pair := range []struct {
		claim Claim
		row   gitBlameRow
	}{
		{claimA, rowA}, {claimB, rowB},
	} {
		effect := gitLabBlameEffect(t, pair.row)
		if err := sink.WriteEffect(ctx, pair.claim, effect); err != nil {
			t.Fatal(err)
		}
		if got, err := sink.InspectEffect(ctx, pair.claim, effect); err != nil || got != EffectExact {
			t.Fatalf("tenant=%s inspection=%s error=%v", pair.claim.OrgID, got, err)
		}
	}
	coverage := GitLabBlameClickHouseCoverage{Conn: conn, Lease: lease}
	pathsA, err := coverage.BlamedPaths(ctx, claimA, rowA.RepoID)
	if err != nil {
		t.Fatal(err)
	}
	pathsB, err := coverage.BlamedPaths(ctx, claimB, rowB.RepoID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pathsA) != 1 || pathsA[0] != "src/shared.go" || len(pathsB) != 1 || pathsB[0] != "src/shared.go" {
		t.Fatalf("tenant paths A=%v B=%v", pathsA, pathsB)
	}
	var count uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM git_blame FINAL WHERE org_id = ?`, claimA.OrgID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("org-a rows=%d", count)
	}
}
