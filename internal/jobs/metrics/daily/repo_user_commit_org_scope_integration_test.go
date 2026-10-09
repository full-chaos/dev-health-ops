//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/opfixture"
)

// TestComputeFamilyWritesRealOrgIDAndIsolatesTenants is CHAOS-4341's live-
// ClickHouse proof, run through the real production entry point
// (RepoUserCommitExecutor.ComputeFamily), not a unit test of the writer in
// isolation:
//
//  1. Red-on-baseline shape: a repo_user_commit partition for org A must
//     leave org-scoped rows behind -- `SELECT count() FROM repo_metrics_daily
//     WHERE org_id = <org A>` > 0. Before this ticket's fix, the writer
//     hard-coded org_id="", so this assertion fails against unfixed code
//     even though the partition itself "succeeds" (matching the exact prod
//     shape: 580/580 partitions succeeded, 0 org-scoped rows -- CHAOS-4341,
//     deploy 5.3 readback #2).
//  2. Cross-tenant guard: two orgs, each with its own repo, run in the same
//     process. Org A's org-scoped read must see ONLY org A's row (never org
//     B's, and never a stray "" row), and vice versa.
func TestComputeFamilyWritesRealOrgIDAndIsolatesTenants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// The schema is the migrated head (opfixture.Start applies the real chain):
	// the executor also reads deployments, the operational incident tables and
	// the deployment-incident links for change failure rate (CHAOS-8981), so a
	// hand-written subset of tables no longer describes what it runs against.
	_, conn := opfixture.Start(ctx, t)

	const (
		orgA = "00000000-0000-4000-8000-0000000000a0"
		orgB = "00000000-0000-4000-8000-0000000000b0"
	)
	repoA := "00000000-0000-4000-8000-0000000000a1"
	repoB := "00000000-0000-4000-8000-0000000000b1"

	if err := conn.Exec(ctx, `
INSERT INTO git_commits (repo_id, hash, author_name, author_email, committer_when, org_id, last_synced) VALUES
(toUUID('`+repoA+`'), 'a1', 'Dev A', 'dev-a@example.com', toDateTime64('2026-08-24 12:00:00', 3, 'UTC'), '`+orgA+`', now64(3)),
(toUUID('`+repoB+`'), 'b1', 'Dev B', 'dev-b@example.com', toDateTime64('2026-08-24 12:00:00', 3, 'UTC'), '`+orgB+`', now64(3))`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewRepoUserCommitExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

	runA := Run{OrganizationID: orgA, TargetDay: targetDay}
	partitionA := Partition{
		ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
		RepoIDs: []RepositoryID{RepositoryID(repoA)},
	}
	if _, err := executor.ComputeFamily(ctx, runA, partitionA); err != nil {
		t.Fatalf("org A partition: %v", err)
	}

	runB := Run{OrganizationID: orgB, TargetDay: targetDay}
	partitionB := Partition{
		ID: "00000000-0000-4000-8000-0000000000c3", RunID: "00000000-0000-4000-8000-0000000000c2",
		RepoIDs: []RepositoryID{RepositoryID(repoB)},
	}
	if _, err := executor.ComputeFamily(ctx, runB, partitionB); err != nil {
		t.Fatalf("org B partition: %v", err)
	}

	for _, table := range []string{"repo_metrics_daily", "user_metrics_daily", "commit_metrics"} {
		// Point 1: red-on-baseline -- org-scoped read must see org A's row.
		assertOrgScopedCount(ctx, t, conn, table, orgA, 1)
		// Point 2: cross-tenant guard -- org A's read must NOT see org B's
		// row, org B's read must NOT see org A's, and neither org's read may
		// pick up a stray org_id="" row (the exact pre-fix shape).
		assertOrgScopedCount(ctx, t, conn, table, orgB, 1)
		assertOrgScopedCount(ctx, t, conn, table, "", 0)
	}
}

func assertOrgScopedCount(ctx context.Context, t *testing.T, conn driver.Conn, table, orgID string, want int) {
	t.Helper()
	row := conn.QueryRow(ctx, "SELECT count() FROM "+table+" WHERE org_id = ?", orgID)
	var got uint64
	if err := row.Scan(&got); err != nil {
		t.Fatalf("%s org_id=%q: query row: %v", table, orgID, err)
	}
	if int(got) != want {
		t.Fatalf("%s: count(org_id=%q) = %d, want %d", table, orgID, got, want)
	}
}
