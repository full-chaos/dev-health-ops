//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A re-synced deployment (deployed_at moved to another day, or status moved to
// canceled) keeps its old part until ClickHouse merges. With merges stopped,
// the loader and a recompute must count only the current version, per org.
func TestDeployLoaderReadsCurrentVersionOfResyncedDeployment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	chschema.CreateTables(ctx, t, conn, "deploy_metrics_daily", "deployments")
	if err := conn.Exec(ctx, `SYSTEM STOP MERGES deployments`); err != nil {
		t.Fatal(err)
	}

	const (
		orgA = "00000000-0000-4000-8000-0000000000a0"
		orgB = "00000000-0000-4000-8000-0000000000b0"
	)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000000a1")
	dayX := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	dayY := dayX.Add(24 * time.Hour)

	insert := func(org, id, status, deployedAt, synced string) {
		t.Helper()
		if err := conn.Exec(ctx, `INSERT INTO deployments (repo_id, deployment_id, status, deployed_at, org_id, last_synced) VALUES
(toUUID('`+repo.String()+`'), '`+id+`', '`+status+`', toDateTime64('`+deployedAt+`', 3, 'UTC'), '`+org+`', toDateTime64('`+synced+`', 3, 'UTC'))`); err != nil {
			t.Fatal(err)
		}
	}
	// moved: day X first, then day Y.
	insert(orgA, "moved", "success", "2026-08-24 12:00:00", "2026-08-24 13:00:00")
	insert(orgA, "moved", "success", "2026-08-25 12:00:00", "2026-08-25 13:00:00")
	// canceled: success on day X first, then canceled on day X.
	insert(orgA, "canceled", "success", "2026-08-24 15:00:00", "2026-08-24 16:00:00")
	insert(orgA, "canceled", "canceled", "2026-08-24 15:00:00", "2026-08-25 16:00:00")
	// stays: one version, day X.
	insert(orgA, "stays", "success", "2026-08-24 17:00:00", "2026-08-24 18:00:00")
	// other org, same repo and deployment id, own old version on day X only.
	insert(orgB, "moved", "success", "2026-08-24 12:00:00", "2026-08-24 13:00:00")

	var parts uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'deployments' AND active`).Scan(&parts); err != nil {
		t.Fatal(err)
	}
	if parts < 2 {
		t.Fatalf("active parts = %d, want >= 2 so the old versions stay unmerged", parts)
	}

	rowsFor := func(org string, day time.Time) int {
		t.Helper()
		rows, err := LoadDeployments(ctx, conn, org, []uuid.UUID{repo}, day, day.Add(24*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		return len(rows)
	}
	if got := rowsFor(orgA, dayX); got != 1 {
		t.Fatalf("org A day X: %d deployments, want 1 (only stays; moved is on day Y, canceled is excluded)", got)
	}
	if got := rowsFor(orgA, dayY); got != 1 {
		t.Fatalf("org A day Y: %d deployments, want 1 (moved)", got)
	}
	if got := rowsFor(orgB, dayX); got != 1 {
		t.Fatalf("org B day X: %d deployments, want 1 (its own moved row)", got)
	}

	executor, err := NewDeployExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: orgA, TargetDay: dayX}
	partition := Partition{
		ID: "00000000-0000-4000-8000-0000000000c1", RunID: "00000000-0000-4000-8000-0000000000c0",
		RepoIDs: []RepositoryID{RepositoryID(repo.String())},
	}
	if _, err := executor.ComputeFamily(ctx, run, partition); err != nil {
		t.Fatalf("recompute day X: %v", err)
	}
	var counted uint64
	if err := conn.QueryRow(ctx, `SELECT toUInt64(ifNull(sum(deployments_count), 0)) FROM deploy_metrics_daily FINAL WHERE org_id = ? AND repo_id = ? AND day = ?`,
		orgA, repo, dayX).Scan(&counted); err != nil {
		t.Fatal(err)
	}
	if counted != 1 {
		t.Fatalf("recomputed day X deployments_count = %d, want 1", counted)
	}
}
