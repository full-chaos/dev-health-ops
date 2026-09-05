//go:build integration

package icfinalize

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// CHAOS-5151. Pins the defect class that made the native ic_finalize executor
// fail EVERY River attempt in the metrics-executed-proof E2E: loadGitMetrics,
// loadWorkItemMetrics and LoadRollingStats scanned ClickHouse UInt32/UInt64
// columns (declared as such in 001_metrics_v2.sql and 005_ic_metrics.sql)
// straight into Go int64/float64 destinations. clickhouse-go's Scan refuses
// that outright ("converting UInt32 to *int64 is unsupported"), so every
// finalize attempt failed on its very first readback, before it ever reached
// a write -- and PR1's no-fail-open policy turned that into a permanent
// redrive loop.
//
// This was invisible under a hand-typed test DDL (fixed separately, this
// package's redrive_idempotency_integration_test.go) because the type of a
// hand-typed column is whatever the author typed, not what ClickHouse's own
// aggregate/column-promotion rules actually produce. Only the real, migrated
// schema exercises the actual types: sum() over UInt32 promotes to UInt64,
// max() over UInt32 stays UInt32, and the base columns are UInt32 outright.
// A test asserting only "no error" against a hand-typed copy would have
// stayed green through this regressing -- it has to run against
// chschema.Apply's real schema to mean anything.
func TestLoaderScansAgreeWithTheRealSchemaColumnTypes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const orgID = "00000000-0000-4000-8000-000000000701"
	repoID := uuid.MustParse("8f5c1f2e-6b4a-4a1e-9f0c-2f2a2d6d5a11")
	day := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)

	if err := conn.Exec(ctx, `INSERT INTO user_metrics_daily
        (repo_id, day, author_email, identity_id, team_id, loc_added, loc_deleted,
         prs_authored, prs_merged, median_pr_cycle_hours, pr_cycle_p90_hours,
         computed_at, org_id)
        VALUES (?, ?, 'scan-types@example.com', 'scan-types@example.com', 'team-a',
                40, 10, 3, 2, 6.5, 12.0, ?, ?)`,
		repoID, day, day, orgID); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO work_item_user_metrics_daily
        (day, provider, work_scope_id, user_identity, team_id, team_name,
         items_started, items_completed, wip_count_end_of_day,
         cycle_time_p50_hours, cycle_time_p90_hours, computed_at, org_id)
        VALUES (?, 'synthetic', 'scope-1', 'scan-types@example.com', 'team-a',
                'Team A', 4, 3, 2, 5.0, 9.0, ?, ?)`,
		day, day, orgID); err != nil {
		t.Fatal(err)
	}

	executor := NewExecutor(conn)

	// The bug fires on the SCAN, not on any assertion about values -- a query
	// error here means the type mapping is wrong again.
	gitMetrics, err := executor.loadGitMetrics(ctx, orgID, day)
	if err != nil {
		t.Fatalf("loadGitMetrics: %v", err)
	}
	if len(gitMetrics) != 1 {
		t.Fatalf("loadGitMetrics: got %d rows, want 1", len(gitMetrics))
	}
	if got := gitMetrics[0].RepoID; got != repoID {
		t.Errorf("loadGitMetrics: RepoID = %s, want the seeded row's own %s "+
			"(CHAOS-5151's second defect: a git-backed row must keep its real "+
			"repo_id, never a placeholder)", got, repoID)
	}
	if got := gitMetrics[0].LOCAdded + gitMetrics[0].LOCDeleted; got != 50 {
		t.Errorf("loadGitMetrics: LOCAdded+LOCDeleted = %d, want 50 (40+10) -- "+
			"a wrong-width scan destination can silently truncate as well as error",
			got)
	}

	workItems, err := executor.loadWorkItemMetrics(ctx, orgID, day)
	if err != nil {
		t.Fatalf("loadWorkItemMetrics: %v", err)
	}
	if len(workItems) != 1 || workItems[0].ItemsCompleted != 3 {
		t.Fatalf("loadWorkItemMetrics: got %+v, want one row with ItemsCompleted=3", workItems)
	}

	// LoadRollingStats aggregates with sum()/max(), which promotes UInt32 to
	// UInt64 (sum) or keeps it UInt32 (max) -- a different failure point from
	// the two loaders above, and the one that surfaced second in practice.
	stats, err := LoadRollingStats(ctx, conn, orgID, day)
	if err != nil {
		t.Fatalf("LoadRollingStats: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("LoadRollingStats: got %d rows, want 1", len(stats))
	}
	if stats[0].ChurnLOC30d != 50 {
		t.Errorf("LoadRollingStats: ChurnLOC30d = %v, want 50", stats[0].ChurnLOC30d)
	}
}
