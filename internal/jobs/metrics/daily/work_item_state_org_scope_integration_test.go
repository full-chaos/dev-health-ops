//go:build integration

package daily

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestWorkItemStateComputeFamilyIsolatesTenantsAndReadsPrimaryAttribution is
// CHAOS-4278's live-ClickHouse proof, run through the real production entry
// point (WorkItemStateExecutor.ComputeFamily), not a unit test of the
// aggregation logic in isolation:
//
//  1. Org-scoped write: a work_item_state partition for org A leaves
//     org-scoped rows behind in work_item_state_durations_daily.
//  2. Cross-tenant guard: two orgs, each with its own repo/work item/
//     transitions/attribution row, run in the same process. Org A's
//     org-scoped read must see ONLY org A's row, never org B's.
//  3. Team-attribution READ contract (CHAOS-4278's core design decision):
//     org A's work item has an is_primary=1 row in
//     work_item_team_attributions pointing at "team-a" -- the written
//     work_item_state_durations_daily row must carry that team_id, proving
//     ComputeFamily reads the attribution table rather than defaulting
//     every item to "unassigned".
//  4. Unattributed item defaults to "unassigned": org B's work item has NO
//     attribution row at all.
func TestWorkItemStateComputeFamilyIsolatesTenantsAndReadsPrimaryAttribution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer clickhouseInstance.Close(context.Background())
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(clickhouseInstance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for _, statement := range []string{
		`CREATE TABLE work_items (
    repo_id UUID, work_item_id String, provider String, status String,
    project_key String, project_id String, native_team_key String, project_name String,
    created_at DateTime64(3, 'UTC'), completed_at Nullable(DateTime64(3, 'UTC')),
    org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, work_item_id)`,
		`CREATE TABLE work_item_transitions (
    repo_id UUID, work_item_id String, occurred_at DateTime64(3, 'UTC'), provider String,
    from_status String, to_status String, from_status_raw String, to_status_raw String,
    actor String, org_id String, last_synced DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, work_item_id, occurred_at)`,
		`CREATE TABLE work_item_team_attributions (
    org_id String, repo_id UUID, work_item_id String, provider String,
    team_id Nullable(String), team_name Nullable(String),
    source Enum8('native_team' = 1, 'linked_issue' = 2, 'project_ownership' = 3, 'repo_ownership' = 4, 'assignee_membership' = 5, 'unassigned' = 6),
    is_primary UInt8, confidence Enum8('high' = 1, 'medium' = 2, 'low' = 3), evidence String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at) ORDER BY (org_id, repo_id, work_item_id, ifNull(team_id, ''), source)`,
		`CREATE TABLE work_item_state_durations_daily (
    day Date, provider String, work_scope_id String, team_id String, team_name String,
    status String, duration_hours Float64, items_touched UInt32, computed_at DateTime,
    avg_wip Float64, org_id String
) ENGINE = MergeTree PARTITION BY toYYYYMM(day) ORDER BY (provider, work_scope_id, team_id, status, day)`,
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	const (
		orgA = "00000000-0000-4000-8000-0000000000a0"
		orgB = "00000000-0000-4000-8000-0000000000b0"
	)
	repoA := "00000000-0000-4000-8000-0000000000a1"
	repoB := "00000000-0000-4000-8000-0000000000b1"
	targetDay := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

	if err := conn.Exec(ctx, `
INSERT INTO work_items (repo_id, work_item_id, provider, status, project_key, project_id, native_team_key, project_name, created_at, completed_at, org_id, last_synced) VALUES
(toUUID('`+repoA+`'), 'gh:a/repo#1', 'github', 'in_progress', '', 'a/repo', '', '', toDateTime64('2026-08-24 00:00:00', 3, 'UTC'), NULL, '`+orgA+`', now64(3)),
(toUUID('`+repoB+`'), 'gh:b/repo#1', 'github', 'in_progress', '', 'b/repo', '', '', toDateTime64('2026-08-24 00:00:00', 3, 'UTC'), NULL, '`+orgB+`', now64(3))`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced) VALUES
(toUUID('`+repoA+`'), 'gh:a/repo#1', toDateTime64('2026-08-24 06:00:00', 3, 'UTC'), 'github', 'todo', 'in_progress', 'Todo', 'In Progress', '', '`+orgA+`', now64(3)),
(toUUID('`+repoB+`'), 'gh:b/repo#1', toDateTime64('2026-08-24 06:00:00', 3, 'UTC'), 'github', 'todo', 'in_progress', 'Todo', 'In Progress', '', '`+orgB+`', now64(3))`); err != nil {
		t.Fatal(err)
	}
	// Only org A's item has a primary attribution row -- org B's item is
	// left unattributed to prove the "unassigned" default (point 4).
	if err := conn.Exec(ctx, `
INSERT INTO work_item_team_attributions (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES
('`+orgA+`', toUUID('`+repoA+`'), 'gh:a/repo#1', 'github', 'team-a', 'Team A', 'native_team', 1, 'high', 'test', now64(3))`); err != nil {
		t.Fatal(err)
	}

	executor, err := NewWorkItemStateExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}

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

	// Each item transitions todo -> in_progress, so it contributes TWO
	// distinct (provider, work_scope_id, team_id, status) rows for the day:
	// a "todo" segment (created_at -> the transition) and an "in_progress"
	// segment (the transition -> computed_at, since the item never
	// completes) -- see TestComputeWorkItemStateDurationsGolden for the
	// same multi-segment-per-item shape.
	assertWorkItemStateOrgScopedCount(ctx, t, conn, orgA, 2)
	assertWorkItemStateOrgScopedCount(ctx, t, conn, orgB, 2)
	assertWorkItemStateOrgScopedCount(ctx, t, conn, "", 0)

	assertWorkItemStateTeamID(ctx, t, conn, orgA, "team-a")
	assertWorkItemStateTeamID(ctx, t, conn, orgB, unassignedTeamID)
}

func assertWorkItemStateOrgScopedCount(ctx context.Context, t *testing.T, conn driver.Conn, orgID string, want int) {
	t.Helper()
	row := conn.QueryRow(ctx, "SELECT count() FROM work_item_state_durations_daily WHERE org_id = ?", orgID)
	var got uint64
	if err := row.Scan(&got); err != nil {
		t.Fatalf("org_id=%q: query row: %v", orgID, err)
	}
	if int(got) != want {
		t.Fatalf("count(org_id=%q) = %d, want %d", orgID, got, want)
	}
}

func assertWorkItemStateTeamID(ctx context.Context, t *testing.T, conn driver.Conn, orgID, wantTeamID string) {
	t.Helper()
	row := conn.QueryRow(ctx, "SELECT team_id FROM work_item_state_durations_daily WHERE org_id = ? AND status = 'in_progress'", orgID)
	var got string
	if err := row.Scan(&got); err != nil {
		t.Fatalf("org_id=%q: query row: %v", orgID, err)
	}
	if got != wantTeamID {
		t.Fatalf("org_id=%q team_id = %q, want %q", orgID, got, wantTeamID)
	}
}
