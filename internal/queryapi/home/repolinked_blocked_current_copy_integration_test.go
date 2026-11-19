//go:build integration

package home

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// blocked_work under a repository filter reads the per-item blocked snapshots of the work_item_state family. An item
// whose current copy (the newest copy) completed before the window is not an item of the window's days, as the
// work_item family has it: an older open, blocked copy and the snapshots an earlier compute stored from it give no
// blocked hours.
func TestRepoLinkedBlockedWorkLeavesAnItemWhoseCurrentCopyIsDone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)
	const org = "oracle-blocked-current-copy"
	repoLinkedSeedRepos(ctx, t, admin, org)
	insert := func(repo, status, completed, synced string) {
		t.Helper()
		if err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO work_items (repo_id, work_item_id, provider, status, type, project_id, created_at, started_at, completed_at, org_id, last_synced)
VALUES (toUUID('%s'), 'gh:a/r#1', 'github', '%s', 'task', 'a/r', %s, %s, %s, '%s', %s)`,
			repo, status, oracleTS("2026-08-01 09:00:00"), oracleTS("2026-08-02 09:00:00"), oracleTS(completed), org, oracleTS(synced))); err != nil {
			t.Fatal(err)
		}
	}
	insert(oracleR1, "blocked", "", "2026-08-20 00:00:00")
	// The current copy: newer, completed before the window.
	insert(oracleR2, "done", "2026-08-12 10:00:00", "2026-08-26 00:00:00")
	synced := oracleTS("2026-08-26 00:00:00")
	for _, statement := range []string{
		fmt.Sprintf(`INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
VALUES (toUUID('%s'), 'gh:a/r#1', %s, 'github', 'todo', 'blocked', 'todo', 'blocked', '', '%s', %s)`, oracleR1, oracleTS("2026-08-05 09:00:00"), org, synced),
		fmt.Sprintf(`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
VALUES (toUUID('%s'), 'gh:a/r#1', 1, 1.0, 'native', '', %s, '%s')`, oracleR1, synced, org),
	} {
		if err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	start, end := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	// The snapshots an earlier compute stored from the older copy, one for each day of the window.
	for day := start; day.Before(end); day = day.AddDate(0, 0, 1) {
		if err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO work_item_blocked_durations_daily (day, provider, work_scope_id, team_id, team_name, work_item_id, duration_hours, org_id, computed_at)
VALUES (toDate('%s'), 'github', 'a/r', 'unassigned', 'Unassigned', 'gh:a/r#1', 24, '%s', %s)`, day.Format("2006-01-02"), org, synced)); err != nil {
			t.Fatal(err)
		}
	}
	fillDailyTables(ctx, t, admin, org, start, end)
	got, err := computeMetricDelta(ctx, client, metricSpecByName(t, "blocked_work"), start, end, time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC), start,
		Filters{What: WhatFilter{Repos: []string{oracleR1}}}, org, end)
	if err != nil {
		t.Fatal(err)
	}
	if got.HasData || got.Value != 0 {
		t.Errorf("blocked_work = %v (has data %v), want no data: the item's current copy completed before the window", got.Value, got.HasData)
	}
}
