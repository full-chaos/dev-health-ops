//go:build integration

package home

import (
	"context"
	"fmt"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
)

// A work item stored under two repository ids counts in the request-time read of a repository's linked items on its
// current copy (the newest last_synced over every copy; on a tie, the lower repository id), as the daily work item
// tables decide: the request-time read and the daily tables agree, day by day and in the served metrics.

type currentCopyRow struct {
	repo, status, synced, completed string
}

type currentCopyWorld struct {
	name, item string
	copies     []currentCopyRow
}

func seedCurrentCopyWorld(ctx context.Context, t *testing.T, admin stdclickhouse.Conn, org string, world currentCopyWorld) {
	t.Helper()
	repoLinkedSeedRepos(ctx, t, admin, org)
	for _, stored := range world.copies {
		if err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO work_items (repo_id, work_item_id, provider, status, type, assignees, story_points, project_key, project_id, native_team_key, project_name, created_at, started_at, completed_at, org_id, last_synced)
VALUES (toUUID('%s'), '%s', 'github', '%s', 'task', ['ann'], 1, '', 'a/r', '', '', %s, %s, %s, '%s', %s)`,
			stored.repo, world.item, stored.status, oracleTS("2026-08-12 09:00:00"), oracleTS("2026-08-13 09:00:00"),
			oracleTS(stored.completed), org, oracleTS(stored.synced))); err != nil {
			t.Fatal(err)
		}
	}
	if err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)
VALUES (toUUID('%s'), '%s', 1, 1.0, 'native', '', %s, '%s')`, oracleR1, world.item, oracleTS("2026-08-26 00:00:00"), org)); err != nil {
		t.Fatal(err)
	}
}

type currentCopySums struct {
	completed, wip float64
	rows           int
}

func TestRepoLinkedItemCountsOnItsCurrentCopyAsTheDailyTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	admin, client := newHomeTestClickHouse(ctx, t)

	from, to := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	start, end := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	older, newer := "2026-08-25 20:00:00", "2026-08-25 22:00:00"
	worlds := []currentCopyWorld{
		{"newest copy done with no completed_at", "gh:a/r#21", []currentCopyRow{
			{oracleR1, "done", older, "2026-08-20 10:00:00"}, {oracleR2, "done", newer, ""}}},
		{"newest copy done on an earlier day", "gh:a/r#22", []currentCopyRow{
			{oracleR1, "done", older, "2026-08-20 10:00:00"}, {oracleR2, "done", newer, "2026-08-18 10:00:00"}}},
		{"tie on last_synced, the lower repository id is current", "gh:a/r#23", []currentCopyRow{
			{oracleR1, "done", newer, ""}, {oracleR2, "done", newer, "2026-08-20 10:00:00"}}},
		{"single copy", "gh:a/r#24", []currentCopyRow{
			{oracleR1, "done", newer, "2026-08-20 10:00:00"}}},
		{"newest copy done before the window", "gh:a/r#25", []currentCopyRow{
			{oracleR1, "done", older, "2026-08-20 10:00:00"}, {oracleR2, "done", newer, "2026-08-05 10:00:00"}}},
		{"newest copy open, an older copy done in the window", "gh:a/r#26", []currentCopyRow{
			{oracleR1, "done", older, "2026-08-20 10:00:00"}, {oracleR2, "in_progress", newer, ""}}},
	}
	for index, world := range worlds {
		t.Run(world.name, func(t *testing.T) {
			org := fmt.Sprintf("current-copy-%d", index)
			seedCurrentCopyWorld(ctx, t, admin, org, world)
			fillDailyTables(ctx, t, admin, org, from, to)

			var table currentCopySums
			var tableRows uint64
			if err := admin.QueryRow(ctx, `
SELECT toFloat64(sum(items_completed)), toFloat64(sum(wip_count_end_of_day)), count()
FROM work_item_metrics_daily FINAL
WHERE org_id = ? AND row_view = 'org'`, org).Scan(&table.completed, &table.wip, &tableRows); err != nil {
				t.Fatal(err)
			}
			table.rows = int(tableRows)

			loader := linkeditems.Loader{Client: client, OrgID: org, RepoIDs: []string{oracleR1}, WindowStart: from, WindowEnd: to}
			versions, attributions, err := loader.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var request currentCopySums
			for _, rows := range linkeditems.RowsByDay(from, to, versions, attributions) {
				for _, row := range rows {
					request.completed += float64(row.ItemsCompleted)
					request.wip += float64(row.WIPCountEndOfDay)
					request.rows++
				}
			}
			t.Logf("completed/wip/rows: request-time %v/%v/%d, daily table %v/%v/%d",
				request.completed, request.wip, request.rows, table.completed, table.wip, table.rows)
			if request != table {
				t.Errorf("request-time completed/wip/rows %+v, daily table %+v", request, table)
			}

			for _, name := range []string{"throughput", "cycle_time", "wip_saturation"} {
				spec := metricSpecByName(t, name)
				linked, err := computeMetricDelta(ctx, client, spec, start, end, from, start,
					Filters{What: WhatFilter{Repos: []string{oracleR1}}}, org, end)
				if err != nil {
					t.Fatal(err)
				}
				daily, err := computeMetricDelta(ctx, client, spec, start, end, from, start, Filters{}, org, end)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("%s: request-time %v (has data %v), daily tables %v (has data %v)", name, linked.Value, linked.HasData, daily.Value, daily.HasData)
				sameDelta(t, name, linked, daily)
			}
		})
	}
}
