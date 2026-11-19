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

// A work item done two days before the day and reopened the day before, whose provider kept its completed_at: on
// the day it is in progress for all 24 hours, in the org view and the scope view, and it has its blocked-hours row.
// The same history with a terminal status, done or canceled on the day, ends at its completed_at as before. An open
// item whose last known transition is terminal also ends at its completed_at.
func TestWorkItemStateKeepsAccruingAReopenedItemWithAKeptCompletedAt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
	chschema.CreateTables(ctx, t, conn,
		"work_items", "work_item_transitions", "work_item_team_attributions",
		"work_item_dependencies", "work_item_dependency_first_seen", "work_item_relations_read",
		"work_item_state_durations_daily", "work_item_blocked_durations_daily",
	)

	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	created := day.Add(-96 * time.Hour)
	started := day.Add(-72 * time.Hour)
	doneBefore := day.Add(-48 * time.Hour)
	reopened := day.Add(-24 * time.Hour)
	doneOnDay := day.Add(10 * time.Hour)
	synced := time.Now().UTC().Truncate(time.Second)

	for _, tc := range []struct {
		name, status string
		completedAt  time.Time
		history      [][3]any // occurred_at, from_status, to_status
		wantStatus   string
		wantHours    float64
		wantBlocked  uint64
	}{
		{"reopened_in_progress", "in_progress", doneBefore, [][3]any{
			{started, "todo", "in_progress"}, {doneBefore, "in_progress", "done"}, {reopened, "done", "in_progress"},
		}, "in_progress", 24, 1},
		{"done_on_day", "done", doneOnDay, [][3]any{
			{started, "todo", "in_progress"}, {doneOnDay, "in_progress", "done"},
		}, "in_progress", 10, 1},
		{"canceled_on_day", "canceled", doneOnDay, [][3]any{
			{started, "todo", "in_progress"}, {doneOnDay, "in_progress", "canceled"},
		}, "in_progress", 10, 1},
		// The reopen is not in the transitions: the last known transition is terminal, so the item ends at its
		// completed_at as before and gets no hours in that terminal status (ticket 14170, filed separately).
		{"reopen_missing_last_done", "in_progress", doneBefore, [][3]any{
			{started, "todo", "in_progress"}, {doneBefore, "in_progress", "done"},
		}, "done", 0, 0},
		// The reopen comes after the day: the day reads only the transitions before its end, so on the day the last
		// known transition is terminal and the item gets no hours in that terminal status.
		{"reopened_after_day", "in_progress", doneBefore, [][3]any{
			{started, "todo", "in_progress"}, {doneBefore, "in_progress", "done"}, {day.Add(30 * time.Hour), "done", "in_progress"},
		}, "done", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orgID := "org-14155-" + tc.name + "-" + uuid.NewString()
			repo := uuid.New()
			workItemID := "gh:a/repo#" + tc.name
			completedAt := tc.completedAt
			storeWorkItemCopy(t, ctx, conn, orgID, repo, workItemID, tc.status, "proj-a", created, &completedAt, synced)
			for _, transition := range tc.history {
				if err := conn.Exec(ctx, `
INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
VALUES (?, ?, ?, 'github', ?, ?, ?, ?, '', ?, ?)`,
					repo, workItemID, transition[0], transition[1], transition[2], transition[1], transition[2], orgID, synced,
				); err != nil {
					t.Fatal(err)
				}
			}

			executor, err := NewWorkItemStateExecutor(conn)
			if err != nil {
				t.Fatal(err)
			}
			executor.nowUTC = func() time.Time { return day.Add(36 * time.Hour) }
			run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
			if _, err := executor.ComputeFamily(ctx, run, Partition{ID: "p", RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}); err != nil {
				t.Fatal(err)
			}

			var orgHours, scopeHours float64
			if err := conn.QueryRow(ctx, `
SELECT sumIf(duration_hours, row_view = 'org'), sumIf(duration_hours, row_view = 'scope')
FROM work_item_state_durations_daily
WHERE org_id = ? AND day = ? AND status = ?`, orgID, day, tc.wantStatus,
			).Scan(&orgHours, &scopeHours); err != nil {
				t.Fatalf("read work_item_state_durations_daily: %v", err)
			}
			var otherRows, blockedRows uint64
			if err := conn.QueryRow(ctx, `
SELECT count() FROM work_item_state_durations_daily WHERE org_id = ? AND day = ? AND status != ?`, orgID, day, tc.wantStatus,
			).Scan(&otherRows); err != nil {
				t.Fatalf("read other statuses: %v", err)
			}
			if err := conn.QueryRow(ctx, `
SELECT count() FROM work_item_blocked_durations_daily FINAL WHERE org_id = ? AND day = ? AND work_item_id = ?`, orgID, day, workItemID,
			).Scan(&blockedRows); err != nil {
				t.Fatalf("read work_item_blocked_durations_daily: %v", err)
			}
			if orgHours != tc.wantHours || scopeHours != tc.wantHours || otherRows != 0 || blockedRows != tc.wantBlocked {
				t.Errorf("status %s, completed_at %s, day %s: %s hours org %v, scope %v; rows of other statuses %d; blocked rows %d; want %v hours in each view, 0 other rows, %d blocked rows",
					tc.status, tc.completedAt.Format(time.RFC3339), day.Format("2006-01-02"), tc.wantStatus,
					orgHours, scopeHours, otherRows, blockedRows, tc.wantHours, tc.wantBlocked)
			}
		})
	}
}
