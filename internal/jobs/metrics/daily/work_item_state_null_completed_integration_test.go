//go:build integration

package daily

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// A terminal item with no completed_at (its status or its last known transition is done or canceled) gets the same
// state hours on a day from two compute runs, in the org view and the scope view, and no hours in a terminal status
// after its last transition. An open item whose last transition is open still runs to the compute time.
func TestWorkItemStateEndsATerminalItemWithNullCompletedAtAtItsLastTransition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
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
	onDay := day.Add(6 * time.Hour)
	beforeDay := day.Add(-48 * time.Hour)
	duringDay := []time.Time{day.Add(12 * time.Hour), day.Add(18 * time.Hour)}
	afterDay := []time.Time{day.Add(36 * time.Hour), day.Add(60 * time.Hour)}
	synced := time.Now().UTC().Truncate(time.Second)

	for _, tc := range []struct {
		name, status string
		history      [][3]any // occurred_at, from_status, to_status
		computedAt   []time.Time
		want         []map[string]float64 // status -> hours on the day, one map for each compute time
	}{
		{"canceled_on_day", "canceled", [][3]any{
			{started, "todo", "in_progress"}, {onDay, "in_progress", "canceled"},
		}, duringDay, []map[string]float64{{"in_progress": 6}, {"in_progress": 6}}},
		{"open_last_done_on_day", "in_progress", [][3]any{
			{started, "todo", "in_progress"}, {onDay, "in_progress", "done"},
		}, duringDay, []map[string]float64{{"in_progress": 6}, {"in_progress": 6}}},
		{"canceled_before_day", "canceled", [][3]any{
			{started, "todo", "in_progress"}, {beforeDay, "in_progress", "canceled"},
		}, afterDay, []map[string]float64{{}, {}}},
		{"open_last_canceled_before_day", "in_progress", [][3]any{
			{started, "todo", "in_progress"}, {beforeDay, "in_progress", "canceled"},
		}, afterDay, []map[string]float64{{}, {}}},
		{"canceled_closing_transition_missing", "canceled", [][3]any{
			{onDay, "todo", "in_progress"},
		}, duringDay, []map[string]float64{{"todo": 6}, {"todo": 6}}},
		{"open_control", "in_progress", [][3]any{
			{started, "todo", "in_progress"},
		}, duringDay, []map[string]float64{{"in_progress": 12}, {"in_progress": 18}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i, computedAt := range tc.computedAt {
				orgID := fmt.Sprintf("org-14196-%s-%d-%s", tc.name, i, uuid.NewString())
				repo := uuid.New()
				workItemID := "gh:a/repo#" + tc.name
				storeWorkItemCopy(t, ctx, conn, orgID, repo, workItemID, tc.status, "proj-a", created, nil, synced)
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
				executor.nowUTC = func() time.Time { return computedAt }
				run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
				if _, err := executor.ComputeFamily(ctx, run, Partition{ID: "p", RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}); err != nil {
					t.Fatal(err)
				}

				org, scope := readWorkItemStateHoursByStatus(t, ctx, conn, orgID, day)
				if !reflect.DeepEqual(org, tc.want[i]) || !reflect.DeepEqual(scope, tc.want[i]) {
					t.Errorf("status %s, completed_at NULL, day %s, computed at %s: hours by status org %v, scope %v; want %v in each view",
						tc.status, day.Format("2006-01-02"), computedAt.Format(time.RFC3339), org, scope, tc.want[i])
				}
			}
		})
	}
}

func readWorkItemStateHoursByStatus(
	t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time,
) (org, scope map[string]float64) {
	t.Helper()
	rows, err := conn.Query(ctx, `
SELECT status, sumIf(duration_hours, row_view = 'org'), sumIf(duration_hours, row_view = 'scope')
FROM work_item_state_durations_daily FINAL
WHERE org_id = ? AND day = ? AND duration_hours != 0
GROUP BY status`, orgID, day)
	if err != nil {
		t.Fatalf("read work_item_state_durations_daily: %v", err)
	}
	defer rows.Close()
	org, scope = map[string]float64{}, map[string]float64{}
	for rows.Next() {
		var (
			status               string
			orgHours, scopeHours float64
		)
		if err := rows.Scan(&status, &orgHours, &scopeHours); err != nil {
			t.Fatal(err)
		}
		if orgHours != 0 {
			org[status] = orgHours
		}
		if scopeHours != 0 {
			scope[status] = scopeHours
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return org, scope
}
