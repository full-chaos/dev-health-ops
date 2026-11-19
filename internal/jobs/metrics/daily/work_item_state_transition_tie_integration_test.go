//go:build integration

package daily

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestWorkItemStateSameInstantReopenAndDoneIsRowOrderFree stores a move to done
// and a reopen at one occurred_at for one item in several orgs. The actor is a
// GROUP BY column of the transition read and not an input of the segments, so
// the variants make ClickHouse return the two rows in both orders. Every
// variant must write the same state hours.
func TestWorkItemStateSameInstantReopenAndDoneIsRowOrderFree(t *testing.T) {
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
	created := day.Add(-24 * time.Hour)
	started := day.Add(2 * time.Hour)
	tie := day.Add(6 * time.Hour)
	synced := time.Now().UTC().Truncate(time.Second)
	want := map[string]float64{"todo": 2, "in_progress": 22}

	const variants = 16
	orders := map[bool]int{}
	for variant := range variants {
		orgID := fmt.Sprintf("org-14172-tie-%02d", variant)
		actor := fmt.Sprintf("actor-%02d", variant)
		repo := uuid.New()
		workItemID := "gh:a/repo#1"
		storeWorkItemCopy(t, ctx, conn, orgID, repo, workItemID, "in_progress", "proj-a", created, nil, synced)
		for _, transition := range [][3]any{
			{started, "todo", "in_progress"}, {tie, "in_progress", "done"}, {tie, "done", "in_progress"},
		} {
			if err := conn.Exec(ctx, `
INSERT INTO work_item_transitions (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
VALUES (?, ?, ?, '', ?, ?, ?, ?, ?, ?, ?)`,
				uuid.Nil, workItemID, transition[0], transition[1], transition[2], transition[1], transition[2], actor, orgID, synced,
			); err != nil {
				t.Fatal(err)
			}
		}

		read, err := loadWorkItemScopeTransitions(ctx, conn, orgID, day.AddDate(0, 0, 1), nil, false, false)
		if err != nil {
			t.Fatal(err)
		}
		doneFirst := false
		for _, transition := range read {
			if transition.OccurredAt.Equal(tie) {
				doneFirst = transition.ToStatus == "done"
				break
			}
		}
		orders[doneFirst]++

		executor, err := NewWorkItemStateExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		executor.nowUTC = func() time.Time { return day.Add(36 * time.Hour) }
		run := Run{ID: uuid.NewString(), OrganizationID: orgID, TargetDay: day}
		if _, err := executor.ComputeFamily(ctx, run, Partition{ID: "p", RunID: run.ID, RepoIDs: []RepositoryID{RepositoryID(repo.String())}}); err != nil {
			t.Fatal(err)
		}
		rows, err := conn.Query(ctx, `
SELECT status, sum(duration_hours) FROM work_item_state_durations_daily
WHERE org_id = ? AND day = ? AND row_view = 'org' GROUP BY status`, orgID, day)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]float64{}
		for rows.Next() {
			var status string
			var hours float64
			if err := rows.Scan(&status, &hours); err != nil {
				t.Fatal(err)
			}
			got[status] = hours
		}
		rows.Close()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("variant %02d (done read first: %v): hours by status %v, want %v", variant, doneFirst, got, want)
		}
	}
	if orders[true] == 0 || orders[false] == 0 {
		t.Fatalf("precondition: the %d variants read the tie in one order only (done first %d, reopen first %d); add variants", variants, orders[true], orders[false])
	}
	t.Logf("tie read order over %d variants: done first %d, reopen first %d", variants, orders[true], orders[false])
}
