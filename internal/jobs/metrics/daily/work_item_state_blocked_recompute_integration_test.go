//go:build integration

package daily

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

// A recompute of a written day must not leave the old "blocked" row beside the
// new own-status rows. The day is computed while the blocking relation looks
// open (blocked 24 h), the relation then ends before the day (the item is
// synced again and the relation is not), and the day is computed again: the
// readers must see 48 hours for the two items, not 72. A second item of the
// same scope that the later sync did not touch keeps its hours.
func TestRecomputeOfAWrittenDayRetractsTheDroppedBlockedStatus(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)

	const (
		org       = "00000000-0000-4000-8000-0000008646a0"
		blockedID = "gh:a/repo#1"
		controlID = "gh:a/repo#2"
		blockerID = "linear:OPS-9"
	)
	repo := uuid.MustParse("00000000-0000-4000-8000-0000008646a1")
	blockerRepo := uuid.MustParse("00000000-0000-4000-8000-0000008646a2")
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	linkWritten := day.Add(-15 * time.Hour) // the Monday 09:00 of the ticket
	firstCompute := day.Add(25 * time.Hour)
	itemResynced := day.Add(34 * time.Hour)
	secondCompute := day.Add(34*time.Hour + 30*time.Minute)

	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	insertItem := func(repoID uuid.UUID, id, provider, status, project string, synced time.Time) {
		exec("insert work item", `INSERT INTO work_items (
    repo_id, work_item_id, provider, type, status, project_id, created_at, started_at, org_id, last_synced)
    VALUES (?, ?, ?, 'story', ?, ?, ?, ?, ?, ?)`,
			repoID, id, provider, status, project, day, day.Add(6*time.Hour), org, synced)
	}
	insertItem(repo, blockedID, "github", "in_progress", "a/repo", linkWritten)
	insertItem(repo, controlID, "github", "in_progress", "a/repo", linkWritten)
	insertItem(blockerRepo, blockerID, "linear", "todo", "", linkWritten)
	for _, id := range []string{blockedID, controlID} {
		exec("insert transition", `INSERT INTO work_item_transitions
    (repo_id, work_item_id, occurred_at, provider, from_status, to_status, from_status_raw, to_status_raw, actor, org_id, last_synced)
    VALUES (?, ?, ?, 'github', 'todo', 'in_progress', 'Todo', 'In Progress', '', ?, ?)`,
			repo, id, day.Add(6*time.Hour), org, linkWritten)
	}
	exec("insert blocking relation", `INSERT INTO work_item_dependencies
    (source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw, last_synced, org_id,
     relationship_semantics_version, relation_started_at)
    VALUES (?, ?, 'blocks', 'linear_relation:blocks', ?, ?, 'canonical-blocks.v2', ?)`,
		blockerID, blockedID, linkWritten, org, day.Add(-48*time.Hour))

	run := Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day, DiscoveredRepoIDs: []RepositoryID{RepositoryID(repo.String())}}
	compute := func(clock time.Time) {
		t.Helper()
		runSharedScopeFamily(t, ctx, conn, "work_item_state", run, repo, clock)
		endStaleKeyRun(t, ctx, conn, run, clock.Add(time.Minute))
	}

	// What a reader serves: the newest row of each key, summed by status.
	served := func() map[string]float64 {
		t.Helper()
		rows, err := conn.Query(ctx, `
SELECT status, sum(hours) FROM (
  SELECT provider, work_scope_id, team_id, status, argMax(duration_hours, computed_at) AS hours
  FROM work_item_state_durations_daily
  WHERE org_id = ? AND day = ?
  GROUP BY provider, work_scope_id, team_id, status, row_view
) GROUP BY status`, org, day)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		totals := map[string]float64{}
		for rows.Next() {
			var status string
			var hours float64
			if err := rows.Scan(&status, &hours); err != nil {
				t.Fatal(err)
			}
			if hours != 0 {
				totals[status] = hours
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return totals
	}

	compute(firstCompute)
	wantFirst := map[string]float64{"blocked": 24, "todo": 6, "in_progress": 18}
	if got := served(); !reflect.DeepEqual(got, wantFirst) {
		t.Fatalf("first compute serves %+v, want %+v", got, wantFirst)
	}

	// The blocked item is synced again; its relation is not written again, so
	// it ended at the Monday write, before the day. The control item is not
	// touched.
	insertItem(repo, blockedID, "github", "in_progress", "a/repo", itemResynced)
	compute(secondCompute)
	wantSecond := map[string]float64{"todo": 12, "in_progress": 36}
	if got := served(); !reflect.DeepEqual(got, wantSecond) {
		t.Fatalf("recompute serves %+v, want %+v (an old blocked row must not stay beside the new rows)", got, wantSecond)
	}
}
