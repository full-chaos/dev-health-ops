//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// A card moved twice inside one second, on a real ClickHouse: two route runs
// (stored read, settle, write), then a merge. The stored work_items status is
// the one stored transition of the second, whichever status sorts last.
func TestProjectsV2BoardReMoveInOneSecondStoresTheNewValueAsTheStatus(t *testing.T) {
	raw := map[string]string{"todo": "Todo", "in_review": "In Review", "done": "Done", "canceled": "wontfix"}
	for _, order := range [][2]string{
		{"todo", "in_review"}, {"in_review", "todo"},
		{"done", "canceled"}, {"canceled", "done"},
	} {
		stored, batch := order[0], order[1]
		t.Run("stored "+stored+" batch "+batch, func(t *testing.T) {
			ctx, conn := newWorkItemEffectsConn(t)
			orgID := workItemTestOrgID(t)
			reader := GitHubProjectV2StoredTransitionsClickHouseReader{Conn: conn}
			first := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
			for index, status := range []string{stored, batch} {
				at := first.Add(time.Duration(index) * time.Hour)
				board := sameSecondBoard(t, "ghprojv2:acme/1", chaos14184Item("OPEN", "", raw[status], sameSecondAt), at)
				rows, err := mergeGitHubProjectV2RowsWithStored(ctx, reader, orgID, chaos14184EmptyRows(), board)
				if err != nil {
					t.Fatal(err)
				}
				if got := rows.WorkItems[0].Status; got != status {
					t.Fatalf("run %d: settled status=%q, want %q", index+1, got, status)
				}
				chaos14184Write(t, conn, rows)
			}
			for _, table := range []string{"work_items", "work_item_transitions"} {
				if err := conn.Exec(ctx, "OPTIMIZE TABLE "+table+" FINAL"); err != nil {
					t.Fatal(err)
				}
			}
			// Read directly: the daily item loader skips a done item with no completed_at.
			var status string
			if err := conn.QueryRow(ctx, `SELECT status FROM work_items FINAL WHERE org_id = ?`, orgID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			end := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
			transitions, err := daily.LoadWorkItemStateTransitions(ctx, conn, orgID, uuid.Nil, end)
			if err != nil || len(transitions) != 1 {
				t.Fatalf("transitions=%d err=%v, want the one board event of the second", len(transitions), err)
			}
			if status != batch || transitions[0].ToStatus != batch {
				t.Fatalf("stored status=%q, stored transition to_status=%q, want both %q", status, transitions[0].ToStatus, batch)
			}
		})
	}
}

// Two boards of one issue set Todo at one instant (one stored event at two
// keys); in a later run the higher-slot board moves to In Review in that second
// and the lower-slot board emits no event. The table keeps Todo at the lower
// slot, so the stored work_items status and status_raw are the last stored
// transition's: todo / Todo.
func TestProjectsV2TwoBoardsHoldingOneEventStoreTheLastTransitionAsTheStatus(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := workItemTestOrgID(t)
	reader := GitHubProjectV2StoredTransitionsClickHouseReader{Conn: conn}
	lower, higher := sameSecondScopes()
	first := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	for index, values := range [][2]string{{"Todo", "Todo"}, {"", "In Review"}} {
		at := first.Add(time.Duration(index) * time.Hour)
		board := sameSecondBoards(
			sameSecondBoardHalf(t, lower, values[0], at),
			sameSecondBoardHalf(t, higher, values[1], at),
		)
		rows, err := mergeGitHubProjectV2RowsWithStored(ctx, reader, orgID, chaos14184EmptyRows(), board)
		if err != nil {
			t.Fatal(err)
		}
		chaos14184Write(t, conn, rows)
	}
	for _, table := range []string{"work_items", "work_item_transitions"} {
		if err := conn.Exec(ctx, "OPTIMIZE TABLE "+table+" FINAL"); err != nil {
			t.Fatal(err)
		}
	}
	var status, statusRaw string
	if err := conn.QueryRow(ctx, `SELECT status, status_raw FROM work_items FINAL WHERE org_id = ?`, orgID).Scan(&status, &statusRaw); err != nil {
		t.Fatal(err)
	}
	end := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	transitions, err := daily.LoadWorkItemStateTransitions(ctx, conn, orgID, uuid.Nil, end)
	if err != nil || len(transitions) != 2 {
		t.Fatalf("transitions=%d err=%v, want Todo and In Review of the second", len(transitions), err)
	}
	ordered := make([]workitemmetrics.Transition, 0, len(transitions))
	for _, transition := range transitions {
		ordered = append(ordered, workitemmetrics.Transition{
			WorkItemID: transition.WorkItemID, OccurredAt: transition.OccurredAt,
			FromStatus: transition.FromStatus, ToStatus: transition.ToStatus,
		})
	}
	last := workitemmetrics.OrderTransitions(ordered)[len(ordered)-1]
	var lastRaw string
	if err := conn.QueryRow(ctx, `SELECT min(to_status_raw) FROM work_item_transitions FINAL
		WHERE org_id = ? AND occurred_at = toDateTime64(?, 3, 'UTC') AND from_status = ? AND to_status = ?`,
		orgID, clickHouseMillisParam(last.OccurredAt), last.FromStatus, last.ToStatus).Scan(&lastRaw); err != nil {
		t.Fatal(err)
	}
	if status != last.ToStatus || statusRaw != lastRaw || status != "todo" || statusRaw != "Todo" {
		t.Fatalf("stored status %q/%q, last stored transition %q/%q, want todo/Todo", status, statusRaw, last.ToStatus, lastRaw)
	}
}
