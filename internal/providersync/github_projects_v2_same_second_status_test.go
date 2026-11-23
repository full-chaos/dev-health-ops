package providersync

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"testing"
	"time"
)

// Status events of one work item in one second: the merged work item row must
// take the status of the last event work_item_transitions holds once the
// writer stored the batch (its sorting key and the engine's keep-newer rule),
// so the row and the stored history cannot disagree.

const sameSecondAt = "2026-10-03T00:00:00Z"

// sameSecondBoard is one board's half for item PVTI_1 as the fetch builds it:
// the row, and the Status event in the board's slot.
func sameSecondBoard(t *testing.T, scope, item string, normalizedAt time.Time) githubWorkItemRows {
	t.Helper()
	var payload gitHubProjectV2ItemPayload
	if err := json.Unmarshal([]byte(item), &payload); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("github", "work-items")
	row, kept, err := normalizeGitHubProjectV2Item(claim, payload, scope, nil, normalizedAt)
	if err != nil || !kept {
		t.Fatalf("kept=%v err=%v", kept, err)
	}
	rows := githubWorkItemRows{WorkItems: []githubWorkItemRow{row}, StatusTransitions: []githubWorkItemTransitionRow{}}
	event, hasEvent, _, err := githubProjectV2StatusEvent(claim, payload, row.WorkItemID, normalizedAt)
	if err != nil || !hasEvent {
		t.Fatalf("hasEvent=%v err=%v", hasEvent, err)
	}
	event.BoardSlot = int(boardTieSlot(scope))
	rows.StatusTransitions = append(rows.StatusTransitions, event)
	return rows
}

func sameSecondBoards(halves ...githubWorkItemRows) githubWorkItemRows {
	out := chaos14184EmptyRows()
	for _, half := range halves {
		out.WorkItems = append(out.WorkItems, half.WorkItems...)
		out.StatusTransitions = append(out.StatusTransitions, half.StatusTransitions...)
	}
	return out
}

// sameSecondReader returns the stored rows as the production reader scans them.
type sameSecondReader []workItemTransitionStoredRow

func (table sameSecondReader) StoredTransitions(_ context.Context, _ string, ids []string) ([]githubWorkItemTransitionRow, error) {
	var out []githubWorkItemTransitionRow
	for _, row := range table {
		if slices.Contains(ids, row.WorkItemID) {
			out = append(out, sameSecondScan(row))
		}
	}
	return out, nil
}

func sameSecondScan(row workItemTransitionStoredRow) githubWorkItemTransitionRow {
	return githubWorkItemTransitionRow{
		WorkItemID: row.WorkItemID, OccurredAt: row.OccurredAt.UTC(), FromStatus: row.FromStatus,
		ToStatus: row.ToStatus, ToStatusRaw: nullableString(row.ToStatusRaw),
	}
}

// sameSecondRun is one route run: the board merge on the stored history, then
// the transitions writer's rows (rankTransitionTies) into the table, where a
// row replaces the stored row of its sorting key (ReplacingMergeTree(last_synced):
// every later run has a later last_synced, so the later row is kept).
func sameSecondRun(
	t *testing.T, table []workItemTransitionStoredRow, repository, board githubWorkItemRows,
) (githubWorkItemRows, []workItemTransitionStoredRow) {
	t.Helper()
	merged, err := mergeGitHubProjectV2RowsWithStored(
		context.Background(), sameSecondReader(table), nativeTestClaim("github", "work-items").OrgID, repository, board,
	)
	if err != nil {
		t.Fatal(err)
	}
	written := rankTransitionTies(projectRows(merged.StatusTransitions, projectWorkItemTransition))
	after := dedupeBySortingKey(append(slices.Clone(table), written...), func(row workItemTransitionStoredRow) string {
		return transitionInstantKey(row) + workItemKeySeparator + strconv.FormatUint(uint64(row.TieOrdinal), 10)
	})
	return merged, after
}

// sameSecondCheck: the row's status is the last event of the table after the
// write, in the order the state segments use.
func sameSecondCheck(t *testing.T, merged githubWorkItemRows, table []workItemTransitionStoredRow) {
	t.Helper()
	if len(merged.WorkItems) != 1 {
		t.Fatalf("work items=%d, want 1", len(merged.WorkItems))
	}
	history := make([]githubWorkItemTransitionRow, 0, len(table))
	for _, row := range table {
		history = append(history, sameSecondScan(row))
	}
	last := chaos14184Last(t, history)
	if got := merged.WorkItems[0].Status; got != last.ToStatus {
		t.Fatalf("merged row status=%q, last stored event %s->%s; table=%s", got, last.FromStatus, last.ToStatus, sameSecondRender(table))
	}
}

func sameSecondRender(table []workItemTransitionStoredRow) string {
	out := ""
	for _, row := range table {
		out += "[" + row.OccurredAt.Format(time.RFC3339) + " " + strconv.FormatUint(uint64(row.TieOrdinal), 10) + " " +
			row.FromStatus + "->" + row.ToStatus + " " + row.ToStatusRaw + "]"
	}
	return out
}

func sameSecondCount(table []workItemTransitionStoredRow, at string) int {
	instant, _ := time.Parse(time.RFC3339, at)
	count := 0
	for _, row := range table {
		if row.OccurredAt.Equal(instant) {
			count++
		}
	}
	return count
}

// A completed close and a board value "wontfix"/"duplicate" (canceled) in one
// second are two stored events; one batch, then a re-sync of the same batch.
func TestProjectsV2BoardCanceledAndCloseInOneSecondKeepBothEvents(t *testing.T) {
	for _, value := range []string{"wontfix", "duplicate"} {
		t.Run(value, func(t *testing.T) {
			repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
			item := chaos14184Item("CLOSED", sameSecondAt, value, sameSecondAt)
			merged, table := sameSecondRun(t, nil, repository, sameSecondBoard(t, "ghprojv2:acme/1", item, chaos14184Now()))
			sameSecondCheck(t, merged, table)
			if got := sameSecondCount(table, sameSecondAt); got != 2 {
				t.Fatalf("stored events in the second=%d, want 2 (close and board): %s", got, sameSecondRender(table))
			}
			again, table := sameSecondRun(t, table,
				chaos14184RepositoryAt(t, chaos14184Now().Add(time.Hour), chaos14184ClosedIssue, chaos14184CloseEvent),
				sameSecondBoard(t, "ghprojv2:acme/1", item, chaos14184Now().Add(time.Hour)))
			sameSecondCheck(t, again, table)
		})
	}
}

// A status label and the values of two boards in one second are three stored
// events.
func TestProjectsV2LabelAndTwoBoardValuesInOneSecondKeepEveryEvent(t *testing.T) {
	cases := []struct{ label, first, second string }{
		{"in review", "Todo", "In Progress"},
		{"todo", "In Review", "wontfix"},
		{"in progress", "Done", "Todo"},
	}
	for _, c := range cases {
		t.Run(c.label+"/"+c.first+"/"+c.second, func(t *testing.T) {
			repository := chaos14184Repository(t, chaos14301LabelIssue("open", "", c.label), chaos14301LabeledEvent(c.label, sameSecondAt))
			board := sameSecondBoards(
				sameSecondBoard(t, "ghprojv2:acme/1", chaos14184Item("OPEN", "", c.first, sameSecondAt), chaos14184Now()),
				sameSecondBoard(t, "ghprojv2:acme/2", chaos14184Item("OPEN", "", c.second, sameSecondAt), chaos14184Now()),
			)
			merged, table := sameSecondRun(t, nil, repository, board)
			sameSecondCheck(t, merged, table)
			if got := sameSecondCount(table, sameSecondAt); got != 3 {
				t.Fatalf("stored events in the second=%d, want 3 (label and two boards): %s", got, sameSecondRender(table))
			}
		})
	}
}

// A card moved twice inside the one second GitHub stamps: the second run's
// board event has the stored event's sorting key (the board's slot at that
// instant) and replaces it, so the row takes the new value whichever status
// sorts last. Every precedence pair, both ways.
func TestProjectsV2BoardReMoveInOneSecondSettlesToTheNewValue(t *testing.T) {
	raw := map[string]string{
		"todo": "Todo", "in_progress": "In Progress", "in_review": "In Review", "done": "Done", "canceled": "wontfix",
	}
	pairs := [][2]string{
		{"todo", "in_review"},
		{"todo", "canceled"}, {"in_progress", "canceled"}, {"in_review", "canceled"}, {"done", "canceled"},
	}
	for _, pair := range pairs {
		for _, order := range [][2]string{{pair[0], pair[1]}, {pair[1], pair[0]}} {
			stored, batch := order[0], order[1]
			t.Run("stored "+stored+" batch "+batch, func(t *testing.T) {
				_, table := sameSecondRun(t, nil, chaos14184EmptyRows(),
					sameSecondBoard(t, "ghprojv2:acme/1", chaos14184Item("OPEN", "", raw[stored], sameSecondAt), chaos14184Now()))
				merged, table := sameSecondRun(t, table, chaos14184EmptyRows(),
					sameSecondBoard(t, "ghprojv2:acme/1", chaos14184Item("OPEN", "", raw[batch], sameSecondAt), chaos14184Now().Add(time.Hour)))
				if got := sameSecondCount(table, sameSecondAt); got != 1 || table[0].ToStatus != batch {
					t.Fatalf("table=%s, want the one batch event to %s", sameSecondRender(table), batch)
				}
				sameSecondCheck(t, merged, table)
				if merged.WorkItems[0].StatusRaw == nil || *merged.WorkItems[0].StatusRaw != raw[batch] {
					t.Fatalf("status_raw=%v, want %q", merged.WorkItems[0].StatusRaw, raw[batch])
				}
			})
		}
	}
}
