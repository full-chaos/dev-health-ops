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

// sameSecondBoardHalf is sameSecondBoard for a value that may be absent
// (status "": the board item has no Status value, so the board emits no event).
func sameSecondBoardHalf(t *testing.T, scope, status string, normalizedAt time.Time) githubWorkItemRows {
	t.Helper()
	if status != "" {
		return sameSecondBoard(t, scope, chaos14184Item("OPEN", "", status, sameSecondAt), normalizedAt)
	}
	var payload gitHubProjectV2ItemPayload
	if err := json.Unmarshal([]byte(chaos14184Item("OPEN", "", "", "")), &payload); err != nil {
		t.Fatal(err)
	}
	row, kept, err := normalizeGitHubProjectV2Item(nativeTestClaim("github", "work-items"), payload, scope, nil, normalizedAt)
	if err != nil || !kept {
		t.Fatalf("kept=%v err=%v", kept, err)
	}
	return githubWorkItemRows{WorkItems: []githubWorkItemRow{row}, StatusTransitions: []githubWorkItemTransitionRow{}}
}

func sameSecondBoards(halves ...githubWorkItemRows) githubWorkItemRows {
	out := chaos14184EmptyRows()
	for _, half := range halves {
		out.WorkItems = append(out.WorkItems, half.WorkItems...)
		out.StatusTransitions = append(out.StatusTransitions, half.StatusTransitions...)
	}
	return out
}

// sameSecondReadRow is one row of the stored read: an event's content and the
// sorting keys (tie_ordinal) its copies hold, ascending.
type sameSecondReadRow struct {
	event workItemTransitionStoredRow
	keys  []uint32
}

// sameSecondRead models gitHubProjectV2StoredTransitionsQuery on the table: a
// stale tie_ordinal 0 board row is dropped (workitemmetrics.StaleBoardTieZeroSQL;
// no cell writes one: every row here comes through rankTransitionTies), then
// one row per (work_item_id, occurred_at, from_status, to_status,
// from_status_raw, to_status_raw, actor) with the keys of its copies.
func sameSecondRead(table []workItemTransitionStoredRow) []sameSecondReadRow {
	var out []sameSecondReadRow
	index := map[string]int{}
	for _, row := range table {
		if sameSecondStaleTieZero(row, table) {
			continue
		}
		content := transitionInstantKey(row) + workItemKeySeparator + transitionTieOrder(row)
		if at, seen := index[content]; seen {
			if !slices.Contains(out[at].keys, row.TieOrdinal) {
				out[at].keys = append(out[at].keys, row.TieOrdinal)
				slices.Sort(out[at].keys)
			}
			continue
		}
		index[content] = len(out)
		out = append(out, sameSecondReadRow{event: row, keys: []uint32{row.TieOrdinal}})
	}
	return out
}

func sameSecondStaleTieZero(row workItemTransitionStoredRow, table []workItemTransitionStoredRow) bool {
	if row.TieOrdinal != 0 || row.FromStatus != "unknown" || row.FromStatusRaw != "" || row.Actor != "" {
		return false
	}
	for _, slot := range table {
		if slot.TieOrdinal >= 1<<31 && slot.ToStatus != "unknown" && transitionInstantKey(slot) == transitionInstantKey(row) &&
			slot.ToStatusRaw == row.ToStatusRaw && (slot.FromStatus != row.FromStatus || slot.ToStatus != row.ToStatus) {
			return true
		}
	}
	return false
}

// sameSecondReader returns the stored read as the production reader builds it
// from its scan (githubStoredTransitionRow).
type sameSecondReader []workItemTransitionStoredRow

func (table sameSecondReader) StoredTransitions(_ context.Context, _ string, ids []string) ([]githubWorkItemTransitionRow, error) {
	var out []githubWorkItemTransitionRow
	for _, read := range sameSecondRead(table) {
		row := read.event
		if slices.Contains(ids, row.WorkItemID) {
			out = append(out, githubStoredTransitionRow(
				row.WorkItemID, row.OccurredAt, row.FromStatus, row.ToStatus, row.ToStatusRaw, read.keys,
			))
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
// write, read the way the daily state family reads it (one row per event
// content), in the order the state segments use.
func sameSecondCheck(t *testing.T, merged githubWorkItemRows, table []workItemTransitionStoredRow) {
	t.Helper()
	if len(merged.WorkItems) != 1 {
		t.Fatalf("work items=%d, want 1", len(merged.WorkItems))
	}
	last := chaos14184Last(t, sameSecondHistory(table))
	if got := merged.WorkItems[0].Status; got != last.ToStatus {
		t.Fatalf("merged row status=%q, last stored event %s->%s; table=%s", got, last.FromStatus, last.ToStatus, sameSecondRender(table))
	}
}

func sameSecondHistory(table []workItemTransitionStoredRow) []githubWorkItemTransitionRow {
	read := sameSecondRead(table)
	history := make([]githubWorkItemTransitionRow, 0, len(read))
	for _, row := range read {
		history = append(history, sameSecondScan(row.event))
	}
	return history
}

// sameSecondCheckRaw: the row's status and status_raw are those of the table's
// last event; of events at one instant with the same statuses, the smaller raw
// value (lastGitHubTransition's tie rule).
func sameSecondCheckRaw(t *testing.T, merged githubWorkItemRows, table []workItemTransitionStoredRow, wantStatus string) {
	t.Helper()
	sameSecondCheck(t, merged, table)
	history := sameSecondHistory(table)
	last := chaos14184Last(t, history)
	var want *string
	found := false
	for _, event := range history {
		if event.OccurredAt.Equal(last.OccurredAt) && event.FromStatus == last.FromStatus && event.ToStatus == last.ToStatus &&
			(!found || rawLess(event.ToStatusRaw, want)) {
			want, found = event.ToStatusRaw, true
		}
	}
	item := merged.WorkItems[0]
	if item.Status != wantStatus || derefString(item.StatusRaw) != derefString(want) {
		t.Fatalf("merged row %s/%q, table's last event %s/%q (want status %s); table=%s",
			item.Status, derefString(item.StatusRaw), last.ToStatus, derefString(want), wantStatus, sameSecondRender(table))
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

// sameSecondScopes returns two board scopes ordered by their slot: lower, higher.
func sameSecondScopes() (string, string) {
	one, two := "ghprojv2:acme/1", "ghprojv2:acme/2"
	if boardTieSlot(one) < boardTieSlot(two) {
		return one, two
	}
	return two, one
}

// Two boards of one issue set one value at one instant: the read returns one
// event with two sorting keys. A later run re-moves the boards in that second
// (a value of "" = the board emits no event: its Status value was cleared).
func TestProjectsV2TwoBoardsHoldingOneEventSettleToTheStoredHistory(t *testing.T) {
	lower, higher := sameSecondScopes()
	cases := []struct {
		name, stored, lower, higher, want string
	}{
		{"a higher moves lower no event", "Todo", "", "In Review", "todo"},
		{"b higher moves lower re-emits", "In Review", "In Review", "Todo", "todo"},
		{"b higher moves lower no event", "In Review", "", "Todo", "todo"},
		{"c lower moves higher re-emits", "Todo", "In Review", "Todo", "todo"},
		{"c lower moves higher no event", "Todo", "In Review", "", "todo"},
		{"d higher moves lower re-emits", "Todo", "Todo", "In Review", "todo"},
		{"e both move", "Todo", "In Review", "In Review", "in_review"},
		{"f higher moves lower no event", "Ready", "", "Todo", "todo"},
		{"f higher moves lower re-emits", "Ready", "Ready", "Todo", "todo"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			first, table := sameSecondRun(t, nil, chaos14184EmptyRows(), sameSecondBoards(
				sameSecondBoardHalf(t, lower, c.stored, chaos14184Now()),
				sameSecondBoardHalf(t, higher, c.stored, chaos14184Now()),
			))
			if read := sameSecondRead(table); len(read) != 1 || len(read[0].keys) != 2 {
				t.Fatalf("stored read=%+v, want one event at two keys", read)
			}
			sameSecondCheck(t, first, table)
			at := chaos14184Now().Add(time.Hour)
			merged, table := sameSecondRun(t, table, chaos14184EmptyRows(), sameSecondBoards(
				sameSecondBoardHalf(t, lower, c.lower, at),
				sameSecondBoardHalf(t, higher, c.higher, at),
			))
			sameSecondCheckRaw(t, merged, table, c.want)
		})
	}
}

// sameSecondOneEventTwoKeys runs a repository half and a board value that the
// producers store as one event content at two keys (timeline tie 0 and the
// board's slot), then a run with the issue outside the REST window (no timeline
// rows) and the board at value next in the same second.
func sameSecondOneEventTwoKeys(t *testing.T, repository githubWorkItemRows, item, next string) (githubWorkItemRows, []workItemTransitionStoredRow) {
	t.Helper()
	first, table := sameSecondRun(t, nil, repository, sameSecondBoard(t, "ghprojv2:acme/1", item, chaos14184Now()))
	sameSecondCheck(t, first, table)
	twoKeys := false
	for _, read := range sameSecondRead(table) {
		twoKeys = twoKeys || len(read.keys) == 2 && read.keys[0] < 1<<31 && read.keys[1] >= 1<<31
	}
	if !twoKeys {
		t.Fatalf("no event stored at a timeline key and a board slot: %s", sameSecondRender(table))
	}
	return sameSecondRun(t, table, chaos14184EmptyRows(),
		sameSecondBoard(t, "ghprojv2:acme/1", next, chaos14184Now().Add(time.Hour)))
}

// g: a first status label and a board column of the same name in one second.
func TestProjectsV2FirstLabelAndBoardValueOfOneEventSettleToTheStoredHistory(t *testing.T) {
	repository := chaos14184Repository(t, chaos14301LabelIssue("open", "", "todo"), chaos14301LabeledEvent("todo", sameSecondAt))
	merged, table := sameSecondOneEventTwoKeys(t, repository,
		chaos14184Item("OPEN", "", "todo", sameSecondAt), chaos14184Item("OPEN", "", "in review", sameSecondAt))
	sameSecondCheckRaw(t, merged, table, "todo")
}

// h: a first close and a board column "closed" in one second.
func TestProjectsV2FirstCloseAndBoardValueOfOneEventSettleToTheStoredHistory(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
	merged, table := sameSecondOneEventTwoKeys(t, repository,
		chaos14184Item("CLOSED", sameSecondAt, "closed", sameSecondAt), chaos14184Item("CLOSED", sameSecondAt, "wontfix", sameSecondAt))
	sameSecondCheckRaw(t, merged, table, "done")
}

// One event at two keys inside a chain of its second (label todo, label in
// progress, board column todo): the history holds the event once, so the chain
// ends at in_progress in the batch run and after a board-only re-sync.
func TestProjectsV2OneEventAtTwoKeysCountsOnceInTheChainOfItsSecond(t *testing.T) {
	repository := chaos14184Repository(t, chaos14301LabelIssue("open", "", "in progress"),
		chaos14301LabeledEvent("todo", sameSecondAt), chaos14301LabeledEvent("in progress", sameSecondAt))
	item := chaos14184Item("OPEN", "", "todo", sameSecondAt)
	merged, table := sameSecondOneEventTwoKeys(t, repository, item, item)
	sameSecondCheckRaw(t, merged, table, "in_progress")
}
