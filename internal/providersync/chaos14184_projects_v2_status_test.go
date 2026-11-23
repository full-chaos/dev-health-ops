package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// A GitHub issue on a Projects v2 board has two status sources: the
// repository (close, reopen, status labels) and the board (its current Status
// value, set at updatedAt; GitHub serves no past board moves). The item's
// status is the last event of the union of both, in the order the state
// segments use, so the item ends its history in its status whichever source
// moved last and whichever run stored which event.

const chaos14184RepoID = "c7198fbc-1945-3717-05d8-eb78866b4e79"

func chaos14184Now() time.Time { return time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC) }

func chaos14184Repository(t *testing.T, issue string, events ...string) githubWorkItemRows {
	t.Helper()
	return chaos14184RepositoryAt(t, chaos14184Now(), issue, events...)
}

func chaos14184RepositoryAt(t *testing.T, normalizedAt time.Time, issue string, events ...string) githubWorkItemRows {
	t.Helper()
	raw := make([]json.RawMessage, 0, len(events))
	for _, event := range events {
		raw = append(raw, json.RawMessage(event))
	}
	rows, err := normalizeGitHubIssueBundle(
		nativeTestClaim("github", "work-items"), sameGitHubRepoNames("acme/api"), uuid.MustParse(chaos14184RepoID),
		json.RawMessage(issue), raw, nil, nil, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// chaos14184Item is board item PVTI_1 for issue acme/api#9. status "" = no
// Status value; statusAt "" = a Status value with no updatedAt.
func chaos14184Item(state, closedAt, status, statusAt string) string {
	return chaos14184ItemReason(state, closedAt, "", status, statusAt)
}

// chaos14184ItemReason is chaos14184Item with the issue's stateReason ("" =
// absent).
func chaos14184ItemReason(state, closedAt, reason, status, statusAt string) string {
	closed := ""
	if closedAt != "" {
		closed = `,"closedAt":"` + closedAt + `"`
	}
	if reason != "" {
		closed += `,"stateReason":"` + reason + `"`
	}
	values := ""
	if status != "" {
		at := ""
		if statusAt != "" {
			at = `,"updatedAt":"` + statusAt + `"`
		}
		values = `{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"` + status + `"` + at + `,"field":{"name":"Status"}}`
	}
	return `{"id":"PVTI_1","createdAt":"2026-10-01T00:00:00Z",
	"content":{"__typename":"Issue","id":"I_1","number":9,"title":"T","state":"` + state + `",
	"createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-03T00:00:00Z"` + closed + `,
	"repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[]},"assignees":{"nodes":[]}},
	"fieldValues":{"nodes":[` + values + `]}}`
}

// chaos14184Board is the board half of one item as GitHubProjectV2Fetcher.Fetch
// builds it: the row, plus the Status event when the value has a time.
func chaos14184Board(t *testing.T, item string, normalizedAt time.Time) githubWorkItemRows {
	t.Helper()
	var payload gitHubProjectV2ItemPayload
	if err := json.Unmarshal([]byte(item), &payload); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("github", "work-items")
	row, kept, err := normalizeGitHubProjectV2Item(claim, payload, "ghprojv2:acme/1", nil, normalizedAt)
	if err != nil || !kept {
		t.Fatalf("kept=%v err=%v", kept, err)
	}
	rows := githubWorkItemRows{WorkItems: []githubWorkItemRow{row}, StatusTransitions: []githubWorkItemTransitionRow{}}
	event, hasEvent, _, err := githubProjectV2StatusEvent(claim, payload, row.WorkItemID, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if hasEvent {
		rows.StatusTransitions = append(rows.StatusTransitions, event)
	}
	return rows
}

type chaos14184Stored []githubWorkItemTransitionRow

func (stored chaos14184Stored) StoredTransitions(
	_ context.Context, orgID string, ids []string,
) ([]githubWorkItemTransitionRow, error) {
	// Each row at the key the transitions writer gives it.
	keys := map[string]uint32{}
	for _, row := range rankTransitionTies(projectRows(stored, projectWorkItemTransition)) {
		keys[chaos14184StoredContent(row)] = row.TieOrdinal
	}
	var out []githubWorkItemTransitionRow
	for _, row := range stored {
		for _, id := range ids {
			if row.OrgID == orgID && row.WorkItemID == id {
				row.storedTieOrdinals = []uint32{keys[chaos14184StoredContent(projectWorkItemTransition(row))]}
				out = append(out, row)
			}
		}
	}
	return out, nil
}

func chaos14184StoredContent(row workItemTransitionStoredRow) string {
	return transitionInstantKey(row) + workItemKeySeparator + strconv.FormatUint(uint64(row.BoardSlot), 10) +
		workItemKeySeparator + transitionTieOrder(row)
}

func chaos14184Merge(t *testing.T, repository, board githubWorkItemRows, stored ...githubWorkItemTransitionRow) githubWorkItemRows {
	t.Helper()
	merged, err := mergeGitHubProjectV2RowsWithStored(
		context.Background(), chaos14184Stored(stored), nativeTestClaim("github", "work-items").OrgID, repository, board,
	)
	if err != nil {
		t.Fatal(err)
	}
	return merged
}

// chaos14170RouteBoardMerge is the route's board merge (the stored history
// read through a fake reader) for the CHAOS-14170 board cells.
func chaos14170RouteBoardMerge(
	t *testing.T, repository, board githubWorkItemRows, stored []githubWorkItemTransitionRow,
) githubWorkItemRows {
	t.Helper()
	return chaos14184Merge(t, repository, board, stored...)
}

// chaos14184Last is the last event of an item's history in the shared order.
func chaos14184Last(t *testing.T, transitions []githubWorkItemTransitionRow) workitemmetrics.Transition {
	t.Helper()
	ordered := make([]workitemmetrics.Transition, 0, len(transitions))
	for _, transition := range transitions {
		ordered = append(ordered, workitemmetrics.Transition{
			WorkItemID: transition.WorkItemID, OccurredAt: transition.OccurredAt,
			FromStatus: transition.FromStatus, ToStatus: transition.ToStatus,
		})
	}
	if len(ordered) == 0 {
		t.Fatal("no transitions")
	}
	ordered = workitemmetrics.OrderTransitions(ordered)
	return ordered[len(ordered)-1]
}

// chaos14184Check asserts the one item's status, raw status, and that the
// status is the last event of the history (stored ∪ batch).
func chaos14184Check(
	t *testing.T, merged githubWorkItemRows, wantStatus, wantRaw string, stored ...githubWorkItemTransitionRow,
) {
	t.Helper()
	if len(merged.WorkItems) != 1 {
		t.Fatalf("work items=%d, want 1", len(merged.WorkItems))
	}
	item := merged.WorkItems[0]
	if item.Status != wantStatus {
		t.Fatalf("status=%q, want %q", item.Status, wantStatus)
	}
	if item.StatusRaw == nil || *item.StatusRaw != wantRaw {
		got := "<nil>"
		if item.StatusRaw != nil {
			got = *item.StatusRaw
		}
		t.Fatalf("status_raw=%q, want %q", got, wantRaw)
	}
	history := append(append([]githubWorkItemTransitionRow{}, stored...), merged.StatusTransitions...)
	if last := chaos14184Last(t, history); last.ToStatus != item.Status {
		t.Fatalf("status=%q but last event (%s) to_status=%q", item.Status, last.OccurredAt.Format(time.RFC3339), last.ToStatus)
	}
}

const chaos14184ClosedIssue = `{"number":9,"title":"T","state":"closed","closed_at":"2026-10-03T00:00:00Z",
	"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-03T00:00:00Z","labels":[],"assignees":[]}`

const chaos14184OpenIssue = `{"number":9,"title":"T","state":"open",
	"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-03T00:00:00Z","labels":[],"assignees":[]}`

const chaos14184CloseEvent = `{"event":"closed","created_at":"2026-10-03T00:00:00Z"}`

// U1: the board Status was set to In Progress on 10-02 and not moved again;
// the issue is closed in the repository on 10-03. The close is the last event.
func TestChaos14184RepositoryCloseWithNoBoardMove(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
	board := chaos14184Board(t, chaos14184Item("CLOSED", "2026-10-03T00:00:00Z", "In Progress", "2026-10-02T00:00:00Z"), chaos14184Now())
	chaos14184Check(t, chaos14184Merge(t, repository, board), "done", "closed")
}

// U2: the board Status was set to Done on 10-02; the issue is closed on 10-02
// and reopened on 10-03 with no board move. The reopen is the last event.
func TestChaos14184RepositoryReopenAfterBoardDone(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184OpenIssue,
		`{"event":"closed","created_at":"2026-10-02T00:00:00Z"}`,
		`{"event":"reopened","created_at":"2026-10-03T00:00:00Z"}`)
	board := chaos14184Board(t, chaos14184Item("OPEN", "", "Done", "2026-10-02T12:00:00Z"), chaos14184Now())
	chaos14184Check(t, chaos14184Merge(t, repository, board), "todo", "reopened")
}

// R1: the board Status is set to In Review on 10-04, after the repository
// close on 10-03. The board event is the last event.
func TestChaos14184BoardValueAfterRepositoryClose(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
	board := chaos14184Board(t, chaos14184Item("CLOSED", "2026-10-03T00:00:00Z", "In Review", "2026-10-04T00:00:00Z"), chaos14184Now())
	chaos14184Check(t, chaos14184Merge(t, repository, board), "in_review", "In Review")
}

// B1: the board value is older than every repository event.
func TestChaos14184BoardValueOlderThanEveryRepositoryEvent(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
	board := chaos14184Board(t, chaos14184Item("CLOSED", "2026-10-03T00:00:00Z", "In Progress", "2026-09-30T00:00:00Z"), chaos14184Now())
	chaos14184Check(t, chaos14184Merge(t, repository, board), "done", "closed")
}

// B2: the board value is newer than every repository event (close 10-03,
// reopen 10-04, board Todo 10-05).
func TestChaos14184BoardValueNewerThanEveryRepositoryEvent(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184OpenIssue,
		chaos14184CloseEvent, `{"event":"reopened","created_at":"2026-10-04T00:00:00Z"}`)
	board := chaos14184Board(t, chaos14184Item("OPEN", "", "Todo", "2026-10-05T00:00:00Z"), chaos14184Now())
	chaos14184Check(t, chaos14184Merge(t, repository, board), "todo", "Todo")
}

// T1: the repository close and the board value share one instant. The status
// is the last of workitemmetrics.OrderTransitions over the pair, the order the
// state segments use, not a source preference.
func TestChaos14184SameInstantOrdersByTheSharedOrder(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
	board := chaos14184Board(t, chaos14184Item("CLOSED", "2026-10-03T00:00:00Z", "In Progress", "2026-10-03T00:00:00Z"), chaos14184Now())
	merged := chaos14184Merge(t, repository, board)
	want := chaos14184Last(t, merged.StatusTransitions)
	if want.ToStatus != "in_progress" {
		t.Fatalf("shared order last=%+v, want in_progress", want)
	}
	chaos14184Check(t, merged, "in_progress", "In Progress")
	// The answer does not depend on the order the events are read in.
	reversed := make([]githubWorkItemTransitionRow, 0, len(merged.StatusTransitions))
	for index := len(merged.StatusTransitions) - 1; index >= 0; index-- {
		reversed = append(reversed, merged.StatusTransitions[index])
	}
	forward, _ := lastGitHubTransition(merged.StatusTransitions)
	backward, _ := lastGitHubTransition(reversed)
	if !reflect.DeepEqual(forward, backward) || forward.ToStatus != "in_progress" {
		t.Fatalf("read order changes the last event:\n%+v\n%+v", forward, backward)
	}
}

// The batch orders its events at the stored precision (milliseconds), so the
// status it writes is the last event a later read of the stored rows finds:
// a repository close 0.2 ms after the board value is the same stored instant.
func TestChaos14184OrdersAtTheStoredPrecision(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
	for index := range repository.StatusTransitions {
		if repository.StatusTransitions[index].ToStatus == "done" {
			repository.StatusTransitions[index].OccurredAt = repository.StatusTransitions[index].OccurredAt.Add(600 * time.Microsecond)
		}
	}
	board := chaos14184Board(t, chaos14184Item("CLOSED", "2026-10-03T00:00:00Z", "In Progress", "2026-10-03T00:00:00.0004Z"), chaos14184Now())
	merged := chaos14184Merge(t, repository, board)
	stored := make([]githubWorkItemTransitionRow, 0, len(merged.StatusTransitions))
	for _, transition := range merged.StatusTransitions {
		transition.OccurredAt = clickHouseMillis(transition.OccurredAt)
		stored = append(stored, transition)
	}
	if last := chaos14184Last(t, stored); merged.WorkItems[0].Status != last.ToStatus || last.ToStatus != "in_progress" {
		t.Fatalf("status=%q, last stored event to_status=%q, want both in_progress", merged.WorkItems[0].Status, last.ToStatus)
	}
}

// status_raw keeps the payload value as GitHub sent it (the board row's raw
// value); only the mapping reads it trimmed.
func TestChaos14184StatusRawKeepsThePayloadValue(t *testing.T) {
	board := chaos14184Board(t, chaos14184Item("OPEN", "", " In Progress ", "2026-10-02T00:00:00Z"), chaos14184Now())
	if len(board.StatusTransitions) != 1 || *board.StatusTransitions[0].ToStatusRaw != " In Progress " ||
		board.StatusTransitions[0].ToStatus != "in_progress" {
		t.Fatalf("events=%+v", board.StatusTransitions)
	}
	chaos14184Check(t, chaos14184Merge(t, chaos14184EmptyRows(), board), "in_progress", " In Progress ")
}

// A board value the mapping does not know (a custom column) is an event too:
// to "unknown", raw kept. The item's status is then the last event: here the
// card left "In Progress" (stored) for a custom column on an open issue.
func TestChaos14184UnmappedBoardValueIsAnUnknownEvent(t *testing.T) {
	stored := chaos14184Board(t, chaos14184Item("OPEN", "", "In Progress", "2026-10-02T00:00:00Z"), chaos14184Now()).StatusTransitions
	for _, content := range []string{"Issue", "DraftIssue"} {
		item := chaos14184Item("OPEN", "", "Customer Validation", "2026-10-04T00:00:00Z")
		if content == "DraftIssue" {
			item = `{"id":"PVTI_1","createdAt":"2026-10-01T00:00:00Z",
			"content":{"__typename":"DraftIssue","title":"Draft","createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-04T00:00:00Z"},
			"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"Customer Validation","updatedAt":"2026-10-04T00:00:00Z","field":{"name":"Status"}}]}}`
		}
		board := chaos14184Board(t, item, chaos14184Now())
		if len(board.StatusTransitions) != 1 || board.StatusTransitions[0].ToStatus != "unknown" ||
			*board.StatusTransitions[0].ToStatusRaw != "Customer Validation" {
			t.Fatalf("%s: board events=%+v, want one to unknown with the raw value", content, board.StatusTransitions)
		}
		if content == "Issue" {
			chaos14184Check(t, chaos14184Merge(t, chaos14184EmptyRows(), board, stored...), "unknown", "Customer Validation", stored...)
		} else {
			chaos14184Check(t, chaos14184Merge(t, chaos14184EmptyRows(), board), "unknown", "Customer Validation")
		}
	}
}

// The native close outranks an unmapped column: an issue closed on 10-03 and
// then moved to a custom "Shipped" column on 10-04 stays done and keeps its
// completion. A custom column set BEFORE the close is "unknown", and the close
// after it is the last event.
func TestChaos14184UnmappedValueAfterTheCloseKeepsDone(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
	board := chaos14184Board(t, chaos14184Item("CLOSED", "2026-10-03T00:00:00Z", "Shipped", "2026-10-04T00:00:00Z"), chaos14184Now())
	if len(board.StatusTransitions) != 1 || board.StatusTransitions[0].ToStatus != "done" {
		t.Fatalf("board events=%+v, want one to done", board.StatusTransitions)
	}
	merged := chaos14184Merge(t, repository, board)
	chaos14184Check(t, merged, "done", "Shipped")
	if completed := merged.WorkItems[0].CompletedAt; completed == nil || !completed.Equal(time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("completed_at=%v, want the close 10-03", completed)
	}

	before := chaos14184Board(t, chaos14184Item("CLOSED", "2026-10-03T00:00:00Z", "Shipped", "2026-10-02T00:00:00Z"), chaos14184Now())
	if len(before.StatusTransitions) != 1 || before.StatusTransitions[0].ToStatus != "unknown" {
		t.Fatalf("value before the close: board events=%+v, want one to unknown", before.StatusTransitions)
	}
	chaos14184Check(t, chaos14184Merge(t, repository, before), "done", "closed")
}

// A not-planned close and an unmapped column after it: canceled.
func TestChaos14184UnmappedValueAfterANotPlannedCloseIsCanceled(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
	board := chaos14184Board(t, chaos14184ItemReason("CLOSED", "2026-10-03T00:00:00Z", "NOT_PLANNED", "Shipped", "2026-10-04T00:00:00Z"), chaos14184Now())
	chaos14184Check(t, chaos14184Merge(t, repository, board), "canceled", "Shipped")
	completed := chaos14184Board(t, chaos14184ItemReason("CLOSED", "2026-10-03T00:00:00Z", "COMPLETED", "Shipped", "2026-10-04T00:00:00Z"), chaos14184Now())
	chaos14184Check(t, chaos14184Merge(t, repository, completed), "done", "Shipped")
}

// A mapped board value on a closed issue keeps today's rule (the mapped value
// wins over the issue state): the close does not override it.
func TestChaos14184MappedValueOnAClosedIssueKeepsTheMappedStatus(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
	board := chaos14184Board(t, chaos14184ItemReason("CLOSED", "2026-10-03T00:00:00Z", "NOT_PLANNED", "In Progress", "2026-10-04T00:00:00Z"), chaos14184Now())
	chaos14184Check(t, chaos14184Merge(t, repository, board), "in_progress", "In Progress")
}

// An issue that is open now takes no close from a closedAt older than the
// value: the unmapped column is "unknown".
func TestChaos14184UnmappedValueOnAnOpenIssueIgnoresAnOldClosedAt(t *testing.T) {
	board := chaos14184Board(t, chaos14184Item("OPEN", "2026-10-02T00:00:00Z", "Shipped", "2026-10-04T00:00:00Z"), chaos14184Now())
	if len(board.StatusTransitions) != 1 || board.StatusTransitions[0].ToStatus != "unknown" {
		t.Fatalf("board events=%+v, want one to unknown", board.StatusTransitions)
	}
}

// F1: no Status value on the board item: no board event, and the status is
// today's fallback from the issue state.
func TestChaos14184NoStatusValueKeepsTheFallback(t *testing.T) {
	repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
	board := chaos14184Board(t, chaos14184Item("CLOSED", "2026-10-03T00:00:00Z", "", ""), chaos14184Now())
	if len(board.StatusTransitions) != 0 {
		t.Fatalf("board events=%+v, want none", board.StatusTransitions)
	}
	merged := chaos14184Merge(t, repository, board)
	if merged.WorkItems[0].Status != "done" {
		t.Fatalf("status=%q, want done", merged.WorkItems[0].Status)
	}
}

// F2: a Status value with no (or an unparsable) updatedAt is no event: the
// board row keeps today's board status.
func TestChaos14184StatusValueWithoutTimeIsNoEvent(t *testing.T) {
	for _, at := range []string{"", "not-a-time"} {
		board := chaos14184Board(t, chaos14184Item("OPEN", "", "In Progress", at), chaos14184Now())
		if len(board.StatusTransitions) != 0 {
			t.Fatalf("updatedAt=%q: board events=%+v, want none", at, board.StatusTransitions)
		}
		merged := chaos14184Merge(t, chaos14184EmptyRows(), board)
		if merged.WorkItems[0].Status != "in_progress" {
			t.Fatalf("updatedAt=%q: status=%q, want the board value in_progress", at, merged.WorkItems[0].Status)
		}
	}
}

// E1: a draft with no Status value and no event keeps today's fallback and
// writes no transition; the stored read is not asked.
func TestChaos14184EmptyTimelineKeepsTheFallback(t *testing.T) {
	board := chaos14184Board(t, `{"id":"PVTI_D","createdAt":"2026-10-01T00:00:00Z",
		"content":{"__typename":"DraftIssue","title":"Draft","createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-01T00:00:00Z"},
		"fieldValues":{"nodes":[]}}`, chaos14184Now())
	before := board.WorkItems[0].Status
	merged, err := mergeGitHubProjectV2RowsWithStored(
		context.Background(), chaos14184RefusingReader{t: t}, nativeTestClaim("github", "work-items").OrgID, chaos14184EmptyRows(), board,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.StatusTransitions) != 0 || merged.WorkItems[0].Status != before || before == "" {
		t.Fatalf("status=%q before=%q transitions=%+v", merged.WorkItems[0].Status, before, merged.StatusTransitions)
	}
}

type chaos14184RefusingReader struct{ t *testing.T }

func (reader chaos14184RefusingReader) StoredTransitions(context.Context, string, []string) ([]githubWorkItemTransitionRow, error) {
	reader.t.Fatal("stored history read for a batch with no board event")
	return nil, nil
}

// Stored: a run whose REST window does not hold the issue update carries the
// board row alone. The repository close an earlier run stored is still the last
// event, so the status stays done.
func TestChaos14184StoredRepositoryCloseWinsOverAnOlderBoardValue(t *testing.T) {
	stored := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent).StatusTransitions
	board := chaos14184Board(t, chaos14184Item("CLOSED", "2026-10-03T00:00:00Z", "In Progress", "2026-10-02T00:00:00Z"), chaos14184Now())
	chaos14184Check(t, chaos14184Merge(t, chaos14184EmptyRows(), board, stored...), "done", "closed", stored...)
}

// A stored read error fails the batch; it is not read as "nothing stored".
func TestChaos14184StoredReadErrorFailsTheMerge(t *testing.T) {
	board := chaos14184Board(t, chaos14184Item("OPEN", "", "Todo", "2026-10-05T00:00:00Z"), chaos14184Now())
	boom := errors.New("read failed")
	_, err := mergeGitHubProjectV2RowsWithStored(
		context.Background(), chaos14184FailingReader{err: boom}, nativeTestClaim("github", "work-items").OrgID, chaos14184EmptyRows(), board,
	)
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v, want the read error", err)
	}
}

type chaos14184FailingReader struct{ err error }

func (reader chaos14184FailingReader) StoredTransitions(context.Context, string, []string) ([]githubWorkItemTransitionRow, error) {
	return nil, reader.err
}

// D1: a re-sync of the same board value writes the same event (same key and
// semantics; only last_synced moves) and leaves the status unchanged.
func TestChaos14184ResyncWritesTheSameBoardEvent(t *testing.T) {
	item := chaos14184Item("OPEN", "", "In Progress", "2026-10-02T00:00:00Z")
	first := chaos14184Board(t, item, chaos14184Now())
	second := chaos14184Board(t, item, chaos14184Now().Add(time.Hour))
	if len(first.StatusTransitions) != 1 || len(second.StatusTransitions) != 1 {
		t.Fatalf("events first=%d second=%d, want 1 each", len(first.StatusTransitions), len(second.StatusTransitions))
	}
	a, b := first.StatusTransitions[0], second.StatusTransitions[0]
	if a.LastSynced.Equal(b.LastSynced) {
		t.Fatal("re-sync kept the same last_synced")
	}
	a.LastSynced, b.LastSynced = time.Time{}, time.Time{}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("re-sync event differs:\n%+v\n%+v", a, b)
	}
	once := chaos14184Merge(t, chaos14184EmptyRows(), second, first.StatusTransitions...)
	if once.WorkItems[0].Status != "in_progress" {
		t.Fatalf("status=%q, want in_progress", once.WorkItems[0].Status)
	}
}

// The board event as Fetch emits it: the Status value at its updatedAt, from
// "unknown" (the board knows no prior status), raw = the value name.
func TestChaos14184FetchEmitsTheBoardStatusEvent(t *testing.T) {
	doer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[{"id":"PVTI_1","content":{"__typename":"Issue","number":7,"title":"Ship it","state":"OPEN","createdAt":"2026-08-01T08:00:00Z","updatedAt":"2026-08-02T08:00:00Z","repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"assignees":{"nodes":[]}},"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"In Review","updatedAt":"2026-08-03T09:30:00Z","field":{"name":"Status"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}}
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	var result GitHubProjectV2FetchResult
	if lines := chaos14184StatusTimeMissingLines(t, func() {
		var err error
		result, err = (GitHubProjectV2Fetcher{}).Fetch(
			context.Background(), claim, providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
			githubProjectV2TestClient(t, fakehttp.Client(doer)), normalizedAt, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
	}); len(lines) != 0 {
		t.Fatalf("a well-formed board logged status_value_time_missing: %v", lines)
	}
	raw := "In Review"
	want := []githubWorkItemTransitionRow{{
		WorkItemID: "gh:acme/api#7", Provider: "github", OccurredAt: time.Date(2026, 8, 3, 9, 30, 0, 0, time.UTC),
		ToStatusRaw: &raw, FromStatus: "unknown", ToStatus: "in_review", OrgID: claim.OrgID, LastSynced: normalizedAt,
		BoardSlot: int(boardTieSlot("ghprojv2:acme#3")),
	}}
	if !reflect.DeepEqual(result.Rows.StatusTransitions, want) {
		t.Fatalf("events=%+v, want %+v", result.Rows.StatusTransitions, want)
	}
}

// chaos14184StatusTimeMissingLines are the status_value_time_missing records
// logged while fetch runs.
func chaos14184StatusTimeMissingLines(t *testing.T, fetch func()) []map[string]any {
	t.Helper()
	var logs bytes.Buffer
	swapDefaultLogger(t, slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	fetch()
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if line == "" || json.Unmarshal([]byte(line), &record) != nil {
			continue
		}
		if record["msg"] == "github_projects_v2.status_value_time_missing" {
			lines = append(lines, record)
		}
	}
	return lines
}

// A Status value with no or an unparsable updatedAt (GitHub serves it as
// DateTime!) is malformed: no board event, the item keeps the board value as
// its status (also next to a newer repository close), and the unit logs ONE
// WARN line with the count. A well-formed item on the same board still emits
// its event.
func TestChaos14184MalformedStatusTimeIsLoudAndKeepsTheBoardStatus(t *testing.T) {
	item := func(id string, number int, status, at string) string {
		updated := ""
		if at != "" {
			updated = `,"updatedAt":"` + at + `"`
		}
		return `{"id":"` + id + `","content":{"__typename":"Issue","number":` + strconv.Itoa(number) +
			`,"title":"t","state":"OPEN","createdAt":"2026-08-01T08:00:00Z","updatedAt":"2026-08-02T08:00:00Z","repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"assignees":{"nodes":[]}},` +
			`"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"` + status + `"` + updated +
			`,"field":{"name":"Status"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`
	}
	doer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[` +
			item("PVTI_1", 7, "In Progress", "") + `,` +
			item("PVTI_2", 8, "In Progress", "not-a-time") + `,` +
			item("PVTI_3", 9, "Done", "2026-08-03T09:30:00Z") +
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}}
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
	var result GitHubProjectV2FetchResult
	lines := chaos14184StatusTimeMissingLines(t, func() {
		var err error
		result, err = (GitHubProjectV2Fetcher{}).Fetch(
			context.Background(), claim, providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
			githubProjectV2TestClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC), nil,
		)
		if err != nil {
			t.Fatal(err)
		}
	})
	if len(lines) != 1 || lines[0]["level"] != "WARN" || lines[0]["items"] != float64(2) ||
		lines[0]["unit"] != claim.ID || lines[0]["org_id"] != claim.OrgID {
		t.Fatalf("status_value_time_missing lines=%v, want one WARN line with items=2 for the unit", lines)
	}
	if len(result.Rows.StatusTransitions) != 1 || result.Rows.StatusTransitions[0].WorkItemID != "gh:acme/api#9" {
		t.Fatalf("events=%+v, want only the well-formed item's", result.Rows.StatusTransitions)
	}
	statuses := map[string]string{}
	for _, row := range result.Rows.WorkItems {
		statuses[row.WorkItemID] = row.Status
	}
	if statuses["gh:acme/api#7"] != "in_progress" || statuses["gh:acme/api#8"] != "in_progress" || statuses["gh:acme/api#9"] != "done" {
		t.Fatalf("statuses=%v", statuses)
	}
	// Section 1 of the rule: no board event = the item keeps today's status,
	// even next to a newer repository close.
	repository := chaos14184Repository(t, chaos14184ClosedIssue, chaos14184CloseEvent)
	board := chaos14184Board(t, chaos14184Item("CLOSED", "2026-10-03T00:00:00Z", "In Progress", ""), chaos14184Now())
	if merged := chaos14184Merge(t, repository, board); merged.WorkItems[0].Status != "in_progress" {
		t.Fatalf("status=%q, want the board value in_progress", merged.WorkItems[0].Status)
	}
}

// N1: a handler built without the stored reader is refused before any request.
func TestChaos14184RouteRefusesAnUnwiredStoredReader(t *testing.T) {
	claim := githubWorkItemsRESTClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	doer := &githubWorkItemsRouteDoer{
		t:    t,
		rest: &githubWorkItemsRESTDoer{t: t, replies: githubWorkItemsRESTFixtures()},
	}
	batch, err := (GitHubWorkItemsRouteHandler{
		Projects: GitHubProjectV2Fetcher{}, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{},
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"), time.Now().UTC(),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v want ErrInvalidConfiguration", err)
	}
	if !reflect.DeepEqual(batch, CompleteRouteBatch{}) {
		t.Fatalf("misconstructed route returned batch=%+v", batch)
	}
	if doer.graphqlCalls != 0 || len(doer.rest.requests) != 0 {
		t.Fatalf("misconstructed route spent requests: graphql=%d rest=%d", doer.graphqlCalls, len(doer.rest.requests))
	}
}

// S1: the board's Status value time is asked and decoded; the items query asks
// no ProjectV2Item.changes (GitHub does not serve it).
func TestChaos14184StatusValueTimeIsAskedAndDecoded(t *testing.T) {
	var value gitHubProjectV2FieldValuePayload
	if err := json.Unmarshal([]byte(`{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"Done",
		"updatedAt":"2026-10-05T00:00:00Z","field":{"name":"Status"}}`), &value); err != nil {
		t.Fatal(err)
	}
	if value.UpdatedAt == nil || *value.UpdatedAt != "2026-10-05T00:00:00Z" {
		t.Fatalf("decoded updatedAt=%v", value.UpdatedAt)
	}
	if !strings.Contains(gitHubProjectV2FieldValueNodeFields, "ProjectV2ItemFieldSingleSelectValue { name updatedAt ") {
		t.Fatal("the single-select fragment does not ask updatedAt")
	}
	if strings.Contains(gitHubProjectsV2ItemsQuery, "changes(") {
		t.Fatal("items query asks ProjectV2Item.changes, absent on live GitHub")
	}
}

func chaos14184EmptyRows() githubWorkItemRows {
	return githubWorkItemRows{WorkItems: []githubWorkItemRow{}, StatusTransitions: []githubWorkItemTransitionRow{}}
}
