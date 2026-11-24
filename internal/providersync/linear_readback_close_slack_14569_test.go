package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

func linearCloseReproPayload14569(t *testing.T, identifier, state, stateType, history string) linearWorkItemPayload {
	t.Helper()
	var payload linearWorkItemPayload
	if err := json.Unmarshal([]byte(`{"id":"id-`+identifier+`","identifier":"`+identifier+`","title":"T",
		"createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-11-01T00:00:00Z",
		"state":{"name":"`+state+`","type":"`+stateType+`"},
		"history":{"nodes":`+history+`,"pageInfo":{"hasNextPage":false,"endCursor":""}}}`), &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

// readBackCloseSlack14569 mirrors the close window of the read-back rule (FORM-14190-READBACK-1 F2): a close counts for a
// reopen when it lies in [previous reopen, reopen + readBackCloseSlack14569]. Today the window ends at the reopen (0).
const readBackCloseSlack14569 = 0 * time.Millisecond

// linearReadBackMissingCloses14569 returns, per reopen, whether the read-back rule finds its close in the transitions, and
// how many transitions in that window go to done or canceled.
func linearReadBackMissingCloses14569(transitions []linearWorkItemTransitionRow, reopens []linearWorkItemReopenRow) (missing []time.Time, closesInWindow []int) {
	previous := time.Time{}
	for _, reopen := range reopens {
		closes := 0
		for _, transition := range transitions {
			inWindow := !transition.OccurredAt.Before(previous) && !transition.OccurredAt.After(reopen.OccurredAt.Add(readBackCloseSlack14569))
			sameRaw := transition.ToStatusRaw != nil && reopen.FromStatusRaw != nil && *transition.ToStatusRaw == *reopen.FromStatusRaw
			if inWindow && (transition.ToStatus == "done" || transition.ToStatus == "canceled" || sameRaw) {
				closes++
			}
		}
		if closes == 0 {
			missing = append(missing, reopen.OccurredAt)
		}
		closesInWindow = append(closesInWindow, closes)
		previous = reopen.OccurredAt
	}
	return missing, closesInWindow
}

// Linear can stamp a reopen BEFORE the close it reopens: "Done -> In Progress" at T+0.552 s and "In Progress -> Done"
// at T+0.559 s, with no earlier node into Done. The writer stores three rows at three instants (distinct keys) and one
// reopen at .552; the read-back rule looks for the close in [previous reopen, reopen] and the close sits 7 ms later.
// The proposed fix is the read-back rule (readBackCloseSlack14569 1 s, here and in the FORM text): until then this is red.
func TestLinearReopenStampedBeforeItsCloseIsReadAsClosed(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("linear", "work-items")
	payload := linearCloseReproPayload14569(t, "ENG-8", "In Progress", "started", `[
		{"id":"h1","createdAt":"2026-10-01T10:00:00.000Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"}},
		{"id":"h2","createdAt":"2026-10-15T10:00:00.552Z","fromState":{"name":"Done","type":"completed"},"toState":{"name":"In Progress","type":"started"}},
		{"id":"h3","createdAt":"2026-10-15T10:00:00.559Z","fromState":{"name":"In Progress","type":"started"},"toState":{"name":"Done","type":"completed"}}]`)
	item, transitions, err := normalizeLinearWorkItem(claim, payload, now)
	if err != nil {
		t.Fatal(err)
	}
	reopens := normalizeLinearReopens(claim, item.WorkItemID, payload.History.Nodes, now)
	reopenAt := time.Date(2026, 10, 15, 10, 0, 0, 552_000_000, time.UTC)
	if len(reopens) != 1 || !reopens[0].OccurredAt.Equal(reopenAt) {
		t.Fatalf("reopens=%+v, want one at .552", reopens)
	}
	keys := map[string]bool{}
	for _, row := range rankTransitionTies(projectRows(transitions, projectLinearTransition)) {
		keys[transitionSortingKey(row)] = true
	}
	if len(transitions) != 3 || len(keys) != 3 {
		t.Fatalf("stored %d rows under %d keys, want 3 rows under 3 keys", len(transitions), len(keys))
	}
	for _, row := range transitions {
		t.Logf("stored %s %s -> %s", row.OccurredAt.Format(time.RFC3339Nano), row.FromStatus, row.ToStatus)
	}
	if missing, _ := linearReadBackMissingCloses14569(transitions, reopens); len(missing) > 0 {
		t.Errorf("reopen at %v: the read-back rule (window [previous reopen, reopen + %s]) finds no close; the close is stored 7 ms after the reopen",
			missing, readBackCloseSlack14569)
	}
}
