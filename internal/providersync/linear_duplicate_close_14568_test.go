package providersync

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// CHAOS-14568 red cell: a Linear duplicate marking carries only relationChanges, so the close is never written.

func linearCloseReproPayload14568(t *testing.T, identifier, state, stateType, history string) linearWorkItemPayload {
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

// readBackCloseSlack14568 mirrors the close window of the read-back rule (FORM-14190-READBACK-1 F2): a close counts for a
// reopen when it lies in [previous reopen, reopen + readBackCloseSlack14568]. Today the window ends at the reopen (0).
const readBackCloseSlack14568 = 0 * time.Millisecond

// linearReadBackMissingCloses14568 returns, per reopen, whether the read-back rule finds its close in the transitions, and
// how many transitions in that window go to done or canceled.
func linearReadBackMissingCloses14568(transitions []linearWorkItemTransitionRow, reopens []linearWorkItemReopenRow) (missing []time.Time, closesInWindow []int) {
	previous := time.Time{}
	for _, reopen := range reopens {
		closes := 0
		for _, transition := range transitions {
			inWindow := !transition.OccurredAt.Before(previous) && !transition.OccurredAt.After(reopen.OccurredAt.Add(readBackCloseSlack14568))
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

// Linear writes "mark as duplicate" as a relation-change history node with no states (relationChanges "ad", "ar"+"ad",
// or "am" about 0.3 s before the state moves) and no node for the move into the Duplicate state; the next state node
// moves the item out of Duplicate. The writer drops the stateless node (linear_work_items_route.go:1713, :1716), builds
// the reopen from the next node's fromState (:1145-1151), and writes no close. The control serves the node into
// Duplicate as well: one close, and it must stay one after the fix (no double write).
func TestLinearDuplicateMarkingRelationNodeHasItsCloseInTheTransitions(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("linear", "work-items")
	start := `{"id":"h1","createdAt":"2026-09-20T10:00:00.000Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"}}`
	relation := func(id, at string, types ...string) string {
		changes := make([]string, 0, len(types))
		for _, changeType := range types {
			changes = append(changes, fmt.Sprintf(`{"identifier":"ENG-900","type":%q}`, changeType))
		}
		return fmt.Sprintf(`{"id":%q,"createdAt":%q,"fromState":null,"toState":null,"relationChanges":[%s]}`, id, at, strings.Join(changes, ","))
	}
	outOfDuplicate := func(id, at string) string {
		return fmt.Sprintf(`{"id":%q,"createdAt":%q,"fromState":{"name":"Duplicate","type":"duplicate"},"toState":{"name":"In Progress","type":"started"}}`, id, at)
	}
	cases := []struct {
		name, history string
		reopens       int
		control       bool
	}{
		{"ad", "[" + start + "," + relation("h2", "2026-09-25T10:00:00.000Z", "ad") + "," + outOfDuplicate("h3", "2026-09-30T10:00:00.000Z") + "]", 1, false},
		{"ar-ad-twice", "[" + start + "," + relation("h2", "2026-09-25T10:00:00.000Z", "ar", "ad") + "," + outOfDuplicate("h3", "2026-09-26T10:00:00.000Z") +
			"," + relation("h4", "2026-09-28T10:00:00.000Z", "ar", "ad") + "," + outOfDuplicate("h5", "2026-09-30T10:00:00.000Z") + "]", 2, false},
		{"am", "[" + start + "," + relation("h2", "2026-09-25T10:00:00.000Z", "am") + "," + outOfDuplicate("h3", "2026-09-30T10:00:00.000Z") + "]", 1, false},
		{"control-served-node", "[" + start + "," + relation("h2", "2026-09-25T10:00:00.000Z", "ad") +
			`,{"id":"h2b","createdAt":"2026-09-25T10:00:00.300Z","fromState":{"name":"In Progress","type":"started"},"toState":{"name":"Duplicate","type":"duplicate"}},` +
			outOfDuplicate("h3", "2026-09-30T10:00:00.000Z") + "]", 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := linearCloseReproPayload14568(t, "ENG-7", "In Progress", "started", tc.history)
			item, transitions, err := normalizeLinearWorkItem(claim, payload, now)
			if err != nil {
				t.Fatal(err)
			}
			reopens := normalizeLinearReopens(claim, item.WorkItemID, payload.History.Nodes, now)
			if len(reopens) != tc.reopens {
				t.Fatalf("reopens=%+v, want %d from Duplicate", reopens, tc.reopens)
			}
			missing, closes := linearReadBackMissingCloses14568(transitions, reopens)
			if len(missing) > 0 {
				t.Errorf("reopen(s) at %v from Duplicate: no transition in the read-back window goes to done or canceled; transitions=%+v", missing, transitions)
			}
			if tc.control && (len(closes) != 1 || closes[0] != 1) {
				t.Errorf("served node into Duplicate: closes in the window %v, want exactly one (no double write)", closes)
			}
		})
	}
}
