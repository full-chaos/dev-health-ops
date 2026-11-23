package providersync

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// CHAOS-14190 red cells. The work_item_transitions rows of a Linear item must hold the close that every reopen of the
// item proves: a reopen from a closed state means the item entered that state, so some transition in
// [previous reopen, reopen] goes to done or canceled (the read-back rule that counts missing closes).

func linearCloseReproPayload(t *testing.T, identifier, state, stateType, history string) linearWorkItemPayload {
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

// readBackCloseSlack mirrors the close window of the read-back rule (FORM-14190-READBACK-1 F2): a close counts for a
// reopen when it lies in [previous reopen, reopen + readBackCloseSlack]. Today the window ends at the reopen (0).
const readBackCloseSlack = 0 * time.Millisecond

// linearReadBackMissingCloses returns, per reopen, whether the read-back rule finds its close in the transitions, and
// how many transitions in that window go to done or canceled.
func linearReadBackMissingCloses(transitions []linearWorkItemTransitionRow, reopens []linearWorkItemReopenRow) (missing []time.Time, closesInWindow []int) {
	previous := time.Time{}
	for _, reopen := range reopens {
		closes := 0
		for _, transition := range transitions {
			inWindow := !transition.OccurredAt.Before(previous) && !transition.OccurredAt.After(reopen.OccurredAt.Add(readBackCloseSlack))
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
			payload := linearCloseReproPayload(t, "ENG-7", "In Progress", "started", tc.history)
			item, transitions, err := normalizeLinearWorkItem(claim, payload, now)
			if err != nil {
				t.Fatal(err)
			}
			reopens := normalizeLinearReopens(claim, item.WorkItemID, payload.History.Nodes, now)
			if len(reopens) != tc.reopens {
				t.Fatalf("reopens=%+v, want %d from Duplicate", reopens, tc.reopens)
			}
			missing, closes := linearReadBackMissingCloses(transitions, reopens)
			if len(missing) > 0 {
				t.Errorf("reopen(s) at %v from Duplicate: no transition in the read-back window goes to done or canceled; transitions=%+v", missing, transitions)
			}
			if tc.control && (len(closes) != 1 || closes[0] != 1) {
				t.Errorf("served node into Duplicate: closes in the window %v, want exactly one (no double write)", closes)
			}
		})
	}
}

// Linear can stamp a reopen BEFORE the close it reopens: "Done -> In Progress" at T+0.552 s and "In Progress -> Done"
// at T+0.559 s, with no earlier node into Done. The writer stores three rows at three instants (distinct keys) and one
// reopen at .552; the read-back rule looks for the close in [previous reopen, reopen] and the close sits 7 ms later.
// The proposed fix is the read-back rule (readBackCloseSlack 1 s, here and in the FORM text): until then this is red.
func TestLinearReopenStampedBeforeItsCloseIsReadAsClosed(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("linear", "work-items")
	payload := linearCloseReproPayload(t, "ENG-8", "In Progress", "started", `[
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
	if missing, _ := linearReadBackMissingCloses(transitions, reopens); len(missing) > 0 {
		t.Errorf("reopen at %v: the read-back rule (window [previous reopen, reopen + %s]) finds no close; the close is stored 7 ms after the reopen",
			missing, readBackCloseSlack)
	}
}

// A close and its reopen at one instant (the same millisecond) are two rows of one sorting key prefix, told apart by
// tie_ordinal. The executors split each destination of an emission into sub-chunks of DefaultChunkPolicy rows, and the
// Linear sink ranks the ties of each sub-chunk it is handed (WriteLinearWorkItemEffect -> rankTransitionTies): when the
// two rows land in two sub-chunks, both take ordinal 0, share one sorting key, and the table keeps one of them.
func TestLinearSameInstantCloseAndReopenKeepTwoKeysAcrossEffectChunks(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("linear", "work-items")
	start := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	var filler []string
	for index := 0; index < 98; index++ {
		from, to := `{"name":"Todo","type":"unstarted"}`, `{"name":"In Progress","type":"started"}`
		if index%2 == 1 {
			from, to = to, from
		}
		filler = append(filler, fmt.Sprintf(`{"id":"f%d","createdAt":"%s","fromState":%s,"toState":%s}`,
			index, start.Add(time.Duration(index)*time.Minute).Format(time.RFC3339Nano), from, to))
	}
	payloads := []linearWorkItemPayload{
		linearCloseReproPayload(t, "AAA-1", "Todo", "unstarted", "["+strings.Join(filler, ",")+"]"),
		linearCloseReproPayload(t, "ZZZ-5", "Todo", "unstarted", `[
			{"id":"z1","createdAt":"2026-10-01T10:00:00.000Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"Backlog","type":"backlog"}},
			{"id":"z2","createdAt":"2026-10-15T10:00:00.050Z","fromState":{"name":"Backlog","type":"backlog"},"toState":{"name":"Canceled","type":"canceled"}},
			{"id":"z3","createdAt":"2026-10-15T10:00:00.050Z","fromState":{"name":"Canceled","type":"canceled"},"toState":{"name":"Todo","type":"unstarted"}}]`),
	}
	rows := linearWorkItemRows{}
	for _, payload := range payloads {
		item, transitions, err := normalizeLinearWorkItem(claim, payload, now)
		if err != nil {
			t.Fatal(err)
		}
		rows.WorkItems = append(rows.WorkItems, item)
		rows.StatusTransitions = append(rows.StatusTransitions, transitions...)
	}
	effects, err := buildLinearWorkItemEffectsFromRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := SplitCompleteRouteBatch(CompleteRouteBatch{Effects: effects}, DefaultChunkPolicy())
	if err != nil {
		t.Fatal(err)
	}
	instant := time.Date(2026, 10, 15, 10, 0, 0, 50_000_000, time.UTC)
	pairChunks := map[int]bool{}
	keys := map[string][]string{}
	for index, chunk := range chunks {
		for _, effect := range chunk.Effects {
			if effect.Destination != "work_item_transitions" || len(effect.Rows) == 0 {
				continue
			}
			decoded, err := decodeEffectRows[linearWorkItemTransitionRow](effect)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rankTransitionTies(projectRows(decoded, projectLinearTransition)) {
				if row.WorkItemID == "linear:ZZZ-5" && row.OccurredAt.Equal(instant) {
					pairChunks[index] = true
				}
				key := transitionSortingKey(row)
				keys[key] = append(keys[key], row.FromStatus+"->"+row.ToStatus)
			}
		}
	}
	if len(pairChunks) != 2 {
		t.Fatalf("setup: the close and the reopen of linear:ZZZ-5 sit in chunks %v of %d, want two chunks", pairChunks, len(chunks))
	}
	for key, members := range keys {
		if len(members) > 1 {
			t.Errorf("sorting key %q holds %d rows %v: the table keeps one of them", key, len(members), members)
		}
	}
}
