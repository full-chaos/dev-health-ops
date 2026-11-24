package providersync

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
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

// The implied close sits at the last relation-change node before the move out of Duplicate; without one it takes the
// instant of that move (closed span 0 s) and still sorts before the reopen. A history that opens in Duplicate has no
// earlier state to close from.
func TestLinearImpliedDuplicateCloseTimeAndShape14568(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("linear", "work-items")
	start := `{"id":"h1","createdAt":"2026-09-20T10:00:00.000Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"}}`
	out := `{"id":"h9","createdAt":"2026-09-30T10:00:00.000Z","fromState":{"name":"Duplicate","type":"duplicate"},"toState":{"name":"In Progress","type":"started"}}`
	relation := func(id, at string) string {
		return `{"id":"` + id + `","createdAt":"` + at + `","fromState":null,"toState":null,"actor":{"name":"Ann","email":"ann@example.test"},"relationChanges":[{"identifier":"ENG-900","type":"ad"}]}`
	}
	cases := []struct {
		name, history string
		wantAt        time.Time
		wantRows      int
		wantActor     bool
	}{
		{"last relation node wins", "[" + start + "," + relation("h2", "2026-09-25T10:00:00.000Z") + "," + relation("h3", "2026-09-27T10:00:00.000Z") + "," + out + "]",
			time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC), 3, true},
		{"no relation node falls back to 1 ms before the move out", "[" + start + "," + out + "]",
			time.Date(2026, 9, 30, 9, 59, 59, 999_000_000, time.UTC), 3, false},
		{"relation node before an earlier state entry is not reused", "[" + relation("h0", "2026-09-10T10:00:00.000Z") + "," + start + "," + out + "]",
			time.Date(2026, 9, 30, 9, 59, 59, 999_000_000, time.UTC), 3, false},
		{"history opening in Duplicate has nothing to close", "[" + out + "]", time.Time{}, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := linearCloseReproPayload14568(t, "ENG-7", "In Progress", "started", tc.history)
			_, transitions, err := normalizeLinearWorkItem(claim, payload, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(transitions) != tc.wantRows {
				t.Fatalf("transitions=%d, want %d: %+v", len(transitions), tc.wantRows, transitions)
			}
			if tc.wantRows == 1 {
				return
			}
			implied := transitions[1]
			if !implied.OccurredAt.Equal(tc.wantAt) || implied.ToStatus != "canceled" || implied.FromStatus != "in_progress" ||
				implied.ToStatusRaw == nil || *implied.ToStatusRaw != "Duplicate" || implied.FromStatusRaw == nil || *implied.FromStatusRaw != "In Progress" {
				t.Fatalf("implied row=%+v, want In Progress -> Duplicate at %v", implied, tc.wantAt)
			}
			if (implied.Actor != nil) != tc.wantActor {
				t.Fatalf("implied actor=%v, want set=%v", implied.Actor, tc.wantActor)
			}
			rows := rankTransitionTies(projectRows(transitions, projectLinearTransition))
			if sortKeyOf := transitionSortingKey(rows[1]); sortKeyOf == transitionSortingKey(rows[2]) {
				t.Fatalf("implied close and reopen share a key %q", sortKeyOf)
			}
			if rows[1].ToStatusRaw != "Duplicate" || rows[1].TieOrdinal >= rows[2].TieOrdinal && transitionInstantKey(rows[1]) == transitionInstantKey(rows[2]) {
				t.Fatalf("close must rank before the reopen: %+v / %+v", rows[1], rows[2])
			}
		})
	}
}

// Only a move out of a duplicate-type state implies a close; a gap before any other state is left alone.
func TestLinearNoImpliedCloseOutsideDuplicate14568(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("linear", "work-items")
	payload := linearCloseReproPayload14568(t, "ENG-7", "In Progress", "started", `[
		{"id":"h1","createdAt":"2026-09-20T10:00:00.000Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"}},
		{"id":"h2","createdAt":"2026-09-25T10:00:00.000Z","fromState":null,"toState":null,"relationChanges":[{"identifier":"ENG-900","type":"ar"}]},
		{"id":"h3","createdAt":"2026-09-30T10:00:00.000Z","fromState":{"name":"Done","type":"completed"},"toState":{"name":"In Progress","type":"started"}}]`)
	_, transitions, err := normalizeLinearWorkItem(claim, payload, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 2 {
		t.Fatalf("transitions=%d, want 2 (no implied row for a gap before a non-duplicate state): %+v", len(transitions), transitions)
	}
}

func linearHistoryNodes14568(t *testing.T, history string) []string {
	t.Helper()
	var nodes []json.RawMessage
	if err := json.Unmarshal([]byte(history), &nodes); err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(nodes))
	for index, node := range nodes {
		out[index] = string(node)
	}
	return out
}

func linearTransitionSet14568(rows []linearWorkItemTransitionRow) []string {
	set := make([]string, 0, len(rows))
	for _, row := range rows {
		actor, from, to := "", "", ""
		if row.Actor != nil {
			actor = *row.Actor
		}
		if row.FromStatusRaw != nil {
			from = *row.FromStatusRaw
		}
		if row.ToStatusRaw != nil {
			to = *row.ToStatusRaw
		}
		set = append(set, fmt.Sprintf("%s|%s|%s|%s|%s|%s", row.OccurredAt.Format(time.RFC3339Nano), from, to, row.FromStatus, row.ToStatus, actor))
	}
	sort.Strings(set)
	return set
}

// The walk must not depend on the served order of the history.
func TestLinearImpliedDuplicateCloseIgnoresServedOrder14568(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("linear", "work-items")
	start := `{"id":"h1","createdAt":"2026-09-20T10:00:00.000Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"}}`
	relation := func(id, at string) string {
		return `{"id":"` + id + `","createdAt":"` + at + `","fromState":null,"toState":null,"actor":{"name":"Ann","email":"ann@example.test"},"relationChanges":[{"identifier":"ENG-900","type":"ad"}]}`
	}
	out := `{"id":"h3","createdAt":"2026-09-30T10:00:00.000Z","fromState":{"name":"Duplicate","type":"duplicate"},"toState":{"name":"In Progress","type":"started"}}`
	served := `{"id":"h2b","createdAt":"2026-09-25T10:00:00.300Z","fromState":{"name":"In Progress","type":"started"},"toState":{"name":"Duplicate","type":"duplicate"}}`
	cases := []struct{ name, history string }{
		{"ad", "[" + start + "," + relation("h2", "2026-09-25T10:00:00.000Z") + "," + out + "]"},
		{"ar-ad-twice", "[" + start + "," + relation("h2", "2026-09-25T10:00:00.000Z") +
			`,{"id":"h2c","createdAt":"2026-09-26T10:00:00.000Z","fromState":{"name":"Duplicate","type":"duplicate"},"toState":{"name":"In Progress","type":"started"}},` +
			relation("h4", "2026-09-28T10:00:00.000Z") + "," + out + "]"},
		{"control-served-node", "[" + start + "," + relation("h2", "2026-09-25T10:00:00.000Z") + "," + served + "," + out + "]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nodes := linearHistoryNodes14568(t, tc.history)
			want := func(history string) []string {
				payload := linearCloseReproPayload14568(t, "ENG-7", "In Progress", "started", history)
				_, transitions, err := normalizeLinearWorkItem(claim, payload, now)
				if err != nil {
					t.Fatal(err)
				}
				return linearTransitionSet14568(transitions)
			}
			ascending := want(tc.history)
			reversed := make([]string, len(nodes))
			for index, node := range nodes {
				reversed[len(nodes)-1-index] = node
			}
			if got := want("[" + strings.Join(reversed, ",") + "]"); !slices.Equal(got, ascending) {
				t.Fatalf("reversed order gives %v, want %v", got, ascending)
			}
			// the relation node served after the move out of Duplicate
			if tc.name == "ad" {
				interleaved := "[" + nodes[0] + "," + nodes[2] + "," + nodes[1] + "]"
				if got := want(interleaved); !slices.Equal(got, ascending) {
					t.Fatalf("interleaved order gives %v, want %v", got, ascending)
				}
				if !strings.Contains(strings.Join(ascending, "\n"), "2026-09-25T10:00:00Z|In Progress|Duplicate") {
					t.Fatalf("implied close not at the relation node time: %v", ascending)
				}
			}
		})
	}
}

// An entry whose createdAt does not parse sorts after every stamped entry.
func TestLinearHistoryAscendingPlacesUnparseableLast14568(t *testing.T) {
	history := []linearHistoryEntry{{ID: "c", CreatedAt: "bad"}, {ID: "b", CreatedAt: "2026-02-01T00:00:00Z"}, {ID: "a", CreatedAt: "2026-01-01T00:00:00Z"}, {ID: "b2", CreatedAt: "2026-02-01T00:00:00Z"}}
	got := linearHistoryAscending(history)
	ids := []string{got[0].ID, got[1].ID, got[2].ID, got[3].ID}
	if !slices.Equal(ids, []string{"a", "b", "b2", "c"}) || history[0].ID != "c" {
		t.Fatalf("order=%v (input head %q), want a b b2 c and an untouched input", ids, history[0].ID)
	}
}

func linearRows14568(t *testing.T, history string) []linearWorkItemTransitionRow {
	t.Helper()
	payload := linearCloseReproPayload14568(t, "ENG-7", "In Progress", "started", history)
	_, transitions, err := normalizeLinearWorkItem(nativeTestClaim("linear", "work-items"), payload, time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return transitions
}

const (
	linearStart14568 = `{"id":"h1","createdAt":"2026-09-20T10:00:00.000Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"}}`
	linearRel14568   = `{"id":"hr","createdAt":"2026-09-30T10:00:00.000Z","fromState":null,"toState":null,"relationChanges":[{"identifier":"ENG-900","type":"ad"}]}`
)

func linearExit14568(at, toName, toType string) string {
	return `{"id":"hx","createdAt":"` + at + `","fromState":{"name":"Duplicate","type":"duplicate"},"toState":{"name":"` + toName + `","type":"` + toType + `"}}`
}

func linearEnter14568(id, at, fromName, fromType, toName, toType string) string {
	return `{"id":"` + id + `","createdAt":"` + at + `","fromState":{"name":"` + fromName + `","type":"` + fromType + `"},"toState":{"name":"` + toName + `","type":"` + toType + `"}}`
}

// O1(a): a previous state that already maps to canceled gets no implied canceled -> canceled row; a previous Done keeps its
// done -> canceled row.
func TestLinearImpliedCloseSkipsOnlyCanceledToCanceled14568(t *testing.T) {
	for _, tc := range []struct{ name, exitName, exitType string }{
		{"canceled to backlog", "Backlog", "backlog"},
		{"canceled to triage", "Triage", "triage"},
		{"canceled to in progress", "In Progress", "started"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := linearRows14568(t, "["+linearStart14568+","+linearEnter14568("h2", "2026-09-25T10:00:00.000Z", "In Progress", "started", "Canceled", "canceled")+","+
				linearExit14568("2026-09-30T10:00:00.000Z", tc.exitName, tc.exitType)+"]")
			if len(rows) != 3 {
				t.Fatalf("rows=%d, want 3 (no implied canceled -> canceled row): %+v", len(rows), rows)
			}
		})
	}
	t.Run("done keeps its implied row (control)", func(t *testing.T) {
		rows := linearRows14568(t, "["+linearStart14568+","+linearEnter14568("h2", "2026-09-25T10:00:00.000Z", "In Progress", "started", "Done", "completed")+","+
			linearExit14568("2026-09-30T10:00:00.000Z", "Backlog", "backlog")+"]")
		if len(rows) != 4 || rows[2].FromStatus != "done" || rows[2].ToStatus != "canceled" {
			t.Fatalf("rows=%+v, want an implied done -> canceled row", rows)
		}
	})
}

// O1(b): without a relation entry the close is stamped 1 ms before the exit (occurred_at is DateTime64(3),
// internal/chmigrate/history/009_raw_work_items.sql:33), clamped to stay strictly after the entry into the previous state;
// when no such millisecond exists the close takes the exit instant (the tie order may then rank it after the reopen: named limit).
func TestLinearFallbackCloseIsOneMillisecondBeforeExit14568(t *testing.T) {
	exitAt := "2026-09-30T10:00:00.000Z"
	for _, tc := range []struct {
		name, previousAt string
		want             time.Time
	}{
		{"previous far before", "2026-09-25T10:00:00.000Z", time.Date(2026, 9, 30, 9, 59, 59, 999_000_000, time.UTC)},
		{"previous 2 ms before", "2026-09-30T09:59:59.998Z", time.Date(2026, 9, 30, 9, 59, 59, 999_000_000, time.UTC)},
		{"previous 1 ms before: no free millisecond (control)", "2026-09-30T09:59:59.999Z", time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)},
		{"previous at the exit instant (control)", exitAt, time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := linearRows14568(t, "["+linearEnter14568("h1", tc.previousAt, "Todo", "unstarted", "In Progress", "started")+","+linearExit14568(exitAt, "Backlog", "backlog")+"]")
			if len(rows) != 3 || !rows[1].OccurredAt.Equal(tc.want) || rows[1].ToStatus != "canceled" {
				t.Fatalf("rows=%+v, want the implied close at %v", rows, tc.want)
			}
		})
	}
}

// C3: any relationChanges entry counts, whatever its type: the rule does not read Linear's undocumented change-type codes.
// WANT: the close takes the instant of the last relation entry in between, even a non-duplicate one.
func TestLinearCloseTakesAnyRelationEntryInBetween14568(t *testing.T) {
	other := `{"id":"hr2","createdAt":"2026-09-27T10:00:00.000Z","fromState":null,"toState":null,"relationChanges":[{"identifier":"ENG-901","type":"related"}]}`
	rel := strings.Replace(linearRel14568, "2026-09-30T10:00:00.000Z", "2026-09-25T10:00:00.000Z", 1)
	rows := linearRows14568(t, "["+linearStart14568+","+rel+","+other+","+linearExit14568("2026-09-30T10:00:00.000Z", "In Progress", "started")+"]")
	want := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	if len(rows) != 3 || !rows[1].OccurredAt.Equal(want) {
		t.Fatalf("rows=%+v, want the close at %v", rows, want)
	}
}

// O2 + C4: at one instant a relation entry sorts before a state entry whatever the served order; state-state ties keep served order.
func TestLinearRelationStateTieIgnoresServedOrder14568(t *testing.T) {
	exit := linearExit14568("2026-09-30T10:00:00.000Z", "Backlog", "backlog")
	intoPrevious := linearEnter14568("h1b", "2026-09-25T10:00:00.000Z", "In Progress", "started", "In Review", "started")
	relAtPrevious := strings.Replace(linearRel14568, "2026-09-30T10:00:00.000Z", "2026-09-25T10:00:00.000Z", 1)
	withActor := strings.Replace(linearRel14568, `"fromState":null`, `"actor":{"name":"Ann","email":"ann@example.test"},"fromState":null`, 1)
	for _, tc := range []struct {
		name         string
		a, b         string
		withExit     bool
		wantCloseAt  time.Time
		wantActorSet bool
	}{
		{"relation and exit", withActor, exit, false, time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC), true},
		{"relation and the entry into the previous state", relAtPrevious, intoPrevious, true, time.Date(2026, 9, 30, 9, 59, 59, 999_000_000, time.UTC), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			build := func(first, second string) []linearWorkItemTransitionRow {
				history := "[" + linearStart14568 + "," + first + "," + second
				if tc.withExit {
					history += "," + exit
				}
				return linearRows14568(t, history+"]")
			}
			ab, ba := build(tc.a, tc.b), build(tc.b, tc.a)
			if !slices.Equal(linearTransitionSet14568(ab), linearTransitionSet14568(ba)) {
				t.Fatalf("served order changes the rows:\n%v\n%v", linearTransitionSet14568(ab), linearTransitionSet14568(ba))
			}
			var implied *linearWorkItemTransitionRow
			for index := range ab {
				if ab[index].ToStatusRaw != nil && *ab[index].ToStatusRaw == "Duplicate" {
					implied = &ab[index]
				}
			}
			if implied == nil || !implied.OccurredAt.Equal(tc.wantCloseAt) || (implied.Actor != nil) != tc.wantActorSet {
				t.Fatalf("implied=%+v, want the close at %v with actor set=%v", implied, tc.wantCloseAt, tc.wantActorSet)
			}
		})
	}
}
