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

// Linear's history can hold no node for a move into a closed-type state (recorded: the move to Duplicate,
// linear_work_items_duplicate_state_test.go). Here no node moves the item into Canceled; the next node moves it out of
// Canceled. The reopen is written from that node's fromState alone, the close it proves is written nowhere.
func TestLinearReopenFromCanceledHasItsCloseInTheTransitions(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("linear", "work-items")
	payload := linearCloseReproPayload(t, "ENG-7", "Todo", "unstarted", `[
		{"id":"h1","createdAt":"2026-09-20T10:00:00.000Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"}},
		{"id":"h2","createdAt":"2026-09-30T10:00:00.000Z","fromState":{"name":"Canceled","type":"canceled"},"toState":{"name":"Todo","type":"unstarted"}}]`)
	item, transitions, err := normalizeLinearWorkItem(claim, payload, now)
	if err != nil {
		t.Fatal(err)
	}
	reopens := normalizeLinearReopens(claim, item.WorkItemID, payload.History.Nodes, now)
	if len(reopens) != 1 || reopens[0].FromStatus != "canceled" {
		t.Fatalf("reopens=%+v, want one reopen from Canceled", reopens)
	}
	previous := time.Time{}
	for _, reopen := range reopens {
		found := false
		for _, transition := range transitions {
			inWindow := !transition.OccurredAt.Before(previous) && !transition.OccurredAt.After(reopen.OccurredAt)
			sameRaw := transition.ToStatusRaw != nil && reopen.FromStatusRaw != nil && *transition.ToStatusRaw == *reopen.FromStatusRaw
			if inWindow && (transition.ToStatus == "done" || transition.ToStatus == "canceled" || sameRaw) {
				found = true
			}
		}
		if !found {
			t.Errorf("reopen at %s from %s: no transition in [%s, %s] goes to done or canceled; transitions=%+v",
				reopen.OccurredAt.Format(time.RFC3339Nano), reopen.FromStatus, previous.Format(time.RFC3339Nano),
				reopen.OccurredAt.Format(time.RFC3339Nano), transitions)
		}
		previous = reopen.OccurredAt
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
