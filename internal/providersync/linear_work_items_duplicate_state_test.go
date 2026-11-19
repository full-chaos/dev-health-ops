package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

var linearDuplicateNow = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

func normalizeLinearDuplicateFixture(t *testing.T, history string) (linearWorkItemRow, []linearWorkItemTransitionRow, []linearHistoryEntry) {
	t.Helper()
	var payload linearWorkItemPayload
	if err := json.Unmarshal([]byte(`{"id":"l1","identifier":"ENG-1","title":"T",
		"createdAt":"2026-11-16T02:48:59Z","updatedAt":"2026-11-17T21:33:38Z",
		"canceledAt":"2026-11-17T21:33:36.950Z",
		"state":{"name":"Duplicate","type":"duplicate"},
		"history":{"nodes":`+history+`,"pageInfo":{"hasNextPage":false,"endCursor":""}}}`), &payload); err != nil {
		t.Fatal(err)
	}
	row, transitions, err := normalizeLinearWorkItem(nativeTestClaim("linear", "work-items"), payload, linearDuplicateNow)
	if err != nil {
		t.Fatal(err)
	}
	return row, transitions, payload.History.Nodes
}

func TestLinearStatusMapsDuplicateStateTypeToCanceled(t *testing.T) {
	for _, stateType := range []string{"duplicate", "Duplicate", " DUPLICATE "} {
		if got := linearStatus(stateType); got != "canceled" {
			t.Errorf("linearStatus(%q) = %q, want canceled", stateType, got)
		}
	}
}

// Linear marks a duplicate by moving the issue to Canceled, then to the
// Duplicate state (type duplicate) about a second later; the history holds the
// move to Canceled and no node for the move to Duplicate.
func TestLinearDuplicateItemAgreesWithItsLastCanceledTransition(t *testing.T) {
	row, transitions, _ := normalizeLinearDuplicateFixture(t, `[
		{"id":"h1","createdAt":"2026-11-17T20:02:45.443Z","fromState":{"name":"Backlog","type":"backlog"},"toState":{"name":"In Progress","type":"started"}},
		{"id":"h2","createdAt":"2026-11-17T21:33:36.960Z","fromState":{"name":"In Progress","type":"started"},"toState":{"name":"Canceled","type":"canceled"}}]`)
	if len(transitions) != 2 {
		t.Fatalf("transitions=%d, want 2", len(transitions))
	}
	last := transitions[len(transitions)-1]
	if row.Status != "canceled" || last.ToStatus != row.Status {
		t.Fatalf("item status=%q, last transition to=%q: a Duplicate item must be canceled and agree with its last transition", row.Status, last.ToStatus)
	}
	if row.ClosedAt == nil || row.CompletedAt != nil {
		t.Fatalf("closed_at=%v completed_at=%v, want closed_at = canceledAt and completed_at NULL", row.ClosedAt, row.CompletedAt)
	}
}

func TestLinearTransitionIntoDuplicateStateIsCanceled(t *testing.T) {
	row, transitions, _ := normalizeLinearDuplicateFixture(t, `[
		{"id":"h1","createdAt":"2026-11-18T09:27:39.366Z","fromState":{"name":"Backlog","type":"backlog"},"toState":{"name":"Duplicate","type":"duplicate"}}]`)
	if len(transitions) != 1 || transitions[0].ToStatus != "canceled" || transitions[0].FromStatus != "backlog" || row.Status != "canceled" {
		t.Fatalf("transitions=%+v item=%q, want backlog -> canceled on a canceled item", transitions, row.Status)
	}
	if raw := transitions[0].ToStatusRaw; raw == nil || *raw != "Duplicate" {
		t.Fatalf("to_status_raw=%v, want Duplicate", raw)
	}
}

func TestLinearMoveOutOfDuplicateStateIsAReopen(t *testing.T) {
	_, _, history := normalizeLinearDuplicateFixture(t, `[
		{"id":"h1","createdAt":"2026-11-18T09:27:39.366Z","fromState":{"name":"Backlog","type":"backlog"},"toState":{"name":"Duplicate","type":"duplicate"}},
		{"id":"h2","createdAt":"2026-11-18T10:00:00Z","fromState":{"name":"Duplicate","type":"duplicate"},"toState":{"name":"Todo","type":"unstarted"}}]`)
	reopens := normalizeLinearReopens(nativeTestClaim("linear", "work-items"), "linear:ENG-1", history, linearDuplicateNow)
	if len(reopens) != 1 || reopens[0].FromStatus != "canceled" || reopens[0].ToStatus != "todo" {
		t.Fatalf("reopens=%+v, want one canceled -> todo reopen from Duplicate", reopens)
	}
}

func TestLinearStatusMapsTriageStateTypeToBacklog(t *testing.T) {
	for _, stateType := range []string{"triage", "Triage"} {
		if got := linearStatus(stateType); got != "backlog" {
			t.Errorf("linearStatus(%q) = %q, want backlog", stateType, got)
		}
	}
}

func TestLinearMoveFromClosedIntoTriageIsAReopen(t *testing.T) {
	var history []linearHistoryEntry
	if err := json.Unmarshal([]byte(`[
		{"id":"h1","createdAt":"2026-11-18T09:00:00Z","fromState":{"name":"In Progress","type":"started"},"toState":{"name":"Done","type":"completed"}},
		{"id":"h2","createdAt":"2026-11-18T10:00:00Z","fromState":{"name":"Done","type":"completed"},"toState":{"name":"Triage","type":"triage"}}]`), &history); err != nil {
		t.Fatal(err)
	}
	reopens := normalizeLinearReopens(nativeTestClaim("linear", "work-items"), "linear:ENG-1", history, linearDuplicateNow)
	if len(reopens) != 1 || reopens[0].FromStatus != "done" || !reopens[0].OccurredAt.Equal(time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("reopens=%+v, want one reopen at the move from Done into Triage", reopens)
	}
}
