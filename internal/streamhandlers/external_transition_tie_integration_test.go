//go:build integration

package streamhandlers

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// Named residual (ticket 14236, filed separately): an external work_item_transition.v1 row names no tie_ordinal, so
// it writes 0, and two different transitions a push sends for one item at one instant still share a key and
// collapse to one row after a merge, as on main.
func TestExternalTransitionsAtOneInstantStillCollapse(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC) }
	first := externalTransitionRecord("github")
	second := externalTransitionRecord("github")
	second.Index, second.ExternalID = 1, "t-github-2"
	second.Payload = map[string]any{
		"externalKey": "7", "provider": "github", "workItemType": "issue",
		"occurredAt": "2026-07-22T11:00:00Z", "fromStatus": "in_progress", "toStatus": "done",
	}
	pointer := projectMembershipPointer()
	pointer.SourceSystem = "github"
	if _, err := sink.Write(ctx, externalSinkBatch{
		Pointer: pointer, SourceID: uuid.MustParse("9749bda0-fc9f-4076-b19d-7b26c4f306ff"),
		Records: []externalSinkRecord{first, second},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := conn.Exec(ctx, `OPTIMIZE TABLE work_item_transitions FINAL`); err != nil {
		t.Fatal(err)
	}
	item := externalWorkItemID("github", externalWorkItemInstance("github", pointer.SourceInstance, ""), "7", "issue")
	var rows uint64
	var ordinals string
	if err := conn.QueryRow(ctx, `SELECT count(), toString(groupUniqArray(tie_ordinal)) FROM work_item_transitions WHERE org_id = ? AND work_item_id = ?`,
		projectMembershipTestOrg, item).Scan(&rows, &ordinals); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || ordinals != "[0]" {
		t.Fatalf("external tie: %d rows, ordinals %s; want 1 row at ordinal 0 (residual 14236)", rows, ordinals)
	}
}
