//go:build integration

package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

// This uses the shared chschema-backed ClickHouse fixture from the direct
// work-item integration suite. It authors no DDL: the real migration chain
// supplies both destination tables.
func TestLinearClickHouseAdaptersWriteAndReadBackTenantFencedRows(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("linear", "work-items")
	now := time.Date(2026, 8, 3, 12, 0, 0, 123000000, time.UTC)
	item := linearWorkItemRow{
		WorkItemID: "linear:ENG-100", Provider: "linear", Title: "ClickHouse proof",
		Type: "task", Status: "in_progress", StatusRaw: stringPtr("In Progress"),
		NativeTeamKey: stringPtr("ENG"), ProjectID: stringPtr("project-platform"),
		ProjectName: stringPtr("Platform"), Assignees: []string{"alice@example.com"},
		Reporter: stringPtr("bob@example.com"), CreatedAt: now, UpdatedAt: now,
		StartedAt: &now, Labels: []string{"delivery"}, StoryPoints: floatPtr(3),
		SprintID: stringPtr("linear:cycle:7"), SprintName: stringPtr("Sprint 7"),
		ParentID: stringPtr("linear:ENG-1"), URL: stringPtr("https://linear.app/ENG-100"),
		PriorityRaw: stringPtr("medium"), ServiceClass: stringPtr("standard"),
		DueAt: timePtr(now.Add(24 * time.Hour)), OrgID: claim.OrgID, LastSynced: now,
	}
	itemRaw, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	itemEffect, err := BuildEffectBatch("work_items", EffectReadbackRequired, []json.RawMessage{itemRaw})
	if err != nil {
		t.Fatal(err)
	}
	itemIdentity, err := newLinearWorkItemEffectIdentity(claim, itemEffect)
	if err != nil {
		t.Fatal(err)
	}
	itemAdapter := LinearWorkItemsClickHouseAdapter{Conn: conn}
	if err := itemAdapter.WriteLinearWorkItemEffect(ctx, itemIdentity, itemEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := itemAdapter.InspectLinearWorkItemEffect(ctx, itemIdentity, itemEffect); err != nil || inspection != EffectExact {
		t.Fatalf("work item inspection=%s error=%v", inspection, err)
	}

	transition := linearWorkItemTransitionRow{
		WorkItemID: item.WorkItemID, Provider: "linear", OccurredAt: now,
		FromStatusRaw: stringPtr("Todo"), ToStatusRaw: stringPtr("In Progress"),
		FromStatus: "todo", ToStatus: "in_progress", Actor: stringPtr("alice@example.com"),
		OrgID: claim.OrgID, LastSynced: now,
	}
	transitionRaw, err := json.Marshal(transition)
	if err != nil {
		t.Fatal(err)
	}
	transitionEffect, err := BuildEffectBatch("work_item_transitions", EffectReadbackRequired, []json.RawMessage{transitionRaw})
	if err != nil {
		t.Fatal(err)
	}
	transitionIdentity, err := newLinearWorkItemEffectIdentity(claim, transitionEffect)
	if err != nil {
		t.Fatal(err)
	}
	transitionAdapter := LinearWorkItemTransitionsClickHouseAdapter{Conn: conn}
	if err := transitionAdapter.WriteLinearWorkItemEffect(ctx, transitionIdentity, transitionEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := transitionAdapter.InspectLinearWorkItemEffect(ctx, transitionIdentity, transitionEffect); err != nil || inspection != EffectExact {
		t.Fatalf("transition inspection=%s error=%v", inspection, err)
	}

	foreignClaim := claim
	foreignClaim.OrgID = "org-other"
	foreignIdentity, err := newLinearWorkItemEffectIdentity(foreignClaim, itemEffect)
	if err != nil {
		t.Fatal(err)
	}
	if inspection, err := itemAdapter.InspectLinearWorkItemEffect(ctx, foreignIdentity, itemEffect); err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign tenant inspection=%s error=%v", inspection, err)
	}
}

func stringPtr(value string) *string { return &value }

func floatPtr(value float64) *float64 { return &value }

func timePtr(value time.Time) *time.Time { return &value }
