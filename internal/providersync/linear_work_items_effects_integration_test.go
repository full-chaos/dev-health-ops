//go:build integration

package providersync

import (
	"encoding/json"
	"errors"
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

	dependency := linearWorkItemDependencyRow{
		SourceWorkItemID: "ghpr:acme/repo#9", TargetWorkItemID: item.WorkItemID,
		RelationshipType: "relates_to", RelationshipTypeRaw: "linear_attachment",
		RelationshipSemanticsVersion: "canonical-blocks.v2", LastSynced: now, OrgID: claim.OrgID,
	}
	dependencyEffect := linearIntegrationEffect(t, "work_item_dependencies", dependency)
	dependencyIdentity, err := newLinearWorkItemEffectIdentity(claim, dependencyEffect)
	if err != nil {
		t.Fatal(err)
	}
	dependencyAdapter := LinearWorkItemDependenciesClickHouseAdapter{
		Delegate: GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn},
	}
	if err := dependencyAdapter.WriteLinearWorkItemEffect(ctx, dependencyIdentity, dependencyEffect); err != nil {
		t.Fatal(err)
	}
	if err := dependencyAdapter.WriteLinearWorkItemEffect(ctx, dependencyIdentity, dependencyEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := dependencyAdapter.InspectLinearWorkItemEffect(ctx, dependencyIdentity, dependencyEffect); err != nil || inspection != EffectExact {
		t.Fatalf("dependency inspection=%s error=%v", inspection, err)
	}

	reopen := linearWorkItemReopenRow{
		WorkItemID: item.WorkItemID, OccurredAt: now, FromStatus: "done", ToStatus: "in_progress",
		FromStatusRaw: stringPtr("Done"), ToStatusRaw: stringPtr("In Progress"),
		Actor: stringPtr("alice@example.com"), LastSynced: now, OrgID: claim.OrgID,
	}
	reopenEffect := linearIntegrationEffect(t, "work_item_reopen_events", reopen)
	reopenIdentity, err := newLinearWorkItemEffectIdentity(claim, reopenEffect)
	if err != nil {
		t.Fatal(err)
	}
	reopenAdapter := LinearWorkItemReopenEventsClickHouseAdapter{
		Delegate: GitHubWorkItemReopenEventsClickHouseAdapter{Conn: conn},
	}
	if err := reopenAdapter.WriteLinearWorkItemEffect(ctx, reopenIdentity, reopenEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := reopenAdapter.InspectLinearWorkItemEffect(ctx, reopenIdentity, reopenEffect); err != nil || inspection != EffectExact {
		t.Fatalf("reopen inspection=%s error=%v", inspection, err)
	}

	interaction := linearWorkItemInteractionRow{
		WorkItemID: item.WorkItemID, Provider: "linear", InteractionType: "comment",
		OccurredAt: now, Actor: stringPtr("alice@example.com"), BodyLength: 12,
		LastSynced: now, OrgID: claim.OrgID,
	}
	interactionEffect := linearIntegrationEffect(t, "work_item_interactions", interaction)
	interactionIdentity, err := newLinearWorkItemEffectIdentity(claim, interactionEffect)
	if err != nil {
		t.Fatal(err)
	}
	interactionAdapter := LinearWorkItemInteractionsClickHouseAdapter{
		Delegate: GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn},
	}
	if err := interactionAdapter.WriteLinearWorkItemEffect(ctx, interactionIdentity, interactionEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := interactionAdapter.InspectLinearWorkItemEffect(ctx, interactionIdentity, interactionEffect); err != nil || inspection != EffectExact {
		t.Fatalf("interaction inspection=%s error=%v", inspection, err)
	}

	sprint := linearSprintRow{
		Provider: "linear", SprintID: "linear:cycle:7", Name: stringPtr("Sprint 7"),
		State: stringPtr("active"), NativeTeamKey: stringPtr("ENG"),
		StartedAt: &now, LastSynced: now, OrgID: claim.OrgID,
	}
	sprintEffect := linearIntegrationEffect(t, "sprints", sprint)
	sprintIdentity, err := newLinearWorkItemEffectIdentity(claim, sprintEffect)
	if err != nil {
		t.Fatal(err)
	}
	sprintAdapter := LinearSprintsClickHouseAdapter{
		Delegate: GitHubSprintsClickHouseAdapter{Conn: conn},
	}
	if err := sprintAdapter.WriteLinearWorkItemEffect(ctx, sprintIdentity, sprintEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sprintAdapter.InspectLinearWorkItemEffect(ctx, sprintIdentity, sprintEffect); err != nil || inspection != EffectExact {
		t.Fatalf("sprint inspection=%s error=%v", inspection, err)
	}

	foreignClaim := claim
	foreignClaim.OrgID = "org-other"
	foreignIdentity, err := newLinearWorkItemEffectIdentity(foreignClaim, itemEffect)
	if err != nil {
		t.Fatal(err)
	}
	if inspection, err := itemAdapter.InspectLinearWorkItemEffect(ctx, foreignIdentity, itemEffect); !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
		t.Fatalf("foreign tenant inspection=%s error=%v", inspection, err)
	}
}

func stringPtr(value string) *string { return &value }

func floatPtr(value float64) *float64 { return &value }

func timePtr(value time.Time) *time.Time { return &value }

func linearIntegrationEffect(t *testing.T, destination string, row any) EffectBatch {
	t.Helper()
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch(destination, EffectReadbackRequired, []json.RawMessage{raw})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}
