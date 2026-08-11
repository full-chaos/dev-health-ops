//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// This test uses the shared migration-backed ClickHouse fixture. It authors no
// DDL: every table and column is supplied by the real ClickHouse migration
// chain, and all sixteen adapters used by the route are exercised through the
// provider-owned composite dispatcher. The ten derived effects are empty here
// because their non-empty schema projections are covered by the companion
// derived integration suite.
func TestGitLabWorkItemEffectsComposeAllSixteenAgainstRealClickHouse(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	now := time.Date(2026, 8, 3, 12, 0, 0, 123000000, time.UTC)
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	item := gitlabWorkItemRow{
		WorkItemID: "gitlab:acme/api#42", Provider: "gitlab", Title: "ClickHouse proof",
		Type: "issue", Status: "in_progress", StatusRaw: gitlabStringPtr("opened"),
		Description: gitlabStringPtr("raw facts"), RepoID: &repoID, ProjectID: gitlabStringPtr("acme/api"),
		Assignees: []string{"alice@example.com"}, Reporter: gitlabStringPtr("bob@example.com"),
		CreatedAt: now, UpdatedAt: now, StartedAt: &now, Labels: []string{"bug"},
		StoryPoints: gitlabFloatPtr(3), SprintID: gitlabStringPtr("7"), SprintName: gitlabStringPtr("July"),
		URL: gitlabStringPtr("https://gitlab.example/acme/api/-/issues/42"), OrgID: claim.OrgID, LastSynced: now,
	}
	transition := gitlabWorkItemTransitionRow{
		WorkItemID: item.WorkItemID, Provider: "gitlab", OccurredAt: now,
		FromStatusRaw: gitlabStringPtr("opened"), ToStatusRaw: gitlabStringPtr("done"),
		FromStatus: "unknown", ToStatus: "done", Actor: gitlabStringPtr("alice@example.com"),
		OrgID: claim.OrgID, LastSynced: now,
	}
	dependency := gitlabWorkItemDependencyRow{
		SourceWorkItemID: item.WorkItemID, TargetWorkItemID: "gitlab:acme/api#7",
		RelationshipType: "blocks", RelationshipTypeRaw: "blocks",
		RelationshipSemanticsVersion: "canonical-blocks.v2", LastSynced: now, OrgID: claim.OrgID,
	}
	reopen := gitlabWorkItemReopenRow{
		WorkItemID: item.WorkItemID, OccurredAt: now, FromStatus: "done", ToStatus: "in_progress",
		FromStatusRaw: gitlabStringPtr("closed"), ToStatusRaw: gitlabStringPtr("reopened"),
		Actor: gitlabStringPtr("alice@example.com"), LastSynced: now, OrgID: claim.OrgID,
	}
	interaction := gitlabWorkItemInteractionRow{
		WorkItemID: item.WorkItemID, Provider: "gitlab", InteractionType: "comment", OccurredAt: now,
		Actor: gitlabStringPtr("alice@example.com"), BodyLength: 9, LastSynced: now, OrgID: claim.OrgID,
	}
	sprint := gitlabSprintRow{
		Provider: "gitlab", SprintID: "gitlab:acme/api:milestone:7", Name: gitlabStringPtr("July"),
		State: gitlabStringPtr("active"), StartedAt: &now, EndedAt: gitlabTimePtr(now.Add(24 * time.Hour)),
		LastSynced: now, OrgID: claim.OrgID,
	}
	effects, err := buildGitLabWorkItemEffectsFromRows(gitlabWorkItemRows{
		WorkItems: []gitlabWorkItemRow{item}, StatusTransitions: []gitlabWorkItemTransitionRow{transition},
		Dependencies: []gitlabWorkItemDependencyRow{dependency}, ReopenEvents: []gitlabWorkItemReopenRow{reopen},
		Interactions: []gitlabWorkItemInteractionRow{interaction}, Sprints: []gitlabSprintRow{sprint},
	})
	if err != nil {
		t.Fatal(err)
	}
	derived, err := BuildGitLabWorkItemDerivedEffects(GitLabWorkItemDerivedEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	effects = append(effects, derived...)
	if len(effects) != 16 {
		t.Fatalf("composed effects=%d want=16", len(effects))
	}
	sink, err := NewGitLabWorkItemFamilyClickHouseEffects(
		conn, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range effects {
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("write %s: %v", effect.Destination, err)
		}
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("replay %s: %v", effect.Destination, err)
		}
		inspection, err := sink.InspectEffect(ctx, claim, effect)
		wantInspection := EffectExact
		if len(effect.Rows) == 0 {
			wantInspection = EffectAbsent
		}
		if err != nil || inspection != wantInspection {
			t.Fatalf("inspect %s: %s %v", effect.Destination, inspection, err)
		}
	}
	foreign := claim
	foreign.OrgID = "org-other"
	if inspection, err := sink.InspectEffect(ctx, foreign, effects[0]); inspection != EffectConflict || err == nil {
		t.Fatalf("foreign tenant inspection=%s err=%v", inspection, err)
	}
}

func gitlabFloatPtr(value float64) *float64    { return &value }
func gitlabTimePtr(value time.Time) *time.Time { return &value }
