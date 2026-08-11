package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
	"github.com/google/uuid"
)

type recordingGitLabWorkItemAdapter struct {
	writes      int
	inspections int
	identity    GitLabWorkItemEffectIdentity
}

func (adapter *recordingGitLabWorkItemAdapter) WriteGitLabWorkItemEffect(
	_ context.Context, identity GitLabWorkItemEffectIdentity, _ EffectBatch,
) error {
	adapter.writes++
	adapter.identity = identity
	return nil
}

func (adapter *recordingGitLabWorkItemAdapter) InspectGitLabWorkItemEffect(
	_ context.Context, identity GitLabWorkItemEffectIdentity, _ EffectBatch,
) (EffectInspection, error) {
	adapter.inspections++
	adapter.identity = identity
	return EffectExact, nil
}

func recordingGitLabWorkItemSink(lease providerfoundation.LeaseGuard) (*GitLabWorkItemClickHouseEffects, *recordingGitLabWorkItemAdapter) {
	adapter := &recordingGitLabWorkItemAdapter{}
	return &GitLabWorkItemClickHouseEffects{
		Lease: lease, WorkItems: adapter, StatusTransitions: adapter, Dependencies: adapter,
		ReopenEvents: adapter, Interactions: adapter, Sprints: adapter,
	}, adapter
}

func TestBuildGitLabWorkItemEffectsIncludesExactlySixRawDestinations(t *testing.T) {
	effects, err := BuildGitLabWorkItemEffects(GitLabWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != 6 {
		t.Fatalf("effects=%d", len(effects))
	}
	for index, destination := range gitLabWorkItemRawDestinations {
		if effects[index].Destination != destination || effects[index].Recovery != EffectReadbackRequired || effects[index].Rows == nil {
			t.Fatalf("effect[%d]=%+v", index, effects[index])
		}
	}
	if len(gitLabWorkItemDerivedGap) != 10 {
		t.Fatalf("derived gap=%v", gitLabWorkItemDerivedGap)
	}
}

func TestGitLabWorkItemEffectsFenceTenantGenerationAndLease(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	effects, err := BuildGitLabWorkItemEffects(GitLabWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	leaseCalls := 0
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error {
		leaseCalls++
		return nil
	})
	sink, adapter := recordingGitLabWorkItemSink(lease)
	if err := sink.WriteEffect(context.Background(), claim, effects[0]); err != nil {
		t.Fatal(err)
	}
	if adapter.writes != 1 || adapter.identity.OrgID != claim.OrgID ||
		adapter.identity.Provider != "gitlab" || adapter.identity.Dataset != "work-items" ||
		adapter.identity.Generation != claim.GenerationKey() || adapter.identity.RowCount != 0 || leaseCalls != 2 {
		t.Fatalf("adapter=%+v lease_calls=%d", adapter, leaseCalls)
	}
	if inspection, err := sink.InspectEffect(context.Background(), claim, effects[0]); err != nil || inspection != EffectExact {
		t.Fatalf("inspection=%s error=%v", inspection, err)
	}
	if adapter.inspections != 1 || leaseCalls != 3 {
		t.Fatalf("inspection_calls=%d lease_calls=%d", adapter.inspections, leaseCalls)
	}

	tampered := effects[0]
	tampered.ContentDigest = "0000000000000000000000000000000000000000000000000000000000000000"
	if err := sink.WriteEffect(context.Background(), claim, tampered); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("tampered digest error=%v", err)
	}
	foreignClaim := claim
	foreignClaim.OrgID = "org-other"
	if err := sink.WriteEffect(context.Background(), foreignClaim, effects[0]); err != nil {
		t.Fatalf("empty effect cannot prove row tenant mismatch, but identity should remain scoped: %v", err)
	}
	if adapter.identity.OrgID != foreignClaim.OrgID {
		t.Fatalf("foreign identity=%+v", adapter.identity)
	}
}

func TestGitLabWorkItemEffectsRejectLeaseLossAndDirectAliases(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	effects, err := BuildGitLabWorkItemEffects(GitLabWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	leaseLost := providerfoundation.LeaseGuardFunc(func(context.Context) error {
		return providerfoundation.ErrLeaseLost
	})
	sink, _ := recordingGitLabWorkItemSink(leaseLost)
	if err := sink.WriteEffect(context.Background(), claim, effects[0]); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("lease error=%v", err)
	}
	for _, dataset := range workitemcontract.FamilyDatasets()[1:] {
		alias := effects[0]
		alias.Destination = dataset
		if _, err := newGitLabWorkItemEffectIdentity(claim, alias); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("alias %s identity error=%v", dataset, err)
		}
	}
}

func TestGitLabWorkItemAdaptersRejectCrossTenantRowsBeforeClickHouse(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	row := gitlabWorkItemRow{
		WorkItemID: "gitlab:acme/api#42", Provider: "gitlab", Title: "tenant fence",
		Type: "issue", Status: "todo", RepoID: stringUUIDForTest(t, "c7198fbc-1945-3717-05d8-eb78866b4e79"),
		ProjectID: gitlabStringPtr("acme/api"), Assignees: []string{}, Labels: []string{},
		CreatedAt: now, UpdatedAt: now, OrgID: claim.OrgID, LastSynced: now,
	}
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch("work_items", EffectReadbackRequired, []json.RawMessage{raw})
	if err != nil {
		t.Fatal(err)
	}
	foreign := claim
	foreign.OrgID = "org-other"
	identity, err := newGitLabWorkItemEffectIdentity(foreign, effect)
	if err != nil {
		t.Fatal(err)
	}
	adapter := GitLabWorkItemsClickHouseAdapter{}
	if err := adapter.WriteGitLabWorkItemEffect(context.Background(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("cross-tenant write error=%v", err)
	}
}

func gitlabStringPtr(value string) *string { return &value }

func stringUUIDForTest(t *testing.T, value string) *uuid.UUID {
	t.Helper()
	parsed, err := uuid.Parse(value)
	if err != nil {
		t.Fatal(err)
	}
	return &parsed
}
