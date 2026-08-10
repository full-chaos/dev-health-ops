package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestLinearRawEffectAdaptersFenceProviderBeforeDelegating(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	rows := LinearWorkItemEffectRows{
		Dependencies: []json.RawMessage{json.RawMessage(`{"source_work_item_id":"ghpr:acme/repo#9","target_work_item_id":"linear:ENG-42","relationship_type":"relates_to","relationship_type_raw":"linear_attachment","relationship_semantics_version":"canonical-blocks.v2","org_id":"org-acme"}`)},
		ReopenEvents: []json.RawMessage{json.RawMessage(`{"work_item_id":"linear:ENG-42","occurred_at":"2026-07-27T11:00:00Z","from_status":"done","to_status":"in_progress","org_id":"org-acme"}`)},
		Interactions: []json.RawMessage{json.RawMessage(`{"work_item_id":"linear:ENG-42","provider":"github","interaction_type":"comment","occurred_at":"2026-07-27T12:00:00Z","body_length":1,"org_id":"org-acme"}`)},
		Sprints:      []json.RawMessage{json.RawMessage(`{"provider":"github","sprint_id":"linear:cycle:7","name":"Cycle 7","state":"active","org_id":"org-acme"}`)},
	}
	effects, err := BuildLinearWorkItemEffects(rows)
	if err != nil {
		t.Fatal(err)
	}
	byDestination := make(map[string]EffectBatch, len(effects))
	for _, effect := range effects {
		byDestination[effect.Destination] = effect
	}
	identity, err := newLinearWorkItemEffectIdentity(claim, byDestination["work_item_dependencies"])
	if err != nil {
		t.Fatal(err)
	}
	dependencyAdapter := LinearWorkItemDependenciesClickHouseAdapter{}
	foreign := identity
	foreign.Provider = "github"
	if err := dependencyAdapter.WriteLinearWorkItemEffect(context.Background(), foreign, byDestination["work_item_dependencies"]); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign provider dependency error=%v", err)
	}
	interactionIdentity, err := newLinearWorkItemEffectIdentity(claim, byDestination["work_item_interactions"])
	if err != nil {
		t.Fatal(err)
	}
	if err := (LinearWorkItemInteractionsClickHouseAdapter{}).WriteLinearWorkItemEffect(context.Background(), interactionIdentity, byDestination["work_item_interactions"]); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign row interaction error=%v", err)
	}
	sprintIdentity, err := newLinearWorkItemEffectIdentity(claim, byDestination["sprints"])
	if err != nil {
		t.Fatal(err)
	}
	if err := (LinearSprintsClickHouseAdapter{}).WriteLinearWorkItemEffect(context.Background(), sprintIdentity, byDestination["sprints"]); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign row sprint error=%v", err)
	}
}
