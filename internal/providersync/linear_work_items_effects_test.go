package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestBuildLinearWorkItemEffectsIsDeterministicAndExhaustive(t *testing.T) {
	t.Parallel()
	first, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems: []json.RawMessage{
			json.RawMessage(`{"org_id":"org-acme","work_item_id":"linear:ENG-2"}`),
			json.RawMessage(`{"org_id":"org-acme","work_item_id":"linear:ENG-1"}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems: []json.RawMessage{
			json.RawMessage(`{"org_id":"org-acme","work_item_id":"linear:ENG-1"}`),
			json.RawMessage(`{"org_id":"org-acme","work_item_id":"linear:ENG-2"}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("effect construction is order-sensitive:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if len(first) != 8 || first[0].Destination != "work_items" ||
		first[1].Destination != "work_item_transitions" ||
		first[2].Destination != "work_item_dependencies" ||
		first[3].Destination != "work_item_reopen_events" ||
		first[4].Destination != "work_item_interactions" ||
		first[5].Destination != "sprints" ||
		first[6].Destination != "project_membership_transitions" ||
		first[7].Destination != "projects" {
		t.Fatalf("effects=%+v", first)
	}
	for index, effect := range first {
		if effect.Recovery != EffectReadbackRequired || !validDigest(effect.ContentDigest) {
			t.Fatalf("effect[%d]=%+v", index, effect)
		}
	}
	if len(first[0].Rows) != 2 || len(first[1].Rows) != 0 || len(first[2].Rows) != 0 ||
		len(first[3].Rows) != 0 || len(first[4].Rows) != 0 || len(first[5].Rows) != 0 ||
		len(first[6].Rows) != 0 || len(first[7].Rows) != 0 {
		t.Fatalf("empty destination was dropped: %+v", first)
	}
	if _, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems: []json.RawMessage{json.RawMessage(`[]`)},
	}); !errors.Is(err, ErrEffectRecoveryUnsafe) {
		t.Fatalf("invalid row error=%v", err)
	}
}

func TestLinearWorkItemEffectsFenceLeaseTenantAndTampering(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("linear", "work-items")
	effects, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems: []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","provider":"linear","work_item_id":"linear:ENG-1"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := newLinearSemanticEffectBackend()
	sink := linearWorkItemEffectsFixture(backend, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))

	tampered := effects[0]
	tampered.PayloadBytes++
	if err := sink.WriteEffect(context.Background(), claim, tampered); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("tampered payload error=%v", err)
	}
	tampered = effects[0]
	tampered.ContentDigest = effects[1].ContentDigest
	if err := sink.WriteEffect(context.Background(), claim, tampered); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("tampered digest error=%v", err)
	}
	incomplete := sink
	incomplete.StatusTransitions = nil
	if err := incomplete.WriteEffect(context.Background(), claim, effects[0]); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("missing adapter error=%v", err)
	}
	foreignRow, err := BuildEffectBatch("work_items", EffectReadbackRequired, []json.RawMessage{
		json.RawMessage(`{"org_id":"org-other","provider":"linear","work_item_id":"linear:ENG-1"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(context.Background(), claim, foreignRow); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign row error=%v", err)
	}
	if len(backend.identities) != 0 {
		t.Fatalf("backend called before fail-closed validation: %+v", backend.identities)
	}
	if err := sink.WriteEffect(context.Background(), claim, effects[0]); err != nil {
		t.Fatal(err)
	}
	foreignClaim := claim
	foreignClaim.OrgID = "org-other"
	inspection, err := sink.InspectEffect(context.Background(), foreignClaim, effects[0])
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign tenant inspection=%s error=%v", inspection, err)
	}
	staleClaim := claim
	staleClaim.ID = "77777777-7777-4777-8777-777777777777"
	inspection, err = sink.InspectEffect(context.Background(), staleClaim, effects[0])
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign generation inspection=%s error=%v", inspection, err)
	}

	leaseLost := providerfoundation.LeaseGuardFunc(func(context.Context) error {
		return providerfoundation.ErrLeaseLost
	})
	leaseSink := linearWorkItemEffectsFixture(backend, leaseLost)
	if err := leaseSink.WriteEffect(context.Background(), claim, effects[0]); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("lease loss error=%v", err)
	}

	postWriteBackend := newLinearSemanticEffectBackend()
	postWriteGuard := &secondAssertionLosesLease{}
	postWriteSink := linearWorkItemEffectsFixture(postWriteBackend, postWriteGuard)
	if err := postWriteSink.WriteEffect(context.Background(), claim, effects[0]); !errors.Is(err, providerfoundation.ErrLeaseLost) ||
		postWriteGuard.calls != 2 || postWriteBackend.writeCounts["work_items"] != 1 {
		t.Fatalf("post-write lease error=%v calls=%d writes=%v", err, postWriteGuard.calls, postWriteBackend.writeCounts)
	}
}

func TestLinearWorkItemEffectsRecoverAfterMidWrite(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("linear", "work-items")
	effects, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems:         []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","provider":"linear","work_item_id":"linear:ENG-1"}`)},
		StatusTransitions: []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","provider":"linear","work_item_id":"linear:ENG-1","occurred_at":"2026-07-30T11:00:00Z","from_status":"todo","to_status":"in_progress"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := newLinearSemanticEffectBackend()
	backend.failAfterWrite = "work_item_transitions"
	backend.failure = errors.New("simulated death after durable write")
	ledger := &memoryEffectLedger{}
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	firstSink := linearWorkItemEffectsFixture(backend, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	_, err = (EffectCommitter{
		Ledger: ledger, Sink: firstSink, Readback: firstSink,
		Now: func() time.Time { return now },
	}).Commit(context.Background(), claim, effects, now)
	if err == nil || backend.writeCounts["work_item_transitions"] != 1 {
		t.Fatalf("first commit error=%v writes=%v", err, backend.writeCounts)
	}

	backend.failAfterWrite = ""
	secondSink := linearWorkItemEffectsFixture(backend, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	result, err := (EffectCommitter{
		Ledger: ledger, Sink: secondSink, Readback: secondSink,
		Now: func() time.Time { return now.Add(time.Minute) },
	}).Commit(context.Background(), claim, effects, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.MarkedCommitted != 1 || backend.writeCounts["work_item_transitions"] != 1 ||
		backend.writeCounts["work_items"] != 1 {
		t.Fatalf("recovery result=%+v writes=%v", result, backend.writeCounts)
	}
}

func TestLinearWorkItemEffectsDispatchEveryRawDestination(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	effects, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems:          []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","provider":"linear","work_item_id":"linear:ENG-1"}`)},
		StatusTransitions:  []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","provider":"linear","work_item_id":"linear:ENG-1","occurred_at":"2026-07-30T11:00:00Z","from_status":"todo","to_status":"in_progress"}`)},
		Dependencies:       []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","source_work_item_id":"ghpr:acme/repo#9","target_work_item_id":"linear:ENG-1","relationship_type":"relates_to","relationship_type_raw":"linear_attachment","relationship_semantics_version":"canonical-blocks.v2"}`)},
		ReopenEvents:       []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","work_item_id":"linear:ENG-1","occurred_at":"2026-07-30T11:00:00Z","from_status":"done","to_status":"in_progress"}`)},
		Interactions:       []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","provider":"linear","work_item_id":"linear:ENG-1","interaction_type":"comment","occurred_at":"2026-07-30T11:00:00Z","body_length":4}`)},
		Sprints:            []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","provider":"linear","sprint_id":"linear:cycle:7","name":"Cycle 7","state":"active"}`)},
		ProjectMemberships: []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","provider":"linear","subject_kind":"work_item","subject_id":"linear:ENG-1","repo_id":"00000000-0000-0000-0000-000000000000","from_project_id":"","to_project_id":"project-platform","from_project_key":"","to_project_key":"","actor":"","occurred_at":"2026-07-30T11:00:00Z","last_synced":"2026-07-30T11:00:00Z","event_id":"linear:hist-1"}`)},
		Projects:           []json.RawMessage{json.RawMessage(`{"id":"project-platform","org_id":"org-acme","provider":"linear","project_key":"","name":"Platform","is_active":1,"updated_at":"2026-07-30T11:00:00Z","last_synced":"2026-07-30T11:00:00Z"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := newLinearSemanticEffectBackend()
	sink := linearWorkItemEffectsFixture(backend, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	for _, effect := range effects {
		if len(effect.Rows) == 0 {
			continue
		}
		if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
			t.Fatalf("destination %s write error=%v", effect.Destination, err)
		}
	}
	for _, destination := range linearWorkItemEffectDestinations {
		if backend.writeCounts[destination] != 1 {
			t.Fatalf("destination %s writes=%v", destination, backend.writeCounts)
		}
	}
}

func TestLinearDirectAdaptersFenceTenantAndMirrorSinkColumnOwnership(t *testing.T) {
	t.Parallel()
	if containsColumn(linearWorkItemsInsert, "description") ||
		containsColumn(linearWorkItemsInsert, "priority_raw") ||
		containsColumn(linearWorkItemsInsert, "service_class") ||
		containsColumn(linearWorkItemsInsert, "due_at") {
		t.Fatal("work_items adapter writes columns the Python sink does not own")
	}
	if containsColumn(linearWorkItemTransitionsInsert, "provider") {
		t.Fatal("transition adapter writes provider, which the Python sink omits")
	}
	for destination, query := range map[string]string{
		"work_items":            linearWorkItemsSelect,
		"work_item_transitions": linearWorkItemTransitionsSelect,
	} {
		if !containsColumn(query, "org_id = ?") || !containsColumn(query, "FINAL") {
			t.Fatalf("%s readback is not tenant/version fenced: %s", destination, query)
		}
	}

	claim := nativeTestClaim("linear", "work-items")
	workEffect, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems: []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","provider":"linear","work_item_id":"linear:ENG-1"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := newLinearWorkItemEffectIdentity(claim, workEffect[0])
	if err != nil {
		t.Fatal(err)
	}
	adapter := LinearWorkItemsClickHouseAdapter{}
	if err := adapter.WriteLinearWorkItemEffect(context.Background(), identity, workEffect[0]); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("non-empty write without ClickHouse connection error=%v", err)
	}
	if _, err := adapter.InspectLinearWorkItemEffect(context.Background(), identity, workEffect[0]); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("non-empty readback without ClickHouse connection error=%v", err)
	}
	emptyEffect := workEffect[0]
	emptyEffect.Rows = nil
	emptyEffect, err = BuildEffectBatch(emptyEffect.Destination, emptyEffect.Recovery, nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyIdentity, err := newLinearWorkItemEffectIdentity(claim, emptyEffect)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.WriteLinearWorkItemEffect(context.Background(), emptyIdentity, emptyEffect); err != nil {
		t.Fatalf("empty write should not need connection: %v", err)
	}
	inspection, err := adapter.InspectLinearWorkItemEffect(context.Background(), emptyIdentity, emptyEffect)
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("empty readback=%s error=%v", inspection, err)
	}
}

type linearSemanticEffectBackend struct {
	rows           map[string]string
	writeCounts    map[string]int
	identities     []LinearWorkItemEffectIdentity
	failAfterWrite string
	failure        error
}

func newLinearSemanticEffectBackend() *linearSemanticEffectBackend {
	return &linearSemanticEffectBackend{
		rows: make(map[string]string), writeCounts: make(map[string]int),
	}
}

func (backend *linearSemanticEffectBackend) key(identity LinearWorkItemEffectIdentity) string {
	return identity.OrgID + "\x00" + identity.Generation + "\x00" + identity.Destination
}

func (backend *linearSemanticEffectBackend) WriteLinearWorkItemEffect(
	_ context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) error {
	for _, raw := range effect.Rows {
		var row struct {
			OrgID    string `json:"org_id"`
			Provider string `json:"provider"`
		}
		if err := json.Unmarshal(raw, &row); err != nil ||
			row.OrgID != identity.OrgID || (row.Provider != "" && row.Provider != "linear") {
			return ErrInvalidConfiguration
		}
	}
	backend.identities = append(backend.identities, identity)
	backend.rows[backend.key(identity)] = effect.ContentDigest
	backend.writeCounts[identity.Destination]++
	if backend.failAfterWrite == identity.Destination {
		return backend.failure
	}
	return nil
}

func (backend *linearSemanticEffectBackend) InspectLinearWorkItemEffect(
	_ context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	backend.identities = append(backend.identities, identity)
	digest, ok := backend.rows[backend.key(identity)]
	if !ok {
		return EffectAbsent, nil
	}
	if digest != effect.ContentDigest {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

func linearWorkItemEffectsFixture(
	backend LinearWorkItemEffectAdapter,
	lease providerfoundation.LeaseGuard,
) LinearWorkItemClickHouseEffects {
	return LinearWorkItemClickHouseEffects{
		Lease: lease,
		WorkItems: linearDestinationCheckingAdapter{
			destination: "work_items", delegate: backend,
		},
		StatusTransitions: linearDestinationCheckingAdapter{
			destination: "work_item_transitions", delegate: backend,
		},
		Dependencies: linearDestinationCheckingAdapter{
			destination: "work_item_dependencies", delegate: backend,
		},
		ReopenEvents: linearDestinationCheckingAdapter{
			destination: "work_item_reopen_events", delegate: backend,
		},
		Interactions: linearDestinationCheckingAdapter{
			destination: "work_item_interactions", delegate: backend,
		},
		Sprints: linearDestinationCheckingAdapter{
			destination: "sprints", delegate: backend,
		},
		ProjectMemberships: linearDestinationCheckingAdapter{
			destination: "project_membership_transitions", delegate: backend,
		},
		Projects: linearDestinationCheckingAdapter{
			destination: "projects", delegate: backend,
		},
	}
}

type linearDestinationCheckingAdapter struct {
	destination string
	delegate    LinearWorkItemEffectAdapter
}

func (adapter linearDestinationCheckingAdapter) WriteLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) error {
	if identity.Destination != adapter.destination || effect.Destination != adapter.destination {
		return ErrInvalidConfiguration
	}
	return adapter.delegate.WriteLinearWorkItemEffect(ctx, identity, effect)
}

func (adapter linearDestinationCheckingAdapter) InspectLinearWorkItemEffect(
	ctx context.Context, identity LinearWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	if identity.Destination != adapter.destination || effect.Destination != adapter.destination {
		return EffectConflict, ErrInvalidConfiguration
	}
	return adapter.delegate.InspectLinearWorkItemEffect(ctx, identity, effect)
}
