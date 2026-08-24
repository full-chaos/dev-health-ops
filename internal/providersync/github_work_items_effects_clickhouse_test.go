package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestBuildGitHubWorkItemEffectsIsDeterministicAndExhaustive(t *testing.T) {
	t.Parallel()
	firstRows := GitHubWorkItemEffectRows{
		WorkItems: []json.RawMessage{
			json.RawMessage(`{"org_id":"org-acme","work_item_id":"gh:acme/api#2"}`),
			json.RawMessage(`{"org_id":"org-acme","work_item_id":"gh:acme/api#1"}`),
		},
		Sprints: []json.RawMessage{
			json.RawMessage(`{"org_id":"org-acme","sprint_id":"7"}`),
		},
	}
	secondRows := firstRows
	secondRows.WorkItems = slices.Clone(firstRows.WorkItems)
	slices.Reverse(secondRows.WorkItems)

	first, err := BuildGitHubWorkItemEffects(firstRows)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildGitHubWorkItemEffects(secondRows)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("effect construction is order-sensitive:\nfirst=%+v\nsecond=%+v", first, second)
	}
	wantDestinations := githubWorkItemRouteDestinations()
	if len(first) != len(wantDestinations) {
		t.Fatalf("effects=%d want=%d", len(first), len(wantDestinations))
	}
	seen := make(map[string]bool, len(first))
	for index, effect := range first {
		if effect.Destination != wantDestinations[index] {
			t.Fatalf("effect[%d]=%q want=%q", index, effect.Destination, wantDestinations[index])
		}
		if seen[effect.Destination] {
			t.Fatalf("duplicate destination %q", effect.Destination)
		}
		seen[effect.Destination] = true
		if effect.Recovery != EffectReadbackRequired || !validDigest(effect.ContentDigest) {
			t.Fatalf("effect[%d]=%+v", index, effect)
		}
	}
	if len(first[1].Rows) != 0 {
		t.Fatalf("conditional-empty destination was dropped or populated: %+v", first[1])
	}
	if len(first[5].Rows) != 1 || first[5].Destination != "sprints" {
		t.Fatalf("sprint effect=%+v", first[5])
	}
}

func TestBuildGitHubWorkItemEffectsRejectsInvalidRows(t *testing.T) {
	t.Parallel()
	_, err := BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{
		WorkItems: []json.RawMessage{json.RawMessage(`[]`)},
	})
	if !errors.Is(err, ErrEffectRecoveryUnsafe) {
		t.Fatalf("error=%v", err)
	}
}

func TestGitHubWorkItemEffectsFailClosedForTamperingMissingAdaptersAndLeaseLoss(
	t *testing.T,
) {
	t.Parallel()
	claim := nativeTestClaim("github", "work-items")
	effects, err := BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	backend := newSemanticWorkItemEffectBackend()
	sink := githubWorkItemEffectsFixture(backend)

	tampered := effects[0]
	tampered.PayloadBytes++
	if err := sink.WriteEffect(context.Background(), claim, tampered); !errors.Is(
		err, ErrInvalidConfiguration,
	) {
		t.Fatalf("tampered effect error=%v", err)
	}
	tampered = effects[0]
	tampered.ContentDigest = effects[1].ContentDigest
	if err := sink.WriteEffect(context.Background(), claim, tampered); !errors.Is(
		err, ErrInvalidConfiguration,
	) {
		t.Fatalf("digest-tampered effect error=%v", err)
	}

	incomplete := sink
	incomplete.WorkItems = nil
	if err := incomplete.WriteEffect(
		context.Background(), claim, effects[0],
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("incomplete adapter set error=%v", err)
	}

	sink.Lease = providerfoundation.LeaseGuardFunc(func(context.Context) error {
		return providerfoundation.ErrLeaseLost
	})
	if err := sink.WriteEffect(
		context.Background(), claim, effects[0],
	); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("lease-loss error=%v", err)
	}
	if len(backend.identities) != 0 {
		t.Fatalf("adapter called after fail-closed validation: %+v", backend.identities)
	}

	postWriteBackend := newSemanticWorkItemEffectBackend()
	postWrite := githubWorkItemEffectsFixture(postWriteBackend)
	guard := &secondAssertionLosesLease{}
	postWrite.Lease = guard
	if err := postWrite.WriteEffect(
		context.Background(), claim, effects[0],
	); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("post-write lease-loss error=%v", err)
	}
	if guard.calls != 2 || postWriteBackend.writeCounts[effects[0].Destination] != 1 {
		t.Fatalf("lease assertions=%d writes=%v", guard.calls, postWriteBackend.writeCounts)
	}
}

func TestGitHubWorkItemClickHouseEffectsRoutesEverySurfaceWithFrozenIdentity(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "work-items")
	rows := GitHubWorkItemEffectRows{
		WorkItems: []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","work_item_id":"gh:acme/api#1"}`)},
	}
	effects, err := BuildGitHubWorkItemEffects(rows)
	if err != nil {
		t.Fatal(err)
	}
	backend := newSemanticWorkItemEffectBackend()
	sink := githubWorkItemEffectsFixture(backend)

	for _, effect := range effects {
		if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
			t.Fatalf("write %s: %v", effect.Destination, err)
		}
		inspection, err := sink.InspectEffect(context.Background(), claim, effect)
		if err != nil || inspection != EffectExact {
			t.Fatalf("inspect %s=%s error=%v", effect.Destination, inspection, err)
		}
	}
	if len(backend.identities) != 2*len(githubWorkItemRouteDestinations()) {
		t.Fatalf("identity calls=%d", len(backend.identities))
	}
	for _, identity := range backend.identities {
		if identity.OrgID != claim.OrgID || identity.Generation != claim.GenerationKey() ||
			identity.Provider != claim.Provider || identity.Dataset != claim.Dataset {
			t.Fatalf("identity=%+v claim=%+v", identity, claim)
		}
	}

	foreign := claim
	foreign.OrgID = "org-other"
	inspection, err := sink.InspectEffect(context.Background(), foreign, effects[len(effects)-1])
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("foreign tenant inspection=%s error=%v", inspection, err)
	}

	conflicting := effectBatchFixture(
		t, effects[len(effects)-1].Destination, EffectReadbackRequired,
		`{"org_id":"org-acme","work_item_id":"gh:acme/api#conflict"}`,
	)
	inspection, err = sink.InspectEffect(context.Background(), claim, conflicting)
	if err != nil || inspection != EffectConflict {
		t.Fatalf("conflicting inspection=%s error=%v", inspection, err)
	}
}

func TestGitHubWorkItemEffectsRecoverMidWriteAndAccountForEmptyDestinations(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "work-items")
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	effects, err := BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{
		WorkItems: []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","work_item_id":"gh:acme/api#1"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	backend := newSemanticWorkItemEffectBackend()
	crash := errors.New("simulated death after durable destination write")
	backend.failAfterWrite = "work_item_interactions"
	backend.failure = crash
	ledger := &memoryEffectLedger{}
	firstSink := githubWorkItemEffectsFixture(backend)
	_, err = (EffectCommitter{
		Ledger: ledger, Sink: firstSink, Readback: firstSink,
		Now: func() time.Time { return now },
	}).Commit(context.Background(), claim, effects, now)
	if !errors.Is(err, crash) {
		t.Fatalf("first commit error=%v", err)
	}
	if backend.writeCounts["work_item_interactions"] != 1 {
		t.Fatalf("crashed effect writes=%d", backend.writeCounts["work_item_interactions"])
	}

	backend.failAfterWrite = ""
	recoveredSink := githubWorkItemEffectsFixture(backend)
	result, err := (EffectCommitter{
		Ledger: ledger, Sink: recoveredSink, Readback: recoveredSink,
		Now: func() time.Time { return now.Add(time.Minute) },
	}).Commit(context.Background(), claim, effects, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.MarkedCommitted != 1 || result.Written+result.Skipped+result.MarkedCommitted != len(githubWorkItemRouteDestinations()) {
		t.Fatalf("recovery result=%+v", result)
	}
	if backend.writeCounts["work_item_interactions"] != 1 {
		t.Fatalf("exact readback replayed crashed effect: %d", backend.writeCounts["work_item_interactions"])
	}
	for _, destination := range githubWorkItemRouteDestinations() {
		if backend.writeCounts[destination] != 1 {
			t.Fatalf("destination %s writes=%d", destination, backend.writeCounts[destination])
		}
	}
	for _, effect := range ledger.state.Effects {
		if effect.Status != GenerationBlockCommitted {
			t.Fatalf("effect not committed: %+v", effect)
		}
	}
}

type semanticWorkItemEffectBackend struct {
	rows           map[string]string
	writeCounts    map[string]int
	identities     []GitHubWorkItemEffectIdentity
	failAfterWrite string
	failure        error
}

type secondAssertionLosesLease struct{ calls int }

func (guard *secondAssertionLosesLease) Assert(context.Context) error {
	guard.calls++
	if guard.calls == 2 {
		return providerfoundation.ErrLeaseLost
	}
	return nil
}

func newSemanticWorkItemEffectBackend() *semanticWorkItemEffectBackend {
	return &semanticWorkItemEffectBackend{
		rows: make(map[string]string), writeCounts: make(map[string]int),
	}
}

func (backend *semanticWorkItemEffectBackend) key(
	identity GitHubWorkItemEffectIdentity,
) string {
	return identity.OrgID + "\x00" + identity.Generation + "\x00" + identity.Destination
}

func (backend *semanticWorkItemEffectBackend) WriteGitHubWorkItemEffect(
	_ context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	backend.identities = append(backend.identities, identity)
	backend.rows[backend.key(identity)] = effect.ContentDigest
	backend.writeCounts[identity.Destination]++
	if backend.failAfterWrite == identity.Destination {
		return backend.failure
	}
	return nil
}

func (backend *semanticWorkItemEffectBackend) InspectGitHubWorkItemEffect(
	_ context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
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

func githubWorkItemEffectsFixture(
	backend GitHubWorkItemEffectAdapter,
) GitHubWorkItemClickHouseEffects {
	adapter := func(destination string) GitHubWorkItemEffectAdapter {
		return destinationCheckingWorkItemEffectAdapter{
			destination: destination, delegate: backend,
		}
	}
	return GitHubWorkItemClickHouseEffects{
		Lease:                          providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		AIAttribution:                  adapter("ai_attribution"),
		EstimateCoverageMetricsDaily:   adapter("estimate_coverage_metrics_daily"),
		InvestmentClassificationsDaily: adapter("investment_classifications_daily"),
		InvestmentMetricsDaily:         adapter("investment_metrics_daily"),
		IssueTypeMetricsDaily:          adapter("issue_type_metrics_daily"),
		Sprints:                        adapter("sprints"),
		WorkItemCycleTimes:             adapter("work_item_cycle_times"),
		WorkItemDependencies:           adapter("work_item_dependencies"),
		WorkItemInteractions:           adapter("work_item_interactions"),
		WorkItemMetricsDaily:           adapter("work_item_metrics_daily"),
		WorkItemReopenEvents:           adapter("work_item_reopen_events"),
		WorkItemStateDurationsDaily:    adapter("work_item_state_durations_daily"),
		WorkItemTeamAttributions:       adapter("work_item_team_attributions"),
		WorkItemTransitions:            adapter("work_item_transitions"),
		WorkItemUserMetricsDaily:       adapter("work_item_user_metrics_daily"),
		WorkItems:                      adapter("work_items"),
		ProjectMembershipTransitions:   adapter("project_membership_transitions"),
		Projects:                       adapter("projects"),
	}
}

type destinationCheckingWorkItemEffectAdapter struct {
	destination string
	delegate    GitHubWorkItemEffectAdapter
}

func (adapter destinationCheckingWorkItemEffectAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	if identity.Destination != adapter.destination ||
		effect.Destination != adapter.destination {
		return ErrInvalidConfiguration
	}
	return adapter.delegate.WriteGitHubWorkItemEffect(ctx, identity, effect)
}

func (adapter destinationCheckingWorkItemEffectAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	if identity.Destination != adapter.destination ||
		effect.Destination != adapter.destination {
		return EffectConflict, ErrInvalidConfiguration
	}
	return adapter.delegate.InspectGitHubWorkItemEffect(ctx, identity, effect)
}
