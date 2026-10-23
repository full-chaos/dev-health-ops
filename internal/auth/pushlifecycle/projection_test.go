package pushlifecycle

import (
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

const (
	org    = "org_EXAMPLE0000000000000001"
	source = "src_EXAMPLE0000000000000001"
	credA  = "cred-A"
	credB  = "cred-B"
)

func event(typ string, id string, revision int64, change contracts.ExternalPushLifecycleChange) contracts.ExternalPushLifecycleEvent {
	return contracts.ExternalPushLifecycleEvent{
		SchemaVersion: contracts.ExternalPushLifecycleEventSurface,
		AggregateType: contracts.ExternalPushCredentialAggregate,
		AggregateID:   id,
		EventType:     typ,
		Payload: contracts.ExternalPushLifecyclePayload{
			Revision: contracts.Revision(revision), OrganizationID: org, SourceID: source, Change: change,
		},
	}
}

func created(id string, scopes ...string) contracts.ExternalPushLifecycleEvent {
	return event(contracts.ExternalPushCreated, id, 1, contracts.ExternalPushLifecycleChange{
		Status: "active", Fingerprint: "0123456789abcdef", Scopes: scopes})
}

func scoped(id string, revision int64, scopes ...string) contracts.ExternalPushLifecycleEvent {
	return event(contracts.ExternalPushScoped, id, revision, contracts.ExternalPushLifecycleChange{Scopes: scopes})
}

func revoked(id string, revision int64) contracts.ExternalPushLifecycleEvent {
	return event(contracts.ExternalPushRevoked, id, revision, contracts.ExternalPushLifecycleChange{Status: "revoked"})
}

func expired(id string, revision int64) contracts.ExternalPushLifecycleEvent {
	at := contracts.Timestamp{Time: time.Date(2027, 10, 23, 10, 0, 0, 0, time.UTC)}
	return event(contracts.ExternalPushExpired, id, revision, contracts.ExternalPushLifecycleChange{Status: "expired", ExpiresAt: &at})
}

func rotated(id, parent string, scopes ...string) contracts.ExternalPushLifecycleEvent {
	return event(contracts.ExternalPushRotated, id, 1, contracts.ExternalPushLifecycleChange{
		Status: "active", Fingerprint: "fedcba9876543210", Scopes: scopes, RotationParentID: parent})
}

func mustApply(t *testing.T, state State, e contracts.ExternalPushLifecycleEvent, parent *Parent) State {
	t.Helper()
	next, outcome := Apply(state, e, parent)
	if outcome != Applied {
		t.Fatalf("%s revision %d: outcome %s, want applied", e.EventType, e.Payload.Revision, outcome)
	}
	return next
}

func TestProjectionAppliesInOrder(t *testing.T) {
	state := mustApply(t, State{}, created(credA, "ingest:write", "schema:read"), nil)
	if state.Status != StatusActive || state.Revision != 1 || state.OrganizationID != org || state.Fingerprint == "" {
		t.Fatalf("after created: %+v", state)
	}
	state = mustApply(t, state, scoped(credA, 2, "ingest:write"), nil)
	if !slices.Equal(state.Scopes, []string{"ingest:write"}) || state.Revision != 2 {
		t.Fatalf("after scoped: %+v", state)
	}
	state = mustApply(t, state, revoked(credA, 3), nil)
	if state.Status != StatusRevoked || state.Revision != 3 {
		t.Fatalf("after revoked: %+v", state)
	}
	exp := mustApply(t, mustApply(t, State{}, created(credB, "ingest:write"), nil), expired(credB, 2), nil)
	if exp.Status != StatusExpired || exp.ExpiresAt != "2027-10-23T10:00:00.000000Z" {
		t.Fatalf("after expired: %+v", exp)
	}
}

func TestProjectionDuplicateAndStaleAreNoOps(t *testing.T) {
	state := mustApply(t, mustApply(t, State{}, created(credA, "ingest:write", "schema:read"), nil), scoped(credA, 2, "ingest:write"), nil)
	for _, tc := range []struct {
		e    contracts.ExternalPushLifecycleEvent
		want Outcome
	}{
		{scoped(credA, 2, "ingest:write"), Duplicate},
		{created(credA, "ingest:write", "schema:read"), Stale},
		{revoked(credA, 1), Stale},
	} {
		next, outcome := Apply(state, tc.e, nil)
		if outcome != tc.want || !sameState(next, state) {
			t.Errorf("%s r%d: outcome %s state changed=%v, want %s unchanged", tc.e.EventType, tc.e.Payload.Revision, outcome, !sameState(next, state), tc.want)
		}
	}
}

func sameState(a, b State) bool {
	return a.AggregateID == b.AggregateID && a.OrganizationID == b.OrganizationID && a.SourceID == b.SourceID &&
		a.Revision == b.Revision && a.Status == b.Status && a.Fingerprint == b.Fingerprint &&
		slices.Equal(a.Scopes, b.Scopes) && a.RotationParentID == b.RotationParentID && a.ExpiresAt == b.ExpiresAt
}

func TestProjectionGapIsRefused(t *testing.T) {
	state := mustApply(t, State{}, created(credA, "ingest:write"), nil)
	next, outcome := Apply(state, revoked(credA, 3), nil)
	if outcome != Gap || !sameState(next, state) {
		t.Fatalf("gap: outcome %s, changed=%v", outcome, !sameState(next, state))
	}
	if _, outcome := Apply(State{}, scoped(credA, 2, "ingest:write"), nil); outcome != Gap {
		t.Fatalf("a first sight at revision 2 is %s, want gap", outcome)
	}
}

func TestProjectionRevokedAndExpiredAreTerminal(t *testing.T) {
	active := mustApply(t, State{}, created(credA, "ingest:write", "schema:read"), nil)
	for name, terminal := range map[string]State{
		"revoked": mustApply(t, active, revoked(credA, 2), nil),
		"expired": mustApply(t, active, expired(credA, 2), nil),
	} {
		for _, e := range []contracts.ExternalPushLifecycleEvent{
			scoped(credA, 3, "ingest:write"), revoked(credA, 3), expired(credA, 3),
			event(contracts.ExternalPushCreated, credA, 3, contracts.ExternalPushLifecycleChange{Fingerprint: "0123456789abcdef", Scopes: []string{"ingest:write"}}),
		} {
			next, outcome := Apply(terminal, e, nil)
			if outcome != RefusedTerminal || !sameState(next, terminal) {
				t.Errorf("%s then %s: outcome %s, want refused_terminal with state unchanged", name, e.EventType, outcome)
			}
		}
	}
}

func TestProjectionNeverWidensScopes(t *testing.T) {
	state := mustApply(t, State{}, created(credA, "ingest:status"), nil)
	next, outcome := Apply(state, scoped(credA, 2, "ingest:status", "ingest:write"), nil)
	if outcome != RefusedWiden || !sameState(next, state) {
		t.Fatalf("widening: outcome %s", outcome)
	}
	if _, outcome := Apply(state, scoped(credA, 2, "schema:read"), nil); outcome != RefusedWiden {
		t.Fatalf("a different scope is %s, want refused_widen", outcome)
	}
	if _, outcome := Apply(state, scoped(credA, 2), nil); outcome != Applied {
		t.Fatalf("narrowing to none is %s, want applied", outcome)
	}
}

func TestProjectionRotationNeedsTheActiveParent(t *testing.T) {
	parent := &Parent{Scopes: []string{"ingest:write", "schema:read"}}
	state := mustApply(t, State{}, rotated(credB, credA, "ingest:write"), parent)
	if state.RotationParentID != credA || state.Status != StatusActive {
		t.Fatalf("after rotated: %+v", state)
	}
	cases := map[string]struct {
		e      contracts.ExternalPushLifecycleEvent
		parent *Parent
		want   Outcome
	}{
		"no parent view":       {rotated(credB, credA, "ingest:write"), nil, RefusedParent},
		"wider than parent":    {rotated(credB, credA, "ingest:write", "ingest:status"), parent, RefusedWiden},
		"its own parent":       {rotated(credB, credB, "ingest:write"), parent, RefusedTransition},
		"no parent id":         {rotated(credB, "", "ingest:write"), parent, RefusedTransition},
		"rotated after create": {rotated(credB, credA, "ingest:write"), parent, RefusedTransition},
	}
	for name, tc := range cases {
		from := State{}
		if name == "rotated after create" {
			from = mustApply(t, State{}, created(credB, "ingest:write"), nil)
			tc.e.Payload.Revision = 2
		}
		if _, outcome := Apply(from, tc.e, tc.parent); outcome != tc.want {
			t.Errorf("%s: outcome %s, want %s", name, outcome, tc.want)
		}
	}
}

func TestProjectionRefusesAChangedIdentityAndAForeignAggregate(t *testing.T) {
	state := mustApply(t, State{}, created(credA, "ingest:write"), nil)
	other := scoped(credA, 2, "ingest:write")
	other.Payload.OrganizationID = "org_EXAMPLE0000000000000002"
	if _, outcome := Apply(state, other, nil); outcome != RefusedTransition {
		t.Errorf("another tenant: outcome %s, want refused_transition", outcome)
	}
	if _, outcome := Apply(state, scoped(credB, 2, "ingest:write"), nil); outcome != RefusedTransition {
		t.Errorf("another credential: outcome %s, want refused_transition", outcome)
	}
}

// TestProjectionNeverAppliesToATerminalAggregateAndNeverPanics replays random
// event sequences. The invariants: no panic; once terminal, never Applied
// again; the revision never decreases; scopes never grow after creation; the
// same sequence replayed twice ends in the same state (idempotent).
func TestProjectionNeverAppliesToATerminalAggregateAndNeverPanics(t *testing.T) {
	scopePool := []string{"ingest:write", "ingest:status", "schema:read"}
	rng := rand.New(rand.NewSource(10894))
	for run := 0; run < 2000; run++ {
		var events []contracts.ExternalPushLifecycleEvent
		for i := 0; i < 1+rng.Intn(8); i++ {
			revision := int64(rng.Intn(6))
			var picked []string
			for _, s := range scopePool {
				if rng.Intn(2) == 0 {
					picked = append(picked, s)
				}
			}
			var e contracts.ExternalPushLifecycleEvent
			switch rng.Intn(5) {
			case 0:
				e = created(credA, picked...)
			case 1:
				e = scoped(credA, revision, picked...)
			case 2:
				e = revoked(credA, revision)
			case 3:
				e = expired(credA, revision)
			default:
				e = rotated(credA, credB, picked...)
			}
			if rng.Intn(4) == 0 {
				e.AggregateID = credB
			}
			if rng.Intn(6) == 0 {
				e.Payload.Revision = contracts.Revision(rng.Int63n(1<<53) - 5)
			}
			events = append(events, e)
		}
		parent := &Parent{Scopes: scopePool}
		replay := func() State {
			state := State{}
			for _, e := range events {
				before := state
				next, outcome := Apply(state, e, parent)
				if before.Status.Terminal() && outcome == Applied {
					t.Fatalf("run %d: applied %s to a terminal credential", run, e.EventType)
				}
				if next.Revision < before.Revision {
					t.Fatalf("run %d: revision went from %d to %d", run, before.Revision, next.Revision)
				}
				if outcome != Applied && !sameState(next, before) {
					t.Fatalf("run %d: %s changed the state", run, outcome)
				}
				if outcome == Applied && before.Revision > 0 && !subset(next.Scopes, before.Scopes) {
					t.Fatalf("run %d: scopes grew from %v to %v", run, before.Scopes, next.Scopes)
				}
				if !slices.Contains(Outcomes, outcome) {
					t.Fatalf("run %d: outcome %q is outside the closed set", run, outcome)
				}
				state = next
			}
			return state
		}
		first := replay()
		if second := replay(); !sameState(first, second) {
			t.Fatalf("run %d: replaying the same events gave a different state", run)
		}
		doubled := State{}
		for _, e := range events {
			doubled, _ = Apply(doubled, e, parent)
			doubled, _ = Apply(doubled, e, parent)
		}
		if !sameState(first, doubled) {
			t.Fatalf("run %d: delivering every event twice changed the result: %+v vs %+v", run, first, doubled)
		}
	}
}
