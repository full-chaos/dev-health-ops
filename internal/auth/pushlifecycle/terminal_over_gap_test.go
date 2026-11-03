package pushlifecycle

import (
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

var togScopes = []string{"ingest:write", "ingest:status", "schema:read", "context:read", "evidence:read", "data:read"}

// togDenies fails unless the credential is not active and permits no scope, now or before the event.
func togDenies(t *testing.T, label string, s State) {
	t.Helper()
	if s.Status == StatusActive {
		t.Errorf("%s: status is active", label)
	}
	for _, at := range []time.Time{t0, t0.Add(-24 * time.Hour), t0.Add(24 * time.Hour)} {
		if Live(s, at) {
			t.Errorf("%s: a %q credential is live at %s", label, s.Status, at)
		}
		for _, scope := range togScopes {
			if Permits(s, scope, at) {
				t.Errorf("%s: a %q credential permits %s", label, s.Status, scope)
			}
		}
	}
}

// togFold drives Apply as a consumer does: a retry parks the event and every step retries the parked ones. Every
// step checks that a terminal credential never moves or permits again and that no step widens the applied scopes.
type togFold struct {
	t      *testing.T
	state  State
	parked []contracts.ExternalPushLifecycleEvent
	ended  bool
}

func (f *togFold) deliver(e contracts.ExternalPushLifecycleEvent) {
	f.t.Helper()
	if !f.try(e) {
		f.parked = append(f.parked, e)
	}
	for progress := true; progress; {
		progress = false
		for i, p := range f.parked {
			if f.try(p) {
				f.parked = slices.Delete(f.parked, i, i+1)
				progress = true
				break
			}
		}
	}
}

func (f *togFold) try(e contracts.ExternalPushLifecycleEvent) bool {
	f.t.Helper()
	before := f.state
	next, outcome := Apply(before, e, nil)
	if outcome.Retry() {
		if !sameState(next, before) {
			f.t.Fatalf("%s r%d: a retry changed the state", e.EventType, e.Payload.Revision)
		}
		return false
	}
	if before.Status.Terminal() && (outcome == Applied || next.Status != before.Status) {
		f.t.Fatalf("%s r%d: a %s credential moved to %q (%s)", e.EventType, e.Payload.Revision, before.Status, next.Status, outcome)
	}
	if before.Revision > 0 && !subset(next.Scopes, before.Scopes) {
		f.t.Fatalf("%s r%d: scopes grew from %v to %v", e.EventType, e.Payload.Revision, before.Scopes, next.Scopes)
	}
	if outcome != Applied && !sameState(next, before) {
		f.t.Fatalf("%s r%d: %s changed the state", e.EventType, e.Payload.Revision, outcome)
	}
	if next.Revision < before.Revision {
		f.t.Fatalf("%s r%d: the revision went from %d to %d", e.EventType, e.Payload.Revision, before.Revision, next.Revision)
	}
	f.state = next
	if next.Status.Terminal() {
		f.ended = true
	}
	if f.ended {
		togDenies(f.t, e.EventType+" after the end", next)
	}
	return true
}

func togOrders(events []contracts.ExternalPushLifecycleEvent, yield func([]contracts.ExternalPushLifecycleEvent)) int {
	n := 0
	var walk func(k int)
	walk = func(k int) {
		if k == len(events) {
			n++
			yield(slices.Clone(events))
			return
		}
		for i := k; i < len(events); i++ {
			events[k], events[i] = events[i], events[k]
			walk(k + 1)
			events[k], events[i] = events[i], events[k]
		}
	}
	walk(0)
	return n
}

func togNames(order []contracts.ExternalPushLifecycleEvent) []string {
	var out []string
	for _, e := range order {
		out = append(out, e.EventType+":"+string(rune('0'+e.Payload.Revision)))
	}
	return out
}

// T1: a revoke or an expiry that skips revisions ends an active credential at its own revision.
func TestTerminalOverAGapEndsThePushCredential(t *testing.T) {
	active := mustApply(t, State{}, created(credA, "ingest:write", "schema:read"), nil)
	gone, outcome := Apply(active, revoked(credA, 3), nil)
	if outcome != Applied || gone.Status != StatusRevoked || gone.Revision != 3 || !gone.TerminalAt.Equal(t0) {
		t.Fatalf("revoked r3 over r1: outcome %s status %s r%d, want applied, revoked at r3", outcome, gone.Status, gone.Revision)
	}
	togDenies(t, "revoked r3 over r1", gone)
	ended, outcome := Apply(active, expired(credA, 4), nil)
	if outcome != Applied || ended.Status != StatusExpired || ended.Revision != 4 || ended.ExpiresAt != "2027-10-23T10:00:00.000000Z" {
		t.Fatalf("expired r4 over r1: outcome %s status %s r%d expires %q, want applied, expired at r4", outcome, ended.Status, ended.Revision, ended.ExpiresAt)
	}
	togDenies(t, "expired r4 over r1", ended)
}

// T2: after a terminal over a gap, a lower revision is stale and a higher one is refused, in order or over a gap.
func TestAfterATerminalOverAGapLowerIsStaleAndHigherIsRefused(t *testing.T) {
	active := mustApply(t, State{}, created(credA, "ingest:write", "schema:read"), nil)
	gone := mustApply(t, active, revoked(credA, 3), nil)
	for name, tc := range map[string]struct {
		e    contracts.ExternalPushLifecycleEvent
		want Outcome
	}{
		"created r1":           {created(credA, "ingest:write", "schema:read", "ingest:status"), Stale},
		"narrowing scoped r2":  {scoped(credA, 2, "ingest:write"), Stale},
		"expired r2":           {expired(credA, 2), Stale},
		"widening scoped r4":   {scoped(credA, 4, "ingest:write", "schema:read", "ingest:status"), RefusedTerminal},
		"scoped r4":            {scoped(credA, 4, "ingest:write"), RefusedTerminal},
		"revoked r4":           {revoked(credA, 4), RefusedTerminal},
		"expired r4":           {expired(credA, 4), RefusedTerminal},
		"scoped r6 over a gap": {scoped(credA, 6, "ingest:write"), RefusedTerminal},
		"revoked r6 over gap":  {revoked(credA, 6), RefusedTerminal},
		"expired r9 over gap":  {expired(credA, 9), RefusedTerminal},
	} {
		next, outcome := Apply(gone, tc.e, nil)
		if outcome != tc.want || !sameState(next, gone) {
			t.Errorf("%s after revoked r3: outcome %s changed=%v, want %s unchanged", name, outcome, !sameState(next, gone), tc.want)
		}
		togDenies(t, name, next)
	}
}

// T3: only a terminal event applies over a gap; a narrowing that skips a revision waits.
func TestANonTerminalEventOverAGapStaysAGap(t *testing.T) {
	active := mustApply(t, State{}, created(credA, "ingest:write", "schema:read"), nil)
	for name, e := range map[string]contracts.ExternalPushLifecycleEvent{
		"narrowing scoped r3": scoped(credA, 3, "ingest:write"),
		"created-form r3":     event(contracts.ExternalPushCreated, credA, 3, contracts.ExternalPushLifecycleChange{Fingerprint: "0123456789abcdef", Scopes: []string{"ingest:write"}}),
	} {
		if next, outcome := Apply(active, e, nil); outcome != Gap || !sameState(next, active) {
			t.Errorf("%s over r1: outcome %s, want gap unchanged", name, outcome)
		}
	}
}

// T4: a refusal over a gap is not decided: the missing narrowing still applies when it arrives, and the refused event
// is then refused against the narrowed scopes.
func TestARefusedEventOverAGapStaysAGapAndTheLateNarrowingApplies(t *testing.T) {
	active := mustApply(t, State{}, created(credA, "ingest:write", "schema:read"), nil)
	widen := scoped(credA, 3, "ingest:write", "schema:read")
	if next, outcome := Apply(active, widen, nil); outcome != Gap || !sameState(next, active) {
		t.Fatalf("scoped r3 over r1: outcome %s, want gap unchanged", outcome)
	}
	narrowed := mustApply(t, active, scoped(credA, 2, "ingest:write"), nil)
	refused, outcome := Apply(narrowed, widen, nil)
	if outcome != RefusedWiden || !sameState(refused, narrowed) {
		t.Fatalf("scoped r3 after the narrowing: outcome %s, want refused_widen unchanged", outcome)
	}
	if next, outcome := Apply(refused, scoped(credA, 4, "ingest:write", "schema:read"), nil); outcome != RefusedWiden || !slices.Equal(next.Scopes, []string{"ingest:write"}) {
		t.Fatalf("a widening r4 after the refused r3: outcome %s scopes %v, want refused_widen [ingest:write]", outcome, next.Scopes)
	}
	next, outcome := Apply(refused, scoped(credA, 4, "ingest:write"), nil)
	if outcome != Applied || next.Revision != 4 || !slices.Equal(next.Scopes, []string{"ingest:write"}) {
		t.Fatalf("scoped r4 after the refused r3: outcome %s r%d scopes %v, want applied r4 [ingest:write]", outcome, next.Revision, next.Scopes)
	}
}

// T5: a revoke or an expiry over a gap with another identity is no terminal event of this credential.
func TestAForeignIdentityTerminalOverAGapIsAGap(t *testing.T) {
	active := mustApply(t, State{}, created(credA, "ingest:write", "schema:read"), nil)
	for change, mutate := range map[string]func(contracts.ExternalPushLifecycleEvent) contracts.ExternalPushLifecycleEvent{
		"org": func(e contracts.ExternalPushLifecycleEvent) contracts.ExternalPushLifecycleEvent {
			return withOrg(e, "org_EXAMPLE0000000000000002")
		},
		"source": func(e contracts.ExternalPushLifecycleEvent) contracts.ExternalPushLifecycleEvent {
			return withSource(e, "src_EXAMPLE0000000000000002")
		},
		"binding": orgWide,
	} {
		for name, e := range map[string]contracts.ExternalPushLifecycleEvent{"revoked r3": revoked(credA, 3), "expired r4": expired(credA, 4)} {
			next, outcome := Apply(active, mutate(e), nil)
			if outcome != Gap || !sameState(next, active) || !Permits(next, "ingest:write", t0) {
				t.Errorf("%s with another %s over r1: outcome %s, want gap and the credential unchanged", name, change, outcome)
			}
		}
	}
}

// T6: a refused revision is decided: later revisions apply against the last applied state, in every arrival order,
// and a revoke that lands before a narrowing still ends the credential.
func TestARefusedRevisionIsDecidedAndLaterRevisionsApply(t *testing.T) {
	base := created(credA, "ingest:write", "schema:read")
	narrow, widen := scoped(credA, 2, "ingest:write"), scoped(credA, 3, "ingest:write", "schema:read")

	orders := togOrders([]contracts.ExternalPushLifecycleEvent{base, narrow, widen, scoped(credA, 4, "ingest:write")}, func(order []contracts.ExternalPushLifecycleEvent) {
		f := &togFold{t: t}
		for _, e := range order {
			f.deliver(e)
		}
		if f.state.Status != StatusActive || f.state.Revision != 4 || !slices.Equal(f.state.Scopes, []string{"ingest:write"}) || len(f.parked) != 0 {
			t.Fatalf("order %v: ended %s r%d scopes %v parked %d, want active r4 [ingest:write] none parked", togNames(order), f.state.Status, f.state.Revision, f.state.Scopes, len(f.parked))
		}
	})
	if orders != 24 {
		t.Fatalf("%d orders, want 24", orders)
	}

	togOrders([]contracts.ExternalPushLifecycleEvent{base, narrow, widen, revoked(credA, 4)}, func(order []contracts.ExternalPushLifecycleEvent) {
		f := &togFold{t: t}
		for _, e := range order {
			f.deliver(e)
		}
		if f.state.Status != StatusRevoked || f.state.Revision != 4 || len(f.parked) != 0 {
			t.Fatalf("order %v: ended %s r%d parked %d, want revoked r4 none parked", togNames(order), f.state.Status, f.state.Revision, len(f.parked))
		}
		togDenies(t, "refused r3 then revoked r4", f.state)
	})

	// A revoke can land before the narrowing: it applies over the gap and the narrowing is then stale, so the end
	// scopes are the created ones or the narrowed ones; the credential permits nothing either way.
	twice := []contracts.ExternalPushLifecycleEvent{base, narrow, revoked(credA, 3), base, narrow, revoked(credA, 3)}
	if n := togOrders(twice, func(order []contracts.ExternalPushLifecycleEvent) {
		f := &togFold{t: t}
		for _, e := range order {
			f.deliver(e)
		}
		if f.state.Status != StatusRevoked || f.state.Revision != 3 || !subset(f.state.Scopes, []string{"ingest:write", "schema:read"}) || len(f.parked) != 0 {
			t.Fatalf("order %v: ended %s r%d scopes %v parked %d, want revoked r3", togNames(order), f.state.Status, f.state.Revision, f.state.Scopes, len(f.parked))
		}
		togDenies(t, "revoked r3 in any order", f.state)
	}); n != 720 {
		t.Fatalf("%d orders, want 720", n)
	}

	narrowed := mustApply(t, mustApply(t, State{}, base, nil), narrow, nil)
	refused, outcome := Apply(narrowed, widen, nil)
	if outcome != RefusedWiden {
		t.Fatalf("widen r3: outcome %s, want refused_widen", outcome)
	}
	if next, outcome := Apply(refused, widen, nil); outcome != Duplicate || !sameState(next, refused) {
		t.Fatalf("the refused r3 again: outcome %s, want duplicate unchanged", outcome)
	}
	gone, outcome := Apply(refused, revoked(credA, 4), nil)
	if outcome != Applied || gone.Status != StatusRevoked || gone.Revision != 4 {
		t.Fatalf("revoked r4 after the refused r3: outcome %s status %s r%d, want applied revoked r4", outcome, gone.Status, gone.Revision)
	}
	togDenies(t, "revoked r4 after the refused r3", gone)
}
