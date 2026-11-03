package pushlifecycle

import (
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

func stampedAt(e contracts.ExternalPushLifecycleEvent, at time.Time) contracts.ExternalPushLifecycleEvent {
	e.CreatedAt = contracts.Timestamp{Time: at}
	return e
}

func denies(t *testing.T, label string, s State) {
	t.Helper()
	for _, scope := range []string{"ingest:write", "ingest:status", "schema:read"} {
		if Permits(s, scope, t0) || Permits(s, scope, t0.Add(-24*time.Hour)) {
			t.Errorf("%s: a %s credential permits %s", label, s.Status, scope)
		}
	}
	if Live(s, t0) {
		t.Errorf("%s: a %s credential is live", label, s.Status)
	}
}

// delivery drives Apply the way a consumer does: a Retry outcome parks the event and
// every later step retries the parked events, so only the arrival order varies.
type delivery struct {
	state  State
	parked []contracts.ExternalPushLifecycleEvent
	t      *testing.T
	parent *State
}

func (d *delivery) deliver(e contracts.ExternalPushLifecycleEvent) {
	d.t.Helper()
	d.step(e)
	for progress := true; progress; {
		progress = false
		for i, p := range slices.Clone(d.parked) {
			before := d.state
			if next, outcome := Apply(d.state, p, d.parent); outcome.Retry() {
				continue
			} else {
				d.parked = slices.Delete(d.parked, i, i+1)
				d.check(before, next, outcome, p)
				d.state = next
				progress = true
				break
			}
		}
	}
}

func (d *delivery) step(e contracts.ExternalPushLifecycleEvent) {
	before := d.state
	next, outcome := Apply(before, e, d.parent)
	if outcome.Retry() {
		d.parked = append(d.parked, e)
		return
	}
	d.check(before, next, outcome, e)
	d.state = next
}

func (d *delivery) check(before, next State, outcome Outcome, e contracts.ExternalPushLifecycleEvent) {
	d.t.Helper()
	if before.Status.Terminal() && (next.Status != before.Status || outcome == Applied) {
		d.t.Fatalf("%s r%d: a %s credential moved to %q (%s)", e.EventType, e.Payload.Revision, before.Status, next.Status, outcome)
	}
	if before.Revision > 0 && !subset(next.Scopes, before.Scopes) {
		d.t.Fatalf("%s r%d: scopes grew from %v to %v", e.EventType, e.Payload.Revision, before.Scopes, next.Scopes)
	}
	if outcome != Applied && !sameState(before, next) {
		d.t.Fatalf("%s r%d: %s changed the state", e.EventType, e.Payload.Revision, outcome)
	}
}

func permutations[T any](items []T, yield func([]T)) {
	var walk func(k int)
	walk = func(k int) {
		if k == len(items) {
			yield(slices.Clone(items))
			return
		}
		for i := k; i < len(items); i++ {
			items[k], items[i] = items[i], items[k]
			walk(k + 1)
			items[k], items[i] = items[i], items[k]
		}
	}
	walk(0)
}

// Cell 1: a revoked event delivered twice is a duplicate and leaves the terminal state as it was.
func TestDisorderDuplicateOfARevokedEventIsADuplicateAndKeepsTheTerminalState(t *testing.T) {
	active := mustApply(t, State{}, created(credA, "ingest:write", "schema:read"), nil)
	revoke := revoked(credA, 2)
	terminal := mustApply(t, active, revoke, nil)
	next, outcome := Apply(terminal, revoke, nil)
	if outcome != Duplicate || !sameState(next, terminal) {
		t.Fatalf("revoked r2 delivered twice: outcome %s, changed=%v, want duplicate unchanged", outcome, !sameState(next, terminal))
	}
	exp := mustApply(t, active, expired(credA, 2), nil)
	if next, outcome := Apply(exp, expired(credA, 2), nil); outcome != Duplicate || !sameState(next, exp) {
		t.Fatalf("expired r2 delivered twice: outcome %s, want duplicate unchanged", outcome)
	}
	denies(t, "revoked twice", next)
}

// Cell 2: a replay of any older revision after a revoke is stale and changes nothing.
func TestDisorderReplayOfOlderRevisionsAfterRevokeIsStale(t *testing.T) {
	r1 := created(credA, "ingest:write", "schema:read")
	r2 := scoped(credA, 2, "ingest:write")
	state := mustApply(t, mustApply(t, mustApply(t, State{}, r1, nil), r2, nil), revoked(credA, 3), nil)
	for name, e := range map[string]contracts.ExternalPushLifecycleEvent{
		"created r1":         r1,
		"scoped r2":          r2,
		"wider scoped r2":    scoped(credA, 2, "ingest:write", "schema:read", "ingest:status"),
		"revoked r1":         revoked(credA, 1),
		"expired r2":         expired(credA, 2),
		"rotated-form r1":    rotated(credA, credB, "ingest:write"),
		"created r1, wider":  created(credA, "ingest:write", "schema:read", "ingest:status"),
		"scoped r2, revived": scoped(credA, 2, "ingest:write", "schema:read"),
	} {
		next, outcome := Apply(state, e, &State{})
		if outcome != Stale || !sameState(next, state) {
			t.Errorf("%s: outcome %s changed=%v, want stale unchanged", name, outcome, !sameState(next, state))
		}
		denies(t, name, next)
	}
}

// Cell 3: a rotated child and its parent replayed after the rotation; and a changed identity never retires or narrows a credential.
func TestDisorderReplayAroundARotationNeverRevivesTheParentOrMovesTheChild(t *testing.T) {
	parent := parentState(t, "ingest:write", "schema:read")
	child := mustApply(t, State{}, rotated(credB, credA, "ingest:write"), &parent)
	retired := mustApply(t, parent, revoked(credA, 2), nil)

	// The child's rotated event again, whatever parent state the caller loads now.
	early := retired
	early.TerminalAt = t0.Add(-time.Hour)
	for name, p := range map[string]*State{"parent live": &parent, "parent retired": &retired, "parent retired before": &early, "parent not loaded": nil} {
		next, outcome := Apply(child, rotated(credB, credA, "ingest:write"), p)
		if outcome != Duplicate || !sameState(next, child) {
			t.Errorf("rotated replay, %s: outcome %s, want duplicate unchanged", name, outcome)
		}
	}
	// The parent's old events after its retirement.
	for name, e := range map[string]contracts.ExternalPushLifecycleEvent{
		"parent created r1": created(credA, "ingest:write", "schema:read"),
		"parent revoked r2": revoked(credA, 2),
	} {
		next, outcome := Apply(retired, e, nil)
		if outcome == Applied || next.Status != StatusRevoked {
			t.Errorf("%s: outcome %s status %s, want no change to the revoked parent", name, outcome, next.Status)
		}
	}
	// A child event for the parent aggregate (the child's id on the parent's state) is a foreign aggregate.
	if _, outcome := Apply(parent, scoped(credB, 2, "ingest:write"), nil); outcome != RefusedTransition {
		t.Errorf("child scoped event on the parent: outcome %s, want refused_transition", outcome)
	}
	if next, outcome := Apply(retired, revoked(credB, 3), nil); outcome != RefusedTransition || !sameState(next, retired) {
		t.Errorf("child revoked event on the revoked parent: outcome %s, want refused_transition", outcome)
	}

	// A changed identity (organization, source, binding) never scopes, revokes or expires the credential.
	live := mustApply(t, State{}, created(credA, "ingest:write", "schema:read"), nil)
	changes := map[string]func(contracts.ExternalPushLifecycleEvent) contracts.ExternalPushLifecycleEvent{
		"org": func(e contracts.ExternalPushLifecycleEvent) contracts.ExternalPushLifecycleEvent {
			return withOrg(e, "org_EXAMPLE0000000000000002")
		},
		"source": func(e contracts.ExternalPushLifecycleEvent) contracts.ExternalPushLifecycleEvent {
			return withSource(e, "src_EXAMPLE0000000000000002")
		},
		"binding": orgWide,
	}
	for change, mutate := range changes {
		for name, e := range map[string]contracts.ExternalPushLifecycleEvent{
			"scoped": scoped(credA, 2, "ingest:write"), "revoked": revoked(credA, 2), "expired": expired(credA, 2),
		} {
			next, outcome := Apply(live, mutate(e), nil)
			if outcome != RefusedTransition || !sameState(next, live) {
				t.Errorf("%s event with another %s: outcome %s, want refused_transition unchanged", name, change, outcome)
			}
		}
	}
}

// Cell 4: a parent that a first rotation already retired is no parent for a later rotation, however the events arrive.
func TestDisorderASecondRotationOfARetiredParentIsRefusedAndARotationWaitsForItsParent(t *testing.T) {
	parent := parentState(t, "ingest:write", "schema:read")
	first := stampedAt(rotated(credB, credA, "ingest:write"), t0.Add(time.Minute))
	if _, outcome := Apply(State{}, first, nil); outcome != ParentPending {
		t.Fatalf("rotation before its parent was seen: outcome %s, want parent_pending", outcome)
	}
	retire := stampedAt(revoked(credA, 2), t0.Add(time.Minute))
	retired := mustApply(t, parent, retire, nil)
	mustApply(t, State{}, first, &retired)
	second := stampedAt(rotated("cred-C", credA, "ingest:write"), t0.Add(2*time.Minute))
	if next, outcome := Apply(State{}, second, &retired); outcome != RefusedParent || next.Status != StatusUnknown {
		t.Fatalf("second rotation of a retired parent: outcome %s status %q, want refused_parent and nothing projected", outcome, next.Status)
	}
	if !Outcome.Retry(ParentPending) || RefusedParent.Retry() {
		t.Fatal("parent_pending must retry and refused_parent must be final")
	}
	// The parent, replayed from a stale copy that still reads active but expired before the rotation.
	stale := parent
	stale.ExpiresAt = t0.Add(30 * time.Second).UTC().Format("2006-01-02T15:04:05.000000Z")
	if _, outcome := Apply(State{}, second, &stale); outcome != RefusedParent {
		t.Fatalf("parent copy expired before the rotation: outcome %s, want refused_parent", outcome)
	}
}

// Cell 5: a rotation never widens the parent's scopes, delivered twice or after the parent narrowed.
func TestDisorderARotationNeverWidensTheParentWhateverTheDelivery(t *testing.T) {
	parent := parentState(t, "ingest:write", "schema:read")
	narrowed := mustApply(t, parent, scoped(credA, 2, "ingest:write"), nil)
	wide := rotated(credB, credA, "ingest:write", "schema:read")
	for name, p := range map[string]*State{"parent at creation": &parent, "parent narrowed": &narrowed} {
		for try := 1; try <= 2; try++ {
			next, outcome := Apply(State{}, wide, p)
			want := Applied
			if p == &narrowed {
				want = RefusedWiden
			}
			if outcome != want || (want == RefusedWiden && next.Status != StatusUnknown) {
				t.Errorf("%s, try %d: outcome %s, want %s", name, try, outcome, want)
			}
		}
	}
	if _, outcome := Apply(State{}, rotated(credB, credA, "ingest:write", "ingest:status"), &parent); outcome != RefusedWiden {
		t.Errorf("a scope the parent never held: outcome %s, want refused_widen", outcome)
	}
}

// Cell 6: every arrival order of created, narrowing, revoke and a duplicate of each ends in one state, and no step reactivates or widens.
func TestDisorderEveryArrivalOrderEndsRevokedAndNarrow(t *testing.T) {
	events := []contracts.ExternalPushLifecycleEvent{
		created(credA, "ingest:write", "schema:read"), scoped(credA, 2, "ingest:write"), revoked(credA, 3),
	}
	all := append(slices.Clone(events), events...)
	orders := 0
	permutations(all, func(order []contracts.ExternalPushLifecycleEvent) {
		orders++
		d := &delivery{t: t}
		for _, e := range order {
			d.deliver(e)
		}
		if d.state.Status != StatusRevoked || d.state.Revision != 3 || !slices.Equal(d.state.Scopes, []string{"ingest:write"}) {
			t.Fatalf("order %v: ended %s r%d scopes %v, want revoked r3 [ingest:write]", describe(order), d.state.Status, d.state.Revision, d.state.Scopes)
		}
		denies(t, "end state", d.state)
	})
	if orders != 720 {
		t.Fatalf("%d orders ran, want 720", orders)
	}
}

// Cell 6b: the same with a widening event in the mix; it never lands in any order.
// Pins the CHAOS-12699 defect; the fix changes the expected end state to r4.
func TestDisorderARefusedWideningParksLaterRevisionsToday(t *testing.T) {
	events := []contracts.ExternalPushLifecycleEvent{
		created(credA, "ingest:write", "schema:read"), scoped(credA, 2, "ingest:write"),
		scoped(credA, 3, "ingest:write", "schema:read"), scoped(credA, 4, "ingest:write"),
	}
	permutations(slices.Clone(events), func(order []contracts.ExternalPushLifecycleEvent) {
		d := &delivery{t: t}
		for _, e := range order {
			d.deliver(e)
		}
		// The refused widening is final and holds revision 3 unapplied, so revision 4 stays parked behind it.
		if d.state.Revision != 2 || !slices.Equal(d.state.Scopes, []string{"ingest:write"}) {
			t.Fatalf("order %v: ended r%d scopes %v, want r2 [ingest:write]", describe(order), d.state.Revision, d.state.Scopes)
		}
	})
}

func describe(order []contracts.ExternalPushLifecycleEvent) []string {
	var out []string
	for _, e := range order {
		out = append(out, e.EventType+string(rune('0'+e.Payload.Revision)))
	}
	return out
}

// Cell 7: a credential at the end of any ended lifecycle permits nothing, and a past expiry ends liveness without the event.
func TestDisorderEndedCredentialsPermitNothing(t *testing.T) {
	active := parentState(t, "ingest:write", "schema:read")
	denies(t, "revoked", mustApply(t, active, revoked(credA, 2), nil))
	denies(t, "expired", mustApply(t, active, expired(credA, 2), nil))
	denies(t, "never seen", State{})
	if !Permits(active, "ingest:write", t0) {
		t.Fatal("control: an active credential must permit its scope")
	}
}
