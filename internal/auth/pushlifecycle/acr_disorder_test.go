package pushlifecycle

import (
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

const (
	acrdParent = "cred_parentaaaaaa"
	acrdChild  = "cred_childbbbbbbb"
)

var acrdExpiry = time.Date(2027, 10, 23, 10, 0, 0, 0, time.UTC)

// acrdEvent builds an acr wire event and maps it as the route does: FromAcr, then the engine's derived
// fingerprint and source binding (outboxprojection.Project sets both for the acr domain).
func acrdEvent(kind, id string, revision int64, at time.Time, change contracts.AcrCredentialLifecycleChange) contracts.ExternalPushLifecycleEvent {
	e := FromAcr(contracts.AcrCredentialLifecycleEvent{
		SchemaVersion: contracts.AcrCredentialLifecycleEventSurface, AggregateType: contracts.AcrCredentialAggregate, AggregateID: id,
		EventType: kind, IdempotencyKey: contracts.AcrCredentialAggregate + ":" + id + ":" + string(rune('0'+revision)),
		CreatedAt: contracts.Timestamp{Time: at},
		Payload:   contracts.AcrCredentialLifecyclePayload{Revision: contracts.Revision(revision), OrganizationID: org, ActorType: contracts.AcrActorSystem, Change: change},
	})
	e.Payload.Change.Fingerprint = "acrd" + id[len(id)-8:] + "0000"
	e.Payload.Binding = contracts.ExternalPushBindingSource
	return e
}

func acrdIssued(id string, at time.Time, scopes ...string) contracts.ExternalPushLifecycleEvent {
	return acrdEvent(contracts.AcrCredentialIssued, id, 1, at, contracts.AcrCredentialLifecycleChange{Status: "active", AcrClass: "browser", Scopes: scopes, OwnerSubject: "sub-1"})
}

func acrdRedeemed(id string, at time.Time, scopes ...string) contracts.ExternalPushLifecycleEvent {
	return acrdEvent(contracts.AcrCredentialDeviceRedeemed, id, 1, at, contracts.AcrCredentialLifecycleChange{Status: "active", AcrClass: "native_device", Scopes: scopes, OwnerSubject: "sub-1"})
}

func acrdRotated(id, parent string, at time.Time, scopes ...string) contracts.ExternalPushLifecycleEvent {
	return acrdEvent(contracts.AcrCredentialRotated, id, 1, at, contracts.AcrCredentialLifecycleChange{Status: "active", AcrClass: "native_device", Scopes: scopes, OwnerSubject: "sub-1", RotationParentID: parent})
}

func acrdRevoked(id string, revision int64, at time.Time) contracts.ExternalPushLifecycleEvent {
	return acrdEvent(contracts.AcrCredentialRevoked, id, revision, at, contracts.AcrCredentialLifecycleChange{Status: "revoked"})
}

func acrdExpired(id string, revision int64, at time.Time, expires time.Time) contracts.ExternalPushLifecycleEvent {
	return acrdEvent(contracts.AcrCredentialExpired, id, revision, at, contracts.AcrCredentialLifecycleChange{Status: "expired", ExpiresAt: &contracts.Timestamp{Time: expires}})
}

func acrdDenies(t *testing.T, label string, s State) {
	t.Helper()
	for _, scope := range []string{"context:read", "evidence:read"} {
		for _, now := range []time.Time{t0, t0.Add(-24 * time.Hour)} {
			if Permits(s, scope, now) {
				t.Errorf("%s: a %s credential permits %s at %s", label, s.Status, scope, now)
			}
		}
	}
	if Live(s, t0) {
		t.Errorf("%s: a %s credential is live", label, s.Status)
	}
}

// acrdCheck is the rule every step obeys: a terminal credential never moves, scopes never grow, and a step that is
// not Applied changes nothing.
func acrdCheck(t *testing.T, label string, before, next State, outcome Outcome) {
	t.Helper()
	if before.Status.Terminal() && (next.Status != before.Status || outcome == Applied) {
		t.Fatalf("%s: a %s credential moved to %q (%s)", label, before.Status, next.Status, outcome)
	}
	if before.Revision > 0 && !subset(next.Scopes, before.Scopes) {
		t.Fatalf("%s: scopes grew from %v to %v", label, before.Scopes, next.Scopes)
	}
	if outcome != Applied && !sameState(before, next) {
		t.Fatalf("%s: %s changed the state", label, outcome)
	}
}

type acrdDelivery struct {
	t      *testing.T
	state  State
	parked []contracts.ExternalPushLifecycleEvent
	parent *State
}

func (d *acrdDelivery) deliver(e contracts.ExternalPushLifecycleEvent) {
	d.t.Helper()
	d.attempt(e, true)
	for progress := true; progress; {
		progress = false
		for i, p := range slices.Clone(d.parked) {
			if d.attempt(p, false) {
				d.parked = slices.Delete(d.parked, i, i+1)
				progress = true
				break
			}
		}
	}
}

// attempt reports whether the event left the park (it decided, retry outcomes excepted).
func (d *acrdDelivery) attempt(e contracts.ExternalPushLifecycleEvent, first bool) bool {
	d.t.Helper()
	before := d.state
	next, outcome := Apply(before, e, d.parent)
	if outcome.Retry() {
		if first {
			d.parked = append(d.parked, e)
		}
		return false
	}
	acrdCheck(d.t, e.EventType, before, next, outcome)
	d.state = next
	return !first
}

func acrdPermute[T any](items []T, yield func([]T)) {
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

// Cell U1: every ordered pair of acr events of one credential, from a fresh credential and from every state the first
// event leaves, obeys the rule; a terminal credential permits nothing.
func TestAcrDisorderEveryEventPairOrder(t *testing.T) {
	at := t0.Add(time.Minute)
	events := map[string]contracts.ExternalPushLifecycleEvent{
		"issued r1":          acrdIssued(acrdParent, t0, "context:read", "evidence:read"),
		"device_redeemed r1": acrdRedeemed(acrdParent, t0, "context:read", "evidence:read"),
		"issued r1 wider":    acrdIssued(acrdParent, t0, "context:read", "evidence:read", "admin:all"),
		"revoked r2":         acrdRevoked(acrdParent, 2, at),
		"expired r2":         acrdExpired(acrdParent, 2, at, acrdExpiry),
		"revoked r3":         acrdRevoked(acrdParent, 3, at),
		"expired r3":         acrdExpired(acrdParent, 3, at, acrdExpiry),
	}
	pairs := 0
	for xn, x := range events {
		for yn, y := range events {
			pairs++
			label := xn + " then " + yn
			s1, o1 := Apply(State{}, x, nil)
			acrdCheck(t, label+" (first)", State{}, s1, o1)
			s2, o2 := Apply(s1, y, nil)
			acrdCheck(t, label, s1, s2, o2)
			if s2.Status.Terminal() {
				acrdDenies(t, label, s2)
			}
			if s1.Status.Terminal() && s2.Status != s1.Status {
				t.Errorf("%s: %s became %s", label, s1.Status, s2.Status)
			}
			// the same pair on a credential that was issued first, then x, then y
			if o1 == Applied && x.Payload.Revision == 1 {
				continue
			}
			base := mustApply(t, State{}, events["issued r1"], nil)
			b1, p1 := Apply(base, x, nil)
			acrdCheck(t, label+" (from issued)", base, b1, p1)
			b2, p2 := Apply(b1, y, nil)
			acrdCheck(t, label+" (from issued)", b1, b2, p2)
			if b1.Status.Terminal() && (b2.Status != b1.Status || p2 == Applied) {
				t.Errorf("%s: terminal credential moved to %s (%s)", label, b2.Status, p2)
			}
		}
	}
	if pairs != 49 {
		t.Fatalf("%d pairs ran, want 49", pairs)
	}
}

// Cell U2: every arrival order of issued, the terminal event and a duplicate of each ends in one state.
func TestAcrDisorderEveryArrivalOrderEndsTerminalWithTheIssuedScopes(t *testing.T) {
	for name, tc := range map[string]struct {
		terminal contracts.ExternalPushLifecycleEvent
		want     Status
	}{
		"revoked": {acrdRevoked(acrdParent, 2, t0.Add(time.Minute)), StatusRevoked},
		"expired": {acrdExpired(acrdParent, 2, t0.Add(time.Minute), acrdExpiry), StatusExpired},
	} {
		issued := acrdIssued(acrdParent, t0, "context:read", "evidence:read")
		events := []contracts.ExternalPushLifecycleEvent{issued, tc.terminal}
		all := append(slices.Clone(events), events...)
		orders := 0
		acrdPermute(all, func(order []contracts.ExternalPushLifecycleEvent) {
			orders++
			d := &acrdDelivery{t: t}
			for _, e := range order {
				d.deliver(e)
			}
			if d.state.Status != tc.want || d.state.Revision != 2 || !slices.Equal(d.state.Scopes, []string{"context:read", "evidence:read"}) || len(d.parked) != 0 {
				t.Fatalf("%s: ended %s r%d scopes %v parked %d, want %s r2 with the issued scopes", name, d.state.Status, d.state.Revision, d.state.Scopes, len(d.parked), tc.want)
			}
			acrdDenies(t, name, d.state)
		})
		if orders != 24 {
			t.Fatalf("%s: %d orders ran, want 24", name, orders)
		}
	}
}

// Cell U3: revoke and expiry of one credential race; the first applied wins and the other never moves it.
func TestAcrDisorderRevokeAndExpiryRaceNeverMovesATerminalCredential(t *testing.T) {
	at := t0.Add(time.Minute)
	issued := mustApply(t, State{}, acrdIssued(acrdParent, t0, "context:read"), nil)
	rev, exp := acrdRevoked(acrdParent, 2, at), acrdExpired(acrdParent, 2, at, acrdExpiry)
	for name, tc := range map[string]struct {
		first, second contracts.ExternalPushLifecycleEvent
		status        Status
	}{
		"revoked then expired r2": {rev, exp, StatusRevoked},
		"expired then revoked r2": {exp, rev, StatusExpired},
	} {
		state := mustApply(t, issued, tc.first, nil)
		next, outcome := Apply(state, tc.second, nil)
		if outcome != Duplicate || !sameState(next, state) || next.Status != tc.status {
			t.Errorf("%s: outcome %s status %s, want duplicate unchanged %s", name, outcome, next.Status, tc.status)
		}
		for _, later := range []contracts.ExternalPushLifecycleEvent{acrdRevoked(acrdParent, 3, at), acrdExpired(acrdParent, 3, at, acrdExpiry)} {
			if n, o := Apply(state, later, nil); o != RefusedTerminal || !sameState(n, state) {
				t.Errorf("%s then %s r3: outcome %s, want refused_terminal unchanged", name, later.EventType, o)
			}
		}
		acrdDenies(t, name, state)
	}
}

// Cell U4: an expired event that arrives late never extends the credential, and a past expiry ends liveness before it arrives.
func TestAcrDisorderDelayedExpiredNeverExtendsOrRevives(t *testing.T) {
	short := t0.Add(time.Hour)
	issuedEvent := acrdEvent(contracts.AcrCredentialIssued, acrdParent, 1, t0, contracts.AcrCredentialLifecycleChange{
		Status: "active", AcrClass: "browser", Scopes: []string{"context:read"}, OwnerSubject: "sub-1", ExpiresAt: &contracts.Timestamp{Time: short}})
	issued := mustApply(t, State{}, issuedEvent, nil)
	if Live(issued, short.Add(time.Second)) || Permits(issued, "context:read", short.Add(time.Second)) {
		t.Fatal("a credential past its expiry is live before the expired event arrived")
	}
	late := mustApply(t, issued, acrdExpired(acrdParent, 2, short.Add(24*time.Hour), acrdExpiry), nil)
	if late.ExpiresAt != "2026-10-23T11:00:00.000000Z" || late.Status != StatusExpired {
		t.Fatalf("a late expired event: expiry %q status %s, want the earlier expiry kept", late.ExpiresAt, late.Status)
	}
	acrdDenies(t, "late expired", late)
	revoked := mustApply(t, issued, acrdRevoked(acrdParent, 2, t0.Add(time.Minute)), nil)
	if next, outcome := Apply(revoked, acrdExpired(acrdParent, 3, short.Add(time.Hour), acrdExpiry), nil); outcome != RefusedTerminal || !sameState(next, revoked) {
		t.Fatalf("expired after revoked: outcome %s, want refused_terminal unchanged", outcome)
	}
}

// Cell U5: rotation arrives before, between and after the events of its parent; the child never widens the parent.
func TestAcrDisorderRotationOrdersNeverWidenOrReviveTheParent(t *testing.T) {
	issued := acrdIssued(acrdParent, t0, "context:read", "evidence:read")
	at := t0.Add(time.Minute)
	rotation := acrdRotated(acrdChild, acrdParent, at, "context:read")
	parentRevoke := acrdRevoked(acrdParent, 2, at)
	events := map[string]contracts.ExternalPushLifecycleEvent{"issued": issued, "rotation": rotation, "revoke": parentRevoke}
	orders := 0
	acrdPermute([]string{"issued", "rotation", "revoke"}, func(order []string) {
		orders++
		name := order[0] + ", " + order[1] + ", " + order[2]
		var parent, child State
		var parked []string
		try := func(step string) bool {
			e := events[step]
			if step == "rotation" {
				var p *State
				if parent.Revision > 0 {
					p = &parent
				}
				next, outcome := Apply(child, e, p)
				if outcome.Retry() {
					return false
				}
				acrdCheck(t, name+" "+step, child, next, outcome)
				child = next
				return true
			}
			next, outcome := Apply(parent, e, nil)
			if outcome.Retry() {
				return false
			}
			acrdCheck(t, name+" "+step, parent, next, outcome)
			parent = next
			return true
		}
		for _, step := range order {
			if !try(step) {
				parked = append(parked, step)
			}
			for progress := true; progress; {
				progress = false
				for i, p := range slices.Clone(parked) {
					if try(p) {
						parked = slices.Delete(parked, i, i+1)
						progress = true
						break
					}
				}
			}
		}
		if len(parked) != 0 || child.Status != StatusActive || !slices.Equal(child.Scopes, []string{"context:read"}) || child.RotationParentID != acrdParent {
			t.Errorf("%s: child ended %s scopes %v parent %q parked %v, want active [context:read] naming the parent", name, child.Status, child.Scopes, child.RotationParentID, parked)
		}
		if parent.Status != StatusRevoked || parent.Revision != 2 {
			t.Errorf("%s: parent ended %s r%d, want revoked r2", name, parent.Status, parent.Revision)
		}
		acrdDenies(t, name+" parent", parent)
	})
	if orders != 6 {
		t.Fatalf("%d orders ran, want 6", orders)
	}
	live := mustApply(t, State{}, issued, nil)
	narrow := live
	narrow.Scopes = []string{"context:read"}
	for name, p := range map[string]*State{"parent as issued": &live, "parent re-delivered": &live} {
		wide := acrdRotated(acrdChild, acrdParent, at, "context:read", "evidence:read", "admin:all")
		for try := 1; try <= 2; try++ {
			if next, outcome := Apply(State{}, wide, p); outcome != RefusedWiden || next.Status != StatusUnknown {
				t.Errorf("%s try %d: wide rotation outcome %s status %q, want refused_widen and nothing projected", name, try, outcome, next.Status)
			}
		}
	}
	if _, outcome := Apply(State{}, acrdRotated(acrdChild, acrdParent, at, "evidence:read"), &narrow); outcome != RefusedWiden {
		t.Errorf("a scope the narrowed parent lacks: outcome %s, want refused_widen", outcome)
	}
	// a second rotation of a parent a first rotation retired
	retired := mustApply(t, live, parentRevoke, nil)
	second := acrdRotated("cred_childccccccc", acrdParent, t0.Add(2*time.Minute), "context:read")
	if next, outcome := Apply(State{}, second, &retired); outcome != RefusedParent || next.Status != StatusUnknown {
		t.Errorf("second rotation of a retired parent: outcome %s status %q, want refused_parent", outcome, next.Status)
	}
	// the child's events never reach the parent aggregate
	if next, outcome := Apply(live, acrdRevoked(acrdChild, 2, at), nil); outcome != RefusedTransition || !sameState(next, live) {
		t.Errorf("child revoke on the parent state: outcome %s, want refused_transition unchanged", outcome)
	}
}

// Cell U6: an event of another organization or source never ends or expires a live acr credential.
func TestAcrDisorderChangedIdentityNeverRevokesOrExpires(t *testing.T) {
	live := mustApply(t, State{}, acrdIssued(acrdParent, t0, "context:read"), nil)
	at := t0.Add(time.Minute)
	for name, mutate := range map[string]func(contracts.ExternalPushLifecycleEvent) contracts.ExternalPushLifecycleEvent{
		"org": func(e contracts.ExternalPushLifecycleEvent) contracts.ExternalPushLifecycleEvent {
			return withOrg(e, "org_EXAMPLE0000000000000002")
		},
		"source": func(e contracts.ExternalPushLifecycleEvent) contracts.ExternalPushLifecycleEvent {
			return withSource(e, "other")
		},
	} {
		for kind, e := range map[string]contracts.ExternalPushLifecycleEvent{"revoked": acrdRevoked(acrdParent, 2, at), "expired": acrdExpired(acrdParent, 2, at, acrdExpiry)} {
			if next, outcome := Apply(live, mutate(e), nil); outcome != RefusedTransition || !sameState(next, live) {
				t.Errorf("%s event with another %s: outcome %s, want refused_transition unchanged", kind, name, outcome)
			}
		}
	}
}

// Cell U7 (KnownDefect, pins today's behaviour): a refused rotated child is final and never stored, so its later
// events at revision 2 or more are a gap, parked for ever. The child is never live, so no scope widens. Same class
// as the External Push pin of CHAOS-12699.
func TestAcrDisorderKnownDefectRefusedRotationParksTheChildsLaterRevisions(t *testing.T) {
	parent := mustApply(t, State{}, acrdIssued(acrdParent, t0, "context:read"), nil)
	at := t0.Add(time.Minute)
	d := &acrdDelivery{t: t, parent: &parent}
	d.deliver(acrdRotated(acrdChild, acrdParent, at, "context:read", "admin:all"))
	d.deliver(acrdRevoked(acrdChild, 2, at))
	if d.state.Status != StatusUnknown || len(d.parked) != 1 {
		t.Fatalf("child state %q parked %d, want unprojected with the revoke parked", d.state.Status, len(d.parked))
	}
	if next, outcome := Apply(State{}, acrdRevoked(acrdChild, 2, at), &parent); outcome != Gap || !sameState(next, State{}) {
		t.Fatalf("child revoke r2: outcome %s, want gap", outcome)
	}
	acrdDenies(t, "refused child", d.state)
}

// Cell U8 (KnownDefect, pins today's behaviour): the contract allows a revoked or expired event at revision 3 or more
// (contracts/auth/v1/README.md:434), but the core applies only the next revision, so a revoke that skips over the last
// applied revision is a gap: nothing is applied and the credential stays active and permitted. The fix is decided elsewhere.
func TestDisorderKnownDefectRevokeOverARevisionGapLeavesTheCredentialPermitted(t *testing.T) {
	active := mustApply(t, State{}, acrdIssued(acrdParent, t0, "context:read"), nil)
	for name, e := range map[string]contracts.ExternalPushLifecycleEvent{
		"revoked r3": acrdRevoked(acrdParent, 3, t0.Add(time.Minute)),
		"expired r4": acrdExpired(acrdParent, 4, t0.Add(time.Minute), acrdExpiry),
	} {
		next, outcome := Apply(active, e, nil)
		if outcome != Gap || !outcome.Retry() || !sameState(next, active) || next.Status != StatusActive || !Permits(next, "context:read", t0) {
			t.Errorf("%s over r1: outcome %s status %s permits %v, want gap, active and permitted", name, outcome, next.Status, Permits(next, "context:read", t0))
		}
	}
}
