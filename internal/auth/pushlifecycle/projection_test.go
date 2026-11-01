package pushlifecycle

import (
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

var t0 = time.Date(2026, 10, 23, 10, 0, 0, 0, time.UTC)

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
		CreatedAt:     contracts.Timestamp{Time: t0},
		Payload: contracts.ExternalPushLifecyclePayload{
			Revision: contracts.Revision(revision), OrganizationID: org, Binding: contracts.ExternalPushBindingSource, SourceID: source, Change: change,
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

func mustApply(t *testing.T, state State, e contracts.ExternalPushLifecycleEvent, parent *State) State {
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
	return a.AggregateID == b.AggregateID && a.OrganizationID == b.OrganizationID && a.Binding == b.Binding && a.SourceID == b.SourceID &&
		a.Revision == b.Revision && a.Status == b.Status && a.Fingerprint == b.Fingerprint &&
		slices.Equal(a.Scopes, b.Scopes) && a.RotationParentID == b.RotationParentID && a.ExpiresAt == b.ExpiresAt && a.TerminalAt.Equal(b.TerminalAt)
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

// parentState builds the projected state of the credential a rotation names.
func parentState(t *testing.T, scopes ...string) State {
	t.Helper()
	return mustApply(t, State{}, created(credA, scopes...), nil)
}

func TestProjectionRotationIsBoundToTheNamedParent(t *testing.T) {
	parent := parentState(t, "ingest:write", "schema:read")
	state := mustApply(t, State{}, rotated(credB, credA, "ingest:write"), &parent)
	if state.RotationParentID != credA || state.Status != StatusActive {
		t.Fatalf("after rotated: %+v", state)
	}
	other := func(mutate func(*State)) *State {
		p := parent
		mutate(&p)
		return &p
	}
	cases := map[string]struct {
		e      contracts.ExternalPushLifecycleEvent
		parent *State
		want   Outcome
	}{
		"no parent state":               {rotated(credB, credA, "ingest:write"), nil, RefusedParent},
		"never-seen parent":             {rotated(credB, credA, "ingest:write"), &State{}, RefusedParent},
		"wider than parent":             {rotated(credB, credA, "ingest:write", "ingest:status"), &parent, RefusedWiden},
		"its own parent":                {rotated(credB, credB, "ingest:write"), &parent, RefusedTransition},
		"no parent id":                  {rotated(credB, "", "ingest:write"), &parent, RefusedTransition},
		"another credential":            {rotated(credB, credA, "ingest:write"), other(func(p *State) { p.AggregateID = "cred-C" }), RefusedParent},
		"parent with no id":             {rotated(credB, credA, "ingest:write"), other(func(p *State) { p.AggregateID = "" }), RefusedParent},
		"parent in another org":         {rotated(credB, credA, "ingest:write"), other(func(p *State) { p.OrganizationID = "org_EXAMPLE0000000000000002" }), RefusedParent},
		"parent in another source":      {rotated(credB, credA, "ingest:write"), other(func(p *State) { p.SourceID = "src_EXAMPLE0000000000000002" }), RefusedParent},
		"rotated after create":          {rotated(credB, credA, "ingest:write"), &parent, RefusedTransition},
		"child org differs":             {withOrg(rotated(credB, credA, "ingest:write"), "org_EXAMPLE0000000000000002"), &parent, RefusedParent},
		"child source differs":          {withSource(rotated(credB, credA, "ingest:write"), "src_EXAMPLE0000000000000002"), &parent, RefusedParent},
		"child binding differs only":    {withBinding(rotated(credB, credA, "ingest:write"), contracts.ExternalPushBindingOrganization), &parent, RefusedParent},
		"parent binding differs only":   {rotated(credB, credA, "ingest:write"), other(func(p *State) { p.Binding = contracts.ExternalPushBindingOrganization }), RefusedParent},
		"org-wide child, source parent": {orgWide(rotated(credB, credA, "ingest:write")), &parent, RefusedParent},
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

func withOrg(e contracts.ExternalPushLifecycleEvent, org string) contracts.ExternalPushLifecycleEvent {
	e.Payload.OrganizationID = org
	return e
}

func withBinding(e contracts.ExternalPushLifecycleEvent, binding string) contracts.ExternalPushLifecycleEvent {
	e.Payload.Binding = binding
	return e
}

// orgWide turns an event into the organization-wide form: no source.
func orgWide(e contracts.ExternalPushLifecycleEvent) contracts.ExternalPushLifecycleEvent {
	e.Payload.Binding = contracts.ExternalPushBindingOrganization
	e.Payload.SourceID = ""
	return e
}

func withSource(e contracts.ExternalPushLifecycleEvent, source string) contracts.ExternalPushLifecycleEvent {
	e.Payload.SourceID = source
	return e
}

// TestProjectionRotationParentTerminalOrdering: a parent that became terminal
// before the rotation's created_at is refused; one that became terminal at or
// after it is accepted, so rotate-then-retire works in any arrival order.
func TestProjectionRotationParentTerminalOrdering(t *testing.T) {
	active := parentState(t, "ingest:write")
	retire := func(at time.Time) State {
		e := revoked(credA, 2)
		e.CreatedAt = contracts.Timestamp{Time: at}
		return mustApply(t, active, e, nil)
	}
	rotation := rotated(credB, credA, "ingest:write")
	for name, tc := range map[string]struct {
		parent State
		want   Outcome
	}{
		"retired before the rotation": {retire(t0.Add(-time.Second)), RefusedParent},
		"retired at the rotation":     {retire(t0), Applied},
		"retired after the rotation":  {retire(t0.Add(time.Second)), Applied},
		"still active":                {active, Applied},
	} {
		if _, outcome := Apply(State{}, rotation, &tc.parent); outcome != tc.want {
			t.Errorf("%s: outcome %s, want %s", name, outcome, tc.want)
		}
	}
	expiring := func(expiresAt string, status Status, terminalAt time.Time) State {
		p := active
		p.ExpiresAt, p.Status, p.TerminalAt = expiresAt, status, terminalAt
		return p
	}
	for name, tc := range map[string]struct {
		parent State
		want   Outcome
	}{
		"expired by time, event not yet seen": {expiring("2026-10-22T10:00:00.000000Z", StatusActive, time.Time{}), RefusedParent},
		"expired by time, late expired event": {expiring("2026-10-22T10:00:00.000000Z", StatusExpired, t0.Add(time.Hour)), RefusedParent},
		"expires after the rotation":          {expiring("2026-10-24T10:00:00.000000Z", StatusActive, time.Time{}), Applied},
		"expires at the rotation":             {expiring("2026-10-23T10:00:00.000000Z", StatusActive, time.Time{}), Applied},
		"unparseable expiry":                  {expiring("tomorrow", StatusActive, time.Time{}), RefusedParent},
		"bogus status":                        {expiring("", Status("pending"), time.Time{}), RefusedParent},
		"empty status":                        {expiring("", StatusUnknown, time.Time{}), RefusedParent},
		"terminal, zero stamp":                {expiring("", StatusRevoked, time.Time{}), RefusedParent},
	} {
		if _, outcome := Apply(State{}, rotation, &tc.parent); outcome != tc.want {
			t.Errorf("%s: outcome %s, want %s", name, outcome, tc.want)
		}
	}
	zeroTime := rotated(credB, credA, "ingest:write")
	zeroTime.CreatedAt = contracts.Timestamp{}
	terminalZero := expiring("", StatusRevoked, time.Time{})
	if _, outcome := Apply(State{}, zeroTime, &terminalZero); outcome != RefusedParent {
		t.Errorf("rotation at the zero time over a terminal parent with a zero stamp: outcome %s, want refused_parent", outcome)
	}
	noStamp := retire(t0)
	noStamp.TerminalAt = time.Time{}
	if _, outcome := Apply(State{}, rotation, &noStamp); outcome != RefusedParent {
		t.Errorf("terminal parent with no timestamp: outcome %s, want refused_parent (fail closed)", outcome)
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
		parent := &State{AggregateID: credB, OrganizationID: org, Binding: contracts.ExternalPushBindingSource, SourceID: source, Revision: 1, Status: StatusActive, Scopes: scopePool}
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

func TestProjectionRecordsWhenACredentialBecameTerminal(t *testing.T) {
	active := parentState(t, "ingest:write")
	at := t0.Add(2 * time.Hour)
	for name, e := range map[string]contracts.ExternalPushLifecycleEvent{"revoked": revoked(credA, 2), "expired": expired(credA, 2)} {
		e.CreatedAt = contracts.Timestamp{Time: at}
		got := mustApply(t, active, e, nil)
		if !got.TerminalAt.Equal(at) {
			t.Errorf("%s: TerminalAt = %v, want the event's created_at %v", name, got.TerminalAt, at)
		}
	}
	if !active.TerminalAt.IsZero() {
		t.Error("an active credential has a TerminalAt")
	}
}

func TestProjectionRefusesAParentNeverApplied(t *testing.T) {
	parent := parentState(t, "ingest:write")
	parent.Revision = 0
	if _, outcome := Apply(State{}, rotated(credB, credA, "ingest:write"), &parent); outcome != RefusedParent {
		t.Fatalf("a parent at revision 0: outcome %s, want refused_parent", outcome)
	}
}

// TestProjectionExpiredKeepsTheEarlierExpiry: whichever order the created and
// expired events carry their expiry in, the parent keeps the earlier one, so
// the rotation verdict does not depend on arrival.
func TestProjectionExpiredKeepsTheEarlierExpiry(t *testing.T) {
	early := contracts.Timestamp{Time: t0.Add(-2 * time.Hour)}
	late := contracts.Timestamp{Time: time.Date(2027, 10, 23, 10, 0, 0, 0, time.UTC)}
	for name, tc := range map[string]struct{ created, expiredAt contracts.Timestamp }{
		"created early, expired event later": {early, late},
		"created late, expired event early":  {late, early},
	} {
		c := created(credA, "ingest:write")
		c.Payload.Change.ExpiresAt = &tc.created
		parent := mustApply(t, State{}, c, nil)
		e := expired(credA, 2)
		e.Payload.Change.ExpiresAt = &tc.expiredAt
		e.CreatedAt = contracts.Timestamp{Time: t0.Add(time.Hour)}
		parent = mustApply(t, parent, e, nil)
		if want := "2026-10-23T08:00:00.000000Z"; parent.ExpiresAt != want {
			t.Errorf("%s: ExpiresAt = %s, want the earlier %s", name, parent.ExpiresAt, want)
		}
		if _, outcome := Apply(State{}, rotated(credB, credA, "ingest:write"), &parent); outcome != RefusedParent {
			t.Errorf("%s: rotation outcome %s, want refused_parent", name, outcome)
		}
	}
}

func TestProjectionLiveTreatsAPastExpiryAsNotLiveBeforeTheEventArrives(t *testing.T) {
	expiresAt := contracts.Timestamp{Time: t0.Add(time.Hour)}
	c := created(credA, "ingest:write")
	c.Payload.Change.ExpiresAt = &expiresAt
	active := mustApply(t, State{}, c, nil)
	noExpiry := mustApply(t, State{}, created(credB, "ingest:write"), nil)
	for name, tc := range map[string]struct {
		state State
		now   time.Time
		want  bool
	}{
		"never seen":                  {State{}, t0, false},
		"no expiry":                   {noExpiry, t0.Add(1000 * time.Hour), true},
		"before expiry":               {active, t0.Add(time.Hour - time.Nanosecond), true},
		"at expiry":                   {active, t0.Add(time.Hour), false},
		"past expiry, no event yet":   {active, t0.Add(2 * time.Hour), false},
		"revoked":                     {mustApply(t, noExpiry, revoked(credB, 2), nil), t0, false},
		"expired status":              {mustApply(t, noExpiry, expired(credB, 2), nil), t0, false},
		"unparseable expiry":          {State{AggregateID: credA, Revision: 1, Status: StatusActive, ExpiresAt: "soon"}, t0, false},
		"unknown status with history": {State{AggregateID: credA, Revision: 1, Status: "paused"}, t0, false},
	} {
		if got := Live(tc.state, tc.now); got != tc.want {
			t.Errorf("%s: Live = %v, want %v", name, got, tc.want)
		}
	}
}

func TestProjectionLateExpiredEventNeverExtendsACredential(t *testing.T) {
	early := contracts.Timestamp{Time: t0.Add(time.Hour)}
	late := contracts.Timestamp{Time: t0.Add(48 * time.Hour)}
	c := created(credA, "ingest:write")
	c.Payload.Change.ExpiresAt = &early
	state := mustApply(t, State{}, c, nil)
	now := t0.Add(2 * time.Hour)
	if Live(state, now) {
		t.Fatalf("before the expired event: Live = true past expires_at")
	}
	e := expired(credA, 2)
	e.Payload.Change.ExpiresAt = &late
	e.CreatedAt = contracts.Timestamp{Time: now}
	state = mustApply(t, state, e, nil)
	if Live(state, now) || Live(state, t0.Add(24*time.Hour)) {
		t.Fatalf("after a late expired event carrying a later expires_at: Live = true, want false")
	}
}

func TestProjectionScopesOnlyNarrowWhateverTheDeliveryOrder(t *testing.T) {
	base := mustApply(t, State{}, created(credA, "ingest:write", "schema:read"), nil)
	narrow, widen := scoped(credA, 2, "ingest:write"), scoped(credA, 3, "ingest:write", "schema:read")

	in := mustApply(t, base, narrow, nil)
	next, outcome := Apply(in, widen, nil)
	if outcome != RefusedWiden || !sameState(next, in) {
		t.Fatalf("in order: later widen outcome %s, want refused_widen with state unchanged", outcome)
	}
	if _, outcome := Apply(next, widen, nil); outcome != RefusedWiden {
		t.Fatalf("a redelivered refused widen is %s, want refused_widen again", outcome)
	}

	state := base
	if _, outcome := Apply(state, widen, nil); outcome != Gap {
		t.Fatalf("widen delivered before the narrowing: outcome %s, want gap", outcome)
	}
	state = mustApply(t, state, narrow, nil)
	if _, outcome := Apply(state, widen, nil); outcome != RefusedWiden {
		t.Fatalf("widen after the late narrowing: outcome %s, want refused_widen", outcome)
	}
	if !slices.Equal(state.Scopes, []string{"ingest:write"}) {
		t.Fatalf("scopes = %v, want [ingest:write]", state.Scopes)
	}

	none := mustApply(t, base, scoped(credA, 2), nil)
	if _, outcome := Apply(none, scoped(credA, 3, "ingest:write"), nil); outcome != RefusedWiden {
		t.Fatalf("widening from none: outcome %s, want refused_widen", outcome)
	}
}

func TestProjectionEmptyScopesStayEmptyAndPermitNothing(t *testing.T) {
	none := mustApply(t, mustApply(t, State{}, created(credA, "ingest:write"), nil), scoped(credA, 2), nil)
	if len(none.Scopes) != 0 || none.Status != StatusActive {
		t.Fatalf("after scoping to none: %+v", none)
	}
	next, outcome := Apply(none, scoped(credA, 3), nil)
	if outcome != Applied || len(next.Scopes) != 0 {
		t.Fatalf("empty scoped again: outcome %s scopes %v, want applied and still empty", outcome, next.Scopes)
	}
	if _, outcome := Apply(none, scoped(credA, 3, "ingest:write"), nil); outcome != RefusedWiden {
		t.Fatalf("scoping an empty credential up: outcome %s, want refused_widen", outcome)
	}
	for _, scope := range []string{"ingest:write", "schema:read", "*", ""} {
		if Permits(none, scope, t0) {
			t.Errorf("a credential with no scopes permits %q, want nothing", scope)
		}
		if Permits(State{}, scope, t0) {
			t.Errorf("a credential never seen permits %q, want nothing", scope)
		}
	}
	full := mustApply(t, State{}, created(credA, "ingest:write"), nil)
	if !Permits(full, "ingest:write", t0) || Permits(full, "schema:read", t0) || Permits(full, "", t0) {
		t.Errorf("a credential with one scope must permit exactly that scope")
	}
	revoked := mustApply(t, full, revoked(credA, 2), nil)
	if Permits(revoked, "ingest:write", t0) {
		t.Errorf("a revoked credential permits a scope")
	}
}

func TestProjectionOrganizationWideCredentialLifecycle(t *testing.T) {
	create := orgWide(created(credA, "ingest:status", "ingest:write"))
	state := mustApply(t, State{}, create, nil)
	if state.Binding != contracts.ExternalPushBindingOrganization || state.SourceID != "" || state.Status != StatusActive {
		t.Fatalf("after created: %+v", state)
	}
	state = mustApply(t, state, orgWide(scoped(credA, 2, "ingest:status")), nil)
	rotate := orgWide(rotated(credB, credA, "ingest:status"))
	parent := state
	child := mustApply(t, State{}, rotate, &parent)
	if child.Binding != contracts.ExternalPushBindingOrganization || child.SourceID != "" || child.RotationParentID != credA {
		t.Fatalf("after rotated: %+v", child)
	}
	retired := mustApply(t, state, orgWide(revoked(credA, 3)), nil)
	if retired.Status != StatusRevoked {
		t.Fatalf("after revoked: %+v", retired)
	}
}

func TestProjectionBindingNeverChangesInsideAnAggregate(t *testing.T) {
	sourceBound := mustApply(t, State{}, created(credA, "ingest:write"), nil)
	orgBound := mustApply(t, State{}, orgWide(created(credA, "ingest:write")), nil)
	cases := map[string]struct {
		state State
		e     contracts.ExternalPushLifecycleEvent
	}{
		"source credential, org-wide scoped":  {sourceBound, orgWide(scoped(credA, 2, "ingest:write"))},
		"source credential, org-wide revoked": {sourceBound, orgWide(revoked(credA, 2))},
		"org credential, source scoped":       {orgBound, scoped(credA, 2, "ingest:write")},
		"org credential, source revoked":      {orgBound, revoked(credA, 2)},
		"binding differs, same source id":     {sourceBound, withBinding(revoked(credA, 2), contracts.ExternalPushBindingOrganization)},
	}
	for name, tc := range cases {
		next, outcome := Apply(tc.state, tc.e, nil)
		if outcome != RefusedTransition || !sameState(next, tc.state) {
			t.Errorf("%s: outcome %s, want %s with the state unchanged", name, outcome, RefusedTransition)
		}
	}
}
