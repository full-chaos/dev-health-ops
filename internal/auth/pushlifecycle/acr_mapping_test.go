package pushlifecycle

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// The acr credential lifecycle contract (acr-credential-lifecycle-event.v1)
// has no projection code of its own: these tests drive its golden events
// through the real Apply, so the ordering, terminal and no-widen rules are the
// push rules and cannot drift.
const (
	acrParentID = "cred_exampleparentaaa"
	// The engine derives the acr fingerprint; Apply only needs one at creation.
	acrTestFingerprint = "00112233445566ff"
)

func acrEvent(t *testing.T, name string) contracts.AcrCredentialLifecycleEvent {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "contracts/auth/v1/examples/acr-credential-lifecycle-event", name))
	if err != nil {
		t.Fatal(err)
	}
	event, err := contracts.ParseAcrCredentialLifecycleEvent(root, raw)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return event
}

func applyAcr(t *testing.T, state State, e contracts.AcrCredentialLifecycleEvent, parent *State) (State, Outcome) {
	t.Helper()
	mapped := FromAcr(e)
	if e.Payload.Revision == 1 {
		mapped.Payload.Change.Fingerprint = acrTestFingerprint
	}
	return Apply(state, mapped, parent)
}

func mustApplyAcr(t *testing.T, state State, e contracts.AcrCredentialLifecycleEvent, parent *State) State {
	t.Helper()
	next, outcome := applyAcr(t, state, e, parent)
	if outcome != Applied {
		t.Fatalf("%s revision %d: outcome %s, want applied", e.EventType, e.Payload.Revision, outcome)
	}
	return next
}

func acrParentIssued(t *testing.T, expires string) contracts.AcrCredentialLifecycleEvent {
	t.Helper()
	e := acrEvent(t, "valid-issued.json")
	e.AggregateID = acrParentID
	e.Payload.Change.ExpiresAt = nil
	if expires != "" {
		at, err := time.Parse(time.RFC3339, expires)
		if err != nil {
			t.Fatal(err)
		}
		e.Payload.Change.ExpiresAt = &contracts.Timestamp{Time: at}
	}
	return e
}

func TestAcrIssuedAndDeviceRedeemedOpenAnAggregateAndRevokeIsTerminal(t *testing.T) {
	for _, name := range []string{"valid-issued.json", "valid-issued-minimal.json", "valid-issued-workload-system.json", "valid-device-redeemed.json", "valid-device-redeemed-oauth-user.json"} {
		t.Run(name, func(t *testing.T) {
			state := mustApplyAcr(t, State{}, acrEvent(t, name), nil)
			if state.Status != StatusActive || state.Revision != 1 || len(state.Scopes) == 0 {
				t.Fatalf("state after open = %+v", state)
			}
			revoked := acrEvent(t, "valid-revoked-operator.json")
			state = mustApplyAcr(t, state, revoked, nil)
			if state.Status != StatusRevoked || !state.Status.Terminal() {
				t.Fatalf("state after revoke = %+v", state)
			}
			if Permits(state, "context:read", t0) {
				t.Fatal("a revoked credential permits a scope")
			}
			late := acrEvent(t, "valid-revoked-revision-3.json")
			if _, outcome := applyAcr(t, state, late, nil); outcome != RefusedTerminal {
				t.Fatalf("event after revoke: outcome %s, want refused_terminal", outcome)
			}
			expiredLate := acrEvent(t, "valid-expired-revision-4.json")
			expiredLate.Payload.Revision = 3
			if _, outcome := applyAcr(t, state, expiredLate, nil); outcome != RefusedTerminal {
				t.Fatalf("expired after revoke: outcome %s, want refused_terminal", outcome)
			}
		})
	}
}

func TestAcrOutOfOrderRevisionsAreGapDuplicateAndStale(t *testing.T) {
	issued := acrEvent(t, "valid-issued.json")
	if _, outcome := applyAcr(t, State{}, acrEvent(t, "valid-revoked-operator.json"), nil); outcome != Gap {
		t.Fatalf("revision 2 on a credential never seen: outcome %s, want gap", outcome)
	}
	state := mustApplyAcr(t, State{}, issued, nil)
	// A terminal event that skips revisions ends the credential (no acr event could fill the gap).
	for _, name := range []string{"valid-revoked-revision-3.json", "valid-expired-revision-4.json"} {
		if next, outcome := applyAcr(t, state, acrEvent(t, name), nil); outcome != Applied || !next.Status.Terminal() || Permits(next, "context:read", t0) {
			t.Fatalf("%s: outcome %s state %+v, want applied and ended", name, outcome, next)
		}
	}
	if _, outcome := applyAcr(t, state, issued, nil); outcome != Duplicate {
		t.Fatalf("replayed revision 1: outcome %s, want duplicate", outcome)
	}
	state = mustApplyAcr(t, state, acrEvent(t, "valid-revoked-operator.json"), nil)
	if _, outcome := applyAcr(t, state, issued, nil); outcome != Stale {
		t.Fatalf("revision 1 after revision 2: outcome %s, want stale", outcome)
	}
	if _, outcome := applyAcr(t, state, acrEvent(t, "valid-revoked-operator.json"), nil); outcome != Duplicate {
		t.Fatalf("replayed revision 2: outcome %s, want duplicate", outcome)
	}
}

func TestAcrRotationWithNoOverlapAcceptsEitherOrderOfTheParentRevoke(t *testing.T) {
	rotated := acrEvent(t, "valid-rotated-self.json")
	parentRevoked := acrEvent(t, "valid-revoked-rotated-parent.json")
	parent := mustApplyAcr(t, State{}, acrParentIssued(t, ""), nil)

	child := mustApplyAcr(t, State{}, rotated, &parent)
	if child.Status != StatusActive || child.RotationParentID != acrParentID {
		t.Fatalf("child after rotation = %+v", child)
	}
	retired := mustApplyAcr(t, parent, parentRevoked, nil)
	if retired.Status != StatusRevoked {
		t.Fatalf("parent after revoke = %+v", retired)
	}

	revokedFirst := mustApplyAcr(t, parent, parentRevoked, nil)
	child = mustApplyAcr(t, State{}, rotated, &revokedFirst)
	if child.Status != StatusActive {
		t.Fatalf("child after parent-first order = %+v", child)
	}
}

func TestAcrRotationWithOverlapLeavesTheParentActiveThenExpired(t *testing.T) {
	rotated := acrEvent(t, "valid-rotated-self.json")
	parent := mustApplyAcr(t, State{}, acrParentIssued(t, "2026-10-23T10:15:00Z"), nil)
	child := mustApplyAcr(t, State{}, rotated, &parent)
	if child.Status != StatusActive {
		t.Fatalf("child = %+v", child)
	}
	if !Live(parent, time.Date(2026, 10, 23, 10, 10, 0, 0, time.UTC)) {
		t.Fatal("the parent is not live inside the overlap")
	}
	if Live(parent, time.Date(2026, 10, 23, 10, 16, 0, 0, time.UTC)) {
		t.Fatal("the parent is live after its cut expiry, with no expired event yet")
	}
	expired := mustApplyAcr(t, parent, acrEvent(t, "valid-expired-after-overlap.json"), nil)
	if expired.Status != StatusExpired || !expired.Status.Terminal() {
		t.Fatalf("parent after expired = %+v", expired)
	}
}

func TestAcrRotationRefusesAWiderScopeSetAWrongParentAndAMissingParent(t *testing.T) {
	rotated := acrEvent(t, "valid-rotated-self.json")
	parent := mustApplyAcr(t, State{}, acrParentIssued(t, ""), nil)

	wider := rotated
	wider.Payload.Change.Scopes = []string{"context:read", "data:read"}
	if _, outcome := applyAcr(t, State{}, wider, &parent); outcome != RefusedWiden {
		t.Fatalf("wider rotation: outcome %s, want refused_widen", outcome)
	}
	if _, outcome := applyAcr(t, State{}, rotated, nil); outcome != ParentPending {
		t.Fatalf("no parent: outcome %s, want parent_pending", outcome)
	}
	other := parent
	other.OrganizationID = "org_EXAMPLE0000000000000002"
	if _, outcome := applyAcr(t, State{}, rotated, &other); outcome != RefusedParent {
		t.Fatalf("parent in another organization: outcome %s, want refused_parent", outcome)
	}
}

func TestAcrPastExpiryEndsLivenessWithoutAnEvent(t *testing.T) {
	state := mustApplyAcr(t, State{}, acrEvent(t, "valid-issued.json"), nil)
	if !Permits(state, "context:read", time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("an unexpired credential does not permit its scope")
	}
	if Permits(state, "context:read", time.Date(2027, 10, 23, 10, 0, 1, 0, time.UTC)) {
		t.Fatal("an expired credential permits a scope")
	}
	if Permits(state, "episode:write", time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("a credential permits a scope it does not hold")
	}
}
