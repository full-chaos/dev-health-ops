package pushlifecycle

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// A1: the contract lets an acr revoke or expiry skip revisions (contracts/auth/v1/README.md, revision 2 or more), and
// acr has no event that could fill the gap: the terminal event ends the credential at its own revision.
func TestAcrRevokeOrExpiryOverAGapEndsTheCredential(t *testing.T) {
	issued := mustApplyAcr(t, State{}, acrEvent(t, "valid-issued.json"), nil)
	if !Permits(issued, "context:read", t0) {
		t.Fatal("control: the issued credential must permit context:read")
	}
	for name, want := range map[string]struct {
		status   Status
		revision int64
	}{
		"valid-revoked-revision-3.json": {StatusRevoked, 3},
		"valid-expired-revision-4.json": {StatusExpired, 4},
	} {
		next, outcome := applyAcr(t, issued, acrEvent(t, name), nil)
		if outcome != Applied || next.Status != want.status || next.Revision != want.revision {
			t.Fatalf("%s over r1: outcome %s status %s r%d, want applied %s r%d", name, outcome, next.Status, next.Revision, want.status, want.revision)
		}
		togDenies(t, name, next)
		late := acrEvent(t, "valid-revoked-operator.json")
		if after, outcome := applyAcr(t, next, late, nil); outcome != Stale || !sameState(after, next) {
			t.Fatalf("%s then the skipped r2: outcome %s, want stale unchanged", name, outcome)
		}
	}
	if next, outcome := applyAcr(t, issued, acrEvent(t, "valid-expired-revision-4.json"), nil); outcome == Applied && next.ExpiresAt != "2026-10-23T10:15:00.000000Z" {
		t.Fatalf("expired r4 kept expiry %q, want the earlier 2026-10-23T10:15:00.000000Z", next.ExpiresAt)
	}
}

// A2: a refused rotated child never existed; its later revisions are refused for good, in order or over a gap, and
// never parked. A child never seen at all still waits.
func TestAcrRefusedRotationDecidesTheChildsLaterRevisions(t *testing.T) {
	parent := mustApplyAcr(t, State{}, acrParentIssued(t, ""), nil)
	wider := acrEvent(t, "valid-rotated-self.json")
	wider.Payload.Change.Scopes = []string{"context:read", "data:read"}
	child, outcome := applyAcr(t, State{}, wider, &parent)
	if outcome != RefusedWiden || child.Status != StatusUnknown {
		t.Fatalf("wider rotation: outcome %s status %q, want refused_widen and nothing projected", outcome, child.Status)
	}
	revoke := acrEvent(t, "valid-revoked-revision-3.json")
	for _, revision := range []int64{2, 3} {
		e := revoke
		e.Payload.Revision = contracts.Revision(revision)
		next, outcome := applyAcr(t, child, e, &parent)
		if outcome != RefusedTransition || next.Status != StatusUnknown {
			t.Errorf("child revoked r%d after the refused rotation: outcome %s, want refused_transition", revision, outcome)
		}
		togDenies(t, "refused child", next)
		if _, outcome := applyAcr(t, State{}, e, &parent); outcome != Gap {
			t.Errorf("revoked r%d of a credential never seen: outcome %s, want gap", revision, outcome)
		}
	}
	togDenies(t, "refused child", child)
}
