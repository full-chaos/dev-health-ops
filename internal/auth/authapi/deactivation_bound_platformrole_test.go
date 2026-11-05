package authapi

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// A platform action is never stored: every call reads the state. A revoke of the platform role, or a deactivation of
// the user that holds it, denies the next call, with no feed event and no clock advance. These are pins: they hold on
// main and must keep holding.
func TestDeactivationBound_PlatformRole_PDP_DenyAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name   string
		flip   func(s *fakeSnapshot)
		reason authorization.Reason
	}{
		{"platform role revoked", func(s *fakeSnapshot) { s.platformRole = "" }, authorization.ReasonPlatformRoleAbsent},
		{"holder deactivated", func(s *fakeSnapshot) { s.principal = authorization.StatusSuspended }, authorization.ReasonPrincipalInactive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCacheFixture(t)
			snap := c.state.snapshots[snapKey{c.principal, ""}]
			if snap == nil {
				snap = &fakeSnapshot{principal: authorization.StatusActive, now: authorizeNow}
				c.state.snapshots[snapKey{c.principal, ""}] = snap
			}
			snap.principal, snap.platformRole = authorization.StatusActive, "platform_admin"
			ask := func() wireDecision {
				t.Helper()
				return decodeWire[wireDecision](t, c.one(c.question("admin.users", "platform", "platform", "")))
			}
			if d := ask(); !d.Allowed {
				t.Fatalf("a live platform admin was denied: %+v", d)
			}
			if d := ask(); !d.Allowed || c.state.snapReads != 2 {
				t.Fatalf("second call: %+v after %d snapshot reads, want an allow read from the state again", d, c.state.snapReads)
			}
			tc.flip(snap)
			d := ask()
			if d.Allowed || d.ReasonCode != string(tc.reason) {
				t.Fatalf("after the change: %+v, want a deny with %s", d, tc.reason)
			}
			if c.state.snapReads != 3 {
				t.Fatalf("%d snapshot reads, want 3: every platform call reads the state", c.state.snapReads)
			}
		})
	}
}
