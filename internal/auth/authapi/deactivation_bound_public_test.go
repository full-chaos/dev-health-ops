package authapi

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// A stored allow is served until a deactivation. Once go-api deactivates the user, deactivates the organization or
// removes the membership, the very next call reads the state and denies: no feed event and no clock advance.
// These are pins: they hold on main and must keep holding.
func TestDeactivationBound_UserOrgMembership_PDP_DenyAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name   string
		flip   func(c *cacheFixture)
		reason authorization.Reason
	}{
		{"user deactivated", func(c *cacheFixture) {
			k := snapKey{c.principal, c.org}
			u := c.cs.unfed[k]
			u.UserActive = false
			c.cs.unfed[k] = u
			c.state.snapshots[k].principal = authorization.StatusSuspended
		}, authorization.ReasonPrincipalInactive},
		{"organization deactivated", func(c *cacheFixture) {
			c.state.snapshots[snapKey{c.principal, c.org}].orgStatus = authorization.StatusSuspended
		}, authorization.ReasonOrganizationInactive},
		{"membership removed", func(c *cacheFixture) {
			k := snapKey{c.principal, c.org}
			u := c.cs.unfed[k]
			u.MembershipRoleFound, u.MembershipRole = false, ""
			c.cs.unfed[k] = u
			c.state.snapshots[k].membership = nil
		}, authorization.ReasonNoMembership},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCacheFixture(t)
			if !c.ask().Allowed || !c.ask().Allowed {
				t.Fatal("the allow was not stored and served")
			}
			if c.state.snapReads != 1 {
				t.Fatalf("%d snapshot reads before the change, want 1", c.state.snapReads)
			}
			tc.flip(c)
			d := c.ask()
			if d.Allowed || d.ReasonCode != string(tc.reason) {
				t.Fatalf("after the change: %+v, want a deny with %s", d, tc.reason)
			}
			if c.state.snapReads != 2 {
				t.Fatalf("%d snapshot reads, want 2: the stored allow was served after the change", c.state.snapReads)
			}
		})
	}
}

// A stored allow never outlives the cache bound: at the bound the next call reads the state again.
func TestDeactivationBound_PDP_StoredAllowEndsAtTheCacheBound(t *testing.T) {
	c := newCacheFixture(t)
	c.ask()
	c.advance(29*time.Second)
	if !c.ask().Allowed || c.state.snapReads != 1 {
		t.Fatalf("inside the bound the stored allow was not served (%d reads)", c.state.snapReads)
	}
	c.advance(time.Second)
	if !c.ask().Allowed {
		t.Fatal("a live state denied")
	}
	if c.state.snapReads != 2 {
		t.Fatalf("%d snapshot reads at the bound, want 2: the stored allow outlived the cache bound", c.state.snapReads)
	}
}
