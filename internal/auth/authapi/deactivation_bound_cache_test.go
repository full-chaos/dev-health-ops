package authapi

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
)

// A repository allow by a grant that ends soon is a cached allow of the cache bound. A hit may outlive the grant's
// expiry, but never the cache bound; at the bound the next call reads the state and denies with grant_expired.
func TestDeactivationBound_Grant_PDP_ExpiryEndsTheCachedAllowAtTheCacheBound(t *testing.T) {
	const repoAction = "acr.context.read"
	c := newCacheFixture(t)
	snap := c.state.snapshots[snapKey{c.principal, c.org}]
	snap.grants = []authorization.Grant{{OrgID: c.org, Role: "viewer", ResourceKind: "repository", ResourceKey: "acme/api",
		ExpiresAtUnixNano: authorizeNow + int64(5*time.Second)}}
	ask := func() wireDecision {
		t.Helper()
		rec := c.one(c.question(repoAction, "repository", "acme/api", c.org))
		return decodeWire[wireDecision](t, rec)
	}
	if d := ask(); !d.Allowed {
		t.Fatalf("a live grant denied: %+v", d)
	}
	c.advance(decisioncache.TTL - time.Second)
	if d := ask(); !d.Allowed {
		t.Fatalf("inside the cache bound the stored allow was not served: %+v", d)
	}
	if c.state.snapReads != 1 {
		t.Fatalf("%d snapshot reads inside the bound, want 1", c.state.snapReads)
	}
	c.advance(time.Second)
	d := ask()
	if d.Allowed || d.ReasonCode != string(authorization.ReasonGrantExpired) {
		t.Fatalf("at the cache bound: %+v, want a deny with grant_expired", d)
	}
	if c.state.snapReads != 2 {
		t.Fatalf("%d snapshot reads, want 2: the state was not read at the bound", c.state.snapReads)
	}
}

// A revoked grant ends the stored allow at once, with no feed event and no wait for the cache bound, and the
// membership default does not come back because the grant row is still there.
func TestDeactivationBound_Grant_PDP_RevokeDeniesAtOnceWithoutTheMembershipDefault(t *testing.T) {
	const repoAction = "acr.context.read"
	c := newCacheFixture(t)
	snap := c.state.snapshots[snapKey{c.principal, c.org}]
	grant := authorization.Grant{OrgID: c.org, Role: "viewer", ResourceKind: "repository", ResourceKey: "acme/api"}
	snap.grants = []authorization.Grant{grant}
	ask := func() wireDecision {
		t.Helper()
		return decodeWire[wireDecision](t, c.one(c.question(repoAction, "repository", "acme/api", c.org)))
	}
	if !ask().Allowed || !ask().Allowed {
		t.Fatal("the grant allow was not stored and served")
	}
	grant.Revoked = true
	snap.grants = []authorization.Grant{grant}
	d := ask()
	if d.Allowed || d.ReasonCode != string(authorization.ReasonGrantRevoked) {
		t.Fatalf("after the revoke: %+v, want a deny with grant_revoked", d)
	}
}
