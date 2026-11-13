package authapi

import (
	"fmt"
	"net/http"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// The read listing's cap recount (CHAOS-13618, acp ruling option 3): when the listing would pass 256 keys, only the
// keys with a live repository identity row (the live path of their id) count. 256 or fewer of them is the 200 with
// exactly those keys; still more is the same deny. A listing of 256 or fewer keys is not recounted.

// capTrap is a member snapshot of the fixture's principal with grants and a live identity row for every key in keys
// except those in missing.
func capTrap(f *readableFixture, role string, grants []authorization.Grant, keys []string, missing ...string) *catalogTrap {
	c := newCatalogTrap(f.t, f.member(role, grants), 0)
	for i, key := range keys {
		if !slices.Contains(missing, key) {
			c.identity(fmt.Sprintf("github:%d", 1000+i), key)
		}
	}
	return c
}

func readListing(t *testing.T, f *readableFixture, ls *listingState, snap authorization.GrantSnapshot) (string, []string, string) {
	t.Helper()
	ls.snaps[snapKey{f.principal, f.org}] = snap
	code, answer := f.do(f.body(), issuerBearer())
	if code != http.StatusOK || answer["schema_version"] != "readable_repositories.v1" {
		t.Fatalf("%d %v, want a 200 readable_repositories.v1", code, answer)
	}
	scope, _ := answer["scope"].(string)
	reason, _ := answer["reason_code"].(string)
	var keys []string
	if scope == ReadableScopeRepositories {
		keys = readableKeys(t, answer)
	}
	return scope, keys, reason
}

func TestReadableRepositoriesRecountsAnOverCapListingOnLiveIdentities(t *testing.T) {
	ls := &listingState{snaps: map[snapKey]authorization.GrantSnapshot{}}
	f := newReadableFixtureOn(t, ls)
	rows257, keys257 := ownGrants(f.org, 257)
	rows258, keys258 := ownGrants(f.org, 258)
	rows256, keys256 := ownGrants(f.org, 256)
	rows3, keys3 := ownGrants(f.org, 3)

	shared := capTrap(f, "member", rows257, keys257)
	shared.ids[keys257[7]] = ""
	shared.paths["github:9007"] = keys257[7]
	elsewhere := capTrap(f, "member", rows257, keys257)
	elsewhere.paths["github:1007"] = "acme/elsewhere"

	deadID := listedGrant(f.org, "acme/old-name")
	deadID.RepoID = "github:404"
	liveID := listedGrant(f.org, "acme/old-path")
	liveID.RepoID = "github:900"
	rows255, keys255 := ownGrants(f.org, 255)
	withLiveID := capTrap(f, "member", append(slices.Clone(rows255), listedGrant(f.org, "zeta/not-synced-1"), listedGrant(f.org, "zeta/not-synced-2"), liveID), keys255)
	withLiveID.identity("github:900", "acme/renamed")
	overWithLiveID := capTrap(f, "member", append(slices.Clone(rows256), listedGrant(f.org, "zeta/not-synced-1"), liveID), keys256)
	overWithLiveID.identity("github:900", "acme/renamed")

	for name, tc := range map[string]struct {
		snap   authorization.GrantSnapshot
		scope  string
		keys   []string
		reason string
	}{
		"(a) 257 grants, one with no identity row": {
			capTrap(f, "member", rows257, keys257, keys257[256]), ReadableScopeRepositories, keys257[:256], ""},
		"(a) 257 grants, one path shared by two identity rows": {
			shared, ReadableScopeRepositories, slices.Delete(slices.Clone(keys257), 7, 8), ""},
		"(a) 257 grants, one path whose id lives at another path": {
			elsewhere, ReadableScopeRepositories, slices.Delete(slices.Clone(keys257), 7, 8), ""},
		"(a) an id-bearing grant's live path counts in the recount": {
			withLiveID, ReadableScopeRepositories, slices.Sorted(slices.Values(append(slices.Clone(keys255), "acme/renamed"))), ""},
		"(b) 258 grants, one with no identity row": {
			capTrap(f, "member", rows258, keys258, keys258[0]), ReadableScopeDenied, nil, "repository_allowlist_too_large"},
		"(b) 257 grants, each with an identity row": {
			capTrap(f, "member", rows257, keys257), ReadableScopeDenied, nil, "repository_allowlist_too_large"},
		"(b) 256 identity keys and an id-bearing live path": {
			overWithLiveID, ReadableScopeDenied, nil, "repository_allowlist_too_large"},
		"(c) 3 grants, one with no identity row": {
			capTrap(f, "member", rows3, keys3, keys3[1]), ReadableScopeRepositories, keys3, ""},
		"(c) 256 grants, none with an identity row": {
			capTrap(f, "member", rows256, nil), ReadableScopeRepositories, keys256, ""},
		"(d) 256 grants and an id-bearing grant with no live path": {
			capTrap(f, "member", append(slices.Clone(rows256), deadID), keys256), ReadableScopeRepositories, keys256, ""},
	} {
		t.Run(name, func(t *testing.T) {
			scope, keys, reason := readListing(t, f, ls, tc.snap)
			if scope != tc.scope || !slices.Equal(keys, tc.keys) || reason != tc.reason {
				t.Fatalf("scope %s, %d keys, reason %q; want scope %s, %d keys, reason %q", scope, len(keys), reason, tc.scope, len(tc.keys), tc.reason)
			}
		})
	}
}

// C-4 residual: the other users of the repository scope rule keep the over-cap deny for the reader the read listing now
// serves: the credential-issue listing (/v1/authorize:repositories) and a filtered organization action
// (acr.credential.issue). The per-key decision on the key the recount drops is still the grant's allow.
func TestTheOtherScopeUsersKeepTheCapDenyForARecountedReader(t *testing.T) {
	h := newListingFixture(t)
	rows, keys := ownGrants(h.org, 257)
	trap := newCatalogTrap(t, h.member(h.org, "member", rows), 0)
	for i, key := range keys[:256] {
		trap.identity(fmt.Sprintf("github:%d", 1000+i), key)
	}
	h.plant(h.org, trap)
	if d := h.list(h.listQuestion("acr.context.read", h.org)); d.Allowed || d.ReasonCode != "repository_allowlist_too_large" {
		t.Fatalf("/v1/authorize:repositories: %+v, want the cap deny", d)
	}
	e := authorization.NewEvaluator()
	issue := e.Evaluate(authorization.Request{Action: "acr.credential.issue", ResourceKind: "organization", OrganizationID: h.org}, trap)
	if issue.Allowed || issue.Reason != authorization.ReasonRepositoryAllowlistTooLarge {
		t.Fatalf("acr.credential.issue: %+v, want the cap deny", issue)
	}
	dropped := e.Evaluate(authorization.Request{Action: "acr.context.read", ResourceKind: "repository", ResourceKey: keys[256], OrganizationID: h.org}, trap)
	if !dropped.Allowed || dropped.Reason != authorization.ReasonAllowedByRepositoryGrant {
		t.Fatalf("per-key read of %s: %+v, want the grant's allow", keys[256], dropped)
	}
}

// C-6 (product rule 1): a platform admin's read listing is the answer the listing gave before the recount: no
// membership is no_membership, with or without grant rows, and the membership default is the organization.
func TestThePlatformAdminReadListingIsUnchangedByTheRecount(t *testing.T) {
	ls := &listingState{snaps: map[snapKey]authorization.GrantSnapshot{}}
	f := newReadableFixtureOn(t, ls)
	rows, keys := ownGrants(f.org, 257)
	noMembership := &fakeSnapshot{principal: authorization.StatusActive, platformRole: "platform_admin", org: f.org,
		orgStatus: authorization.StatusActive, now: authorizeNow}
	withGrants := *noMembership
	withGrants.grants = rows
	member := f.member("member", nil)
	member.platformRole = "platform_admin"
	trapped := newCatalogTrap(t, &withGrants, 0)
	for i, key := range keys[:256] {
		trapped.identity(fmt.Sprintf("github:%d", 1000+i), key)
	}
	for name, tc := range map[string]struct {
		snap          authorization.GrantSnapshot
		scope, reason string
	}{
		"no membership":                          {noMembership, ReadableScopeDenied, "no_membership"},
		"no membership, 257 grants, 256 in rows": {trapped, ReadableScopeDenied, "no_membership"},
		"membership default":                     {member, ReadableScopeOrganization, ""},
	} {
		t.Run(name, func(t *testing.T) {
			scope, _, reason := readListing(t, f, ls, tc.snap)
			if scope != tc.scope || reason != tc.reason {
				t.Fatalf("scope %s reason %q, want scope %s reason %q", scope, reason, tc.scope, tc.reason)
			}
		})
	}
}
