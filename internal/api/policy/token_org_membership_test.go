package policy

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

var ownOrgKey = [2]uuid.UUID{userID, uuid.MustParse(ownOrg)}

// tokenOrgStore is newStore with the caller's membership in the token's own
// org set as given ("" role and member=false: no row).
func tokenOrgStore(member bool, role string) *fakeStore {
	store := newStore()
	delete(store.members, ownOrgKey)
	store.roles = map[[2]uuid.UUID]string{}
	if member {
		store.members[ownOrgKey] = true
		store.roles[ownOrgKey] = role
	}
	return store
}

func countReads(store *fakeStore, key [2]uuid.UUID) int {
	n := 0
	for _, seen := range store.membershipSeen {
		if seen == key {
			n++
		}
	}
	return n
}

// The org scope serves the token's own org only while the caller has a
// membership row there, read on the request: with no X-Org-Id and with an
// X-Org-Id naming that org. A caller without one gets the answer a
// header-selected org without a membership gets, byte for byte.
func TestOrgScopeReadsTheMembershipOfTheTokensOwnOrg(t *testing.T) {
	cases := []struct {
		name   string
		member bool
		header string
		status int
		body   string
	}{
		{name: "member, no header", member: true, status: 200, body: "org=" + ownOrg + " "},
		{name: "member, header own org", member: true, header: ownOrg, status: 200, body: "org=" + ownOrg + " "},
		{name: "removed, no header", status: 403, body: `{"detail": "X-Org-Id not permitted for this user"}`},
		{name: "removed, header own org", header: ownOrg, status: 403, body: `{"detail": "X-Org-Id not permitted for this user"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := tokenOrgStore(tc.member, "member")
			headers := map[string]string{"Authorization": "Bearer " + sign(t, claims(nil))}
			if tc.header != "" {
				headers["X-Org-Id"] = tc.header
			}
			got := serve(orgScoped(NewScope(authenticator(t, store), quiet())), http.MethodGet, "/x", headers)
			if got.status != tc.status || got.body != tc.body && !strings.HasPrefix(got.body, tc.body) {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, tc.status, tc.body)
			}
			if n := countReads(store, ownOrgKey); n != 1 {
				t.Fatalf("membership reads of the token's org: %d, want 1", n)
			}
		})
	}
}

// The refusal for the token's own org is the header refusal: same status,
// body and headers.
func TestTokenOrgRefusalIsTheHeaderRefusalByteForByte(t *testing.T) {
	scope := NewScope(authenticator(t, tokenOrgStore(false, "")), quiet())
	token := "Bearer " + sign(t, claims(nil))
	header := serve(orgScoped(scope), http.MethodGet, "/x", map[string]string{"Authorization": token, "X-Org-Id": strangerOrg})
	tokenOrg := serve(orgScoped(scope), http.MethodGet, "/x", map[string]string{"Authorization": token})
	if header.status != http.StatusForbidden || tokenOrg.status != header.status || tokenOrg.body != header.body {
		t.Fatalf("token org %d %q, header %d %q", tokenOrg.status, tokenOrg.body, header.status, header.body)
	}
	for _, name := range []string{"Content-Type", "Content-Length"} {
		if tokenOrg.header.Get(name) != header.header.Get(name) {
			t.Fatalf("%s: token org %q, header %q", name, tokenOrg.header.Get(name), header.header.Get(name))
		}
	}
	if len(tokenOrg.header) != len(header.header) {
		t.Fatalf("headers: token org %v, header %v", tokenOrg.header, header.header)
	}
}

// A failed membership read is never a refusal of the member and never a
// pass: a database that is temporarily unavailable answers 503, any other
// failure 500.
func TestTokenOrgMembershipReadErrorIsNeverAPassOrA403(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		status int
		body   string
	}{
		"unavailable": {err: errors.Join(ErrUnavailable, errors.New("timeout")), status: 503, body: `{"detail":{"message":"Database temporarily unavailable"}}`},
		"other":       {err: errors.New("x"), status: 500, body: `{"detail":"Internal Server Error"}`},
	} {
		for _, header := range []string{"", ownOrg} {
			t.Run(name+"/header="+header, func(t *testing.T) {
				store := tokenOrgStore(true, "admin")
				store.errMember = tc.err
				headers := map[string]string{"Authorization": "Bearer " + sign(t, claims(nil))}
				if header != "" {
					headers["X-Org-Id"] = header
				}
				got := serve(orgScoped(NewScope(authenticator(t, store), quiet())), http.MethodGet, "/x", headers)
				if got.status != tc.status || !strings.HasPrefix(got.body, tc.body) {
					t.Fatalf("%d %q, want %d %q", got.status, got.body, tc.status, tc.body)
				}
			})
		}
	}
}

// A superuser and a token with no org read no membership for the token org;
// their answers are today's.
func TestTokenOrgMembershipIsNotReadForASuperuserOrAnOrglessToken(t *testing.T) {
	superuser := tokenOrgStore(false, "")
	superuser.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	got := serve(orgScoped(NewScope(authenticator(t, superuser), quiet())), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + sign(t, claims(nil))})
	if got.status != 200 || !strings.HasPrefix(got.body, "org="+ownOrg+" ") || len(superuser.membershipSeen) != 0 {
		t.Fatalf("superuser: %d %q, reads %v", got.status, got.body, superuser.membershipSeen)
	}
	orgless := tokenOrgStore(false, "")
	got = serve(orgScoped(NewScope(authenticator(t, orgless), quiet())), http.MethodGet, "/x",
		map[string]string{"Authorization": "Bearer " + sign(t, claims(func(c jwt.MapClaims) { delete(c, "org_id") }))})
	if got.status != 200 || !strings.HasPrefix(got.body, "org= ") || len(orgless.membershipSeen) != 0 {
		t.Fatalf("orgless: %d %q, reads %v", got.status, got.body, orgless.membershipSeen)
	}
}

// The guard decides an admin level on the membership's current role in the
// token's org, not the role claim, and refuses a caller with no membership
// there with the org scope's refusal.
func TestGuardReadsTheRoleOfTheTokensOwnOrgMembership(t *testing.T) {
	cases := []struct {
		name   string
		level  Authz
		claim  string
		member bool
		role   string
		status int
		body   string
	}{
		{name: "claim admin, now member", level: Admin, claim: "admin", member: true, role: "member", status: 403, body: `{"detail":"Admin access required"}`},
		{name: "claim admin, now member, admin org", level: AdminOrg, claim: "admin", member: true, role: "member", status: 403, body: `{"detail":"Admin access required"}`},
		{name: "claim member, now admin", level: Admin, claim: "member", member: true, role: "admin", status: 200},
		{name: "claim owner, still owner", level: AdminOrg, claim: "owner", member: true, role: "owner", status: 200},
		{name: "removed, admin", level: Admin, claim: "admin", status: 403, body: `{"detail": "X-Org-Id not permitted for this user"}`},
		{name: "removed, admin org", level: AdminOrg, claim: "admin", status: 403, body: `{"detail": "X-Org-Id not permitted for this user"}`},
		// Below the admin levels the guard reads no membership; the org
		// scope is the check (TestOrgScopeReadsTheMembershipOfTheTokensOwnOrg).
		{name: "removed, authenticated, guard only", level: Authenticated, claim: "member", status: 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := tokenOrgStore(tc.member, tc.role)
			token := sign(t, claims(func(c jwt.MapClaims) { c["role"] = tc.claim }))
			got := serve(NewGuard(authenticator(t, store), quiet()).Wrap(tc.level, echo), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token})
			if got.status != tc.status || tc.body != "" && !strings.HasPrefix(got.body, tc.body) {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, tc.status, tc.body)
			}
		})
	}
}

// Behind ReadOnce the org scope and the guard share one membership read.
func TestOrgScopeAndGuardReadTheTokenOrgMembershipOncePerRequest(t *testing.T) {
	store := tokenOrgStore(true, "admin")
	auth := authenticator(t, store)
	scope := NewScope(auth, quiet())
	handler := auth.ReadOnce(scope.Principal(scope.OrgScope(scope.Impersonation(NewGuard(auth, quiet()).Wrap(AdminOrg, echo)))))
	got := serve(handler, http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + sign(t, claims(func(c jwt.MapClaims) { c["role"] = "admin" }))})
	if got.status != 200 || countReads(store, ownOrgKey) != 1 {
		t.Fatalf("%d %q, reads %d, want 200 and 1", got.status, got.body, countReads(store, ownOrgKey))
	}
}
