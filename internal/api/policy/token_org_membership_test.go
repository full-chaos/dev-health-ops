package policy

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

var ownOrgKey = [2]uuid.UUID{userID, uuid.MustParse(ownOrg)}

const invalidToken = `{"detail":{"message":"Invalid or expired token"}}`

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

// goAPIScope is a scope with declared rows, as go-api's: every route is an
// org-scoped row unless notOrgScoped names its profile.
func goAPIScope(t *testing.T, store *fakeStore, notOrgScoped ...string) *Scope {
	t.Helper()
	scope := NewScope(authenticator(t, store), quiet())
	scope.DeclaredRows(func(profile string) (BearerRow, bool) {
		return BearerRow{AccessToken: true, NotOrgScoped: slices.Contains(notOrgScoped, profile)}, true
	})
	return scope
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
// membership row there, with no X-Org-Id and with an X-Org-Id naming that org;
// a caller without one gets the header refusal (legacy, CHAOS-13227). Without
// ReadOnce the row is read twice: by the access check and by the org scope.
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
			got := serve(orgScoped(goAPIScope(t, store)), http.MethodGet, "/x", headers)
			if got.status != tc.status || got.body != tc.body && !strings.HasPrefix(got.body, tc.body) {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, tc.status, tc.body)
			}
			if n := countReads(store, ownOrgKey); n != 2 {
				t.Fatalf("membership reads of the token's org: %d, want 2", n)
			}
		})
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
				got := serve(orgScoped(goAPIScope(t, store)), http.MethodGet, "/x", headers)
				if got.status != tc.status || !strings.HasPrefix(got.body, tc.body) {
					t.Fatalf("%d %q, want %d %q", got.status, got.body, tc.status, tc.body)
				}
			})
		}
	}
}

// A token with no org reads no membership for the token org.
func TestTokenOrgMembershipIsNotReadForAnOrglessToken(t *testing.T) {
	orgless := tokenOrgStore(false, "")
	got := serve(orgScoped(goAPIScope(t, orgless)), http.MethodGet, "/x",
		map[string]string{"Authorization": "Bearer " + sign(t, claims(func(c jwt.MapClaims) { delete(c, "org_id") }))})
	if got.status != 200 || !strings.HasPrefix(got.body, "org= ") || len(orgless.membershipSeen) != 0 {
		t.Fatalf("orgless: %d %q, reads %v", got.status, got.body, orgless.membershipSeen)
	}
}

// The guard decides an admin level on the membership's current role in the
// token's org, not the role claim. A caller with no membership there gets the
// legacy answer (CHAOS-13227): the X-Org-Id refusal at an admin level, served
// at a level the guard alone decides.
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
		{name: "removed, authenticated, guard only", level: Authenticated, claim: "member", status: 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := tokenOrgStore(tc.member, tc.role)
			token := sign(t, claims(func(c jwt.MapClaims) { c["role"] = tc.claim }))
			got := serve(wrapLevel(NewGuard(authenticator(t, store), quiet()), tc.level, echo), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token})
			if got.status != tc.status || tc.body != "" && !strings.HasPrefix(got.body, tc.body) {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, tc.status, tc.body)
			}
		})
	}
}

// Behind ReadOnce the org scope and the guard share one membership read.
func TestOrgScopeAndGuardReadTheTokenOrgMembershipOncePerRequest(t *testing.T) {
	store := tokenOrgStore(true, "admin")
	scope := goAPIScope(t, store)
	auth := scope.auth
	handler := auth.ReadOnce(scope.Principal(scope.OrgScope(scope.Impersonation(wrapLevel(NewGuard(auth, quiet()), AdminOrg, echo)))))
	got := serve(handler, http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + sign(t, claims(func(c jwt.MapClaims) { c["role"] = "admin" }))})
	if got.status != 200 || countReads(store, ownOrgKey) != 1 {
		t.Fatalf("%d %q, reads %d, want 200 and 1", got.status, got.body, countReads(store, ownOrgKey))
	}
}

// A failed membership read of an X-Org-Id naming another org (the token org's
// read succeeds) answers 503 on a go-api org-scoped route when the database is
// temporarily unavailable, and main's 500 in a scope with no declared rows.
func TestHeaderOrgMembershipReadErrorOnAnOrgScopedRoute(t *testing.T) {
	unavailable := errors.Join(ErrUnavailable, errors.New("timeout"))
	for name, tc := range map[string]struct {
		scope  func(*fakeStore) *Scope
		status int
		body   string
	}{
		"go-api org-scoped route": {func(s *fakeStore) *Scope { return goAPIScope(t, s) }, 503, `{"detail":{"message":"Database temporarily unavailable"}}`},
		"no declared rows":        {func(s *fakeStore) *Scope { return NewScope(authenticator(t, s), quiet()) }, 500, `{"detail":"Internal Server Error"}`},
	} {
		t.Run(name, func(t *testing.T) {
			store := tokenOrgStore(true, "member")
			store.errMemberOf = map[[2]uuid.UUID]error{{userID, uuid.MustParse(strangerOrg)}: unavailable}
			got := serve(orgScoped(tc.scope(store)), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + sign(t, claims(nil)), "X-Org-Id": strangerOrg})
			if got.status != tc.status || got.body != tc.body {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, tc.status, tc.body)
			}
		})
	}
}

// The guard's failed read of the token org's membership is never a pass and
// never the refusal of a member: 503 when the database is temporarily
// unavailable, else 500.
func TestGuardTokenOrgMembershipReadErrorIsNeverAPassOrA403(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		status int
		body   string
	}{
		"unavailable": {err: errors.Join(ErrUnavailable, errors.New("timeout")), status: 503, body: `{"detail":{"message":"Database temporarily unavailable"}}`},
		"other":       {err: errors.New("x"), status: 500, body: `{"detail":"Internal Server Error"}`},
	} {
		for _, level := range []Authz{Admin, AdminOrg} {
			t.Run(fmt.Sprintf("%s/%v", name, level), func(t *testing.T) {
				store := tokenOrgStore(true, "admin")
				store.errMember = tc.err
				token := sign(t, claims(func(c jwt.MapClaims) { c["role"] = "admin" }))
				got := serve(wrapLevel(NewGuard(authenticator(t, store), quiet()), level, echo), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token})
				if got.status != tc.status || !strings.HasPrefix(got.body, tc.body) {
					t.Fatalf("%d %q, want %d %q", got.status, got.body, tc.status, tc.body)
				}
			})
		}
	}
}
