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
			got := serve(orgScoped(goAPIScope(t, store)), http.MethodGet, "/x", headers)
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
	scope := goAPIScope(t, tokenOrgStore(false, ""))
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
				got := serve(orgScoped(goAPIScope(t, store)), http.MethodGet, "/x", headers)
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
	got := serve(orgScoped(goAPIScope(t, superuser)), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + sign(t, claims(nil))})
	if got.status != 200 || !strings.HasPrefix(got.body, "org="+ownOrg+" ") || len(superuser.membershipSeen) != 0 {
		t.Fatalf("superuser: %d %q, reads %v", got.status, got.body, superuser.membershipSeen)
	}
	orgless := tokenOrgStore(false, "")
	got = serve(orgScoped(goAPIScope(t, orgless)), http.MethodGet, "/x",
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
	scope := goAPIScope(t, store)
	auth := scope.auth
	handler := auth.ReadOnce(scope.Principal(scope.OrgScope(scope.Impersonation(NewGuard(auth, quiet()).Wrap(AdminOrg, echo)))))
	got := serve(handler, http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + sign(t, claims(func(c jwt.MapClaims) { c["role"] = "admin" }))})
	if got.status != 200 || countReads(store, ownOrgKey) != 1 {
		t.Fatalf("%d %q, reads %d, want 200 and 1", got.status, got.body, countReads(store, ownOrgKey))
	}
}

// A route not acting in an org (a declared row with tenant requirement none or
// cross_org_superuser_only) and a scope with no declared rows (query-api's
// /graphql, whose edge reads the membership itself) answer a removed member's
// token as on main, with no header and with an X-Org-Id naming the token's
// org, and read no membership for it here.
func TestTokenOrgIsNotReadOffOrgScopedRoutesOrWithoutDeclaredRows(t *testing.T) {
	for name, build := range map[string]func(*fakeStore) *Scope{
		"not org-scoped row": func(store *fakeStore) *Scope { return goAPIScope(t, store, rigProfile) },
		"no declared rows":   func(store *fakeStore) *Scope { return NewScope(authenticator(t, store), quiet()) },
	} {
		for _, header := range []string{"", ownOrg} {
			t.Run(name+"/header="+header, func(t *testing.T) {
				store := tokenOrgStore(false, "")
				headers := map[string]string{"Authorization": "Bearer " + sign(t, claims(nil))}
				if header != "" {
					headers["X-Org-Id"] = header
				}
				handler := RouteProfiles(func(*http.Request) (string, bool) { return rigProfile, true })(orgScoped(build(store)))
				got := serve(handler, http.MethodGet, "/x", headers)
				if got.status != 200 || !strings.HasPrefix(got.body, "org="+ownOrg+" ") || countReads(store, ownOrgKey) != 0 {
					t.Fatalf("%d %q, reads %d; want main's 200 in the token's org and no read", got.status, got.body, countReads(store, ownOrgKey))
				}
			})
		}
	}
}

// A route without a declared row in a scope that declares rows reads the
// token org's membership (fail closed).
func TestTokenOrgIsReadOnARouteWithoutARow(t *testing.T) {
	store := tokenOrgStore(false, "")
	scope := NewScope(authenticator(t, store), quiet())
	scope.DeclaredRows(func(string) (BearerRow, bool) { return BearerRow{}, false })
	got := serve(orgScoped(scope), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + sign(t, claims(nil))})
	if got.status != http.StatusForbidden || countReads(store, ownOrgKey) != 1 {
		t.Fatalf("%d %q, reads %d; want the X-Org-Id refusal after one read", got.status, got.body, countReads(store, ownOrgKey))
	}
}

// A failed membership read of an X-Org-Id naming another org answers 503 on a
// go-api org-scoped route when the database is temporarily unavailable (as
// the token org's read), and main's 500 where the token org is not read.
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
			store.errMember = unavailable
			got := serve(orgScoped(tc.scope(store)), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + sign(t, claims(nil)), "X-Org-Id": strangerOrg})
			if got.status != tc.status || got.body != tc.body {
				t.Fatalf("%d %q, want %d %q", got.status, got.body, tc.status, tc.body)
			}
		})
	}
}

const (
	absentOrg   = "77777777-7777-4777-8777-777777777777"
	notAUUIDOrg = "not-a-uuid"
)

// absentOrgStore is the caller with no membership row in absentOrg, an org
// with no organizations row.
func absentOrgStore() *fakeStore {
	store := newStore()
	store.absentOrgs = map[uuid.UUID]bool{uuid.MustParse(absentOrg): true}
	return store
}

// A token whose org claim names an org that does not exist, or is not a UUID,
// is answered as on main: the org scope lets the request on in the claimed org
// (the route answers), with no header and with an X-Org-Id naming the claim,
// and refuses nothing. Only an existing org without the caller's row is the
// X-Org-Id refusal.
func TestTokenOrgMissingOrNotAUUIDAnswersAsMainInTheOrgScope(t *testing.T) {
	for _, tc := range []struct {
		name  string
		org   string
		reads int
	}{
		{name: "org missing", org: absentOrg, reads: 1},
		{name: "org not a uuid", org: notAUUIDOrg, reads: 0},
	} {
		for _, header := range []string{"", tc.org} {
			t.Run(tc.name+"/header="+header, func(t *testing.T) {
				store := absentOrgStore()
				headers := map[string]string{"Authorization": "Bearer " + sign(t, claims(func(c jwt.MapClaims) { c["org_id"] = tc.org }))}
				if header != "" {
					headers["X-Org-Id"] = header
				}
				got := serve(orgScoped(goAPIScope(t, store)), http.MethodGet, "/x", headers)
				if got.status != 200 || !strings.HasPrefix(got.body, "org="+tc.org+" ") {
					t.Fatalf("%d %q, want main's 200 in the claimed org", got.status, got.body)
				}
				if len(store.membershipSeen) != tc.reads {
					t.Fatalf("membership reads %v, want %d", store.membershipSeen, tc.reads)
				}
			})
		}
	}
}

// At the admin levels the guard answers a token whose org is missing or not a
// UUID as on main: the role claim decides, and nothing is refused for the
// missing row.
func TestTokenOrgMissingOrNotAUUIDAnswersAsMainInTheGuard(t *testing.T) {
	for _, org := range []string{absentOrg, notAUUIDOrg} {
		for _, tc := range []struct {
			level  Authz
			claim  string
			status int
			body   string
		}{
			{level: Admin, claim: "admin", status: 200},
			{level: AdminOrg, claim: "owner", status: 200},
			{level: Admin, claim: "member", status: 403, body: `{"detail":"Admin access required"}`},
			{level: AdminOrg, claim: "member", status: 403, body: `{"detail":"Admin access required"}`},
		} {
			t.Run(fmt.Sprintf("%s/%v/%s", org, tc.level, tc.claim), func(t *testing.T) {
				token := sign(t, claims(func(c jwt.MapClaims) { c["org_id"], c["role"] = org, tc.claim }))
				got := serve(NewGuard(authenticator(t, absentOrgStore()), quiet()).Wrap(tc.level, echo), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token})
				if got.status != tc.status || tc.body != "" && !strings.HasPrefix(got.body, tc.body) {
					t.Fatalf("%d %q, want %d %q", got.status, got.body, tc.status, tc.body)
				}
			})
		}
	}
}

// On the full chain behind ReadOnce a missing org is read once and answered
// as main at an admin level; an org claim that is not a UUID reads nothing.
func TestTokenOrgMissingOrNotAUUIDOnTheFullChain(t *testing.T) {
	for _, tc := range []struct {
		org   string
		reads int
	}{{absentOrg, 1}, {notAUUIDOrg, 0}} {
		t.Run(tc.org, func(t *testing.T) {
			store := absentOrgStore()
			scope := goAPIScope(t, store)
			auth := scope.auth
			handler := auth.ReadOnce(scope.Principal(scope.OrgScope(scope.Impersonation(NewGuard(auth, quiet()).Wrap(AdminOrg, echo)))))
			got := serve(handler, http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + sign(t, claims(func(c jwt.MapClaims) { c["org_id"], c["role"] = tc.org, "admin" }))})
			if got.status != 200 || !strings.HasPrefix(got.body, "org="+tc.org+" ") || len(store.membershipSeen) != tc.reads {
				t.Fatalf("%d %q, reads %v; want 200 in the claimed org after %d reads", got.status, got.body, store.membershipSeen, tc.reads)
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
				got := serve(NewGuard(authenticator(t, store), quiet()).Wrap(level, echo), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token})
				if got.status != tc.status || !strings.HasPrefix(got.body, tc.body) {
					t.Fatalf("%d %q, want %d %q", got.status, got.body, tc.status, tc.body)
				}
			})
		}
	}
}
