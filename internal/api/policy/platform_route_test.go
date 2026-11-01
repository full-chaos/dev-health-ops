package policy

import (
	"net/http"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// routeActions are the platform actions a platform route is wrapped with.
var routeActions = []string{
	rule.ActionAdminOrgs, rule.ActionAdminFlags, rule.ActionAdminAudit,
	rule.ActionAdminStats, rule.ActionAdminImpersonate, rule.ActionAdminBilling,
}

func routeStore(c platformCase) *fakeStore {
	store := newStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: c.superuser, Platform: c.platform}
	return store
}

func bearer(t *testing.T, mutate func(jwt.MapClaims)) map[string]string {
	return map[string]string{"Authorization": "Bearer " + sign(t, claims(mutate))}
}

func orgless(c jwt.MapClaims) { superClaims(c); c["org_id"] = "" }

// A platform route admits a caller by the route's own action: the flag alone
// (platform_admin) as before, the action alone, never the flag beside a set
// that lacks the action.
func TestPlatformRoute_WrapPlatformNeedsTheNamedAction(t *testing.T) {
	for _, action := range routeActions {
		for _, c := range platformCases(action) {
			store := routeStore(c)
			handler := NewGuard(authenticator(t, store), quiet()).WrapPlatform(action, echo)
			want := 403
			if c.want {
				want = 200
			}
			if got := serve(handler, http.MethodGet, "/x", bearer(t, superClaims)); got.status != want {
				t.Errorf("%s, %s: status %d %s, want %d", action, c.name, got.status, got.body, want)
			}
		}
	}
}

// An org-less caller reaches a route by that route's action, not by
// admin.orgs: a caller with only admin.flags passes the flags route and is
// refused the org context everywhere else.
func TestPlatformRoute_OrglessCallerNeedsTheRoutesAction(t *testing.T) {
	const orgRefusal = "Organization context required"
	for _, action := range append(append([]string{}, routeActions...), rule.ActionAdminUsers) {
		for _, other := range []string{rule.ActionAdminOrgs, rule.ActionAdminFlags} {
			if other == action {
				continue
			}
			for _, c := range []struct {
				name string
				set  *rule.Platform
				want int
			}{{"only the route's action", only(action), 200}, {"only another action", only(other), 403}} {
				store := newStore()
				store.users[userID] = UserState{IsActive: true, Platform: c.set}
				guard := NewGuard(authenticator(t, store), quiet())
				var handler http.Handler = guard.WrapPlatform(action, echo)
				if action == rule.ActionAdminUsers {
					handler = guard.WrapAdminOr(action, echo)
				}
				got := serve(handler, http.MethodGet, "/x", bearer(t, orgless))
				if got.status != c.want || (c.want == 403 && !contains(got.body, orgRefusal)) {
					t.Errorf("%s, org-less, %s: %d %s, want %d", action, c.name, got.status, got.body, c.want)
				}
			}
		}
	}
	// The generic wrap keeps admin.orgs as its org-less rule.
	store := newStore()
	store.users[userID] = UserState{IsActive: true, Platform: only(rule.ActionAdminFlags)}
	handler := NewGuard(authenticator(t, store), quiet()).Wrap(Admin, echo)
	if got := serve(handler, http.MethodGet, "/x", bearer(t, orgless)); got.status != 403 || !contains(got.body, orgRefusal) {
		t.Errorf("generic Admin, org-less, only admin.flags: %d %s, want the org refusal", got.status, got.body)
	}
	store.users[userID] = UserState{IsActive: true, Platform: only(rule.ActionAdminOrgs)}
	if got := serve(handler, http.MethodGet, "/x", bearer(t, orgless)); got.status != 200 {
		t.Errorf("generic Admin, org-less, only admin.orgs: %d %s, want 200", got.status, got.body)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// WrapAdminOr is Admin for org owners and admins, exactly as before, and also
// admits a caller that holds only the action, with no role anywhere.
func TestPlatformRoute_AdminOrKeepsOrgAdminsAndAdmitsTheActionHolder(t *testing.T) {
	cases := []struct {
		name   string
		role   string
		set    *rule.Platform
		orgful bool
		want   int
	}{
		{"org admin", "admin", nil, true, 200},
		{"org owner", "owner", nil, true, 200},
		{"admin.users only, no role", "", only(rule.ActionAdminUsers), false, 200},
		{"neither: a member", "member", nil, true, 403},
		{"another action only, no role", "", allBut(rule.ActionAdminUsers), false, 403},
	}
	for _, c := range cases {
		store := newStore()
		store.users[userID] = UserState{IsActive: true, Platform: c.set}
		mutate := func(m jwt.MapClaims) {
			if c.orgful {
				m["role"] = c.role
				withClaimRole(store, m)
			} else {
				orgless(m)
			}
		}
		handler := NewGuard(authenticator(t, store), quiet()).WrapAdminOr(rule.ActionAdminUsers, echo)
		if got := serve(handler, http.MethodGet, "/x", bearer(t, mutate)); got.status != c.want {
			t.Errorf("%s: %d %s, want %d", c.name, got.status, got.body, c.want)
		}
	}
}

// Impersonation stop in a live session: the middleware starts the session
// only for a holder of admin.impersonate, and the route admits that holder
// alone. The flag beside a set without the action neither starts a session
// nor passes the route.
func TestPlatformRoute_ImpersonationStopByAHolderOfAdminImpersonateOnly(t *testing.T) {
	for _, c := range []struct {
		name     string
		flag     bool
		set      *rule.Platform
		status   int
		sessions bool
	}{
		{"flag only, as before", true, nil, 200, true},
		{"admin.impersonate only", false, only(rule.ActionAdminImpersonate), 200, true},
		{"the flag beside every other action", true, allBut(rule.ActionAdminImpersonate), 403, false},
	} {
		store := newStore()
		store.users[userID] = UserState{IsActive: true, IsSuperuser: c.flag, Platform: c.set}
		store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID, TargetRole: "owner"}
		auth := authenticator(t, store)
		scope := NewScope(auth, quiet())
		handler := scope.Principal(scope.OrgScope(scope.Impersonation(NewGuard(auth, quiet()).WrapPlatform(rule.ActionAdminImpersonate, actingOrg))))
		got := serve(handler, http.MethodPost, "/x", bearer(t, superClaims))
		if got.status != c.status || (got.header.Get("X-Impersonating") == "true") != c.sessions {
			t.Errorf("%s: %d %s, impersonating header %q; want %d, session %v", c.name, got.status, got.body, got.header.Get("X-Impersonating"), c.status, c.sessions)
		}
	}
}

// wrapLevel is Wrap, except that the platform level goes through WrapPlatform
// with admin.orgs: the bare Superuser level admits nobody.
func wrapLevel(g *Guard, level Authz, next http.Handler) http.Handler {
	if level == Superuser {
		return g.WrapPlatform(ActionAdminOrgs, next)
	}
	return g.Wrap(level, next)
}
