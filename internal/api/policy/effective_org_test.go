package policy

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

// actingOrg reports the org the route's handler reads from UserFrom, the
// field every org-scoped admin handler uses (CHAOS-9510).
var actingOrg = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	user := UserFrom(r.Context())
	_, _ = io.WriteString(w, "acting="+user.OrgID)
})

func effectiveOrgStack(t *testing.T, store *fakeStore, level Authz) http.Handler {
	t.Helper()
	auth := authenticator(t, store)
	scope := NewScope(auth, quiet())
	return scope.OrgScope(scope.Impersonation(NewGuard(auth, quiet()).Wrap(level, actingOrg)))
}

func superClaims(c jwt.MapClaims) { c["is_superuser"] = true }

// An impersonating superuser acts in the target's org on every org-scoped
// admin route, whatever X-Org-Id the web sent.
func TestImpersonatingSuperuserActsInTheTargetOrg(t *testing.T) {
	for _, level := range []Authz{Admin, AdminOrg} {
		store := newStore()
		store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
		store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID, TargetRole: "owner"}
		token := sign(t, claims(superClaims))
		for _, header := range []string{"", ownOrg, targetOrgID.String(), strangerOrg} {
			headers := map[string]string{"Authorization": "Bearer " + token}
			if header != "" {
				headers["X-Org-Id"] = header
			}
			got := serve(effectiveOrgStack(t, store, level), http.MethodGet, "/x", headers)
			if got.status != 200 || got.body != "acting="+targetOrgID.String() {
				t.Errorf("level %v X-Org-Id %q: %d %s, want the target org %s", level, header, got.status, got.body, targetOrgID)
			}
		}
	}
}

// An org-less superuser passes AdminOrg while impersonating (the target's org
// is the context) and is refused without a session.
func TestOrglessSuperuserNeedsASessionForAdminOrg(t *testing.T) {
	store := newStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	token := sign(t, claims(func(c jwt.MapClaims) { superClaims(c); c["org_id"] = "" }))
	headers := map[string]string{"Authorization": "Bearer " + token}
	if got := serve(effectiveOrgStack(t, store, AdminOrg), http.MethodGet, "/x", headers); got.status != 403 {
		t.Fatalf("no session: %d %s, want 403", got.status, got.body)
	}
	store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID}
	if got := serve(effectiveOrgStack(t, store, AdminOrg), http.MethodGet, "/x", headers); got.status != 200 || got.body != "acting="+targetOrgID.String() {
		t.Fatalf("session: %d %s, want the target org", got.status, got.body)
	}
}

// The session is read live on every request: when it ends the very next
// request is served in the admin's own org again.
func TestEffectiveOrgFollowsTheLiveSession(t *testing.T) {
	store := newStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	token := sign(t, claims(superClaims))
	headers := map[string]string{"Authorization": "Bearer " + token}
	store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID}
	if got := serve(effectiveOrgStack(t, store, Admin), http.MethodGet, "/x", headers); got.body != "acting="+targetOrgID.String() {
		t.Fatalf("active: %s", got.body)
	}
	delete(store.sessions, userID)
	if got := serve(effectiveOrgStack(t, store, Admin), http.MethodGet, "/x", headers); got.body != "acting="+ownOrg {
		t.Fatalf("ended: %s, want the admin's own org", got.body)
	}
}

// Without a session nothing changes: a superuser's X-Org-Id does not move the
// org these routes act in (the Python rule the venue oracles pin), and a
// non-superuser always acts in the org claim of the token.
func TestNonImpersonatingOrgResolution(t *testing.T) {
	superStore := newStore()
	superStore.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	superToken := sign(t, claims(superClaims))
	if got := serve(effectiveOrgStack(t, superStore, Admin), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + superToken, "X-Org-Id": strangerOrg}); got.body != "acting="+ownOrg {
		t.Errorf("superuser with X-Org-Id: %s, want the token's org", got.body)
	}
	if got := serve(effectiveOrgStack(t, superStore, Admin), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + superToken}); got.body != "acting="+ownOrg {
		t.Errorf("superuser without X-Org-Id: %s", got.body)
	}

	memberStore := newStore()
	memberStore.users[userID] = UserState{IsActive: true}
	adminToken := sign(t, claims(func(c jwt.MapClaims) { c["role"] = "admin" }))
	got := serve(effectiveOrgStack(t, memberStore, Admin), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + adminToken, "X-Org-Id": memberOrg})
	if got.status != 200 || got.body != "acting="+ownOrg {
		t.Errorf("non-superuser member of another org: %d %s, want the token's org %s", got.status, got.body, ownOrg)
	}
	// A session of another admin is never applied to this caller.
	memberStore.sessions[userID] = &Impersonation{AdminUserID: userID, TargetOrgID: targetOrgID}
	got = serve(effectiveOrgStack(t, memberStore, Admin), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + adminToken})
	if !strings.HasSuffix(got.body, ownOrg) {
		t.Errorf("non-superuser with a stray session row: %s, want the token's org", got.body)
	}
}

// Routes below Admin (identity, session, billing, the impersonation routes
// themselves) keep the caller's own org claim.
func TestIdentityRoutesKeepTheOwnOrg(t *testing.T) {
	store := newStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID}
	token := sign(t, claims(superClaims))
	for _, level := range []Authz{Authenticated, Superuser} {
		got := serve(effectiveOrgStack(t, store, level), http.MethodGet, "/x", map[string]string{"Authorization": "Bearer " + token})
		if got.status != 200 || got.body != "acting="+ownOrg {
			t.Errorf("level %v: %d %s, want the admin's own org", level, got.status, got.body)
		}
	}
}

// A non-superuser cannot start an impersonation (the route is Superuser
// level), and a token that merely claims is_superuser, over a users row that
// says otherwise, never gets a session applied.
func TestNonSuperuserCannotImpersonate(t *testing.T) {
	store := newStore()
	store.users[userID] = UserState{IsActive: true}
	store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID}
	forged := sign(t, claims(func(c jwt.MapClaims) { superClaims(c); c["role"] = "admin" }))
	headers := map[string]string{"Authorization": "Bearer " + forged}
	if got := serve(effectiveOrgStack(t, store, Superuser), http.MethodPost, "/impersonate", headers); got.status != 403 {
		t.Fatalf("start by a forged superuser claim: %d %s, want 403", got.status, got.body)
	}
	if got := serve(effectiveOrgStack(t, store, Admin), http.MethodGet, "/x", headers); got.body != "acting="+ownOrg {
		t.Fatalf("forged superuser claim with a session row: %s, want the token's org", got.body)
	}
}
