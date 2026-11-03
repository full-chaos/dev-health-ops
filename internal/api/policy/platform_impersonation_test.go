package policy

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

func impersonatingPlatformStack(t *testing.T, pattern, action string) http.Handler {
	t.Helper()
	store := newStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
	store.sessions[userID] = &Impersonation{AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID, TargetRole: "member"}
	auth := authenticator(t, store)
	scope := NewScope(auth, quiet())
	mux := http.NewServeMux()
	mux.Handle(pattern, NewGuard(auth, quiet()).WrapPlatform(action, echo))
	return scope.Principal(scope.OrgScope(scope.Impersonation(mux)))
}

// A platform action is refused while the session holds an impersonation state.
func TestPlatformImpersonationRefusesEveryPlatformAction(t *testing.T) {
	for _, action := range routeActions {
		for _, method := range []string{http.MethodGet, http.MethodDelete} {
			handler := impersonatingPlatformStack(t, method+" /x", action)
			t.Run(action+"/"+method, func(t *testing.T) {
				got := serve(handler, method, "/x", bearer(t, superClaims))
				if got.status != http.StatusForbidden || got.body != `{"detail":"Not allowed in an impersonation session"}` {
					t.Errorf("%d %s, want 403 refusal", got.status, got.body)
				}
			})
		}
	}
}

// Stop-impersonation stays allowed while the session is active.
func TestPlatformImpersonationAllowsStop(t *testing.T) {
	handler := impersonatingPlatformStack(t, "POST /api/v1/admin/impersonate/stop", rule.ActionAdminImpersonate)
	if got := serve(handler, http.MethodPost, "/api/v1/admin/impersonate/stop", bearer(t, superClaims)); got.status != http.StatusOK {
		t.Errorf("stop while impersonating: %d %s, want 200", got.status, got.body)
	}
}

// Without a session the platform action is served as before.
func TestPlatformImpersonationNoSessionStillServed(t *testing.T) {
	for _, action := range routeActions {
		store := newStore()
		store.users[userID] = UserState{IsActive: true, IsSuperuser: true}
		auth := authenticator(t, store)
		scope := NewScope(auth, quiet())
		handler := scope.Principal(scope.OrgScope(scope.Impersonation(NewGuard(auth, quiet()).WrapPlatform(action, echo))))
		if got := serve(handler, http.MethodGet, "/x", bearer(t, superClaims)); got.status != http.StatusOK {
			t.Errorf("%s without a session: %d %s, want 200", action, got.status, got.body)
		}
	}
}
