package policy

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

// sessionStateRig is the caller (userID, admin of ownOrg) behind Principal, OrgScope and Impersonation, with a read
// route that reports the org, the session and the acting org it saw, and a write route behind RefuseImpersonation.
func sessionStateRig(t *testing.T, platform *rule.Platform, live bool) (*fakeStore, http.Handler, string) {
	t.Helper()
	store := newStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true, Platform: platform}
	store.roles[[2]uuid.UUID{userID, uuid.MustParse(ownOrg)}] = "admin"
	if live {
		store.sessions[userID] = &Impersonation{ID: uuid.New(), AdminUserID: userID, TargetUserID: targetUserID,
			TargetOrgID: targetOrgID, TargetRole: "admin", ExpiresAt: time.Now().Add(time.Hour)}
	}
	auth := authenticator(t, store)
	scope := NewScope(auth, quiet())
	guard := NewGuard(auth, quiet())
	mux := http.NewServeMux()
	mux.Handle("GET /read", guard.Wrap(Authenticated, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		echo.ServeHTTP(w, r)
		_, _ = io.WriteString(w, " acting="+EffectiveOrgID(r.Context(), UserFrom(r.Context())))
	})))
	mux.Handle("POST /write", guard.Wrap(Authenticated, guard.RefuseImpersonation(echo)))
	token := sign(t, claims(func(c jwt.MapClaims) { c["is_superuser"] = true; c["role"] = "admin" }))
	return store, scope.Principal(scope.OrgScope(scope.Impersonation(mux))), token
}

func billingOnly() *rule.Platform { p := rule.PlatformOf(rule.ActionAdminBilling); return &p }

// CHAOS-13143 (acp P1): a live impersonation row is the session state. A caller whose admin.impersonate was revoked
// while the row is live is refused on a RefuseImpersonation write; the row gives them no org swap, no session
// context and no impersonation headers (acting as the target stays behind admin.impersonate).
func TestALiveRowWithoutAdminImpersonateRefusesWritesAndActsAsTheCaller(t *testing.T) {
	_, handler, token := sessionStateRig(t, billingOnly(), true)
	bearer := map[string]string{"Authorization": "Bearer " + token}
	write := serve(handler, http.MethodPost, "/write", bearer)
	if write.status != http.StatusForbidden || write.body != `{"detail":"Not allowed in an impersonation session"}` {
		t.Fatalf("write: %d %s, want the session 403", write.status, write.body)
	}
	read := serve(handler, http.MethodGet, "/read", bearer)
	want := "org=" + ownOrg + " user=" + userID.String() + " imp=- acting=" + ownOrg
	if read.status != http.StatusOK || read.body != want {
		t.Fatalf("read: %d %q, want 200 %q", read.status, read.body, want)
	}
	if read.header.Get("X-Impersonating") != "" || read.header.Get("X-Impersonated-User-Id") != "" {
		t.Fatalf("read stamped impersonation headers: %v", read.header)
	}
}

// Control: with no live row, the same caller writes and reads as themself.
func TestNoLiveRowWithoutAdminImpersonateIsNotRefused(t *testing.T) {
	_, handler, token := sessionStateRig(t, billingOnly(), false)
	bearer := map[string]string{"Authorization": "Bearer " + token}
	want := "org=" + ownOrg + " user=" + userID.String() + " imp=-"
	if write := serve(handler, http.MethodPost, "/write", bearer); write.status != http.StatusOK || write.body != want {
		t.Fatalf("write: %d %q, want 200 %q", write.status, write.body, want)
	}
}

// A holder of admin.impersonate with a live row is unchanged: org swap, session context, headers, write refused.
func TestALiveRowWithAdminImpersonateStillActsAsTheTarget(t *testing.T) {
	_, handler, token := sessionStateRig(t, nil, true)
	bearer := map[string]string{"Authorization": "Bearer " + token}
	read := serve(handler, http.MethodGet, "/read", bearer)
	want := "org=" + targetOrgID.String() + " user=" + userID.String() + " imp=" + targetOrgID.String() + " acting=" + targetOrgID.String()
	if read.status != http.StatusOK || read.body != want || read.header.Get("X-Impersonating") != "true" ||
		read.header.Get("X-Impersonated-User-Id") != targetUserID.String() {
		t.Fatalf("read: %d %q %v, want 200 %q and the headers", read.status, read.body, read.header, want)
	}
	if write := serve(handler, http.MethodPost, "/write", bearer); write.status != http.StatusForbidden {
		t.Fatalf("write: %d %s, want 403", write.status, write.body)
	}
}

// A failed row read fails closed for a caller without admin.impersonate too, as it does for a holder.
func TestARowReadFailureWithoutAdminImpersonateFailsClosed(t *testing.T) {
	store, handler, token := sessionStateRig(t, billingOnly(), true)
	store.errSession = errors.New("store down")
	for _, call := range []struct{ method, path string }{{http.MethodGet, "/read"}, {http.MethodPost, "/write"}} {
		got := serve(handler, call.method, call.path, map[string]string{"Authorization": "Bearer " + token})
		if got.status != http.StatusInternalServerError || strings.Contains(got.body, "org=") {
			t.Fatalf("%s %s: %d %q, want the 500 before the route", call.method, call.path, got.status, got.body)
		}
	}
}
