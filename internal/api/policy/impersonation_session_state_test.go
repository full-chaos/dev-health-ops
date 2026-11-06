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
// route that reports the org, the session and the acting org it saw, a write route behind RefuseImpersonation, and a
// GET route behind RefuseImpersonation (a read method: no row read for it).
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
	mux.Handle("GET /refusing-read", guard.Wrap(Authenticated, guard.RefuseImpersonation(echo)))
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

// CHAOS-13143 (acp): the row is read only where a refusal can happen. A revoked caller with a live row on a
// non-platform GET (a plain read, and a read method behind RefuseImpersonation) is served as itself, and the session
// store is not read at all.
func TestARevokedCallersNonPlatformReadIsServedAsSelfWithNoRowRead(t *testing.T) {
	for _, path := range []string{"/read", "/refusing-read"} {
		t.Run(path, func(t *testing.T) {
			store, handler, token := sessionStateRig(t, billingOnly(), true)
			got := serve(handler, http.MethodGet, path, map[string]string{"Authorization": "Bearer " + token})
			if got.status != http.StatusOK || !strings.HasPrefix(got.body, "org="+ownOrg+" user="+userID.String()+" imp=-") {
				t.Fatalf("%d %q, want 200 served as the caller", got.status, got.body)
			}
			if store.sessionCalls != 0 {
				t.Fatalf("%d session-store reads, want 0", store.sessionCalls)
			}
		})
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

const unavailableBody = `{"detail":{"message":"Database temporarily unavailable"}}`

// A failed row read on a write fails closed for a caller without admin.impersonate: 503, the dependency failure, and
// the route does not run. A plain read reads no row, so it is served.
func TestARowReadFailureWithoutAdminImpersonateFailsClosed(t *testing.T) {
	store, handler, token := sessionStateRig(t, billingOnly(), true)
	store.errSession = errors.New("store down")
	bearer := map[string]string{"Authorization": "Bearer " + token}
	if got := serve(handler, http.MethodPost, "/write", bearer); got.status != http.StatusServiceUnavailable || got.body != unavailableBody {
		t.Fatalf("POST /write: %d %q, want 503 %s", got.status, got.body, unavailableBody)
	}
	if got := serve(handler, http.MethodGet, "/read", bearer); got.status != http.StatusOK {
		t.Fatalf("GET /read: %d %q, want 200 (no row read)", got.status, got.body)
	}
}

// A holder of admin.impersonate whose row read fails gets 503 before the route, on a read too (the scope reads the row
// for the org swap).
func TestARowReadFailureOfAHolderIs503(t *testing.T) {
	store, handler, token := sessionStateRig(t, nil, true)
	store.errSession = errors.New("store down")
	if got := serve(handler, http.MethodGet, "/read", map[string]string{"Authorization": "Bearer " + token}); got.status != http.StatusServiceUnavailable || got.body != unavailableBody {
		t.Fatalf("GET /read: %d %q, want 503 %s", got.status, got.body, unavailableBody)
	}
}

// platformRig is the caller behind Principal, OrgScope and Impersonation, with a platform write and a platform read
// of admin.orgs (not a billing action) behind WrapPlatform; reached counts the handler runs.
func platformRig(t *testing.T, platform *rule.Platform, live bool) (*fakeStore, http.Handler, string, *int) {
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
	reached := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { reached++; _, _ = io.WriteString(w, "reached") })
	mux := http.NewServeMux()
	mux.Handle("POST /platform/orgs", guard.WrapPlatform(rule.ActionAdminOrgs, handler))
	mux.Handle("GET /platform/orgs", guard.WrapPlatform(rule.ActionAdminOrgs, handler))
	token := sign(t, claims(func(c jwt.MapClaims) { c["is_superuser"] = true; c["role"] = "admin" }))
	return store, scope.Principal(scope.OrgScope(scope.Impersonation(mux))), token, &reached
}

// CHAOS-13143 (acp): the platform gate refuses on the live row too. A caller whose admin.impersonate was revoked while
// the row is live, and who holds the route's action, gets the session 403 on a platform write and a platform read;
// the handler does not run.
func TestPlatformRoutesRefuseALiveRowWithoutAdminImpersonate(t *testing.T) {
	orgsAndBilling := rule.PlatformOf(rule.ActionAdminOrgs, rule.ActionAdminBilling)
	cases := []struct {
		name     string
		platform *rule.Platform
		live     bool
		status   int
		body     string
	}{
		{"admin.impersonate revoked, live row", &orgsAndBilling, true, http.StatusForbidden, `{"detail":"Not allowed in an impersonation session"}`},
		{"admin.impersonate holder, live row (unchanged)", nil, true, http.StatusForbidden, `{"detail":"Not allowed in an impersonation session"}`},
		{"admin.impersonate revoked, no row (the normal guard)", &orgsAndBilling, false, http.StatusOK, "reached"},
	}
	for _, c := range cases {
		for _, method := range []string{http.MethodPost, http.MethodGet} {
			t.Run(c.name+"/"+method, func(t *testing.T) {
				_, handler, token, reached := platformRig(t, c.platform, c.live)
				got := serve(handler, method, "/platform/orgs", map[string]string{"Authorization": "Bearer " + token})
				if got.status != c.status || got.body != c.body {
					t.Fatalf("%d %q, want %d %q", got.status, got.body, c.status, c.body)
				}
				if want := c.status == http.StatusOK; (*reached == 1) != want {
					t.Fatalf("handler runs %d, want reached=%v", *reached, want)
				}
			})
		}
	}
}

// A failed row read on a platform route (read or write) of a revoked caller is 503; the handler does not run.
func TestPlatformRoutesAnswer503WhenTheRowReadFails(t *testing.T) {
	orgsAndBilling := rule.PlatformOf(rule.ActionAdminOrgs, rule.ActionAdminBilling)
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			store, handler, token, reached := platformRig(t, &orgsAndBilling, true)
			store.errSession = errors.New("store down")
			got := serve(handler, method, "/platform/orgs", map[string]string{"Authorization": "Bearer " + token})
			if got.status != http.StatusServiceUnavailable || got.body != unavailableBody || *reached != 0 {
				t.Fatalf("%d %q, handler runs %d; want 503 %s, none", got.status, got.body, *reached, unavailableBody)
			}
		})
	}
}
