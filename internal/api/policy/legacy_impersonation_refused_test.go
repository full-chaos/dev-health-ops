package policy

import (
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
)

const (
	statusRoute     = "/api/v1/admin/impersonate/status"
	stopRoute       = "/api/v1/admin/impersonate/stop"
	consumeRoute    = "/api/v1/admin/impersonate/handoff/consume"
	legacyRefusal   = `{"detail":"Not allowed in an impersonation session"}`
	delegatedOKPath = "/delegated-ok"
)

// legacyRoutes mounts one route per level the legacy caller reaches, the
// session's status and stop as go-api mounts them, and look-alikes of
// status and stop that serve 200 when the scope lets the caller through.
func legacyRoutes(guard *Guard) []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: "/admin", Handler: guard.Wrap(Admin, echo)},
		{Method: http.MethodPost, Pattern: "/admin", Handler: guard.Wrap(Admin, echo)},
		{Method: http.MethodGet, Pattern: "/admin-org", Handler: guard.Wrap(AdminOrg, echo)},
		{Method: http.MethodDelete, Pattern: "/admin-org", Handler: guard.Wrap(AdminOrg, echo)},
		{Method: http.MethodGet, Pattern: "/authenticated", Handler: guard.Wrap(Authenticated, echo)},
		{Method: http.MethodGet, Pattern: delegatedOKPath, Handler: guard.Wrap(Authenticated, echo)},
		{Method: http.MethodGet, Pattern: statusRoute, Handler: guard.Wrap(Authenticated, echo)},
		{Method: http.MethodPost, Pattern: stopRoute, Handler: guard.WrapPlatform(rule.ActionAdminImpersonate, echo)},
		{Method: http.MethodPost, Pattern: consumeRoute, Handler: guard.WrapPlatform(rule.ActionAdminImpersonate, echo)},
		{Method: http.MethodPost, Pattern: consumeRoute + "/extra", Handler: guard.Wrap(Authenticated, echo)},
		{Method: http.MethodGet, Pattern: statusRoute + "x", Handler: guard.Wrap(Authenticated, echo)},
		{Method: http.MethodGet, Pattern: statusRoute + "/extra", Handler: guard.Wrap(Authenticated, echo)},
		{Method: http.MethodPost, Pattern: stopRoute + "/extra", Handler: guard.Wrap(Authenticated, echo)},
		{Method: http.MethodPost, Pattern: "/api/v2/admin/impersonate/stop", Handler: guard.Wrap(Authenticated, echo)},
		{Method: http.MethodGet, Pattern: "/prefix" + statusRoute, Handler: guard.Wrap(Authenticated, echo)},
		{Method: http.MethodPost, Pattern: "/api/v1/admin/impersonate", Handler: guard.Wrap(Authenticated, echo)},
	}
}

// legacyStack is the api listener's order over legacyRoutes: the route
// template recorded first, then Principal, OrgScope, Impersonation, then the
// mux. set is nil to leave the option unset, as main builds the scope.
func legacyStack(t *testing.T, store *fakeStore, set *bool) http.Handler {
	t.Helper()
	auth := authenticator(t, store)
	auth.DelegatedRows(func(profile string) bool { return profile == http.MethodGet+" "+delegatedOKPath })
	scope := NewScope(auth, quiet())
	if set != nil {
		scope.RefuseLegacyImpersonation(*set)
	}
	routes := legacyRoutes(NewGuard(auth, quiet()))
	templates, err := httpapi.NewRouteTemplates(routes)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	return RouteProfiles(templates.Match)(scope.Principal(scope.OrgScope(scope.Impersonation(mux))))
}

// legacyCallerStore is the legacy caller: a superuser (userID) with a live
// go-api session row whose target is a plain member.
func legacyCallerStore() *fakeStore {
	store := newStore()
	superRow(store)
	store.sessions[userID] = &Impersonation{ID: uuid.New(), AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID,
		TargetRole: "member", ExpiresAt: time.Now().Add(time.Hour)}
	store.users[targetUserID] = UserState{IsActive: true}
	store.members[[2]uuid.UUID{targetUserID, targetOrgID}] = true
	store.roles[[2]uuid.UUID{targetUserID, targetOrgID}] = "member"
	return store
}

func serveBody(handler http.Handler, method, path string, headers map[string]string) response {
	request := httptest.NewRequest(method, path, strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return response{recorder.Code, recorder.Body.String(), recorder.Header()}
}

type legacyCell struct {
	name, method, path string
}

// refusedLegacyCells are the requests the switch refuses for the legacy
// caller: every route level, and every look-alike of status and stop (other
// method, longer path, other prefix, the start route).
var refusedLegacyCells = []legacyCell{
	{"Admin GET", http.MethodGet, "/admin"},
	{"Admin POST", http.MethodPost, "/admin"},
	{"AdminOrg GET", http.MethodGet, "/admin-org"},
	{"AdminOrg DELETE", http.MethodDelete, "/admin-org"},
	{"Authenticated GET", http.MethodGet, "/authenticated"},
	{"status HEAD", http.MethodHead, statusRoute},
	{"status POST", http.MethodPost, statusRoute},
	{"stop GET", http.MethodGet, stopRoute},
	{"status trailing slash", http.MethodGet, statusRoute + "/"},
	{"status suffix", http.MethodGet, statusRoute + "x"},
	{"status child path", http.MethodGet, statusRoute + "/extra"},
	{"stop child path", http.MethodPost, stopRoute + "/extra"},
	{"stop other version", http.MethodPost, "/api/v2/admin/impersonate/stop"},
	{"status under a prefix", http.MethodGet, "/prefix" + statusRoute},
	{"start", http.MethodPost, "/api/v1/admin/impersonate"},
	{"consume GET", http.MethodGet, consumeRoute},
	{"consume child path", http.MethodPost, consumeRoute + "/extra"},
	{"unknown path", http.MethodGet, "/nowhere"},
}

var admittedLegacyCells = []legacyCell{
	{"status", http.MethodGet, statusRoute},
	{"stop", http.MethodPost, stopRoute},
	{"handoff consume", http.MethodPost, consumeRoute},
}

var switchOn, switchOff = true, false

// With AUTH_PROXY_IMPERSONATION on, the legacy caller is refused with 403
// before any route runs, on every route but status and stop, and the
// response names no impersonation.
func TestLegacyImpersonationCallerIsRefusedUnderTheProxySwitch(t *testing.T) {
	headers := bearer(t, superClaims)
	for _, cell := range refusedLegacyCells {
		t.Run(cell.name, func(t *testing.T) {
			got := serveBody(legacyStack(t, legacyCallerStore(), &switchOn), cell.method, cell.path, headers)
			if got.status != http.StatusForbidden || got.body != legacyRefusal {
				t.Errorf("%s %s: %d %s, want 403 %s", cell.method, cell.path, got.status, got.body, legacyRefusal)
			}
			if got.header.Get("X-Impersonating") != "" || got.header.Get("X-Impersonated-User-Id") != "" {
				t.Errorf("%s %s: the refusal carries the impersonation headers %v", cell.method, cell.path, got.header)
			}
		})
	}
}

// revokedCallerStore is the legacy caller after its admin.impersonate was
// revoked (another platform action still held): its go-api row stays live.
func revokedCallerStore() *fakeStore {
	store := legacyCallerStore()
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true, Platform: only(rule.ActionAdminBilling)}
	return store
}

// revokedLegacyWriteCells and revokedLegacyReadCells are the route levels for a caller
// without admin.impersonate. The scope reads such a caller's row on a write
// only (the read limit of CHAOS-13143): it never applies the row to it, so a
// read answers as the caller itself.
var (
	revokedLegacyWriteCells = []legacyCell{{"Admin POST", http.MethodPost, "/admin"}, {"AdminOrg DELETE", http.MethodDelete, "/admin-org"}}
	revokedLegacyReadCells  = []legacyCell{{"Admin GET", http.MethodGet, "/admin"}, {"AdminOrg GET", http.MethodGet, "/admin-org"},
		{"Authenticated GET", http.MethodGet, "/authenticated"}}
)

// The refusal follows the live row, not the caller's admin.impersonate: a
// legacy caller whose action was revoked is refused on a write like a holder,
// and after the row ends the switch changes none of its answers.
func TestRevokedLegacyImpersonationCallerIsRefusedUnderTheProxySwitch(t *testing.T) {
	headers := bearer(t, superClaims)
	for _, cell := range revokedLegacyWriteCells {
		t.Run(cell.name, func(t *testing.T) {
			got := serveBody(legacyStack(t, revokedCallerStore(), &switchOn), cell.method, cell.path, headers)
			if got.status != http.StatusForbidden || got.body != legacyRefusal {
				t.Errorf("%s %s: %d %s, want 403 %s", cell.method, cell.path, got.status, got.body, legacyRefusal)
			}
			if got.header.Get("X-Impersonating") != "" || got.header.Get("X-Impersonated-User-Id") != "" {
				t.Errorf("%s %s: the refusal carries the impersonation headers %v", cell.method, cell.path, got.header)
			}
			ended := revokedCallerStore()
			delete(ended.sessions, userID)
			on := serveBody(legacyStack(t, ended, &switchOn), cell.method, cell.path, headers)
			off := serveBody(legacyStack(t, ended, nil), cell.method, cell.path, headers)
			assertSameResponse(t, cell, on, off)
			if on.body == legacyRefusal {
				t.Errorf("%s %s: refused after the row ended: %d %s", cell.method, cell.path, on.status, on.body)
			}
		})
	}
}

// A read by the revoked caller with a live row answers as the caller itself:
// byte-equal to the same caller without a row, with the switch on or unset, in
// its own org, with no target data and no impersonation headers. No row is
// read: a failing row read changes nothing.
func TestRevokedLegacyImpersonationCallerReadsAsItselfUnderTheProxySwitch(t *testing.T) {
	headers := bearer(t, superClaims)
	for _, cell := range revokedLegacyReadCells {
		t.Run(cell.name, func(t *testing.T) {
			ended := revokedCallerStore()
			delete(ended.sessions, userID)
			self := serveBody(legacyStack(t, ended, &switchOn), cell.method, cell.path, headers)
			failing := revokedCallerStore()
			failing.errSession = errors.New("session read failed")
			for name, store := range map[string]*fakeStore{"live row": revokedCallerStore(), "failing row read": failing} {
				for _, set := range []*bool{&switchOn, nil} {
					got := serveBody(legacyStack(t, store, set), cell.method, cell.path, headers)
					assertSameResponse(t, cell, got, self)
					if got.body == legacyRefusal || strings.Contains(got.body, targetOrgID.String()) || strings.Contains(got.body, targetUserID.String()) ||
						got.header.Get("X-Impersonating") != "" || got.header.Get("X-Impersonated-User-Id") != "" {
						t.Errorf("%s, %s: %d %s %v, want the caller's own answer", cell.name, name, got.status, got.body, got.header)
					}
				}
			}
			if cell.path == "/authenticated" && (self.status != http.StatusOK || self.body != "org="+ownOrg+" user="+userID.String()+" imp=-") {
				t.Errorf("Authenticated GET as itself: %d %s, want 200 in its own org", self.status, self.body)
			}
		})
	}
}

// With the switch off (set off or never set), the scope neither reads nor
// applies the revoked caller's live row: a write answers as the caller itself,
// byte-equal to the same caller without a row, and a failing row read changes
// nothing.
func TestRevokedLegacyImpersonationCallerWritesAsItselfWithTheSwitchOff(t *testing.T) {
	headers := bearer(t, superClaims)
	for _, cell := range revokedLegacyWriteCells {
		t.Run(cell.name, func(t *testing.T) {
			ended := revokedCallerStore()
			delete(ended.sessions, userID)
			self := serveBody(legacyStack(t, ended, nil), cell.method, cell.path, headers)
			failing := revokedCallerStore()
			failing.errSession = errors.New("session read failed")
			for name, store := range map[string]*fakeStore{"live row": revokedCallerStore(), "failing row read": failing} {
				for _, set := range []*bool{&switchOff, nil} {
					got := serveBody(legacyStack(t, store, set), cell.method, cell.path, headers)
					assertSameResponse(t, cell, got, self)
					if got.header.Get("X-Impersonating") != "" || strings.Contains(got.body, targetOrgID.String()) {
						t.Errorf("%s, %s: %d %s %v, want the caller's own answer", cell.name, name, got.status, got.body, got.header)
					}
				}
			}
		})
	}
}

// A failed row read for the revoked caller on a write is the dependency
// failure 503 under the switch.
func TestRevokedLegacyImpersonationCallerSessionReadFailsClosedUnderTheProxySwitch(t *testing.T) {
	for _, cell := range revokedLegacyWriteCells {
		store := revokedCallerStore()
		store.errSession = errors.New("session read failed")
		got := serveBody(legacyStack(t, store, &switchOn), cell.method, cell.path, bearer(t, superClaims))
		if got.status != http.StatusServiceUnavailable || got.body != `{"detail":{"message":"Database temporarily unavailable"}}` {
			t.Errorf("%s: %d %s, want the dependency failure 503", cell.name, got.status, got.body)
		}
	}
}

// Status and stop still apply the session to the legacy caller: the web
// sends the admin's bearer to exactly these two.
func TestLegacyImpersonationCallerReachesStatusAndStop(t *testing.T) {
	headers := bearer(t, superClaims)
	for _, cell := range admittedLegacyCells {
		t.Run(cell.name, func(t *testing.T) {
			got := serveBody(legacyStack(t, legacyCallerStore(), &switchOn), cell.method, cell.path, headers)
			if got.status != http.StatusOK || !strings.Contains(got.body, "imp="+targetOrgID.String()) || got.header.Get("X-Impersonating") != "true" {
				t.Errorf("%s %s: %d %s %v, want 200 in the session", cell.method, cell.path, got.status, got.body, got.header)
			}
		})
	}
}

// The delegated session's own bearer (the target, with the admin as its
// actor) is no legacy caller: on a row that admits it, it is served as the
// target while the admin's go-api row is live.
func TestDelegatedBearerIsServedUnderTheProxySwitch(t *testing.T) {
	store := legacyCallerStore()
	token := sign(t, claims(func(c jwt.MapClaims) {
		c["sub"] = targetUserID.String()
		c["org_id"] = targetOrgID.String()
		c["impersonating_user_id"] = userID.String()
	}))
	got := serveBody(legacyStack(t, store, &switchOn), http.MethodGet, delegatedOKPath, map[string]string{"Authorization": "Bearer " + token})
	if got.status != http.StatusOK || got.body != "org="+targetOrgID.String()+" user="+targetUserID.String()+" imp=-" {
		t.Fatalf("delegated bearer on an allowed row: %d %s, want 200 as the target", got.status, got.body)
	}
}

// A session read that fails is the dependency failure 503 under the switch,
// as without it.
func TestLegacyImpersonationSessionReadFailsClosedUnderTheProxySwitch(t *testing.T) {
	for _, cell := range append(append([]legacyCell{}, admittedLegacyCells...), refusedLegacyCells[0]) {
		store := legacyCallerStore()
		store.errSession = errors.New("session read failed")
		got := serveBody(legacyStack(t, store, &switchOn), cell.method, cell.path, bearer(t, superClaims))
		if got.status != http.StatusServiceUnavailable || got.body != `{"detail":{"message":"Database temporarily unavailable"}}` {
			t.Errorf("%s: %d %s, want the dependency failure 503", cell.name, got.status, got.body)
		}
	}
}

// A superuser with no live session row is no legacy caller: the switch does
// not change a byte of their answers.
func TestNoSessionCallerIsUnchangedUnderTheProxySwitch(t *testing.T) {
	for _, cell := range append(append([]legacyCell{}, admittedLegacyCells...), refusedLegacyCells...) {
		t.Run(cell.name, func(t *testing.T) {
			store := legacyCallerStore()
			delete(store.sessions, userID)
			on := serveBody(legacyStack(t, store, &switchOn), cell.method, cell.path, bearer(t, superClaims))
			off := serveBody(legacyStack(t, store, nil), cell.method, cell.path, bearer(t, superClaims))
			assertSameResponse(t, cell, on, off)
			if strings.Contains(on.body, "impersonation session") {
				t.Errorf("%s: a caller without a session got %d %s", cell.name, on.status, on.body)
			}
		})
	}
}

// With the switch off, every answer to the legacy caller is main's byte for
// byte: the option set off answers as the option never set. On main the
// legacy caller reaches every route in the target org (the CHAOS-10866
// repro): pinned here so the switch is the only change.
func TestLegacyImpersonationSwitchOffIsUnchanged(t *testing.T) {
	for _, cell := range append(append([]legacyCell{}, admittedLegacyCells...), refusedLegacyCells...) {
		t.Run(cell.name, func(t *testing.T) {
			unset := serveBody(legacyStack(t, legacyCallerStore(), nil), cell.method, cell.path, bearer(t, superClaims))
			off := serveBody(legacyStack(t, legacyCallerStore(), &switchOff), cell.method, cell.path, bearer(t, superClaims))
			assertSameResponse(t, cell, unset, off)
			if off.body == legacyRefusal && off.header.Get("X-Impersonating") == "" {
				t.Errorf("%s: the switch off refused the legacy caller in the scope: %d %s", cell.name, off.status, off.body)
			}
		})
	}
	for _, cell := range refusedLegacyCells[:5] {
		got := serveBody(legacyStack(t, legacyCallerStore(), &switchOff), cell.method, cell.path, bearer(t, superClaims))
		if got.status != http.StatusOK || !strings.Contains(got.body, "imp="+targetOrgID.String()) {
			t.Errorf("%s with the switch off: %d %s, want main's 200 in the target org", cell.name, got.status, got.body)
		}
	}
}

func assertSameResponse(t *testing.T, cell legacyCell, a, b response) {
	t.Helper()
	if a.status != b.status || a.body != b.body || !maps.EqualFunc(a.header, b.header, func(x, y []string) bool { return strings.Join(x, "\x00") == strings.Join(y, "\x00") }) {
		t.Errorf("%s %s: %d %s %v != %d %s %v", cell.method, cell.path, a.status, a.body, a.header, b.status, b.body, b.header)
	}
}
