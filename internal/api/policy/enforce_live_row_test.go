package policy

import (
	"errors"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

const sessionRowUnavailableBody = `{"detail":{"message":"Database temporarily unavailable"}}`

// revokedImpersonatorStore is an admin of its own org whose admin.impersonate was revoked (it still holds admin.orgs)
// while its go-api impersonation row stayed live. No request carries a session context for it: the scope applies a
// row to a holder of admin.impersonate only.
func revokedImpersonatorStore(live bool, readErr error) *accessStore {
	store := personaStore(personaAdmin)
	store.users[userID] = UserState{IsActive: true, IsSuperuser: true, Platform: only(ActionAdminOrgs)}
	if live {
		store.sessions[userID] = &Impersonation{ID: uuid.New(), AdminUserID: userID, TargetUserID: targetUserID, TargetOrgID: targetOrgID,
			TargetRole: "admin", ExpiresAt: time.Now().Add(time.Hour)}
	}
	store.errSession = readErr
	return store
}

// revokedRefusedCells are the enforced rows a live row refuses whatever the caller's actions: a platform row (any
// method) and a write.
var revokedRefusedCells = []enforceCell{
	{"admin", http.MethodGet, "/api/v1/admin/orgs"},
	{"teams", http.MethodPost, "/api/v1/admin/teams"},
}

// revokedReadCells are enforced non-platform reads: the row is not read there, so the decision point judges the caller.
var revokedReadCells = []enforceCell{
	{"teams", http.MethodGet, "/api/v1/admin/teams"},
	{"billing", http.MethodGet, "/api/v1/billing/invoices"},
}

// With go-api enforce on, a live impersonation row refuses an enforced platform row or write, also for a caller whose
// admin.impersonate was revoked: after the row's question only (decideLiveRow; the allow is still the session refusal).
// After the row ends the decision point decides.
func TestARevokedImpersonatorWithALiveRowIsRefusedOnEnforcedWriteAndPlatformRows(t *testing.T) {
	for _, cell := range revokedRefusedCells {
		t.Run(cell.namespace+" "+cell.method, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			store := revokedImpersonatorStore(true, nil)
			got := enforceRequest(enforceHandler(t, store, userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor(cell.namespace))), cell.method, cell.path)
			if got.status != http.StatusForbidden || got.body != impersonationRefusalBody {
				t.Errorf("live row: %d %q, want 403 %q", got.status, got.body, impersonationRefusalBody)
			}
			if n, want := len(pdp.calls()), revokedLiveRowCalls(shadowTestRows[cell.method+" "+cell.path]); n != want {
				t.Errorf("live row: %d PDP calls, want %d", n, want)
			}
			if d := rec.snapshot(); !slices.Equal(d, []string{cell.namespace + "/impersonation_refused"}) {
				t.Errorf("live row: decisions %v, want [%s/impersonation_refused]", d, cell.namespace)
			}

			ended := newTestPDP(t, allowAll)
			got = enforceRequest(enforceHandler(t, revokedImpersonatorStore(false, nil), userClaims(nil), newEnforcerFor(t, ended.client(t), onFor(cell.namespace))), cell.method, cell.path)
			if got.status != http.StatusOK || len(ended.calls()) != 1 {
				t.Errorf("row ended: %d %q, %d PDP calls, want 200 and 1", got.status, got.body, len(ended.calls()))
			}
		})
	}
}

// A live row on an enforced platform row asks the row's role question first (decideLiveRow): a deny is the legacy
// deny (counted deny), an allow or a failed question the session refusal (counted impersonation_refused, never a 503,
// never served). On an organization write row a caller with no admin.impersonate is asked the question it is asked with
// no row, with the same rule; a token with no organization on an AdminOrg row is refused unasked, a deny.
func TestALiveRowOnAPlatformRowAsksTheRoleQuestionFirst(t *testing.T) {
	orgless := func(c *AccessTokenClaims) { c.OrgID = nil }
	for _, cell := range []struct {
		name, verdict, method, path string
		claims                      func(*AccessTokenClaims)
		wantStatus                  int
		wantBody, wantCounted       string
		// wantAsked is "<resource type> <action>" of each decision point request.
		wantAsked []string
		// adminLevel mounts the organization row at the Admin level, whose token with no organization is asked admin.orgs.
		adminLevel bool
	}{
		{"platform deny", "deny", http.MethodGet, "/api/v1/admin/orgs", nil, http.StatusForbidden, superuserRefusalBody, "admin/deny", []string{"platform " + ActionAdminOrgs}, false},
		{"platform deny, no token org", "deny", http.MethodGet, "/api/v1/admin/orgs", orgless, http.StatusForbidden, `{"detail":"Organization context required"}`, "admin/deny", []string{"platform " + ActionAdminOrgs}, false},
		{"platform allow", "allow", http.MethodGet, "/api/v1/admin/orgs", nil, http.StatusForbidden, impersonationRefusalBody, "admin/impersonation_refused", []string{"platform " + ActionAdminOrgs}, false},
		{"platform question fails", "error", http.MethodGet, "/api/v1/admin/orgs", nil, http.StatusForbidden, impersonationRefusalBody, "admin/impersonation_refused", nil, false},
		{"organization write deny", "deny", http.MethodPost, "/api/v1/admin/teams", nil, http.StatusForbidden, `{"detail":"Admin access required"}`, "teams/deny", []string{"organization teams.write"}, false},
		{"organization write allow", "allow", http.MethodPost, "/api/v1/admin/teams", nil, http.StatusForbidden, impersonationRefusalBody, "teams/impersonation_refused", []string{"organization teams.write"}, false},
		{"organization write question fails", "error", http.MethodPost, "/api/v1/admin/teams", nil, http.StatusForbidden, impersonationRefusalBody, "teams/impersonation_refused", nil, false},
		{"organization write, AdminOrg, no token org", "allow", http.MethodPost, "/api/v1/admin/teams", orgless, http.StatusForbidden, `{"detail":"Organization context required"}`, "teams/deny", nil, false},
		{name: "organization write, Admin, no token org, deny", verdict: "deny", method: http.MethodPost, path: "/api/v1/admin/teams", claims: orgless,
			wantStatus: http.StatusForbidden, wantBody: `{"detail":"Organization context required"}`, wantCounted: "teams/deny", wantAsked: []string{"platform " + ActionAdminOrgs}, adminLevel: true},
		{name: "organization write, Admin, no token org, allow", verdict: "allow", method: http.MethodPost, path: "/api/v1/admin/teams", claims: orgless,
			wantStatus: http.StatusForbidden, wantBody: impersonationRefusalBody, wantCounted: "teams/impersonation_refused", wantAsked: []string{"platform " + ActionAdminOrgs}, adminLevel: true},
	} {
		t.Run(cell.name, func(t *testing.T) {
			rec := recordEnforce(t)
			verdict := allowAll
			if cell.verdict != "allow" {
				verdict = denyWith("role_lacks_action")
			}
			pdp := newTestPDP(t, verdict)
			if cell.verdict == "error" {
				pdp.kill()
			}
			namespace := shadowTestRows[cell.method+" "+cell.path].Namespace
			store := revokedImpersonatorStore(true, nil)
			handler := enforceHandler(t, store, userClaims(cell.claims), newEnforcerFor(t, pdp.client(t), onFor(namespace)))
			if cell.adminLevel {
				guard := NewGuard(routed(t, store, &fakeValidator{result: userClaims(cell.claims)}), quiet())
				guard.SetEnforcer(newEnforcerFor(t, pdp.client(t), onFor(namespace)))
				mux := http.NewServeMux()
				mux.Handle("POST /api/v1/admin/teams", guard.Wrap(Admin, echo))
				handler = mux
			}
			got := enforceRequest(handler, cell.method, cell.path)
			if got.status != cell.wantStatus || strings.TrimSpace(got.body) != cell.wantBody {
				t.Errorf("%d %q, want %d %q", got.status, got.body, cell.wantStatus, cell.wantBody)
			}
			if d := rec.snapshot(); !slices.Equal(d, []string{cell.wantCounted}) {
				t.Errorf("counted %v, want [%s]", d, cell.wantCounted)
			}
			var asked []string
			for _, call := range pdp.calls() {
				if call.Principal != shadowBearer || call.HasOrg != (call.Type == "organization") || call.HasOrg && (call.Org != ownOrg || call.Key != ownOrg) {
					t.Errorf("PDP saw %+v, want the question of the caller's own bearer (the token organization's on an organization row)", call)
				}
				asked = append(asked, call.Type+" "+call.Action)
			}
			if !slices.Equal(asked, cell.wantAsked) {
				t.Errorf("PDP asked %v, want %v", asked, cell.wantAsked)
			}
			if hits := pdp.deadHits.Load(); (cell.verdict == "error") != (hits > 0) {
				t.Errorf("%d calls to the failing decision point, want the role question asked only when it fails", hits)
			}
			if store.sessionCalls != 1 {
				t.Errorf("%d row reads, want 1", store.sessionCalls)
			}
		})
	}
}

// revokedLiveRowCalls is the number of decision point requests decideLiveRow makes for a caller with a live row and no
// session context (its admin.impersonate revoked) on row, with a token organization: the role question on a platform row,
// the row's own question on an organization row.
func revokedLiveRowCalls(row ShadowRow) int {
	if row.ResourceType == policycatalog.ScopeOrganization {
		return 1
	}
	return liveRowRoleCalls(row, false, "")
}

// A failed row read on an enforced platform row or write is the dependency failure 503 (fail closed), with no
// decision point call.
func TestARevokedImpersonatorsFailedRowReadOnAnEnforcedWriteOrPlatformRowIs503(t *testing.T) {
	for _, cell := range revokedRefusedCells {
		t.Run(cell.namespace+" "+cell.method, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			store := revokedImpersonatorStore(true, errors.New("impersonation row read failed"))
			got := enforceRequest(enforceHandler(t, store, userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor(cell.namespace))), cell.method, cell.path)
			if got.status != http.StatusServiceUnavailable || strings.TrimSpace(got.body) != sessionRowUnavailableBody {
				t.Errorf("failed row read: %d %q, want 503 %q", got.status, got.body, sessionRowUnavailableBody)
			}
			if n := len(pdp.calls()); n != 0 {
				t.Errorf("failed row read: %d PDP calls, want 0", n)
			}
			if d := rec.snapshot(); !slices.Equal(d, []string{cell.namespace + "/error_deny"}) {
				t.Errorf("failed row read: decisions %v, want [%s/error_deny]", d, cell.namespace)
			}
		})
	}
}

// The read limit: an enforced non-platform read reads no row, so the decision point judges the caller as itself (its
// own bearer), with a live row or a failing row read alike.
func TestARevokedImpersonatorsEnforcedReadIsDecidedAsItselfWithoutARowRead(t *testing.T) {
	for _, cell := range revokedReadCells {
		for name, readErr := range map[string]error{"live row": nil, "failing row read": errors.New("impersonation row read failed")} {
			t.Run(cell.namespace+" "+name, func(t *testing.T) {
				rec := recordEnforce(t)
				pdp := newTestPDP(t, allowAll)
				store := revokedImpersonatorStore(true, readErr)
				got := enforceRequest(enforceHandler(t, store, userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor(cell.namespace))), cell.method, cell.path)
				if got.status != http.StatusOK {
					t.Errorf("%d %q, want the decision point's 200", got.status, got.body)
				}
				if calls := pdp.calls(); len(calls) != 1 || calls[0].Principal != shadowBearer {
					t.Errorf("PDP calls %v, want 1 for the caller's own bearer", calls)
				}
				if d := rec.snapshot(); !slices.Equal(d, []string{cell.namespace + "/allow"}) {
					t.Errorf("decisions %v, want [%s/allow]", d, cell.namespace)
				}
				if store.sessionCalls != 0 {
					t.Errorf("%d row reads, want 0", store.sessionCalls)
				}
			})
		}
	}
}

// The stop route stays the decision point's, as for a holder: no row is read there.
func TestTheStopRouteOfARevokedImpersonatorReadsNoRow(t *testing.T) {
	rows := maps.Clone(shadowTestRows)
	rows[stopImpersonationPattern] = stopRouteRow(t)
	pdp := newTestPDP(t, allowAll)
	enf, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: rows, Switch: onFor("admin"), Timeout: time.Second, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	store := revokedImpersonatorStore(true, errors.New("impersonation row read failed"))
	guard := NewGuard(routed(t, store, &fakeValidator{result: userClaims(nil)}), quiet())
	guard.SetEnforcer(enf)
	mux := http.NewServeMux()
	mux.Handle(stopImpersonationPattern, guard.WrapPlatform(ActionAdminImpersonate, echo))
	got := enforceRequest(mux, http.MethodPost, stopImpersonationRoute)
	if got.status != http.StatusOK || len(pdp.calls()) != 1 || store.sessionCalls != 0 {
		t.Fatalf("stop route: %d %q, %d PDP calls, %d row reads; want 200, 1, 0", got.status, got.body, len(pdp.calls()), store.sessionCalls)
	}
}

// Enforce off for the row's namespace: every answer to the revoked caller is the answer without an enforcer, byte
// for byte, and no row read is added.
func TestEnforceOffIsUnchangedForARevokedImpersonator(t *testing.T) {
	for _, cell := range append(slices.Clone(revokedRefusedCells), revokedReadCells...) {
		for name, readErr := range map[string]error{"live row": nil, "failing row read": errors.New("impersonation row read failed")} {
			t.Run(cell.namespace+" "+cell.method+" "+name, func(t *testing.T) {
				recordEnforce(t)
				pdp := newTestPDP(t, allowAll)
				base := revokedImpersonatorStore(true, readErr)
				want := enforceRequest(enforceHandler(t, base, userClaims(nil), nil), cell.method, cell.path)
				off := revokedImpersonatorStore(true, readErr)
				got := enforceRequest(enforceHandler(t, off, userClaims(nil), newEnforcerFor(t, pdp.client(t), authzenforce.Switch{})), cell.method, cell.path)
				if got.status != want.status || got.body != want.body || !maps.EqualFunc(got.header, want.header, slices.Equal[[]string]) {
					t.Errorf("enforce off: %d %q %v, want %d %q %v", got.status, got.body, got.header, want.status, want.body, want.header)
				}
				if off.sessionCalls != base.sessionCalls || len(pdp.calls()) != 0 {
					t.Errorf("enforce off: %d row reads (want %d), %d PDP calls (want 0)", off.sessionCalls, base.sessionCalls, len(pdp.calls()))
				}
			})
		}
	}
}

// With the shadow beside enforcement, as go-api installs them, a refusal by the live row asks the shadow nothing: the
// shadow records impersonation_refused unasked, and the only call is the Enforcer's question (decideLiveRow). A failed row read leaves the session state unknown, so the shadow records nothing and nothing is asked.
func TestARevokedImpersonatorsRowRefusalAsksTheShadowNothing(t *testing.T) {
	for _, cell := range revokedRefusedCells {
		for name, readErr := range map[string]error{"live row": nil, "failing row read": errors.New("impersonation row read failed")} {
			t.Run(cell.namespace+" "+cell.method+" "+name, func(t *testing.T) {
				recordEnforce(t)
				srec := recordShadow(t)
				pdp := newTestPDP(t, allowAll)
				sh, _ := newShadowFor(t, pdp.client(t), nil)
				guard := NewGuard(routed(t, revokedImpersonatorStore(true, readErr), &fakeValidator{result: userClaims(nil)}), quiet())
				guard.SetEnforcer(newEnforcerFor(t, pdp.client(t), onFor(cell.namespace)))
				guard.SetShadow(sh)
				mux := http.NewServeMux()
				mux.Handle("POST /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
				mux.Handle("GET /api/v1/admin/orgs", guard.WrapPlatform(ActionAdminOrgs, echo))
				got := enforceRequest(mux, cell.method, cell.path)
				sh.wg.Wait()
				want, wantResults, wantCalls := http.StatusForbidden, []string{"impersonation_refused"}, revokedLiveRowCalls(shadowTestRows[cell.method+" "+cell.path])
				if readErr != nil {
					want, wantResults, wantCalls = http.StatusServiceUnavailable, nil, 0
				}
				_, results := srec.snapshot()
				if got.status != want || !slices.Equal(results, wantResults) || len(pdp.calls()) != wantCalls {
					t.Errorf("%d, shadow %v, %d PDP calls; want %d, %v, %d", got.status, results, len(pdp.calls()), want, wantResults, wantCalls)
				}
			})
		}
	}
}

// The same rule on the guard's own platform refusal (enforce off, the shadow on): a refusal by the live row records
// impersonation_refused unasked, and a failed row read asks the shadow nothing.
func TestARevokedImpersonatorsLegacyPlatformRefusalAsksTheShadowNothing(t *testing.T) {
	for name, readErr := range map[string]error{"live row": nil, "failing row read": errors.New("impersonation row read failed")} {
		t.Run(name, func(t *testing.T) {
			srec := recordShadow(t)
			pdp := newTestPDP(t, allowAll)
			sh, _ := newShadowFor(t, pdp.client(t), nil)
			guard := NewGuard(routed(t, revokedImpersonatorStore(true, readErr), &fakeValidator{result: userClaims(nil)}), quiet())
			guard.SetShadow(sh)
			mux := http.NewServeMux()
			mux.Handle("GET /api/v1/admin/orgs", guard.WrapPlatform(ActionAdminOrgs, echo))
			got := enforceRequest(mux, http.MethodGet, "/api/v1/admin/orgs")
			sh.wg.Wait()
			want, wantBody, wantResults := http.StatusForbidden, impersonationRefusalBody, []string{"impersonation_refused"}
			if readErr != nil {
				want, wantBody, wantResults = http.StatusServiceUnavailable, sessionRowUnavailableBody, nil
			}
			_, results := srec.snapshot()
			if got.status != want || strings.TrimSpace(got.body) != wantBody || !slices.Equal(results, wantResults) || len(pdp.calls()) != 0 {
				t.Errorf("%d %s, shadow %v, %d PDP calls; want %d %s, %v, 0", got.status, got.body, results, len(pdp.calls()), want, wantBody, wantResults)
			}
		})
	}
}
