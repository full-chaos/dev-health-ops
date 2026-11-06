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

// With go-api enforce on, a live impersonation row refuses an enforced platform row or write before any decision point
// call, also for a caller whose admin.impersonate was revoked; after the row ends the decision point decides.
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
			if n := len(pdp.calls()); n != 0 {
				t.Errorf("live row: %d PDP calls, want 0", n)
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

// With the shadow beside enforcement, as go-api installs them, a refusal by the live row asks no decision point
// either: the shadow records impersonation_refused unasked. A failed row read leaves the session state unknown, so the
// shadow records nothing and asks nothing.
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
				want, wantResults := http.StatusForbidden, []string{"impersonation_refused"}
				if readErr != nil {
					want, wantResults = http.StatusServiceUnavailable, nil
				}
				_, results := srec.snapshot()
				if got.status != want || !slices.Equal(results, wantResults) || len(pdp.calls()) != 0 {
					t.Errorf("%d, shadow %v, %d PDP calls; want %d, %v, 0", got.status, results, len(pdp.calls()), want, wantResults)
				}
			})
		}
	}
}
