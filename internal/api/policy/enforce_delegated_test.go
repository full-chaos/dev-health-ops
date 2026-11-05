package policy

import (
	"net/http"
	"testing"
)

// delegatedEnforceMux serves a read, a write and a platform row to a delegated session's bearer whose rows admit it.
func delegatedEnforceMux(t *testing.T, enf *Enforcer) http.Handler {
	t.Helper()
	auth := routed(t, personaStore(personaAdmin), &fakeValidator{result: userClaims(func(c *AccessTokenClaims) { c.Actor = &delegatedActorWire })})
	auth.DelegatedRows(func(string) bool { return true })
	guard := NewGuard(auth, quiet())
	guard.SetEnforcer(enf)
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	mux.Handle("POST /api/v1/admin/teams", guard.Wrap(AdminOrg, echo))
	mux.Handle("GET /api/v1/admin/orgs", guard.WrapPlatform(ActionAdminOrgs, echo))
	return mux
}

func delegatedAllowObligations(assurance string) map[string]any {
	return map[string]any{"tenant_scope": ownOrg, "minimum_assurance": assurance}
}

// A delegated read on an enforced row is judged by the PDP (as its target): an allow with minimum_assurance aal1 is
// served; aal2, aal3 and an unknown level stay no verdict (503).
func TestADelegatedReadIsServedOnAnAAL1AllowOnly(t *testing.T) {
	for _, cell := range []struct {
		assurance string
		status    int
		counted   string
	}{
		{"aal1", http.StatusOK, "teams/allow"},
		{"aal2", http.StatusServiceUnavailable, "teams/error_deny"},
		{"aal3", http.StatusServiceUnavailable, "teams/error_deny"},
		{"aal9", http.StatusServiceUnavailable, "teams/error_deny"},
	} {
		t.Run(cell.assurance, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			pdp.obligations = delegatedAllowObligations(cell.assurance)
			got := enforceRequest(delegatedEnforceMux(t, newEnforcerFor(t, pdp.client(t), onFor("teams"))), http.MethodGet, "/api/v1/admin/teams")
			if got.status != cell.status {
				t.Errorf("delegated read with %s: %d %q, want %d", cell.assurance, got.status, got.body, cell.status)
			}
			if calls := pdp.calls(); len(calls) != 1 || calls[0].Principal != shadowBearer || calls[0].Action != "teams.read" || calls[0].Org != ownOrg {
				t.Errorf("PDP saw %+v, want one teams.read question with the delegated bearer on its organization", calls)
			}
			if d := rec.snapshot(); len(d) != 1 || d[0] != cell.counted {
				t.Errorf("decisions %v, want [%s]", d, cell.counted)
			}
		})
	}
}

// A delegated bearer on an enforced row whose action is not a read (a write, a platform action) is refused with 403
// before any PDP call, whatever the PDP would answer.
func TestADelegatedNonReadIsRefusedBeforeThePDP(t *testing.T) {
	for _, cell := range []struct {
		name, namespace, method, path string
	}{
		{"write", "teams", http.MethodPost, "/api/v1/admin/teams"},
		{"platform", "admin", http.MethodGet, "/api/v1/admin/orgs"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, allowAll)
			pdp.obligations = delegatedAllowObligations("aal1")
			got := enforceRequest(delegatedEnforceMux(t, newEnforcerFor(t, pdp.client(t), onFor(cell.namespace))), cell.method, cell.path)
			if got.status != http.StatusForbidden || got.body != `{"detail":"`+msgImpersonationDenied+`"}` || len(pdp.calls()) != 0 {
				t.Errorf("delegated %s: %d %q with %d PDP calls, want 403 %q and none", cell.name, got.status, got.body, len(pdp.calls()), msgImpersonationDenied)
			}
			if d := rec.snapshot(); len(d) != 1 || d[0] != cell.namespace+"/"+enforceOutcomeImpersonationRefused {
				t.Errorf("decisions %v, want [%s/%s]", d, cell.namespace, enforceOutcomeImpersonationRefused)
			}
		})
	}
}
