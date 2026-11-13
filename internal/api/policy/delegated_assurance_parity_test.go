package policy

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// delegatedAllowedProfiles are the go-api profile rows whose delegated_bearer is "allowed", as go-api installs the
// delegated refusal (apiservice parseDelegatedRows reads the same field).
func delegatedAllowedProfiles(t *testing.T) map[string]bool {
	t.Helper()
	var doc struct {
		Rows []struct {
			Method, Route string
			Delegated     string `json:"delegated_bearer"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, row := range doc.Rows {
		if row.Delegated == "allowed" {
			out[row.Method+" "+row.Route] = true
		}
	}
	return out
}

// delegatedServe answers a delegated (act claim) bearer of a target admin of ownOrg on pattern, mounted with
// Authenticated (the level of every delegated-allowed billing and licensing row). admitted is the delegated row
// predicate; enforce installs an Enforcer over the real rows with namespace on.
func delegatedServe(t *testing.T, pdp *testPDP, admitted func(string) bool, pattern, namespace string, enforce bool) response {
	t.Helper()
	rows := goAPIRows(t)
	actor := "prn_" + wireID(actorID)
	auth := routed(t, personaStore(personaAdmin), &fakeValidator{result: userClaims(func(c *AccessTokenClaims) { c.Actor = &actor })})
	auth.DelegatedRows(admitted)
	var enf *Enforcer
	if enforce {
		enf = parityEnforcer(t, pdp, rows, namespace)
	}
	method, route, _ := strings.Cut(pattern, " ")
	path := strings.ReplaceAll(profilePath(route), "/x", "/"+ownOrg)
	m := parityMount{pattern, func(g *Guard) http.Handler { return g.Wrap(Authenticated, echo) }, path}
	return enforceRequest(parityMux(t, auth, enf, m), method, m.path)
}

// A delegated bearer's read on every enforced row that admits it today gets the cells-off answer when the allow
// carries minimum_assurance aal1, the level of every session go-api accepts (session.AssuranceOf).
func TestADelegatedReadWithAnAAL1AllowEqualsTheLegacyAnswerOnEveryDelegatedRow(t *testing.T) {
	rows := goAPIRows(t)
	admitted := delegatedAllowedProfiles(t)
	cells := 0
	for pattern := range admitted {
		row, enforced := rows[pattern]
		if !enforced {
			continue
		}
		cells++
		t.Run(pattern, func(t *testing.T) {
			pdp := newTestPDP(t, allowAll)
			pdp.obligations = map[string]any{"tenant_scope": ownOrg, "minimum_assurance": "aal1"}
			legacy := delegatedServe(t, pdp, func(p string) bool { return admitted[p] }, pattern, row.Namespace, false)
			got := delegatedServe(t, pdp, func(p string) bool { return admitted[p] }, pattern, row.Namespace, true)
			if got.status != legacy.status || got.body != legacy.body || legacy.status != http.StatusOK {
				t.Errorf("enforce-on %d %q, cells-off %d %q; want the cells-off 200", got.status, got.body, legacy.status, legacy.body)
			}
			if n := len(pdp.calls()); n != 1 {
				t.Errorf("%d PDP calls, want 1", n)
			}
		})
	}
	if cells != 10 {
		t.Fatalf("%d enforced delegated-allowed rows, want the 7 billing and licensing reads, the org.read read (GET /api/v1/orgs/me) and the 2 SSO provider reads (settings.read)", cells)
	}
}

// A level no go-api session meets, or one go-api does not know, stays no verdict on a delegated read: 503.
func TestADelegatedReadWithAHigherOrUnknownAssuranceStaysUnavailable(t *testing.T) {
	admitted := delegatedAllowedProfiles(t)
	for _, level := range []string{"aal2", "aal3", "aal9"} {
		t.Run(level, func(t *testing.T) {
			pdp := newTestPDP(t, allowAll)
			pdp.obligations = map[string]any{"tenant_scope": ownOrg, "minimum_assurance": level}
			got := delegatedServe(t, pdp, func(p string) bool { return admitted[p] }, "GET /api/v1/billing/invoices", "billing", true)
			if got.status != http.StatusServiceUnavailable {
				t.Errorf("delegated read, %s allow: %d %q, want 503", level, got.status, got.body)
			}
		})
	}
}

// A delegated write is refused by its row before any PDP call: 403, the delegated refusal.
func TestADelegatedWriteIsRefusedBeforeThePDP(t *testing.T) {
	admitted := delegatedAllowedProfiles(t)
	pdp := newTestPDP(t, allowAll)
	pdp.obligations = map[string]any{"tenant_scope": ownOrg, "minimum_assurance": "aal1"}
	pattern := "POST /api/v1/billing/subscriptions/cancel"
	if admitted[pattern] {
		t.Fatalf("%s admits a delegated bearer, want its row denied", pattern)
	}
	got := delegatedServe(t, pdp, func(p string) bool { return admitted[p] }, pattern, "billing", true)
	if got.status != http.StatusForbidden || got.body != `{"detail":"`+msgDelegatedDenied+`"}` || len(pdp.calls()) != 0 {
		t.Fatalf("delegated write: %d %q, %d PDP calls; want 403 %q and 0", got.status, got.body, len(pdp.calls()), msgDelegatedDenied)
	}
}

// The aal1 carry-out is for reads: a delegated write that a row admitted (in this test only) with an aal1 allow is no
// verdict, as before.
func TestADelegatedWriteWithAnAAL1AllowIsNotServed(t *testing.T) {
	pdp := newTestPDP(t, allowAll)
	pdp.obligations = map[string]any{"tenant_scope": ownOrg, "minimum_assurance": "aal1"}
	pattern := "POST /api/v1/billing/subscriptions/cancel"
	got := delegatedServe(t, pdp, func(p string) bool { return p == pattern }, pattern, "billing", true)
	if got.status != http.StatusServiceUnavailable || len(pdp.calls()) != 1 {
		t.Fatalf("delegated write admitted by its row, aal1 allow: %d %q, %d PDP calls; want 503 and 1", got.status, got.body, len(pdp.calls()))
	}
}
