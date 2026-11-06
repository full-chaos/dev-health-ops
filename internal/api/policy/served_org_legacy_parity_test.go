package policy

import (
	"net/http"
	"testing"
)

// servedOrgInvoiceRows are the census query rows whose handler reads org_id only for an admin.billing holder
// (invoiceOrg, internal/api/billing/invoices.go:31); any other caller is served the token organization.
var servedOrgInvoiceRows = map[string]bool{
	"GET /api/v1/billing/invoices":                    true,
	"GET /api/v1/billing/invoices/{invoice_id}":       true,
	"POST /api/v1/billing/invoices/{invoice_id}/void": true,
}

// Product rule 1 (D6773) and CHAOS-13209 B3/B4: with a census row's namespace switched on, a request that serves
// another organization than the token's (or a token with none) gets the answer the same guard gives with no enforcer:
// status and body, and no decision point call. The one exception is an invoice row asked by a caller the handler
// does not read org_id for: that request serves the token organization, so the decision point judges it there.
func TestServedOrgOtherOrganizationKeepsTheLegacyAnswer(t *testing.T) {
	noOrg := userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
	for _, row := range servedOrgCensus {
		t.Run(row.pattern, func(t *testing.T) {
			namespace := servedOrgNamespace(t, row.pattern)
			for _, tc := range []struct {
				name   string
				setup  servedOrgSetup
				served string
				holder bool
			}{
				{"platform admin with token A serves another org", servedOrgSetup{who: personaSuperuser}, strangerOrg, true},
				{"admin of A and B with token A serves B", servedOrgSetup{who: personaAdmin, adminOf: []string{memberOrg}}, memberOrg, false},
				{"admin of A only with token A serves another org", servedOrgSetup{who: personaAdmin}, strangerOrg, false},
				{"platform admin with no token org serves A", servedOrgSetup{who: personaSuperuser, claims: noOrg}, ownOrg, true},
			} {
				legacy := servedOrgServe(t, row, tc.setup, tc.served)
				pdp := newTestPDP(t, allowAll)
				enforced := tc.setup
				enforced.enforcer = servedOrgEnforcer(t, pdp, namespace)
				got := servedOrgServe(t, row, enforced, tc.served)
				if got.status != legacy.status || got.body != legacy.body {
					t.Errorf("%s: enforced %d %q, want the legacy %d %q", tc.name, got.status, got.body, legacy.status, legacy.body)
				}
				calls := pdp.calls()
				if servedOrgInvoiceRows[row.pattern] && !tc.holder {
					if len(calls) != 1 || calls[0].Org != ownOrg {
						t.Errorf("%s: PDP saw %+v, want one question on the token org %s (the handler ignores org_id)", tc.name, calls, ownOrg)
					}
					continue
				}
				if len(calls) != 0 {
					t.Errorf("%s: PDP saw %+v, want no call", tc.name, calls)
				}
			}
		})
	}
}

// The same rule for a legacy HS256 admin token of org A: another organization keeps the legacy answer.
func TestServedOrgOtherOrganizationKeepsTheLegacyAnswerForHS256(t *testing.T) {
	for _, row := range servedOrgCensus {
		t.Run(row.pattern, func(t *testing.T) {
			pdp := newTestPDP(t, allowAll)
			legacy := servedOrgServeHS256(t, row, nil, strangerOrg)
			got := servedOrgServeHS256(t, row, servedOrgEnforcer(t, pdp, servedOrgNamespace(t, row.pattern)), strangerOrg)
			if got.status != legacy.status || got.body != legacy.body {
				t.Errorf("HS256, another org: %d %q, want the legacy %d %q", got.status, got.body, legacy.status, legacy.body)
			}
			if legacy.status != http.StatusOK {
				t.Errorf("HS256, another org, legacy guard: %d %q, want 200 (the stub handler)", legacy.status, legacy.body)
			}
			if n := len(pdp.calls()); n != 0 {
				t.Errorf("HS256: %d PDP calls, want 0", n)
			}
		})
	}
}
