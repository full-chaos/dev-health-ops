package policy

import (
	"net/http"
	"slices"
	"testing"
)

// Product rule 1 (D6773) and CHAOS-13209 B3/B4: with a census row's namespace switched on, a request that serves
// another organization than the token's (or a token with none) gets the answer the same guard gives with no enforcer:
// status and body, and no decision point call: the legacy guard decides, then the handler's own served-org check.
func TestServedOrgOtherOrganizationKeepsTheLegacyAnswer(t *testing.T) {
	noOrg := userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
	for _, row := range servedOrgCensus {
		t.Run(row.pattern, func(t *testing.T) {
			namespace := servedOrgNamespace(t, row.pattern)
			for _, tc := range []struct {
				name   string
				setup  servedOrgSetup
				served string
			}{
				{"platform admin with token A serves another org", servedOrgSetup{who: personaSuperuser}, strangerOrg},
				{"admin of A and B with token A serves B", servedOrgSetup{who: personaAdmin, adminOf: []string{memberOrg}}, memberOrg},
				{"admin of A only with token A serves another org", servedOrgSetup{who: personaAdmin}, strangerOrg},
				{"platform admin with no token org serves A", servedOrgSetup{who: personaSuperuser, claims: noOrg}, ownOrg},
			} {
				legacy := servedOrgServe(t, row, tc.setup, tc.served)
				pdp := newTestPDP(t, allowAll)
				enforced := tc.setup
				enforced.enforcer = servedOrgEnforcer(t, pdp, namespace)
				got := servedOrgServe(t, row, enforced, tc.served)
				if got.status != legacy.status || got.body != legacy.body {
					t.Errorf("%s: enforced %d %q, want the legacy %d %q", tc.name, got.status, got.body, legacy.status, legacy.body)
				}
				if calls := pdp.calls(); len(calls) != 0 {
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

// D1 is not WIDER (acp Q3): a viewer of A who is an admin of B, with token A, asks an Admin row that serves B. The
// legacy guard refuses the viewer at policy.Admin on the token org (guard.go:178); judging B alone would admit them.
// Both paths answer 403 with the legacy body, and no decision point call is made.
func TestServedOrgViewerOfAAdminOfBIsRefusedOnBothPaths(t *testing.T) {
	viewer := persona{role: "viewer"}
	for _, row := range servedOrgCensus {
		if row.level != Admin {
			continue
		}
		t.Run(row.pattern, func(t *testing.T) {
			setup := servedOrgSetup{who: viewer, adminOf: []string{memberOrg}}
			legacy := servedOrgServe(t, row, setup, memberOrg)
			if legacy.status != http.StatusForbidden || legacy.body != `{"detail":"Admin access required"}` {
				t.Fatalf("legacy: %d %q, want 403 Admin access required", legacy.status, legacy.body)
			}
			pdp := newTestPDP(t, allowAll)
			setup.enforcer = servedOrgEnforcer(t, pdp, servedOrgNamespace(t, row.pattern))
			got := servedOrgServe(t, row, setup, memberOrg)
			if got.status != legacy.status || got.body != legacy.body {
				t.Errorf("enforced: %d %q, want the legacy %d %q", got.status, got.body, legacy.status, legacy.body)
			}
			if calls := pdp.calls(); len(calls) != 0 {
				t.Errorf("PDP saw %+v, want no call", calls)
			}
		})
	}
}

// The impersonation refusal runs before the served-org seam (CHAOS-13210 builds on this order): an impersonating
// caller gets the same answer whether the request names its own organization or another one, with no PDP call.
func TestServedOrgSeamRunsAfterTheImpersonationRefusal(t *testing.T) {
	answer := func(org string) (response, int) {
		pdp := newTestPDP(t, allowAll)
		h := impersonating(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), servedOrgEnforcer(t, pdp, "billing")))
		got := enforceRequest(h, http.MethodGet, "/api/v1/billing/invoices?org_id="+org)
		return got, len(pdp.calls())
	}
	own, ownCalls := answer(ownOrg)
	other, otherCalls := answer(strangerOrg)
	if other.status != own.status || other.body != own.body || ownCalls != 0 || otherCalls != 0 {
		t.Errorf("impersonating, another org: %d %q (%d PDP calls), want the own-org answer %d %q (%d calls) and none",
			other.status, other.body, otherCalls, own.status, own.body, ownCalls)
	}
}

// A token with no organization that names the nil UUID is still another organization (servedOrg.against): the legacy
// answer, counted served_org, not no_org.
func TestServedOrgNilUUIDWithNoTokenOrgIsServedOrg(t *testing.T) {
	noOrg := userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
	row := servedOrgCensus[0]
	rec := recordEnforce(t)
	pdp := newTestPDP(t, allowAll)
	servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser, claims: noOrg, enforcer: servedOrgEnforcer(t, pdp, servedOrgNamespace(t, row.pattern))},
		"00000000-0000-0000-0000-000000000000")
	if got := rec.unjudgedSnapshot(); !slices.Equal(got, []string{row.pattern + "/served_org"}) || len(pdp.calls()) != 0 {
		t.Errorf("unjudged %v, %d PDP calls, want [%s/served_org] and none", got, len(pdp.calls()), row.pattern)
	}
}
