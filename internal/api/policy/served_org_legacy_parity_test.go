package policy

import (
	"net/http"
	"slices"
	"strings"
	"testing"
)

// Product rule 1 (D6773) and CHAOS-10872 S2-parity: with a census row's namespace switched on, a request that serves
// another organization than the token's gets the answer the same guard gives with no enforcer (status and body), and the
// decision point is asked ONE question about the TOKEN organization, never the served one: the verdict is the token
// organization's, the handler's own served-org check follows. A token with no organization has none to judge: that class
// keeps the legacy answer with no call (S1).
func TestServedOrgOtherOrganizationIsJudgedOnTheTokenOrganization(t *testing.T) {
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
				wantCalls := 1
				if tc.setup.claims != nil {
					wantCalls = 0
				}
				if calls := pdp.calls(); len(calls) != wantCalls || (wantCalls == 1 && calls[0].Org != ownOrg) {
					t.Errorf("%s: PDP saw %+v, want %d question(s), on the token organization %s", tc.name, calls, wantCalls, ownOrg)
				}
			}
		})
	}
}

// D1 is not WIDER (acp Q3): a viewer of A who is an admin of B, with token A, asks an Admin row that serves B. The
// legacy guard refuses the viewer at policy.Admin on the token org (guard.go:178); judging B alone would admit them.
// Both paths answer 403 with the legacy body; the decision point (which denies a viewer) is asked once, about A.
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
			pdp := newTestPDP(t, denyWith("role_lacks_action"))
			setup.enforcer = servedOrgEnforcer(t, pdp, servedOrgNamespace(t, row.pattern))
			got := servedOrgServe(t, row, setup, memberOrg)
			if got.status != legacy.status || got.body != legacy.body {
				t.Errorf("enforced: %d %q, want the legacy %d %q", got.status, got.body, legacy.status, legacy.body)
			}
			if calls := pdp.calls(); len(calls) != 1 || calls[0].Org != ownOrg {
				t.Errorf("PDP saw %+v, want one question on the token organization %s", calls, ownOrg)
			}
		})
	}
}

// The impersonation refusal runs before the served-org seam (CHAOS-13210 builds on this order): an impersonating
// caller's write is refused 403 impersonation_refused whether it names its own organization or another one, with no
// PDP call.
func TestServedOrgSeamRunsAfterTheImpersonationRefusal(t *testing.T) {
	const path = "/api/v1/billing/subscriptions/cancel?org_id="
	for _, org := range []string{ownOrg, strangerOrg} {
		rec := recordEnforce(t)
		pdp := newTestPDP(t, allowAll)
		guard := NewGuard(routed(t, personaStore(personaAdmin), &fakeValidator{result: userClaims(nil)}), quiet())
		guard.SetEnforcer(servedOrgEnforcer(t, pdp, "billing"))
		mux := http.NewServeMux()
		mux.Handle("POST /api/v1/billing/subscriptions/cancel", guard.Wrap(Admin, echo))
		got := enforceRequest(impersonating(mux), http.MethodPost, path+org)
		if got.status != http.StatusForbidden || got.body != impersonationRefusalBody || len(pdp.calls()) != 0 ||
			!slices.Equal(rec.snapshot(), []string{"billing/impersonation_refused"}) {
			t.Errorf("impersonating write, org %s: %d %q, %d PDP calls, outcomes %v, want 403 %q, none and [billing/impersonation_refused]",
				org, got.status, got.body, len(pdp.calls()), rec.snapshot(), impersonationRefusalBody)
		}
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

// S2-parity pin (acp condition 2): an allow for the TOKEN organization never widens what the org scope admits. An admin of A
// who names, through X-Org-Id, an organization they are no member of is refused by the scope with 403, with and without an
// enforcer, whatever the decision point answers for A.
func TestServedOrgAnAllowForTheTokenOrganizationNeverWidensTheScope(t *testing.T) {
	for _, row := range servedOrgCensus {
		t.Run(row.pattern, func(t *testing.T) {
			setup := servedOrgSetup{who: personaAdmin, xOrgID: strangerOrg}
			legacy := servedOrgServe(t, row, setup, ownOrg)
			if legacy.status != http.StatusForbidden || !strings.Contains(legacy.body, "X-Org-Id not permitted for this user") {
				t.Fatalf("legacy: %d %q, want the scope's 403 X-Org-Id not permitted", legacy.status, legacy.body)
			}
			pdp := newTestPDP(t, allowAll)
			setup.enforcer = servedOrgEnforcer(t, pdp, servedOrgNamespace(t, row.pattern))
			got := servedOrgServe(t, row, setup, ownOrg)
			if got.status != legacy.status || got.body != legacy.body {
				t.Errorf("enforced: %d %q, want the legacy %d %q", got.status, got.body, legacy.status, legacy.body)
			}
		})
	}
}
