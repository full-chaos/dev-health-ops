package policy

import (
	"bytes"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
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

// The outage row of S2 (F-9): a request that serves another organization than the token's is now asked about the token
// organization, so with the decision point dead it is the 503 of every enforced request, where before S2 it kept the
// legacy answer. A token with no organization still keeps the legacy answer in an outage (S1's class).
func TestServedOrgAnOutageIsTheEnforced503ForAnotherOrgAndTheLegacyAnswerForNoTokenOrg(t *testing.T) {
	row := servedOrgCensusRow{"GET /api/v1/admin/orgs/{org_id}/members", Admin, false}
	namespace := servedOrgNamespace(t, row.pattern)
	pdp := newTestPDP(t, allowAll)
	enforcer := servedOrgEnforcer(t, pdp, namespace)
	pdp.srv.Close()
	got := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, adminOf: []string{memberOrg}, enforcer: enforcer}, memberOrg)
	if got.status != http.StatusServiceUnavailable {
		t.Errorf("another org, PDP dead: %d %q, want the enforced 503", got.status, got.body)
	}
	noOrg := userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
	legacy := servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser, claims: noOrg}, ownOrg)
	got = servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser, claims: noOrg, enforcer: enforcer}, ownOrg)
	if got.status != legacy.status || got.body != legacy.body {
		t.Errorf("no token org, PDP dead: %d %q, want the legacy %d %q", got.status, got.body, legacy.status, legacy.body)
	}
}

// S2-2 (acp condition 3): every decision of a request that serves another organization than the token's writes ONE line at the
// production level (Info) carrying the organization the decision point was asked about, the organization the request names and
// the outcome, on allow, deny and an outage alike; a request that names the token organization writes none.
func TestServedOrgEveryDecisionLogsBothOrganizationsAtInfo(t *testing.T) {
	row := servedOrgCensusRow{"GET /api/v1/admin/orgs/{org_id}/members", Admin, false}
	namespace := servedOrgNamespace(t, row.pattern)
	for _, tc := range []struct {
		mode, outcome string
		served        string
		lines         int
	}{
		{"allow", "allow", memberOrg, 1},
		{"deny", "deny", memberOrg, 1},
		{"dead", "error_deny", memberOrg, 1},
		{"allow", "", ownOrg, 0},
	} {
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))
		pdp := newTestPDP(t, allowAll)
		if tc.mode == "deny" {
			pdp = newTestPDP(t, denyWith("role_lacks_action"))
		}
		enforcer, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: servedOrgRows(t), Switch: onFor(namespace), Timeout: time.Second, Logger: logger})
		if err != nil {
			t.Fatal(err)
		}
		if tc.mode == "dead" {
			pdp.srv.Close()
		}
		servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, adminOf: []string{memberOrg}, enforcer: enforcer}, tc.served)
		var got []string
		for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			if strings.Contains(line, "decided on the token organization") {
				got = append(got, line)
			}
		}
		if len(got) != tc.lines {
			t.Errorf("%s serving %s: %d decision lines %v, want %d", tc.mode, tc.served, len(got), got, tc.lines)
			continue
		}
		if tc.lines == 1 {
			for _, want := range []string{"level=INFO", "route=\"" + row.pattern + "\"", "namespace=" + namespace, "decided_org=" + ownOrg,
				"served_org=" + tc.served, "outcome=" + tc.outcome} {
				if !strings.Contains(got[0], want) {
					t.Errorf("%s: the line %q lacks %s", tc.mode, got[0], want)
				}
			}
		}
	}
}

// The organization a request names is request input: its line breaks are dropped before it is logged.
func TestServedOrgLogValueHasNoLineBreak(t *testing.T) {
	if got := logSafe("a\r\nforged=1\nb"); got != "aforged=1b" {
		t.Errorf("logSafe = %q, want %q", got, "aforged=1b")
	}
	if got := logSafe(memberOrg); got != memberOrg {
		t.Errorf("logSafe(%q) = %q, want it unchanged", memberOrg, got)
	}
}
