package server

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
)

// CHAOS-13945 (S2): the X-DH-Internal-* header carrier names the principal behind acr's credential in
// X-DH-Internal-Principal-Id; with a header cell on, query-api asks the PDP with it as the principal_reference, for the
// header org. On the MCP listener and on the internal listener (/query, /query/run-operation) alike.

func newWirePrincipal(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return "prn_" + base64.RawURLEncoding.EncodeToString(raw)
}

// qmHeadersWith is the four carrier headers for org (role member, no elevated flag) plus the principal header values.
func qmHeadersWith(org string, principals ...string) qmCarrier {
	return func(_ *qmPlane, r *http.Request) {
		for key, values := range mcpHeaders(org, "member", "false", "false") {
			r.Header[key] = values
		}
		for _, principal := range principals {
			r.Header.Add(internalidentity.HeaderPrincipalID, principal)
		}
	}
}

// mcpRunWith sends the MCP hotspots document (metrics.read) for org with carrier's headers.
func (p *qmPlane) mcpRunWith(t *testing.T, org string, carrier qmCarrier) enforceRun {
	t.Helper()
	before := p.mcpCH.calls.Load()
	header := http.Header{}
	request, _ := http.NewRequest(http.MethodPost, "/query", nil)
	request.Header = header
	carrier(p, request)
	recorder := mcpDo(p.mcp, http.MethodPost, request.Header, mcpBody(t, mcpHotspotsQuery, mcpHotspotsVariables(org)))
	p.gate.Wait()
	return enforceRun{code: recorder.Code, body: recorder.Body.String(), statements: int(p.mcpCH.calls.Load() - before)}
}

// headerCarrierRuns are the three header-carrier routes for one carrier: /query and run-operation on the internal
// listener (enforceDocAnalytics, analytics.read) and the MCP listener (hotspots, metrics.read).
func (p *qmPlane) headerCarrierRuns(t *testing.T, org string, carrier qmCarrier) map[string]enforceRun {
	t.Helper()
	return map[string]enforceRun{
		"/query":         p.queryRun(t, "/query", enforceDocAnalytics, carrier),
		runOperationPath: p.queryRun(t, runOperationPath, enforceDocAnalytics, carrier),
		"mcp":            p.mcpRunWith(t, org, carrier),
	}
}

// RC1: a header cell on, the principal named: the PDP is asked once per route with that principal and the header org,
// and an allow is served.
func TestHeaderCarrierWithPrincipalIsDecided(t *testing.T) {
	srv := pdptest.New(t)
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "", "analytics,metrics"), enforceDocAnalytics)
	principal := newWirePrincipal(t)

	for route, run := range p.headerCarrierRuns(t, ecOrg.String(), qmHeadersWith(ecOrg.String(), principal)) {
		if run.code != http.StatusOK || strings.Contains(run.body, "authz_enforce") {
			t.Errorf("%s: %d %s, want the allowed answer", route, run.code, run.body)
		}
	}
	got := asks(srv)
	if len(got) != 3 {
		t.Fatalf("the PDP was asked %d times, want 3 (one per route): %+v", len(got), got)
	}
	for _, ask := range got {
		if ask.PrincipalReference != principal || ask.OrganizationID != strings.ToLower(ecOrg.String()) {
			t.Errorf("ask %+v, want principal_reference %s for the header org %s", ask, principal, ecOrg)
		}
	}
}

// RC2: a deny refuses with 403 and reads nothing.
func TestHeaderCarrierPrincipalDenyIsForbiddenAndReadsNothing(t *testing.T) {
	srv := pdptest.New(t)
	srv.Deny("no_membership")
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "", "analytics,metrics"), enforceDocAnalytics)

	for route, run := range p.headerCarrierRuns(t, ecOrg.String(), qmHeadersWith(ecOrg.String(), newWirePrincipal(t))) {
		if route == "mcp" {
			if run.code != http.StatusForbidden || run.statements != 0 {
				t.Errorf("mcp: %d %s with %d statements, want 403 and nothing read", run.code, run.body, run.statements)
			}
			continue
		}
		if run.statements != 0 || run.roots != 0 || !strings.Contains(run.body, "FORBIDDEN") {
			t.Errorf("%s: %d %s with %d statements and %d roots, want the FORBIDDEN refusal and nothing read", route, run.code, run.body, run.statements, run.roots)
		}
	}
}

// RC3: no principal named, a header cell on: today's refusal, no ask.
func TestHeaderCarrierWithoutPrincipalStaysRefused(t *testing.T) {
	srv := pdptest.New(t)
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "", "analytics,metrics"), enforceDocAnalytics)

	for route, run := range p.headerCarrierRuns(t, ecOrg.String(), qmHeadersWith(ecOrg.String())) {
		if run.code != http.StatusServiceUnavailable || !strings.Contains(run.body, string(pdp.ReasonNoPrincipal)) {
			t.Errorf("%s: %d %s, want 503 %s", route, run.code, run.body, pdp.ReasonNoPrincipal)
		}
	}
	if n := len(asks(srv)); n != 0 {
		t.Fatalf("the PDP was asked %d times for a carrier with no principal", n)
	}
}

// RC4: the principal named, every header cell off (the roll steps before the flip): today's answer, no ask.
func TestHeaderCarrierPrincipalCellOffAsksNothing(t *testing.T) {
	srv := pdptest.New(t)
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "analytics,metrics,git,work_items,integrations,reports,org", ""), enforceDocAnalytics)
	principal := newWirePrincipal(t)

	with := p.headerCarrierRuns(t, ecOrg.String(), qmHeadersWith(ecOrg.String(), principal))
	without := p.headerCarrierRuns(t, ecOrg.String(), qmHeadersWith(ecOrg.String()))
	for route, run := range with {
		if run.code != without[route].code || run.body != without[route].body || run.statements != without[route].statements {
			t.Errorf("%s: with the principal %d %s (%d statements), without %d %s (%d); want the same answer",
				route, run.code, run.body, run.statements, without[route].code, without[route].body, without[route].statements)
		}
		if run.code != http.StatusOK {
			t.Errorf("%s: %d %s, want today's served answer", route, run.code, run.body)
		}
	}
	if n := len(asks(srv)); n != 0 {
		t.Fatalf("the PDP was asked %d times with every header cell off", n)
	}
}

// RC5: a malformed principal header, two of them, or one beside an Authorization header is refused 401 before any read
// and any ask, on every route.
func TestPrincipalHeaderMalformedDuplicateOrWithBearerIsRefused(t *testing.T) {
	principal := newWirePrincipal(t)
	for name, carrier := range map[string]qmCarrier{
		"empty":        qmHeadersWith(ecOrg.String(), ""),
		"not prn_":     qmHeadersWith(ecOrg.String(), "usr_"+strings.TrimPrefix(principal, "prn_")),
		"not 16 bytes": qmHeadersWith(ecOrg.String(), principal+"AA"),
		"padded":       qmHeadersWith(ecOrg.String(), " "+principal),
		"two values":   qmHeadersWith(ecOrg.String(), principal, newWirePrincipal(t)),
		"with a bearer": func(p *qmPlane, r *http.Request) {
			qmHeadersWith(ecOrg.String(), principal)(p, r)
			r.Header.Set("Authorization", "Bearer "+p.user)
		},
	} {
		t.Run(name, func(t *testing.T) {
			srv := pdptest.New(t)
			p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "", "analytics,metrics"), enforceDocAnalytics)
			for route, run := range p.headerCarrierRuns(t, ecOrg.String(), carrier) {
				if run.code != http.StatusUnauthorized || run.statements != 0 {
					t.Errorf("%s: %d %s with %d statements, want 401 and nothing read", route, run.code, run.body, run.statements)
				}
			}
			if n := len(asks(srv)); n != 0 {
				t.Fatalf("the PDP was asked %d times", n)
			}
		})
	}
}

// RC5b: the principal header without the four carrier headers is a partial carrier: 401, no read, no ask.
func TestPrincipalHeaderAloneIsAPartialCarrier(t *testing.T) {
	srv := pdptest.New(t)
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "", "analytics,metrics"), enforceDocAnalytics)
	alone := func(_ *qmPlane, r *http.Request) {
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set(internalidentity.HeaderPrincipalID, newWirePrincipal(t))
	}

	for route, run := range p.headerCarrierRuns(t, ecOrg.String(), alone) {
		if run.code != http.StatusUnauthorized || run.statements != 0 {
			t.Errorf("%s: %d %s with %d statements, want 401 and nothing read", route, run.code, run.body, run.statements)
		}
	}
	if n := len(asks(srv)); n != 0 {
		t.Fatalf("the PDP was asked %d times", n)
	}
}

// RC6: the header org is what the PDP is asked about, never another org: a principal named for a forged org is judged
// in that org, and a deny there is a 403 with nothing read.
func TestForgedOrgWithRealPrincipalIsJudgedInTheHeaderOrg(t *testing.T) {
	srv := pdptest.New(t)
	srv.Deny("no_membership")
	p := newQMPlane(t, qmEnv(t, srv, pdp.ModeEnforce, "", "analytics,metrics"), enforceDocAnalytics)
	forged := "0f0f0f0f-0f0f-4f0f-8f0f-0f0f0f0f0f0f"

	run := p.mcpRunWith(t, forged, qmHeadersWith(forged, newWirePrincipal(t)))

	if run.code != http.StatusForbidden || run.statements != 0 {
		t.Fatalf("mcp, forged org: %d %s with %d statements, want 403 and nothing read", run.code, run.body, run.statements)
	}
	for _, ask := range asks(srv) {
		if ask.OrganizationID != forged {
			t.Fatalf("ask %+v, want the header org %s", ask, forged)
		}
	}
}
