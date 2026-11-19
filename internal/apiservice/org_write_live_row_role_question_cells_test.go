package apiservice

import (
	"net/http"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// A caller whose live go-api impersonation row only the guard's own read finds (it holds no admin.impersonate, so no
// session context) on an enforced organization write row: the decision point is asked the question it asks with no row,
// first. A deny is the legacy refusal; an allow, no verdict or a failure is the session refusal, so a row the legacy
// guard admits never reaches a handler (D6833).

// orgWriteNamespaces turns on every namespace of an organization write row.
const orgWriteNamespaces = "acr,billing,integrations,members,org,settings,sso,teams"

// orgWriteRows is every go-api profile row whose resource kind is organization, that writes, and that the api listener
// mounts, with its routescan level.
func orgWriteRows(t *testing.T) []liveRowRow {
	t.Helper()
	mapping, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	regs := scanRegistrations(t, "../api", ".")
	var out []liveRowRow
	for _, m := range mountedRoutes(t) {
		key := m.Method + " " + m.Pattern
		row, ok := mapping[key]
		if m.Listener != "api" || !ok || row.ResourceType != policycatalog.ScopeOrganization || m.Method == http.MethodGet {
			continue
		}
		level := ""
		if reg, ok := registrationFor(regs, m); ok {
			level = reg.Level
		}
		if level == "" {
			level = dispatchedLevels[m.key()]
		}
		out = append(out, liveRowRow{key: key, level: level, action: row.Action, namespace: row.Namespace})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	if len(out) != 79 {
		t.Fatalf("%d mounted organization write rows, want the go-api profile's 79", len(out))
	}
	return out
}

func orgWriteServe(t *testing.T, rows []liveRowRow, class liveRowClass, on, live, superuser bool, mode string) liveRowServed {
	t.Helper()
	return liveRowServeWith(t, rows, orgWriteNamespaces, lifecycleRoutes, class, on, live, superuser, mode)
}

// orgWritePDPAdmits: the cell decision point allows an organization write to an owner or admin of the token org.
func orgWritePDPAdmits(class liveRowClass) bool {
	return class.orgRole == "owner" || class.orgRole == "admin"
}

// orgWriteGuardRefusal: the answer is a refusal of the legacy guard's role check.
func orgWriteGuardRefusal(answer string) bool {
	return answer == `403 {"detail":"Admin access required"}` || answer == `403 {"detail":"Organization context required"}`
}

const (
	orgWriteSSOCreate = "POST /api/v1/auth/sso/providers"
	orgWriteSSOPatch  = "PATCH /api/v1/auth/sso/providers/{provider_id}"
	orgWriteSSODelete = "DELETE /api/v1/auth/sso/providers/{provider_id}"
	orgWriteOrgsMe    = "PATCH /api/v1/orgs/me"
)

// orgWriteRigOnly names the cells whose cell-off answer here is the rig's, not prod's, each pinned on the prod path by
// its own cell: the SSO provider writes on the mount with no Providers client (the legacy route answers its 402/422
// before the handler's role check; prod mounts Providers and the guard's late checks,
// TestTheSSOProviderWritesAnswerADemotedLiveRowCallerTheLateCheckFirstOnTheProdMount), and PATCH /orgs/me, whose handler
// reads its membership before its own refusal (the rig's pool is closed: 500;
// TestOrganizationWriteLiveRowRoleQuestionRealPDPCallsAndAudit on Postgres). Both answer the no-row answer with a live row.
func orgWriteRigOnly(row liveRowRow, class liveRowClass) bool {
	if orgWritePDPAdmits(class) || !class.orgClaim || class.orgRole == "" {
		return false
	}
	switch row.key {
	case orgWriteSSOCreate, orgWriteSSOPatch, orgWriteSSODelete, orgWriteOrgsMe:
		return true
	}
	return false
}

// A caller whose platform role was removed while its legacy impersonation row is live gets the same answer with the
// organization cells on as with them off, on every organization write row and every caller class (product rule 1). D6833:
// a row the cell-off guard admits (an owner or admin, the handler reached) keeps the session refusal with the cells on.
func TestADemotedCallerWithALiveRowGetsTheCellOffAnswerOnEveryOrganizationWriteRow(t *testing.T) {
	rows := orgWriteRows(t)
	cells, diff, same, d6833, rig, offOn := 0, 0, 0, 0, 0, 0
	for _, class := range liveRowClasses {
		off := orgWriteServe(t, rows, class, false, true, false, "evaluate")
		on := orgWriteServe(t, rows, class, true, true, false, "evaluate")
		norow := orgWriteServe(t, rows, class, true, false, false, "evaluate")
		for _, row := range rows {
			cells++
			o, n := off.answer[row.key], on.answer[row.key]
			if o != n {
				offOn++
			}
			switch {
			case o == n:
				same++
			case orgWritePDPAdmits(class) && !orgWriteGuardRefusal(o):
				d6833++
				if n != "403 "+impersonationBody {
					diff++
					t.Errorf("D6833 %s | %s | off %s | on %s, want the session refusal", class.label, row.key, o, n)
				}
			case orgWriteRigOnly(row, class):
				rig++
				if n != norow.answer[row.key] || !strings.HasPrefix(n, "403 ") {
					diff++
					t.Errorf("RIG %s | %s | off %s | on %s | norow %s, want the no-row refusal", class.label, row.key, o, n, norow.answer[row.key])
				}
			default:
				diff++
				t.Errorf("DIFF %s | %s | level %s | off %s | on %s | norow %s", class.label, row.key, row.level, o, n, norow.answer[row.key])
			}
		}
	}
	if d6833 != 136 {
		t.Errorf("%d D6833 cells, want 136 (owner and admin, 68 rows each, where the cell-off guard admits)", d6833)
	}
	if rig != 8 {
		t.Errorf("%d rig-only cells, want 8 (member and viewer: 3 SSO provider writes, PATCH /orgs/me)", rig)
	}
	t.Logf("ORGWRITE cells=%d diff=%d same=%d d6833=%d rig=%d off!=on %d", cells, diff, same, d6833, rig, offOn)
}

// orgWriteWantCalls is the guard's question of the caller's own bearer on row with no live row (decideOnce): with a token
// organization the row's action in it; with none, admin.orgs on the platform, and nothing on an AdminOrg row.
func orgWriteWantCalls(row liveRowRow, class liveRowClass) []pep.Request {
	if !class.orgClaim {
		if row.level == "AdminOrg" {
			return nil
		}
		return []pep.Request{{Action: policy.ActionAdminOrgs, ResourceType: policycatalog.ScopePlatform, ResourceKey: "platform"}}
	}
	org := strings.ToLower(probeOrg.String())
	return []pep.Request{{Action: row.action, ResourceType: policycatalog.ScopeOrganization, ResourceKey: org, OrganizationID: org}}
}

func orgWriteBare(calls []pep.Request) []pep.Request {
	out := slices.Clone(calls)
	for i := range out {
		out[i].PrincipalReference, out[i].RequestID = "", ""
	}
	return out
}

// The decision point requests of one demoted live-row request on an organization write row: exactly the question the
// guard asks with no row, once (one request; none for a token with no organization on an AdminOrg row), and none on a
// row refused before the guard. Each high-risk request writes one auth.authorize.high_risk_decided event (R1): 333 of the
// 337 requests (the 4 others ask integrations.read, of the discover route).
func TestAnOrganizationWriteLiveRowAsksExactlyTheNoRowQuestion(t *testing.T) {
	rows := orgWriteRows(t)
	asked, highRisk, unasked := 0, 0, 0
	for _, class := range liveRowClasses {
		on := orgWriteServe(t, rows, class, true, true, false, "evaluate")
		norow := orgWriteServe(t, rows, class, true, false, false, "evaluate")
		for _, row := range rows {
			got := orgWriteBare(on.calls[row.key])
			want := orgWriteWantCalls(row, class)
			if len(got) == 0 && (len(norow.calls[row.key]) == 0 || row.namespace == "billing" && on.answer[row.key] == "403 "+impersonationBody) {
				// Refused before the guard with or without a row (membership, body, method), AdminOrg with no
				// organization, or a billing write: RefuseImpersonation refuses a live row before the guard.
				unasked++
				continue
			}
			if !slices.Equal(got, want) {
				t.Errorf("%s | %s: decision point requests %+v, want %+v", class.label, row.key, got, want)
			}
			if first := orgWriteBare(norow.calls[row.key]); len(first) < len(want) || !slices.Equal(first[:len(want)], want) {
				t.Errorf("%s | %s: the no-row request asked %+v first, want %+v", class.label, row.key, first, want)
			}
			for _, request := range got {
				action, err := policycatalog.Default().Action(request.Action)
				if err != nil {
					t.Fatal(err)
				}
				if action.RiskClass == policycatalog.RiskHigh {
					highRisk++
				}
			}
			asked += len(got)
		}
	}
	if asked != 337 || highRisk != 333 {
		t.Errorf("asked %d requests, %d high risk; want 337 and 333", asked, highRisk)
	}
	t.Logf("ASKED requests=%d high_risk=%d unasked_cells=%d", asked, highRisk, unasked)
}

// A caller that holds admin.impersonate (the session context) with a live row is refused on every organization write
// row unasked, as before: its answer as the target organization is not the legacy one (D6833). Logged for the compare
// with main.
func TestAHolderWithALiveRowIsRefusedUnaskedOnEveryOrganizationWriteRow(t *testing.T) {
	rows := orgWriteRows(t)
	refused := 0
	for _, class := range liveRowClasses {
		on := orgWriteServe(t, rows, class, true, true, true, "evaluate")
		for _, row := range rows {
			t.Logf("HOLDER %s | %s | %s | calls %d", class.label, row.key, on.answer[row.key], len(on.calls[row.key]))
			if len(on.calls[row.key]) != 0 {
				t.Errorf("holder %s | %s: %d decision point requests, want none", class.label, row.key, len(on.calls[row.key]))
			}
			if on.answer[row.key] == "403 "+impersonationBody {
				refused++
			}
		}
	}
	if refused == 0 {
		t.Error("no holder cell got the session refusal")
	}
	t.Logf("HOLDER session refusals=%d", refused)
}

// A decision point that fails leaves a live-row caller on an organization write row refused with a 403: never a 503,
// never a handler. Logged for the compare with main's answers.
func TestAnOrganizationWriteLiveRowQuestionFailureIsTheSessionRefusal(t *testing.T) {
	rows := orgWriteRows(t)
	refused := 0
	for _, class := range liveRowClasses {
		for _, c := range []struct {
			label           string
			live, superuser bool
		}{{"demoted-live", true, false}, {"demoted-no-row", false, false}, {"holder-live", true, true}} {
			on := orgWriteServe(t, rows, class, true, c.live, c.superuser, "fail")
			for _, row := range rows {
				answer := on.answer[row.key]
				t.Logf("FAIL %s | %s | %s | %s", c.label, class.label, row.key, answer)
				if !c.live {
					continue
				}
				if !strings.HasPrefix(answer, "403 ") && !strings.HasPrefix(answer, "413 ") && !strings.HasPrefix(answer, "405 ") {
					t.Errorf("%s | %s | %s: %s, want a refusal (the decision point failed on a live row)", c.label, class.label, row.key, answer)
				}
				if answer == "403 "+impersonationBody && len(on.calls[row.key]) > 0 {
					refused++
				}
			}
		}
	}
	t.Logf("FAILCLOSED session refusals after a failed question=%d", refused)
}

// The SSO provider writes on the mount prod builds: go-api owns no providers client only with AUTH_SSO_PROVIDERS_OWNED off;
// prod sets it on (service.go passes deps.SSOProviders as Providers), so each write is WrapLate / BodyFirstLate with its
// 422 and org-aware 402 as the guard's late checks. A demoted member or viewer with a live row gets the cell-off answer,
// the late check's (no organization entitled to sso_saml: 402; a body the route refuses: 422), with the sso cell on, and
// no provider call is made.
func TestTheSSOProviderWritesAnswerADemotedLiveRowCallerTheLateCheckFirstOnTheProdMount(t *testing.T) {
	withSSOLicence(t, false)
	client := ssoProvidersClient(AuthProxyConfig{}, true, nil, quietLogger())
	if client == nil {
		t.Fatal("an owned process builds no providers client: the provider routes would mount without their late checks")
	}
	if ssoProvidersClient(AuthProxyConfig{}, false, nil, quietLogger()) != nil {
		t.Fatal("a process that does not own the providers builds a client")
	}
	recorder := &ssoCellClient{}
	routes := func(t *testing.T, deps Deps) []httpapi.Route {
		return ssoRoutesWith(recorder, &orgEntitlement{granted: map[string]bool{}}, quietLogger())(t, deps)
	}
	cells, late := 0, 0
	for _, class := range []liveRowClass{liveRowClasses[3], liveRowClasses[4]} {
		off, offToken, _ := liveRowRig(t, "", routes, class, false, true, false)
		on, onToken, pdp := liveRowRig(t, "sso", routes, class, true, true, false)
		for _, key := range []string{orgWriteSSOCreate, orgWriteSSOPatch, orgWriteSSODelete} {
			for _, override := range []string{"", `{"name":123}`} {
				if override != "" && key == orgWriteSSODelete {
					continue
				}
				pdp.reset("evaluate")
				o := ssoDoWith(off, key, offToken, "", override)
				n := ssoDoWith(on, key, onToken, "", override)
				cells++
				t.Logf("SSOLATE %s | %s | body %q | off %d %s | on %d %s | calls %d", class.label, key, override, o.status,
					strings.TrimSpace(o.body), n.status, strings.TrimSpace(n.body), len(pdp.calls()))
				if o.status != n.status || o.body != n.body {
					t.Errorf("%s | %s | body %q: on %d %s, off %d %s", class.label, key, override, n.status, n.body, o.status, o.body)
				}
				if n.status == http.StatusPaymentRequired || n.status == http.StatusUnprocessableEntity {
					late++
				}
			}
		}
	}
	if late != cells {
		t.Errorf("%d of %d cells answered a late check (402/422), want all", late, cells)
	}
	if len(recorder.calls) != 0 {
		t.Errorf("provider calls %v, want none", recorder.calls)
	}
	t.Logf("SSOLATE cells=%d late=%d", cells, late)
}
