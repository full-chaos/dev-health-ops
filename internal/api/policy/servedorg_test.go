package policy

import (
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// servedOrgCensusRow is one go-api row with an action whose handler serves an organization a path or query
// parameter names, not the caller's session organization.
type servedOrgCensusRow struct {
	pattern string
	level   Authz
	query   bool
}

// servedOrgCensus is every go-api action row whose handler serves another organization than the session's: the 8
// organization_from_path rows and the 9 billing rows whose handler reads an org_id query parameter (resolveOrg).
var servedOrgCensus = []servedOrgCensusRow{
	{"GET /api/v1/admin/orgs/{org_id}/members", Admin, false},
	{"POST /api/v1/admin/orgs/{org_id}/members", Admin, false},
	{"POST /api/v1/admin/orgs/{org_id}/invites", Admin, false},
	{"PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}", Admin, false},
	{"DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}", Admin, false},
	{"POST /api/v1/admin/orgs/{org_id}/transfer-ownership", Admin, false},
	{"GET /api/v1/billing/entitlements/{org_id}", Authenticated, false},
	{"GET /api/v1/licensing/entitlements/{org_id}", Authenticated, false},
	{"GET /api/v1/billing/invoices", Authenticated, true},
	{"GET /api/v1/billing/invoices/{invoice_id}", Authenticated, true},
	{"POST /api/v1/billing/invoices/{invoice_id}/void", Admin, true},
	{"GET /api/v1/billing/subscriptions", Authenticated, true},
	{"GET /api/v1/billing/subscriptions/list", Authenticated, true},
	{"GET /api/v1/billing/subscriptions/history", Authenticated, true},
	{"POST /api/v1/billing/subscriptions/change-plan", Admin, true},
	{"POST /api/v1/billing/subscriptions/cancel", Admin, true},
	{"POST /api/v1/billing/subscriptions/reactivate", Admin, true},
}

// servedOrgForms are the spellings uuid parsing accepts for one organization id.
func servedOrgForms(org string) map[string]string {
	return map[string]string{
		"canonical":  org,
		"upper":      strings.ToUpper(org),
		"braces":     "{" + org + "}",
		"urn":        "urn:uuid:" + org,
		"no dashes":  strings.ReplaceAll(org, "-", ""),
		"urn upper":  "URN:UUID:" + strings.ToUpper(org),
		"braces 32":  "{" + strings.ReplaceAll(org, "-", "") + "}",
		"dashes odd": strings.ReplaceAll(org, "-", "")[:4] + "-" + strings.ReplaceAll(org, "-", "")[4:],
	}
}

// servedOrgRequestPath routes to row with served as the organization it names; "" names none (a query row only).
func servedOrgRequestPath(row servedOrgCensusRow, served string) string {
	_, route, _ := strings.Cut(row.pattern, " ")
	if !row.query {
		route = strings.ReplaceAll(route, "{org_id}", url.PathEscape(served))
	}
	path := profilePath(route)
	if row.query && served != "" {
		path += "?org_id=" + url.QueryEscape(served)
	}
	return path
}

func servedOrgRows(t *testing.T) map[string]ShadowRow {
	t.Helper()
	rows, err := ParseShadowRows(authv1.GoAPIEndpointProfiles, "dev-health-ops-api")
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

type servedOrgSetup struct {
	who      persona
	enforcer *Enforcer
	shadow   *Shadow
	// xOrgID, when set, is sent as X-Org-Id through the org scope.
	xOrgID string
}

func servedOrgServe(t *testing.T, row servedOrgCensusRow, setup servedOrgSetup, served string) response {
	t.Helper()
	auth := routed(t, personaStore(setup.who), &fakeValidator{result: userClaims(nil)})
	guard := NewGuard(auth, quiet())
	if setup.enforcer != nil {
		guard.SetEnforcer(setup.enforcer)
	}
	if setup.shadow != nil {
		guard.SetShadow(setup.shadow)
	}
	mux := http.NewServeMux()
	mux.Handle(row.pattern, guard.Wrap(row.level, echo))
	var handler http.Handler = mux
	headers := map[string]string{"Authorization": "Bearer " + shadowBearer}
	if setup.xOrgID != "" {
		scope := NewScope(auth, quiet())
		handler = scope.Principal(scope.OrgScope(mux))
		headers["X-Org-Id"] = setup.xOrgID
	}
	method, _, _ := strings.Cut(row.pattern, " ")
	got := serve(handler, method, servedOrgRequestPath(row, served), headers)
	if setup.shadow != nil {
		setup.shadow.wg.Wait()
	}
	return got
}

func servedOrgEnforcer(t *testing.T, pdp *testPDP, namespaces ...string) *Enforcer {
	t.Helper()
	enf, err := NewEnforcer(EnforceConfig{Decider: pdp.client(t), Rows: servedOrgRows(t), Switch: onFor(namespaces...), Timeout: time.Second, Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	return enf
}

func servedOrgDenyBody(level Authz) string {
	raw, _ := json.Marshal(map[string]string{"detail": enforceDenyDetail(level)})
	return string(raw)
}

func servedOrgNamespace(t *testing.T, pattern string) string {
	t.Helper()
	row, ok := servedOrgRows(t)[pattern]
	if !ok {
		t.Fatalf("%s has no action row", pattern)
	}
	return row.Namespace
}

// The census is every go-api row with an action whose resolver reads the organization from the path, plus the
// billing org_id query rows; each is an organization row.
func TestServedOrgCensusCoversEveryPathOrgRowWithAnAction(t *testing.T) {
	var document struct {
		Rows []struct {
			Service, Method, Route, Action, Classification string
			Resolver                                       string `json:"resource_resolver"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	census := map[string]bool{}
	for _, row := range servedOrgCensus {
		census[row.pattern] = true
	}
	pathRows := 0
	for _, entry := range document.Rows {
		if entry.Service != "dev-health-ops-api" || entry.Action == "" || entry.Classification != "protected" ||
			!strings.HasPrefix(entry.Resolver, "organization_from_path") {
			continue
		}
		pathRows++
		if !census[entry.Method+" "+entry.Route] {
			t.Errorf("%s %s reads its organization from the path and is not in the census", entry.Method, entry.Route)
		}
	}
	if pathRows != 8 {
		t.Errorf("%d organization_from_path rows with an action, want 8", pathRows)
	}
	rows := servedOrgRows(t)
	for _, row := range servedOrgCensus {
		shadowRow, ok := rows[row.pattern]
		if !ok || shadowRow.ResourceType != policycatalog.ScopeOrganization {
			t.Errorf("%s: row %+v, want an organization action row", row.pattern, shadowRow)
		}
	}
}

// Cells per census row with its namespace switched on. Token organization A (ownOrg).
func TestServedOrgEnforcedCells(t *testing.T) {
	for _, row := range servedOrgCensus {
		t.Run(row.pattern, func(t *testing.T) {
			namespace := servedOrgNamespace(t, row.pattern)
			t.Run("same org is served and judged on A", func(t *testing.T) {
				for form, served := range servedOrgForms(ownOrg) {
					rec := recordEnforce(t)
					pdp := newTestPDP(t, allowAll)
					got := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, enforcer: servedOrgEnforcer(t, pdp, namespace)}, served)
					calls := pdp.calls()
					if got.status != http.StatusOK || len(calls) != 1 || calls[0].Org != ownOrg {
						t.Errorf("%s: %d %q, PDP saw %+v, want 200 and one question on %s", form, got.status, got.body, calls, ownOrg)
					}
					if !slices.Equal(rec.snapshot(), []string{namespace + "/allow"}) {
						t.Errorf("%s: outcomes %v, want [%s/allow]", form, rec.snapshot(), namespace)
					}
				}
			})
			if row.query {
				t.Run("no org_id is the session org", func(t *testing.T) {
					pdp := newTestPDP(t, allowAll)
					got := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, enforcer: servedOrgEnforcer(t, pdp, namespace)}, "")
					if calls := pdp.calls(); got.status != http.StatusOK || len(calls) != 1 || calls[0].Org != ownOrg {
						t.Errorf("%d %q, PDP saw %+v, want 200 and one question on %s", got.status, got.body, calls, ownOrg)
					}
				})
				t.Run("a repeated org_id serves the last one", func(t *testing.T) {
					for _, tc := range []struct {
						query  string
						status int
					}{
						{"org_id=" + ownOrg + "&org_id=" + strangerOrg, http.StatusForbidden},
						{"org_id=" + strangerOrg + "&org_id=" + ownOrg, http.StatusOK},
						{"org_id=" + ownOrg + "&org_id=", http.StatusOK},
					} {
						auth := routed(t, personaStore(personaAdmin), &fakeValidator{result: userClaims(nil)})
						guard := NewGuard(auth, quiet())
						pdp := newTestPDP(t, allowAll)
						guard.SetEnforcer(servedOrgEnforcer(t, pdp, namespace))
						mux := http.NewServeMux()
						mux.Handle(row.pattern, guard.Wrap(row.level, echo))
						method, route, _ := strings.Cut(row.pattern, " ")
						got := enforceRequest(mux, method, profilePath(route)+"?"+tc.query)
						if tc.status == http.StatusOK && strings.HasSuffix(tc.query, "=") {
							if got.status == http.StatusForbidden {
								t.Errorf("%s: %d %q, want no seam refusal (an empty last value does not parse)", tc.query, got.status, got.body)
							}
							continue
						}
						if got.status != tc.status {
							t.Errorf("%s: %d %q, want %d", tc.query, got.status, got.body, tc.status)
						}
					}
				})
			}
			t.Run("another org is refused by the seam with no PDP call", func(t *testing.T) {
				for _, other := range []string{strangerOrg, memberOrg} {
					for form, served := range servedOrgForms(other) {
						rec := recordEnforce(t)
						pdp := newTestPDP(t, allowAll)
						got := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, enforcer: servedOrgEnforcer(t, pdp, namespace)}, served)
						if got.status != http.StatusForbidden || got.body != servedOrgDenyBody(row.level) {
							t.Errorf("%s %s: %d %q, want 403 %s", other, form, got.status, got.body, servedOrgDenyBody(row.level))
						}
						if n := len(pdp.calls()); n != 0 {
							t.Errorf("%s %s: %d PDP calls, want 0", other, form, n)
						}
						if !slices.Equal(rec.snapshot(), []string{namespace + "/deny"}) {
							t.Errorf("%s %s: outcomes %v, want [%s/deny]", other, form, rec.snapshot(), namespace)
						}
					}
				}
			})
			t.Run("a slug keeps the legacy answer with no PDP call", func(t *testing.T) {
				for _, slug := range []string{"acme", "not-a-uuid", strings.Repeat("g", 32)} {
					legacy := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin}, slug)
					rec := recordEnforce(t)
					pdp := newTestPDP(t, allowAll)
					got := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, enforcer: servedOrgEnforcer(t, pdp, namespace)}, slug)
					if got.status != legacy.status || got.body != legacy.body {
						t.Errorf("%s: %d %q, want the legacy %d %q", slug, got.status, got.body, legacy.status, legacy.body)
					}
					if n := len(pdp.calls()); n != 0 || !slices.Equal(rec.unjudgedSnapshot(), []string{row.pattern + "/no_org"}) {
						t.Errorf("%s: %d PDP calls, unjudged %v, want 0 and [%s/no_org]", slug, n, rec.unjudgedSnapshot(), row.pattern)
					}
				}
			})
			t.Run("X-Org-Id selects only: the judged org stays A", func(t *testing.T) {
				pdp := newTestPDP(t, allowAll)
				got := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, enforcer: servedOrgEnforcer(t, pdp, namespace), xOrgID: memberOrg}, ownOrg)
				if calls := pdp.calls(); got.status != http.StatusOK || len(calls) != 1 || calls[0].Org != ownOrg {
					t.Errorf("X-Org-Id B, served A: %d %q, PDP saw %+v, want 200 and one question on A", got.status, got.body, calls)
				}
				for _, xOrg := range []string{memberOrg, strings.ToUpper(memberOrg), " " + memberOrg + " "} {
					pdp = newTestPDP(t, allowAll)
					got = servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, enforcer: servedOrgEnforcer(t, pdp, namespace), xOrgID: xOrg}, memberOrg)
					if got.status != http.StatusForbidden || len(pdp.calls()) != 0 {
						t.Errorf("X-Org-Id %q, served B: %d %q, %d PDP calls, want 403 and none", xOrg, got.status, got.body, len(pdp.calls()))
					}
				}
			})
			t.Run("superuser", func(t *testing.T) {
				pdp := newTestPDP(t, allowAll)
				got := servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser, enforcer: servedOrgEnforcer(t, pdp, namespace)}, ownOrg)
				if calls := pdp.calls(); got.status != http.StatusOK || len(calls) != 1 || calls[0].Org != ownOrg {
					t.Errorf("own org: %d %q, PDP saw %+v, want 200 and one question on A", got.status, got.body, calls)
				}
				legacy := servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser}, strangerOrg)
				if legacy.status != http.StatusOK {
					t.Errorf("legacy guard, superuser, another org: %d %q, want 200 (the guard passes a superuser)", legacy.status, legacy.body)
				}
				pdp = newTestPDP(t, allowAll)
				got = servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser, enforcer: servedOrgEnforcer(t, pdp, namespace)}, strangerOrg)
				if got.status != http.StatusForbidden || got.body != servedOrgDenyBody(row.level) || len(pdp.calls()) != 0 {
					t.Errorf("enforced, superuser, another org: %d %q, %d PDP calls, want 403 and none", got.status, got.body, len(pdp.calls()))
				}
			})
		})
	}
}

// With the row's namespace off (another namespace on, or none) and no shadow, every cell answers as a guard with no
// enforcer: status, body and headers, and nothing is asked.
func TestServedOrgSwitchOffIsTheLegacyAnswer(t *testing.T) {
	served := []string{"", "acme"}
	for _, org := range []string{ownOrg, memberOrg, strangerOrg} {
		for _, form := range servedOrgForms(org) {
			served = append(served, form)
		}
	}
	for _, row := range servedOrgCensus {
		t.Run(row.pattern, func(t *testing.T) {
			namespace := servedOrgNamespace(t, row.pattern)
			others := []string{}
			for _, candidate := range []string{"teams", "members", "billing", "license", "admin"} {
				if candidate != namespace {
					others = append(others, candidate)
				}
			}
			for _, who := range []persona{personaAdmin, personaMember, personaSuperuser} {
				for _, value := range served {
					if value == "" && !row.query {
						continue
					}
					for _, xOrg := range []string{"", memberOrg} {
						legacy := servedOrgServe(t, row, servedOrgSetup{who: who, xOrgID: xOrg}, value)
						pdp := newTestPDP(t, allowAll)
						for _, enf := range []*Enforcer{servedOrgEnforcer(t, pdp), servedOrgEnforcer(t, pdp, others...)} {
							got := servedOrgServe(t, row, servedOrgSetup{who: who, enforcer: enf, xOrgID: xOrg}, value)
							if got.status != legacy.status || got.body != legacy.body || !headersEqual(got.header, legacy.header) {
								t.Errorf("%+v %q X-Org-Id %q: %d %q %v, want the legacy %d %q %v", who, value, xOrg, got.status, got.body, got.header,
									legacy.status, legacy.body, legacy.header)
							}
						}
						if n := len(pdp.calls()); n != 0 {
							t.Errorf("%+v %q: %d PDP calls with the namespace off, want 0", who, value, n)
						}
					}
				}
			}
		})
	}
}

func headersEqual(a, b http.Header) bool {
	if len(a) != len(b) {
		return false
	}
	for key, values := range a {
		if !slices.Equal(values, b[key]) {
			return false
		}
	}
	return true
}

// Shadow on, enforcement off: the answer is the legacy one; a served org that is not A is recorded as a mismatch
// of class intentional without a PDP call; a slug is skipped; A is asked and matched.
func TestServedOrgShadowRecordsTheMismatchAndNeverChangesTheAnswer(t *testing.T) {
	for _, row := range servedOrgCensus {
		t.Run(row.pattern, func(t *testing.T) {
			namespace := servedOrgNamespace(t, row.pattern)
			cases := []struct {
				name, served, result, mismatch string
				calls                          int
			}{
				{"same org", ownOrg, "match", "", 1},
				{"same org upper", strings.ToUpper(ownOrg), "match", "", 1},
				{"another org", strangerOrg, "mismatch", namespace + "/admin/intentional", 0},
				{"another org braces", "{" + memberOrg + "}", "mismatch", namespace + "/admin/intentional", 0},
				{"slug", "acme", "skipped", "", 0},
			}
			for _, tc := range cases {
				legacy := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin}, tc.served)
				rec := recordShadow(t)
				pdp := newTestPDP(t, allowAll)
				sh, _ := newShadowFor(t, pdp.client(t), func(c *ShadowConfig) { c.Rows = servedOrgRows(t); c.Classes = nil })
				got := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, shadow: sh}, tc.served)
				if got.status != legacy.status || got.body != legacy.body || !headersEqual(got.header, legacy.header) {
					t.Errorf("%s: %d %q, want the legacy %d %q", tc.name, got.status, got.body, legacy.status, legacy.body)
				}
				mismatch, results := rec.snapshot()
				if !slices.Equal(results, []string{tc.result}) {
					t.Errorf("%s: results %v, want [%s]", tc.name, results, tc.result)
				}
				if want := []string{}; tc.mismatch != "" {
					want = []string{tc.mismatch}
					if !slices.Equal(mismatch, want) {
						t.Errorf("%s: mismatch %v, want %v", tc.name, mismatch, want)
					}
				} else if len(mismatch) != 0 {
					t.Errorf("%s: mismatch %v, want none", tc.name, mismatch)
				}
				calls := pdp.calls()
				if len(calls) != tc.calls || (tc.calls == 1 && calls[0].Org != ownOrg) {
					t.Errorf("%s: PDP saw %+v, want %d question(s) on A", tc.name, calls, tc.calls)
				}
			}
		})
	}
}

// A row outside the census serves the session organization: an org_id query parameter or a path value is not
// compared, and the PDP is asked on A as before.
func TestServedOrgARowOutsideTheCensusIsNotCompared(t *testing.T) {
	pdp := newTestPDP(t, allowAll)
	got := enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor("teams"))),
		http.MethodGet, "/api/v1/admin/teams?org_id="+uuid.MustParse(strangerOrg).String())
	if calls := pdp.calls(); got.status != http.StatusOK || len(calls) != 1 || calls[0].Org != ownOrg {
		t.Fatalf("%d %q, PDP saw %+v, want 200 and one question on A", got.status, got.body, calls)
	}
}

// The seam's row table is the census, row for row and source for source.
func TestServedOrgSeamTableIsTheCensus(t *testing.T) {
	if len(goAPIServedOrgRows) != len(servedOrgCensus) {
		t.Fatalf("%d seam rows, want the census's %d", len(goAPIServedOrgRows), len(servedOrgCensus))
	}
	for _, row := range servedOrgCensus {
		param, ok := goAPIServedOrgRows[row.pattern]
		want := servedOrgParam{path: "org_id"}
		if row.query {
			want = servedOrgParam{query: "org_id"}
		}
		if !ok || param != want {
			t.Errorf("%s: seam row %+v (present %v), want %+v", row.pattern, param, ok, want)
		}
	}
}
