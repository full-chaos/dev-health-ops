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

// servedOrgCensusRow is one go-api action row whose handler serves an organization a path segment or a query
// parameter names, not the caller's session organization.
type servedOrgCensusRow struct {
	pattern string
	level   Authz
	query   bool
}

// servedOrgCensus is every go-api action row whose handler serves another organization than the session's: the 8
// organization_from_path rows and the 9 billing organization_from_query_or_session rows, whose handler reads an org_id
// query parameter (resolveOrg).
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

// servedOrgForms are spellings of one organization id that a handler of these rows parses.
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
	// claims, when set, replaces the caller's access token claims.
	claims *RoutedResult
	// adminOf gives the caller an admin row in these organizations too.
	adminOf []string
	// store, when set, replaces the persona's store.
	store func() *accessStore
}

func servedOrgServe(t *testing.T, row servedOrgCensusRow, setup servedOrgSetup, served string) response {
	t.Helper()
	store := personaStore(setup.who)
	if setup.store != nil {
		store = setup.store()
	}
	for _, org := range setup.adminOf {
		store.roles[[2]uuid.UUID{userID, uuid.MustParse(org)}] = "admin"
		store.members[[2]uuid.UUID{userID, uuid.MustParse(org)}] = true
	}
	claims := setup.claims
	if claims == nil {
		claims = userClaims(nil)
	}
	auth := routed(t, store, &fakeValidator{result: claims})
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

// The path rows of the census are every go-api row with an action whose resolver reads the organization from the
// path; every census row is an organization action row.
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
	var pathRows, census []string
	for _, entry := range document.Rows {
		if entry.Service == "dev-health-ops-api" && entry.Action != "" && entry.Classification == "protected" &&
			strings.HasPrefix(entry.Resolver, "organization_from_path") {
			pathRows = append(pathRows, entry.Method+" "+entry.Route)
		}
	}
	rows := servedOrgRows(t)
	for _, row := range servedOrgCensus {
		if !row.query {
			census = append(census, row.pattern)
		}
		shadowRow, ok := rows[row.pattern]
		if !ok || shadowRow.ResourceType != policycatalog.ScopeOrganization {
			t.Errorf("%s: row %+v, want an organization action row", row.pattern, shadowRow)
		}
	}
	slices.Sort(pathRows)
	slices.Sort(census)
	if !slices.Equal(pathRows, census) {
		t.Errorf("organization_from_path rows with an action %v, want the census path rows %v", pathRows, census)
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
						calls  int
					}{
						{"org_id=" + ownOrg + "&org_id=" + strangerOrg, http.StatusOK, 1},
						{"org_id=" + strangerOrg + "&org_id=" + ownOrg, http.StatusOK, 1},
					} {
						auth := routed(t, personaStore(personaAdmin), &fakeValidator{result: userClaims(nil)})
						guard := NewGuard(auth, quiet())
						pdp := newTestPDP(t, allowAll)
						guard.SetEnforcer(servedOrgEnforcer(t, pdp, namespace))
						mux := http.NewServeMux()
						mux.Handle(row.pattern, guard.Wrap(row.level, echo))
						method, route, _ := strings.Cut(row.pattern, " ")
						if got := enforceRequest(mux, method, profilePath(route)+"?"+tc.query); got.status != tc.status || len(pdp.calls()) != tc.calls {
							t.Errorf("%s: %d %q, PDP saw %+v, want %d with %d call(s)", tc.query, got.status, got.body, pdp.calls(), tc.status, tc.calls)
						}
					}
				})
			}
			t.Run("another org is judged on the token organization", func(t *testing.T) {
				for _, other := range []string{strangerOrg, memberOrg} {
					for form, served := range servedOrgForms(other) {
						legacy := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin}, served)
						rec := recordEnforce(t)
						pdp := newTestPDP(t, allowAll)
						got := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, enforcer: servedOrgEnforcer(t, pdp, namespace)}, served)
						if got.status != legacy.status || got.body != legacy.body {
							t.Errorf("%s %s: %d %q, want the legacy %d %q", other, form, got.status, got.body, legacy.status, legacy.body)
						}
						if calls := pdp.calls(); len(calls) != 1 || calls[0].Org != ownOrg {
							t.Errorf("%s %s: PDP saw %+v, want one question on the token organization %s", other, form, calls, ownOrg)
						}
						if !slices.Equal(rec.snapshot(), []string{namespace + "/allow"}) || len(rec.unjudgedSnapshot()) != 0 {
							t.Errorf("%s %s: outcomes %v unjudged %v, want [%s/allow] and none", other, form, rec.snapshot(), rec.unjudgedSnapshot(), namespace)
						}
					}
				}
			})
			t.Run("an admin of A and B with token A is judged on A, the answer is the legacy one on B", func(t *testing.T) {
				legacy := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, adminOf: []string{memberOrg}}, memberOrg)
				if legacy.status != http.StatusOK {
					t.Errorf("legacy guard: %d %q, want 200", legacy.status, legacy.body)
				}
				pdp := newTestPDP(t, allowAll)
				got := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, adminOf: []string{memberOrg}, enforcer: servedOrgEnforcer(t, pdp, namespace)}, memberOrg)
				if calls := pdp.calls(); got.status != legacy.status || got.body != legacy.body || len(calls) != 1 || calls[0].Org != ownOrg {
					t.Errorf("enforced: %d %q, PDP saw %+v, want the legacy %d and one question on %s", got.status, got.body, calls, legacy.status, ownOrg)
				}
			})
			// CHAOS-13596: a served organization that is no UUID is judged on the token organization like any other request;
			// the scope and the handler keep refusing the value, so the composed answer is the legacy one. The double allows,
			// as the decision point allows an admin of its token organization. This cell pairs that one class only; the
			// platform admin with no membership row is paired on the production evaluator below. The other caller classes
			// are unpaired, named residuals in RISK-NOTES until the S2 real decision point table runs.
			t.Run("a slug is judged on the token organization and keeps the legacy answer", func(t *testing.T) {
				for _, slug := range []string{"acme", "not-a-uuid", strings.Repeat("g", 32)} {
					legacy := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin}, slug)
					rec := recordEnforce(t)
					pdp := newTestPDP(t, allowAll)
					got := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, enforcer: servedOrgEnforcer(t, pdp, namespace)}, slug)
					if got.status != legacy.status || got.body != legacy.body {
						t.Errorf("%s: %d %q, want the legacy %d %q", slug, got.status, got.body, legacy.status, legacy.body)
					}
					if calls := pdp.calls(); len(calls) != 1 || calls[0].Org != ownOrg || len(rec.unjudgedSnapshot()) != 0 {
						t.Errorf("%s: PDP saw %+v, unjudged %v, want one question on %s and no unjudged count", slug, calls,
							rec.unjudgedSnapshot(), ownOrg)
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
					legacy := servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, xOrgID: xOrg}, memberOrg)
					pdp = newTestPDP(t, allowAll)
					got = servedOrgServe(t, row, servedOrgSetup{who: personaAdmin, enforcer: servedOrgEnforcer(t, pdp, namespace), xOrgID: xOrg}, memberOrg)
					if calls := pdp.calls(); got.status != legacy.status || got.body != legacy.body || len(calls) != 1 || calls[0].Org != ownOrg {
						t.Errorf("X-Org-Id %q, served B: %d %q, PDP saw %+v, want the legacy %d %q and one question on %s", xOrg, got.status, got.body,
							calls, legacy.status, legacy.body, ownOrg)
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
				if calls := pdp.calls(); got.status != legacy.status || got.body != legacy.body || len(calls) != 1 || calls[0].Org != ownOrg {
					t.Errorf("enforced, superuser, another org: %d %q, PDP saw %+v, want the legacy %d and one question on %s", got.status, got.body,
						calls, legacy.status, ownOrg)
				}
			})
			t.Run("a caller with no organization and a slug keeps the legacy answer, asked admin.orgs", func(t *testing.T) {
				noOrg := userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
				legacy := servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser, claims: noOrg}, "acme")
				rec := recordEnforce(t)
				pdp := newTestPDP(t, allowAll)
				got := servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser, claims: noOrg, enforcer: servedOrgEnforcer(t, pdp, namespace)}, "acme")
				if got.status != legacy.status || got.body != legacy.body || len(rec.unjudgedSnapshot()) != 0 {
					t.Errorf("no token org, a slug: %d %q unjudged %v, want the legacy %d %q and none", got.status, got.body, rec.unjudgedSnapshot(),
						legacy.status, legacy.body)
				}
				if calls := pdp.calls(); len(calls) != 1 || calls[0].Action != ActionAdminOrgs || calls[0].HasOrg {
					t.Errorf("no token org: PDP saw %+v, want one platform question admin.orgs with no organization", calls)
				}
			})
			t.Run("a token with no organization keeps the legacy answer on any named org, asked admin.orgs", func(t *testing.T) {
				noOrg := userClaims(func(c *AccessTokenClaims) { c.OrgID = nil })
				legacy := servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser, claims: noOrg}, ownOrg)
				rec := recordEnforce(t)
				pdp := newTestPDP(t, allowAll)
				got := servedOrgServe(t, row, servedOrgSetup{who: personaSuperuser, claims: noOrg, enforcer: servedOrgEnforcer(t, pdp, namespace)}, ownOrg)
				if got.status != legacy.status || got.body != legacy.body || len(pdp.calls()) != 1 || len(rec.snapshot()) != 1 ||
					len(rec.unjudgedSnapshot()) != 0 {
					t.Errorf("no token org, named A: %d %q, %d PDP calls, outcomes %v unjudged %v, want the legacy %d %q, one, one and none",
						got.status, got.body, len(pdp.calls()), rec.snapshot(), rec.unjudgedSnapshot(), legacy.status, legacy.body)
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

// Shadow on, enforcement off: the answer is the legacy one. The shadow asks what the enforcer asks (CHAOS-13675): one
// question on the token organization A, whatever organization the request serves (A, another one, a slug).
func TestServedOrgShadowAsksTheTokenOrgAndNeverChangesTheAnswer(t *testing.T) {
	for _, row := range servedOrgCensus {
		t.Run(row.pattern, func(t *testing.T) {
			cases := []struct {
				name, served, result, mismatch string
				calls                          int
			}{
				{"same org", ownOrg, "match", "", 1},
				{"same org upper", strings.ToUpper(ownOrg), "match", "", 1},
				{"another org", strangerOrg, "match", "", 1},
				{"another org braces", "{" + memberOrg + "}", "match", "", 1},
				{"slug", "acme", "match", "", 1},
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
				if tc.mismatch != "" {
					if !slices.Equal(mismatch, []string{tc.mismatch}) {
						t.Errorf("%s: mismatch %v, want [%s]", tc.name, mismatch, tc.mismatch)
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

// A row outside the census serves the session organization: an org_id query parameter is not compared, and the PDP
// is asked on A as before.
func TestServedOrgARowOutsideTheCensusIsNotCompared(t *testing.T) {
	pdp := newTestPDP(t, allowAll)
	got := enforceRequest(enforceHandler(t, personaStore(personaAdmin), userClaims(nil), newEnforcerFor(t, pdp.client(t), onFor("teams"))),
		http.MethodGet, "/api/v1/admin/teams?org_id="+uuid.MustParse(strangerOrg).String())
	if calls := pdp.calls(); got.status != http.StatusOK || len(calls) != 1 || calls[0].Org != ownOrg {
		t.Fatalf("%d %q, PDP saw %+v, want 200 and one question on A", got.status, got.body, calls)
	}
}

// The rows the seam compares are the census: OrgFromPath on exactly its path rows, OrgFromQuery on exactly its
// org_id rows, never both on one row.
func TestServedOrgSeamRowsAreTheCensus(t *testing.T) {
	var flagged, query, censusPath, censusQuery []string
	for pattern, row := range servedOrgRows(t) {
		if row.OrgFromPath && row.OrgFromQuery {
			t.Errorf("%s reads its organization from the path and the query", pattern)
		}
		if row.OrgFromPath {
			flagged = append(flagged, pattern)
		}
		if row.OrgFromQuery {
			query = append(query, pattern)
		}
	}
	for _, row := range servedOrgCensus {
		if row.query {
			censusQuery = append(censusQuery, row.pattern)
		} else {
			censusPath = append(censusPath, row.pattern)
		}
	}
	for _, list := range [][]string{flagged, query, censusPath, censusQuery} {
		slices.Sort(list)
	}
	if !slices.Equal(flagged, censusPath) || !slices.Equal(query, censusQuery) {
		t.Fatalf("OrgFromPath rows %v and query rows %v, want the census %v and %v", flagged, query, censusPath, censusQuery)
	}
}
