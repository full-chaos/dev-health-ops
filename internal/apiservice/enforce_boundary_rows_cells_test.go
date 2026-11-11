package apiservice

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

// boundaryRoute selects the leaf 6.8c rows: the credentials, customer-push, identities, ip-allowlist, llm-settings and
// retention rows of the settings, integrations and teams families, and the onboarding skip-integration row.
var boundaryRoute = regexp.MustCompile(`^/api/v1/(admin/(credentials|customer-push|identities|ip-allowlist|llm-settings|retention-policies)(/|$)|auth/onboarding/skip-integration$)`)

// boundaryActions is the action of every 6.8c row ("" = no action: the legacy guard alone decides). The target is the
// legacy answer for every caller class (product rule 1): each action is held by the owner and admin roles only and is
// flagged platform_admin allowed, as the Admin and AdminOrg levels admit. A profile change to any of these rows must
// change this table in the same PR.
var boundaryActions = map[string]string{
	"GET /api/v1/admin/credentials":                                 "integrations.read",
	"POST /api/v1/admin/credentials":                                "integrations.write",
	"POST /api/v1/admin/credentials/test":                           "integrations.write",
	"DELETE /api/v1/admin/credentials/{provider}/{name}":            "integrations.write",
	"GET /api/v1/admin/credentials/{provider}/{name}":               "integrations.read",
	"PATCH /api/v1/admin/credentials/{provider}/{name}":             "integrations.write",
	"GET /api/v1/admin/customer-push/batches/{ingestion_id}":        "integrations.read",
	"GET /api/v1/admin/customer-push/schemas":                       "integrations.read",
	"GET /api/v1/admin/customer-push/schemas/{schema_version}":      "integrations.read",
	"GET /api/v1/admin/customer-push/sources":                       "integrations.read",
	"POST /api/v1/admin/customer-push/sources":                      "integrations.write",
	"GET /api/v1/admin/customer-push/sources/{source_id}":           "integrations.read",
	"PATCH /api/v1/admin/customer-push/sources/{source_id}":         "integrations.write",
	"GET /api/v1/admin/customer-push/sources/{source_id}/batches":   "integrations.read",
	"GET /api/v1/admin/customer-push/sources/{source_id}/tokens":    "integrations.read",
	"POST /api/v1/admin/customer-push/sources/{source_id}/tokens":   "integrations.write",
	"POST /api/v1/admin/customer-push/sources/{source_id}/validate": "integrations.write",
	"GET /api/v1/admin/customer-push/tokens":                        "integrations.read",
	"POST /api/v1/admin/customer-push/tokens":                       "integrations.write",
	"POST /api/v1/admin/customer-push/tokens/{token_id}/revoke":     "integrations.write",
	"POST /api/v1/admin/customer-push/tokens/{token_id}/rotate":     "integrations.write",
	"GET /api/v1/admin/identities":                                  "teams.write",
	"POST /api/v1/admin/identities":                                 "teams.write",
	"GET /api/v1/admin/ip-allowlist":                                "settings.write",
	"POST /api/v1/admin/ip-allowlist":                               "settings.write",
	"DELETE /api/v1/admin/ip-allowlist/{entry_id}":                  "settings.write",
	"GET /api/v1/admin/ip-allowlist/{entry_id}":                     "settings.write",
	"PATCH /api/v1/admin/ip-allowlist/{entry_id}":                   "settings.write",
	"POST /api/v1/admin/ip-allowlist/{entry_id}":                    "settings.write",
	"DELETE /api/v1/admin/llm-settings":                             "settings.write",
	"GET /api/v1/admin/llm-settings":                                "settings.write",
	"PUT /api/v1/admin/llm-settings":                                "settings.write",
	"GET /api/v1/admin/llm-settings/budget":                         "settings.write",
	"GET /api/v1/admin/llm-settings/models":                         "settings.write",
	"GET /api/v1/admin/llm-settings/own-model":                      "settings.write",
	"PUT /api/v1/admin/llm-settings/own-model":                      "settings.write",
	"GET /api/v1/admin/llm-settings/prompt-cache":                   "settings.write",
	"PUT /api/v1/admin/llm-settings/prompt-cache":                   "settings.write",
	"POST /api/v1/admin/llm-settings/readiness":                     "settings.write",
	"GET /api/v1/admin/llm-settings/service-model":                  "settings.write",
	"PUT /api/v1/admin/llm-settings/service-model":                  "settings.write",
	"GET /api/v1/admin/llm-settings/spend":                          "settings.write",
	"GET /api/v1/admin/llm-settings/status":                         "settings.write",
	"GET /api/v1/admin/retention-policies":                          "settings.write",
	"POST /api/v1/admin/retention-policies":                         "settings.write",
	"DELETE /api/v1/admin/retention-policies/{policy_id}":           "settings.write",
	"GET /api/v1/admin/retention-policies/{policy_id}":              "settings.write",
	"PATCH /api/v1/admin/retention-policies/{policy_id}":            "settings.write",
	"POST /api/v1/admin/retention-policies/{policy_id}/execute":     "settings.write",
	"POST /api/v1/auth/onboarding/skip-integration":                 "",
}

func sortedBoundaryKeys() []string {
	keys := make([]string, 0, len(boundaryActions))
	for key := range boundaryActions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestBoundaryRowsMatchTheProfile(t *testing.T) {
	got := map[string]string{}
	for _, row := range loadGoAPIRows(t) {
		route, _ := row["route"].(string)
		method, _ := row["method"].(string)
		if row["service"] != goAPIAccessService || !boundaryRoute.MatchString(route) {
			continue
		}
		action, _ := row["action"].(string)
		got[method+" "+route] = action
	}
	if !slices.Equal(slices.Sorted(func(yield func(string) bool) {
		for key := range got {
			if !yield(key) {
				return
			}
		}
	}), sortedBoundaryKeys()) {
		t.Fatalf("profile rows %v, table %v", got, sortedBoundaryKeys())
	}
	mapped := 0
	for key, want := range boundaryActions {
		if got[key] != want {
			t.Errorf("%s: profile action %q, table %q", key, got[key], want)
		}
		if want != "" {
			mapped++
		}
	}
	if len(boundaryActions) != 50 || mapped != 49 {
		t.Errorf("table has %d rows, %d mapped; want 50 and 49", len(boundaryActions), mapped)
	}
}

// boundaryPath fills a 6.8c route's parameters: the ip-allowlist POST serves only "check" behind the guard (any other
// id is a 405 before it, ipallowlist.go checkOrMethodNotAllowed).
func boundaryPath(key string) string {
	if key == "POST /api/v1/admin/ip-allowlist/{entry_id}" {
		return "/api/v1/admin/ip-allowlist/check"
	}
	_, route, _ := strings.Cut(key, " ")
	return cellRouteParam.ReplaceAllStringFunc(route, func(param string) string {
		switch param {
		case "{provider}":
			return "github"
		case "{name}", "{schema_version}":
			return "n"
		}
		return "00000000-0000-4000-8000-0000000000e1"
	})
}

func boundaryDo(handler http.Handler, key, token string, headers map[string]string) cellAnswer {
	method, _, _ := strings.Cut(key, " ")
	return cellDo(handler, method+" "+boundaryPath(key), token, headers)
}

// boundaryEchoRoutes mounts the 6.8c rows with the principal echo behind the guard level each row's registration
// names in the source (as adminConfigEchoRoutes).
func boundaryEchoRoutes(t *testing.T, deps Deps) []httpapi.Route {
	t.Helper()
	regs := scanRegistrations(t, "../api", ".")
	echo := http.HandlerFunc(principalEcho)
	var routes []httpapi.Route
	for _, m := range mountedRoutes(t) {
		if _, ok := boundaryActions[m.Method+" "+m.Pattern]; !ok || m.Listener != "api" {
			continue
		}
		reg, ok := registrationFor(regs, m)
		if !ok {
			t.Fatalf("%s: no registration found in the source", m.key())
		}
		level := reg.Level
		if level == "" {
			level = dispatchedLevels[m.key()]
		}
		authz := map[string]policy.Authz{"Admin": policy.Admin, "AdminOrg": policy.AdminOrg, "Authenticated": policy.Authenticated}
		a, ok := authz[level]
		if !ok {
			t.Fatalf("%s: guard level %q", m.key(), level)
		}
		routes = append(routes, httpapi.Route{Method: m.Method, Pattern: m.Pattern, Handler: deps.Guard.Wrap(a, echo)})
	}
	if len(routes) != 50 {
		t.Fatalf("%d echo routes, want the 50 rows of boundaryActions", len(routes))
	}
	return routes
}

var (
	cellViewer    = uuid.MustParse("00000000-0000-4000-8000-0000000000f6")
	cellViewerSID = uuid.MustParse("00000000-0000-4000-8000-0000000000f7")
)

// withBoundaryCallers adds the platform admins and an org viewer.
func withBoundaryCallers(store *probeStore) {
	withPlatformAdmins(store)
	store.users[cellViewer] = probeUserRow{state: policy.UserState{IsActive: true}, email: "viewer@example.test"}
	store.members[[2]uuid.UUID{cellViewer, probeOrg}] = "viewer"
	store.sessions[cellViewerSID] = policy.SessionRow{PrincipalID: cellViewer, LegacyTokenVersion: i64ptr(0)}
}

func boundaryCallers(minter *eddsaMinter) map[string]cellCaller {
	callers := cellCallers(minter)
	callers["org viewer"] = cellCaller{name: "org viewer", token: minter.mint(cellViewer, map[string]any{"sid": sessionid.Wire(cellViewerSID)}),
		snap: cellSnapshot{orgRole: "viewer"}}
	return callers
}

// boundaryClasses are the caller classes C1-C4 of the table (C2 is member and viewer).
var boundaryClasses = []string{"org admin", "org member", "org viewer", "platform admin + org member", "platform admin + no membership"}

func short(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])[:12]
}

// boundaryPair builds a server with the cells off and one with the settings, integrations and teams cells on and the
// real evaluator behind the decision point, over build.
func boundaryPair(t *testing.T, pdp *cellPDP, build func(*testing.T, Deps) []httpapi.Route) (off, on http.Handler, offCallers, onCallers map[string]cellCaller) {
	t.Helper()
	off, offMinter := adminConfigServerScoped(t, nil, withBoundaryCallers, build, nil)
	on, onMinter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, adminConfigNamespaces, false)
	}, withBoundaryCallers, build, nil)
	offCallers, onCallers = boundaryCallers(offMinter), boundaryCallers(onMinter)
	return off, on, offCallers, onCallers
}

func realRoutes(_ *testing.T, deps Deps) []httpapi.Route { return Routes(deps, quietLogger()) }

// The executed cell table of 6.8c: on every row, for each caller class, the answer with the cells on (the decision
// point and the real evaluator decide) equals the legacy answer (cells off), on the principal echo behind the
// production guard level and on the real handler. Each cell prints one CELL line. A mapped row is asked exactly once,
// with its action on the token's organization; the unmapped row is never asked.
func TestBoundaryRowsAnswerEveryCallerClassAsLegacy(t *testing.T) {
	pdp := newCellPDP(t)
	for _, surface := range []struct {
		name  string
		build func(*testing.T, Deps) []httpapi.Route
	}{{"echo", boundaryEchoRoutes}, {"handler", realRoutes}} {
		off, on, offCallers, onCallers := boundaryPair(t, pdp, surface.build)
		equal, cells := 0, 0
		for _, key := range sortedBoundaryKeys() {
			action := boundaryActions[key]
			for _, class := range boundaryClasses {
				pdp.register(onCallers)
				pdp.reset("evaluate")
				legacy := boundaryDo(off, key, offCallers[class].token, nil)
				if calls := pdp.calls(); len(calls) != 0 {
					t.Fatalf("%s %s %s: %d decision point calls with the cells off", surface.name, key, class, len(calls))
				}
				got := boundaryDo(on, key, onCallers[class].token, nil)
				calls := pdp.calls()
				result := "EQUAL"
				if got.status != legacy.status || got.body != legacy.body {
					result = "DIFF"
					t.Errorf("%s %s / %s: new %d %s, legacy %d %s", surface.name, key, class, got.status, got.body, legacy.status, legacy.body)
				} else {
					equal++
				}
				cells++
				t.Logf("CELL\t%s\t%s\t%s\tlegacy=%d:%s\tnew=%d:%s\t%s", surface.name, key, class, legacy.status, short(legacy.body), got.status, short(got.body), result)
				switch {
				case action == "" && len(calls) != 0:
					t.Errorf("%s %s / %s: %d calls on the unmapped row, want 0", surface.name, key, class, len(calls))
				case action != "" && (len(calls) != 1 || calls[0].Action != action || calls[0].OrganizationID != probeOrg.String()):
					t.Errorf("%s %s / %s: calls %+v, want one %s call on %s", surface.name, key, class, calls, action, probeOrg)
				}
			}
		}
		if cells != 50*len(boundaryClasses) {
			t.Errorf("%s: %d cells, want %d", surface.name, cells, 50*len(boundaryClasses))
		}
		t.Logf("SUMMARY\t%s\tcells=%d\tequal=%d", surface.name, cells, equal)
	}
}

// On the echo, where no body is decoded first, every mapped row asks exactly one call per judged caller, and a failure
// of the decision point is the 503 with Retry-After 5 (fail closed; the drill's [10861] class).
func TestBoundaryRowsAskTheirActionAndFailClosed(t *testing.T) {
	pdp := newCellPDP(t)
	_, on, _, onCallers := boundaryPair(t, pdp, boundaryEchoRoutes)
	admin := onCallers["org admin"].token
	for _, key := range sortedBoundaryKeys() {
		action := boundaryActions[key]
		pdp.reset("fail")
		got := boundaryDo(on, key, admin, nil)
		calls := pdp.calls()
		if action == "" {
			if len(calls) != 0 || got.status != http.StatusOK {
				t.Errorf("%s: %d %s with %d calls, want the legacy 200 echo and no call", key, got.status, got.body, len(calls))
			}
			continue
		}
		if len(calls) != 1 || calls[0].Action != action || calls[0].ResourceType != "organization" || calls[0].OrganizationID != probeOrg.String() {
			t.Errorf("%s: calls %+v, want one %s call on organization %s", key, calls, action, probeOrg)
		}
		if got.status != http.StatusServiceUnavailable || got.header.Get("Retry-After") != "5" {
			t.Errorf("%s: %d %s, want 503 Retry-After 5", key, got.status, got.body)
		}
	}
}

// The class rows on every 6.8c row, legacy answer and new answer equal: a delegated bearer (403 before any call), a
// token with no organization (not judged), an X-Org-Id of another organization (selection only), and the legacy
// impersonation caller with the proxy on (refused at the scope). With the proxy off, the legacy caller's reads keep
// the legacy answer and its writes are refused on the enforced rows (known flaw, CHAOS-13211 item 6: stated, not fixed).
func TestBoundaryRowsClassRowsAnswerAsBuilt(t *testing.T) {
	pdp := newCellPDP(t)
	edit := func(store *probeStore) { withBoundaryCallers(store); withLegacyImpersonationCaller(store) }
	for _, proxy := range []bool{true, false} {
		off, offMinter := adminConfigServerScoped(t, nil, edit, boundaryEchoRoutes, refuseLegacyCaller(proxy))
		on, onMinter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
			return newCellEnforcer(t, pdp, adminConfigNamespaces, false)
		}, edit, boundaryEchoRoutes, refuseLegacyCaller(proxy))
		tokens := func(m *eddsaMinter) map[string]string {
			return map[string]string{
				"delegated bearer":      m.mint(probeUser, map[string]any{"act": map[string]any{"sub": wirePrincipal(probeAdminUser)}}),
				"no-org platform admin": m.mint(cellPlatformNoRow, map[string]any{"sid": sessionid.Wire(cellNoRowSID), "org_id": nil}),
				"admin, other X-Org-Id": m.mint(probeUser, nil),
				"legacy impersonation":  m.mint(probeAdminUser, map[string]any{"sid": sessionid.Wire(cellLegacyCallerSID)}),
			}
		}
		offTokens, onTokens := tokens(offMinter), tokens(onMinter)
		for _, key := range sortedBoundaryKeys() {
			method, _, _ := strings.Cut(key, " ")
			for _, class := range []string{"delegated bearer", "no-org platform admin", "admin, other X-Org-Id", "legacy impersonation"} {
				headers := map[string]string(nil)
				if class == "admin, other X-Org-Id" {
					headers = map[string]string{"X-Org-Id": probeOtherOrg.String()}
				}
				pdp.reset("allow")
				legacy := boundaryDo(off, key, offTokens[class], headers)
				got := boundaryDo(on, key, onTokens[class], headers)
				flaw := class == "legacy impersonation" && !proxy && method != http.MethodGet && boundaryActions[key] != ""
				switch {
				case flaw:
					if got.status != http.StatusForbidden || got.body != impersonationRefusalBody || got.header.Get("X-Impersonating") != "true" {
						t.Errorf("proxy=%v %s / %s: %d %s, want the enforced 403 (13211 item 6)", proxy, key, class, got.status, got.body)
					}
				case got.status != legacy.status || got.body != legacy.body:
					t.Errorf("proxy=%v %s / %s: new %d %s, legacy %d %s", proxy, key, class, got.status, got.body, legacy.status, legacy.body)
				}
				result := "EQUAL"
				if flaw {
					result = "FLAW-13211-6"
				}
				t.Logf("CELL\tclass\tproxy=%v\t%s\t%s\tlegacy=%d:%s\tnew=%d:%s\t%s", proxy, key, class, legacy.status, short(legacy.body), got.status, short(got.body), result)
				if class == "delegated bearer" && (got.status != http.StatusForbidden || got.body != impersonationRefusalBody) {
					t.Errorf("proxy=%v %s delegated bearer: %d %s, want 403 %s", proxy, key, got.status, got.body, impersonationRefusalBody)
				}
			}
		}
	}
}
