package server

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// oldParserSet is the query-api REST pattern set the retired source parse
// (migrationmatrix.LoadQueryAPIMuxRoutes reading `mux.HandleFunc("/api/v1/...", ident)`
// out of server.go) returned at the commit before the table existed (CHAOS-8307), with
// the builder it cited for each. The table must serve exactly this set: a route that is
// added or removed changes this list in the same commit, on purpose.
var oldParserSet = map[string]string{
	"/api/v1/drilldown/issues":                    "buildDrilldownIssuesRoute",
	"/api/v1/drilldown/prs":                       "buildDrilldownPRsRoute",
	"/api/v1/explain":                             "buildExplainRoute",
	"/api/v1/filters/options":                     "buildFilterOptionsRoute",
	"/api/v1/flame":                               "buildFlameRoute",
	"/api/v1/flame/aggregated":                    "buildFlameAggregatedRoute",
	"/api/v1/heatmap":                             "buildHeatmapRoute",
	"/api/v1/home":                                "buildHomeRoute",
	"/api/v1/investment":                          "buildInvestmentRoute",
	"/api/v1/investment/explain":                  "buildInvestmentExplainRoute",
	"/api/v1/investment/flow":                     "buildInvestmentFlowRoute",
	"/api/v1/investment/flow/repo-team":           "buildInvestmentFlowRoute",
	"/api/v1/investment/sunburst":                 "buildInvestmentSunburstRoute",
	"/api/v1/meta":                                "buildMetaRoute",
	"/api/v1/opportunities":                       "buildOpportunitiesRoute",
	"/api/v1/people":                              "buildPeopleSearchRoute",
	"/api/v1/people/{person_id}/drilldown/issues": "buildPeopleDrilldownIssuesRoute",
	"/api/v1/people/{person_id}/drilldown/prs":    "buildPeopleDrilldownPRsRoute",
	"/api/v1/people/{person_id}/metric":           "buildPeopleMetricRoute",
	"/api/v1/people/{person_id}/summary":          "buildPeopleSummaryRoute",
	"/api/v1/quadrant":                            "buildQuadrantRoute",
	"/api/v1/sankey":                              "buildSankeyRoute",
	"/api/v1/work-units":                          "buildWorkUnitsRoute",
	"/api/v1/work-units/{work_unit_id}/explain":   "buildWorkUnitExplainRoute",
}

// (a) the table serves exactly the set the retired source parse found, pattern for
// pattern and builder for builder.
func TestRESTRoutesAreExactlyTheRetiredParsersSet(t *testing.T) {
	got := map[string]string{}
	for _, route := range RESTRoutes() {
		if prior, ok := got[route.Pattern]; ok && prior != route.Builder {
			t.Errorf("%s: two builders (%s, %s)", route.Pattern, prior, route.Builder)
		}
		got[route.Pattern] = route.Builder
	}
	for pattern, builder := range oldParserSet {
		if got[pattern] != builder {
			t.Errorf("%s: table builder %q, the retired parse cited %q", pattern, got[pattern], builder)
		}
	}
	for pattern := range got {
		if _, ok := oldParserSet[pattern]; !ok {
			t.Errorf("%s is in the table but not in oldParserSet: add it here on purpose", pattern)
		}
	}
	if len(got) != 24 {
		t.Errorf("%d patterns, want 24", len(got))
	}
}

// Every row names a builder that exists in the file it cites (the citation
// internal/migrationmatrix prints), and every Mount has a method set.
func TestEveryRESTRowCitesAnExistingBuilderAndDeclaresMethods(t *testing.T) {
	for _, group := range restGroups {
		if len(group.Mounts) == 0 || group.Build == nil || group.LogBuildError == nil || group.LogNotConfigured == nil {
			t.Errorf("%s: incomplete row", group.Builder)
		}
		for _, mount := range group.Mounts {
			if mount.Pattern == "" || len(mount.Methods) == 0 {
				t.Errorf("%s: mount %+v declares no pattern or no method", group.Builder, mount)
			}
		}
		if !declaresFunc(t, group.File, group.Builder) {
			t.Errorf("%s is not declared in %s", group.Builder, group.File)
		}
	}
}

func declaresFunc(t *testing.T, file, name string) bool {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	for _, decl := range parsed.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return true
		}
	}
	return false
}

var apiV1Registration = regexp.MustCompile(`\.Handle(?:Func)?\(\s*"(/api/v1/[^"]*)"`)

// apiV1Registrations lists the /api/v1/ patterns a Go source text registers on any mux.
func apiV1Registrations(source string) []string {
	var out []string
	for _, match := range apiV1Registration.FindAllStringSubmatch(source, -1) {
		out = append(out, match[1])
	}
	return out
}

// (e) a /api/v1/ registration outside the table loop turns this red: the only literal
// registration the package may carry is the "/api/v1/" catch-all 404, which is not a
// route. The scanner is also run on a planted source text to show it sees one.
func TestNoAPIV1RegistrationOutsideTheTable(t *testing.T) {
	planted := apiV1Registrations("mux.HandleFunc(\"/api/v1/planted\", h)\nmux.Handle(\"/api/v1/planted2\", h)")
	if len(planted) != 2 {
		t.Fatalf("the scanner missed a planted registration: %v", planted)
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(file) //nolint:gosec // package-relative source
		if err != nil {
			t.Fatal(err)
		}
		for _, pattern := range apiV1Registrations(string(raw)) {
			if pattern != "/api/v1/" {
				t.Errorf("%s registers %s outside the restGroups table: add a row to rest_routes.go", file, pattern)
			}
		}
	}
}

// restTestEnv switches every route on and gives each builder the settings it needs to
// construct (nothing is dialled by construction: the read client and the pools connect
// on first use).
func restTestEnv(t *testing.T) (getenv getenvFunc, bearer string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(pub),
		"kid": iaKID, "use": "sig", "alg": "EdDSA",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(jwksPath, doc, 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"CLICKHOUSE_URI":               "clickhouse://default:@127.0.0.1:1/default",
		"GO_API_REGISTRY_POSTGRES_URI": "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
		"GO_API_ENVELOPE_JWKS_PATH":    jwksPath,
		"GO_API_ENVELOPE_ISSUER":       iaIssuer,
		"GO_API_ENVELOPE_AUDIENCE":     iaAudience,
	}
	for _, name := range []string{
		"GO_API_DRILLDOWN_ISSUES_ENABLED", "GO_API_DRILLDOWN_PRS_ENABLED", "GO_API_EXPLAIN_ENABLED", "GO_API_FILTER_OPTIONS_ENABLED",
		"GO_API_FLAME_AGGREGATED_ENABLED", "GO_API_FLAME_ENABLED", "GO_API_HEATMAP_ENABLED", "GO_API_HOME_ENABLED",
		"GO_API_INVESTMENT_ENABLED", "GO_API_INVESTMENT_EXPLAIN_ENABLED", "GO_API_INVESTMENT_FLOW_ENABLED",
		"GO_API_INVESTMENT_SUNBURST_ENABLED", "GO_API_META_ENABLED", "GO_API_OPPORTUNITIES_ENABLED",
		"GO_API_PEOPLE_DRILLDOWN_ISSUES_ENABLED", "GO_API_PEOPLE_DRILLDOWN_PRS_ENABLED", "GO_API_PEOPLE_METRIC_ENABLED",
		"GO_API_PEOPLE_SEARCH_ENABLED", "GO_API_PEOPLE_SUMMARY_ENABLED", "GO_API_QUADRANT_ENABLED", "GO_API_SANKEY_ENABLED",
		"GO_API_WORK_UNIT_EXPLAIN_ENABLED", "GO_API_WORK_UNITS_ENABLED",
	} {
		env[name] = "true"
	}
	return func(name string) string { return env[name] }, iaEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "admin"})
}

// needsLiveWriteConnection names the builders that open a second, WRITE ClickHouse
// connection with a readiness ping when they construct, so they cannot be built in a
// unit test; their methods are probed in the integration shard test over the real plane
// (rest_routes_integration_test.go). The test below fails if one of them builds here
// (a stale entry) and if any other builder does not.
var needsLiveWriteConnection = map[string]bool{
	"buildInvestmentExplainRoute": true,
	"buildWorkUnitExplainRoute":   true,
}

var allMethods = []string{
	http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead, http.MethodOptions,
}

// (d) each mount's declared methods are the methods its handler serves: built through the
// row's own Build adapter, an authenticated request is answered with something other
// than the method refusal for a declared method, and with the handler's own 405 for
// every other method. A handler that serves a method the row does not declare, or
// refuses one it declares, fails here.
func TestEachMountServesExactlyTheMethodsItDeclares(t *testing.T) {
	getenv, bearer := restTestEnv(t)
	for _, group := range restGroups {
		handlers, cleanup, ok, err := group.Build(getenv, nil)
		if needsLiveWriteConnection[group.Builder] {
			if err == nil && ok {
				t.Errorf("%s builds without a live ClickHouse: drop it from needsLiveWriteConnection", group.Builder)
			}
			continue
		}
		if err != nil || !ok {
			t.Errorf("%s: Build ok=%v err=%v with every switch on", group.Builder, ok, err)
			continue
		}
		if cleanup != nil {
			defer cleanup()
		}
		if len(handlers) != len(group.Mounts) {
			t.Fatalf("%s: %d handlers for %d mounts", group.Builder, len(handlers), len(group.Mounts))
		}
		for i, mount := range group.Mounts {
			declared := map[string]bool{}
			for _, method := range mount.Methods {
				declared[method] = true
			}
			for _, method := range allMethods {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(method, pathParameter.ReplaceAllString(mount.Pattern, "x"), nil)
				// Authenticated: some handlers refuse a method before they check the
				// credential, some after (the GET+POST ones dispatch on the method behind
				// the auth check), so only an authenticated request shows the method set.
				request.Header.Set("Authorization", "Bearer "+bearer)
				handlers[i].ServeHTTP(recorder, request)
				refusedMethod := recorder.Code == http.StatusMethodNotAllowed
				switch {
				case declared[method] && refusedMethod:
					t.Errorf("%s %s: declared, but the handler answers 405", method, mount.Pattern)
				case !declared[method] && !refusedMethod:
					t.Errorf("%s %s: not declared, but the handler answers %d (not 405): declare it or refuse it", method, mount.Pattern, recorder.Code)
				}
			}
		}
	}
}

// plantRows swaps restGroups for the given rows for the length of a test.
func plantRows(t *testing.T, rows ...restGroup) {
	t.Helper()
	saved := restGroups
	restGroups = rows
	t.Cleanup(func() { restGroups = saved })
}

func okRow(builder, pattern string, order *[]string, body string) restGroup {
	return restGroup{
		Builder: builder, File: "planted.go",
		Mounts:           []restMount{{Pattern: pattern, Methods: []string{http.MethodGet}}},
		LogBuildError:    func(error) {},
		LogNotConfigured: func() { *order = append(*order, "notconfigured:"+builder) },
		Build: func(getenvFunc, edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
			return []http.HandlerFunc{func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }},
				func() { *order = append(*order, "cleanup:"+builder) }, true, nil
		},
	}
}

// (f) the table is what BuildWithLookup mounts: a planted row is in RESTRoutes AND is
// served by the built plane, wrapped in the proof-provenance header, with its cleanup run
// by Close in reverse collection order; a row that is not configured
// stays unmounted and logs; a row whose build fails fails the build.
func TestBuildMountsExactlyTheTableRows(t *testing.T) {
	var order []string
	notConfigured := okRow("buildPlantedOff", "/api/v1/planted-off", &order, "off")
	notConfigured.Build = func(getenvFunc, edgeUserStore) ([]http.HandlerFunc, func(), bool, error) { return nil, nil, false, nil }
	plantRows(t, okRow("buildPlantedA", "/api/v1/planted-a", &order, "a"), notConfigured, okRow("buildPlantedB", "/api/v1/planted-b", &order, "b"))
	if routes := RESTRoutes(); len(routes) != 3 || routes[0].Pattern != "/api/v1/planted-a" {
		t.Fatalf("RESTRoutes does not show the planted rows: %v", routes)
	}
	plane, err := Build(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		code int
		body string
	}{{"/api/v1/planted-a", 200, "a"}, {"/api/v1/planted-b", 200, "b"}, {"/api/v1/planted-off", 404, ""}} {
		recorder := httptest.NewRecorder()
		plane.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if recorder.Code != tc.code || (tc.body != "" && recorder.Body.String() != tc.body) {
			t.Errorf("%s = %d %q, want %d %q", tc.path, recorder.Code, recorder.Body.String(), tc.code, tc.body)
		}
	}
	plane.Close()
	// The not-configured line is written at build; closeAll then runs the cleanups in the
	// reverse of the order the table collected them (B before A), as it always has.
	want := []string{"notconfigured:buildPlantedOff", "cleanup:buildPlantedB", "cleanup:buildPlantedA"}
	if !slices.Equal(order, want) {
		t.Errorf("events = %v, want %v", order, want)
	}
}

func TestABuildErrorInARowFailsTheBuild(t *testing.T) {
	var order []string
	failing := okRow("buildPlantedFail", "/api/v1/planted-fail", &order, "x")
	failing.Build = func(getenvFunc, edgeUserStore) ([]http.HandlerFunc, func(), bool, error) {
		return nil, nil, false, errPlanted
	}
	logged := false
	failing.LogBuildError = func(err error) { logged = err == errPlanted }
	plantRows(t, failing)
	if _, err := Build(func(string) string { return "" }); err == nil {
		t.Fatal("a row whose build fails did not fail the plane build")
	}
	if !logged {
		t.Error("the row's LogBuildError was not called with the build error")
	}
}

func TestARowThatReturnsTheWrongNumberOfHandlersFailsTheBuild(t *testing.T) {
	var order []string
	row := okRow("buildPlantedCount", "/api/v1/planted-count", &order, "x")
	row.Mounts = append(row.Mounts, restMount{Pattern: "/api/v1/planted-count-2", Methods: []string{http.MethodGet}})
	plantRows(t, row)
	if _, err := Build(func(string) string { return "" }); err == nil {
		t.Fatal("a row with 1 handler for 2 mounts did not fail the plane build")
	}
}

var errPlanted = errors.New("planted build failure")

// The /graphql edge serves exactly the declared methods: run through the real edge chain
// with a member credential, GET and POST are answered and every other method is the 405.
func TestGraphQLEdgeServesExactlyTheDeclaredMethods(t *testing.T) {
	handler, _, member, _, _ := edgeHarness(t)
	declared := map[string]bool{}
	for _, route := range GraphQLEdgeRoutes() {
		if route.Pattern != graphQLEdgePath {
			t.Errorf("unexpected pattern %q", route.Pattern)
		}
		declared[route.Method] = true
	}
	for _, method := range allMethods {
		cell := edgeCell{method: method, carrier: "member", document: "query"}
		recorder := serveEdge(handler, cell, edgeRequest(t, cell, member, "", ""))
		refused := recorder.Code == http.StatusMethodNotAllowed
		switch {
		case declared[method] && refused:
			t.Errorf("%s /graphql: declared but answers 405", method)
		case !declared[method] && !refused:
			t.Errorf("%s /graphql: not declared but answers %d (not 405)", method, recorder.Code)
		}
	}
}
