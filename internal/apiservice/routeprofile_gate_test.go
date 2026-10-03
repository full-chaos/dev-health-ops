package apiservice_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/server"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/routeprofile"
)

// The route-vs-profile gate (CHAOS-8305, S2 of CHAOS-7526; guardrail G-1 for the routes Go
// serves): every (method, path) a Go service serves has an endpoint-profile row and every
// profile row is served, wildcard-served or explained, from the production router
// constructors executed here. See the routeprofile package for the rules.

type connStub struct{ driver.Conn }

type valkeyStub struct{ valkeygo.Client }

// mountingDeps is the Deps under which every area of the api mounts its routes: the
// areas mount only when their dependencies are present (session needs the token keys,
// teams the ClickHouse connection, admin the decryptor, ...), so a Deps that lacks one
// silently shrinks the route set. The route-count floor and the per-area names below
// fail when it does.
func mountingDeps(t *testing.T) (apiservice.Deps, *slog.Logger) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	const secret = "route-profile-gate-secret-key-32-bytes-long!!"
	verifier, err := edgetoken.New(secret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(secret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	return apiservice.Deps{
		Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger), Verifier: verifier, Signer: signer,
		ClickHouse: connStub{}, Valkey: valkeyStub{}, Decryptor: providerfoundation.FernetDecryptor{},
	}, logger
}

// servedRoutes walks every route set the Go services serve.
func servedRoutes(t *testing.T) []routeprofile.Served {
	t.Helper()
	deps, logger := mountingDeps(t)
	var served []routeprofile.Served
	add := func(plane, method, pattern string) {
		served = append(served, routeprofile.Served{Plane: plane, Pair: routeprofile.Pair{Method: method, Path: routeprofile.Normalize(pattern)}})
	}
	for _, route := range apiservice.Routes(deps, logger) {
		add("go-api", route.Method, route.Pattern)
	}
	for _, route := range apiservice.InternalRoutes(deps, logger) {
		add("go-api-internal", route.Method, route.Pattern)
	}
	for _, route := range billing.EdgeRoutes(billing.Deps{Pool: deps.Pool, Logger: logger}) {
		add("billing-edge", route.Method, route.Pattern)
	}
	for _, route := range server.RESTRoutes() {
		add("query-api", route.Method, route.Pattern)
	}
	for _, route := range server.GraphQLEdgeRoutes() {
		add("query-api-graphql-edge", route.Method, route.Pattern)
	}
	return served
}

// Every area the api mounts must show in the walk: a route each, by area. A Deps that
// loses an area's dependency loses its routes and this fails first.
var mountedAreas = map[string]string{
	"session":         "POST /api/v1/auth/login",
	"sso":             "GET /api/v1/auth/sso/providers",
	"billing":         "GET /api/v1/billing/plans",
	"teams":           "GET /api/v1/admin/teams",
	"admin settings":  "GET /api/v1/admin/settings/{}",
	"credentials":     "GET /api/v1/admin/credentials/{}/{}",
	"orgs":            "GET /api/v1/orgs/me",
	"sync admin":      "POST /api/v1/admin/sync-configs",
	"health":          "GET /health",
	"external ingest": "GET /api/v1/external-ingest/schemas",
	"query-api":       "GET /api/v1/quadrant",
}

func contractPath(t *testing.T, relative string) string {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, relative)
}

func TestTheWalkMountsEveryArea(t *testing.T) {
	served := servedRoutes(t)
	if len(served) < 250 {
		t.Fatalf("the walk found %d routes, want at least 250: a Deps lost an area", len(served))
	}
	have := map[string]bool{}
	for _, route := range served {
		have[route.Pair.String()] = true
	}
	var missing []string
	for area, pair := range mountedAreas {
		if !have[pair] {
			missing = append(missing, area+": "+pair)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("the walk does not mount these areas (a Deps lost a dependency?): %v", missing)
	}
}

// profileRows are the rows the comparison reads: the Python-derived inventory
// (endpoint-profiles.ops.json, untouched) and the Go-only rows (endpoint-profiles.go.json).
func profileRows(t *testing.T) []routeprofile.Row {
	t.Helper()
	var rows []routeprofile.Row
	for _, relative := range []string{"contracts/auth/v1/endpoint-profiles.ops.json", "contracts/auth/v1/endpoint-profiles.go.json"} {
		loaded, err := routeprofile.LoadRows(contractPath(t, relative))
		if err != nil {
			t.Fatalf("%s: %v", relative, err)
		}
		rows = append(rows, loaded...)
	}
	return rows
}

func TestEveryServedRouteHasAProfileRowAndEveryRowIsServed(t *testing.T) {
	wildcards, err := routeprofile.LoadWildcards(contractPath(t, "ci/go_wildcard_dispatch.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	pythonOnly, err := routeprofile.LoadPythonOnly(contractPath(t, "ci/endpoint_profiles_python_only.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	stubs, err := routeprofile.LoadStubs(contractPath(t, "ci/go_refusal_stubs.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	report := routeprofile.Compare(servedRoutes(t), profileRows(t), wildcards, pythonOnly, stubs)
	t.Logf("matched pairs: %d", report.Matched)
	for _, problem := range report.Problems {
		t.Error(problem)
	}
}

// goAPIHandler is the built api server over the routes the walk serves, the way the
// binary builds it: the probes below run against it.
func goAPIHandler(t *testing.T) http.Handler {
	t.Helper()
	deps, logger := mountingDeps(t)
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(string) (string, bool) { return "", false }})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	scope := policy.NewScope(deps.Auth, logger)
	built, err := apiservice.NewServer(cfg, logger, apiservice.Routes(deps, logger), scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return built.Handler()
}

// A refusal stub is a registration that only refuses: its probe is executed against the
// built server and must answer the recorded status.
func TestEveryRefusalStubAnswersItsRefusal(t *testing.T) {
	stubs, err := routeprofile.LoadStubs(contractPath(t, "ci/go_refusal_stubs.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	handler := goAPIHandler(t)
	for _, stub := range stubs {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(stub.Method, stub.Probe, nil))
		if recorder.Code != stub.Status {
			t.Errorf("%s %s (%s): answers %d, want the refusal %d", stub.Method, stub.Probe, stub.Site, recorder.Code, stub.Status)
		}
	}
}

// classProbe sends a request with NO credential and a well-formed empty JSON body (a
// body-first route decodes the body before it looks at the credential) and returns the
// status.
func classProbe(handler http.Handler, method, path string) int {
	url := routeprofile.Parameter.ReplaceAllString(path, "x")
	request := httptest.NewRequest(method, url, strings.NewReader("{}"))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder.Code
}

// A row whose credential is the api's own access token is checked by the guard BEFORE the
// handler runs, so an unauthenticated request must be refused with exactly 401: a 500 there
// is a handler that ran without the guard (the store behind it is unreachable in this
// test). The other credential classes (external ingest tokens, webhook secrets) are looked
// up in a store the handler reaches, so with the store unreachable they fail closed with
// 500/503; a 2xx, a redirect, a 404/405 or a validation answer is never a refusal.
func refused(row routeprofile.Row, code int) bool {
	for _, class := range row.Classes {
		if class == "ops_access_token_hs256" {
			return code == http.StatusUnauthorized
		}
	}
	return code == http.StatusUnauthorized || code == http.StatusForbidden ||
		code == http.StatusInternalServerError || code == http.StatusServiceUnavailable
}

// (class) every go-api route whose profile row says "protected" refuses an
// unauthenticated request; the rows the Go code does not enforce as the row says are the
// closed list ci/endpoint_profile_class_exceptions.tsv (each must still answer exactly its
// recorded status).
func TestEveryProtectedRowRefusesAnUnauthenticatedRequest(t *testing.T) {
	handler := goAPIHandler(t)
	exceptions, err := routeprofile.LoadClassExceptions(contractPath(t, "ci/endpoint_profile_class_exceptions.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	excepted := map[routeprofile.Pair]routeprofile.ClassException{}
	for _, entry := range exceptions {
		excepted[routeprofile.Pair{Method: strings.ToUpper(entry.Method), Path: routeprofile.Normalize(entry.Route)}] = entry
	}
	class := map[routeprofile.Pair]string{}
	rowOf := map[routeprofile.Pair]routeprofile.Row{}
	for _, row := range profileRows(t) {
		if row.SurfaceKind == "rest" {
			for _, pair := range row.Pairs() {
				class[pair] = row.Classification
				rowOf[pair] = row
			}
		}
	}
	probed := 0
	seen := map[routeprofile.Pair]bool{}
	for _, route := range servedRoutes(t) {
		if route.Plane != "go-api" || class[route.Pair] != "protected" {
			continue
		}
		code := classProbe(handler, route.Method, route.Path)
		probed++
		seen[route.Pair] = true
		if entry, ok := excepted[route.Pair]; ok {
			if code != entry.Status {
				t.Errorf("%s: listed in the class exceptions with status %d, answers %d (%s)", route.Pair, entry.Status, code, entry.Ref)
			}
			continue
		}
		if !refused(rowOf[route.Pair], code) {
			t.Errorf("%s: its row says protected (%v), an unauthenticated request answers %d: the Go code does not enforce the row", route.Pair, rowOf[route.Pair].Classes, code)
		}
	}
	for pair, entry := range excepted {
		if !seen[pair] {
			t.Errorf("class exception %s (%s) names a route the probe did not reach: stale", pair, entry.Ref)
		}
		if class[pair] != entry.Profile {
			t.Errorf("class exception %s says the row is %q, the row is %q", pair, entry.Profile, class[pair])
		}
	}
	if probed < 150 {
		t.Fatalf("only %d protected routes were probed: the walk or the rows shrank", probed)
	}
	t.Logf("probed %d protected routes, %d class exceptions", probed, len(excepted))
}

// (wildcard proof, unit) a wildcard handler serves a literal path value by comparing it in
// code; a registered pattern cannot show that. Executed here, with no store: the literal
// reaches the route's handler (its guard or public handler answers, never the method
// refusal or a missing route) and a planted sibling literal on the SAME wildcard is
// refused (405 or 404). The wildcards whose literal is dispatched behind the credential
// ("integration" rows) need an authenticated request on real stores and are listed
// separately: the gate fails if the list and the rows differ.
func TestEveryUnitWildcardRowIsServedByItsLiteralAndNotBySiblings(t *testing.T) {
	handler := goAPIHandler(t)
	wildcards, err := routeprofile.LoadWildcards(contractPath(t, "ci/go_wildcard_dispatch.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	units := 0
	for _, entry := range wildcards {
		if entry.Proof != "unit" {
			continue
		}
		units++
		literalPath := routeprofile.Parameter.ReplaceAllString(entry.Route, "x")
		siblingPath := strings.Replace(literalPath, "/"+entry.Literal, "/zzz-planted-sibling", 1)
		if siblingPath == literalPath {
			t.Errorf("%s %s: the literal %q is not a segment of the row's route", entry.Method, entry.Route, entry.Literal)
			continue
		}
		literal := classProbe(handler, entry.Method, literalPath)
		if literal == http.StatusMethodNotAllowed || literal == http.StatusNotFound {
			t.Errorf("%s %s (%s): the literal answers %d: the wildcard does not serve it", entry.Method, entry.Route, entry.Site, literal)
		}
		sibling := classProbe(handler, entry.Method, siblingPath)
		if sibling != http.StatusMethodNotAllowed && sibling != http.StatusNotFound {
			t.Errorf("%s %s: a planted sibling literal answers %d, want 405 or 404: the wildcard serves more than the literal", entry.Method, siblingPath, sibling)
		}
	}
	if units < 5 {
		t.Fatalf("only %d unit wildcard proofs ran", units)
	}
}

func TestTheIntegrationWildcardProofsAreTheDeclaredOnes(t *testing.T) {
	wildcards, err := routeprofile.LoadWildcards(contractPath(t, "ci/go_wildcard_dispatch.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, entry := range wildcards {
		if entry.Proof == "integration" {
			got = append(got, entry.Method+" "+entry.Route)
		}
	}
	sort.Strings(got)
	// The rows whose literal is dispatched behind the credential. The integration proof for
	// them is a separate sub-issue; until it exists they are named here, so a row cannot
	// drift into "integration" (and out of every proof) without this list changing.
	want := []string{
		"GET /api/v1/admin/credentials/{credential_id}/repos",
		"GET /api/v1/admin/retention-policies/resource-types",
		"GET /api/v1/admin/settings/categories",
		"GET /api/v1/admin/teams/discover",
		"GET /api/v1/admin/teams/pending-changes",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("integration-proof wildcard rows:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// (schema) the Go-only rows validate against the same endpoint-profile schema the
// Python-derived rows do.
func TestTheGoProfileRowsValidateAgainstTheSchema(t *testing.T) {
	raw, err := os.ReadFile(contractPath(t, "contracts/auth/v1/endpoint-profile.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	resolved, err := schema.Resolve(&jsonschema.ResolveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"endpoint-profiles.go.json", "endpoint-profiles.ops.json"} {
		document, err := os.ReadFile(contractPath(t, "contracts/auth/v1/"+name))
		if err != nil {
			t.Fatal(err)
		}
		var value any
		if err := json.Unmarshal(document, &value); err != nil {
			t.Fatal(err)
		}
		if err := resolved.Validate(value); err != nil {
			t.Errorf("%s does not validate against endpoint-profile.schema.json: %v", name, err)
		}
	}
}

// (pins) no row of the Python-derived inventory changes: its canonical JSON hashes to the
// value recorded in endpoint-profiles.ops.pins.tsv, so a row cannot be weakened (a class
// edited, a validator dropped) without this test failing and the pin being re-recorded on
// purpose: UPDATE_PROFILE_PINS=1 go test -run TestTheOpsProfileRowsAreWhatTheyWere.
func TestTheOpsProfileRowsAreWhatTheyWere(t *testing.T) {
	rows, err := routeprofile.LoadRows(contractPath(t, "contracts/auth/v1/endpoint-profiles.ops.json"))
	if err != nil {
		t.Fatal(err)
	}
	current := map[string]string{}
	var lines []string
	for _, row := range rows {
		canonical, err := routeprofile.Canonical(row.Raw)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(canonical)
		current[row.ID] = hex.EncodeToString(sum[:])
		lines = append(lines, row.ID+"\t"+current[row.ID])
	}
	sort.Strings(lines)
	pinsPath := contractPath(t, "contracts/auth/v1/endpoint-profiles.ops.pins.tsv")
	if os.Getenv("UPDATE_PROFILE_PINS") == "1" {
		header := "# CHAOS-8305: sha256 of each endpoint-profiles.ops.json row's canonical JSON (keys sorted). A changed, removed or added row fails\n" +
			"# TestTheOpsProfileRowsAreWhatTheyWere until this file is re-recorded on purpose (UPDATE_PROFILE_PINS=1). Deleted with the file (CHAOS-8306).\n"
		if err := os.WriteFile(pinsPath, []byte(header+strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	rawPins, err := os.ReadFile(pinsPath)
	if err != nil {
		t.Fatal(err)
	}
	pinned := map[string]string{}
	for _, line := range strings.Split(string(rawPins), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		columns := strings.Split(line, "\t")
		if len(columns) != 2 {
			t.Fatalf("bad pin line %q", line)
		}
		pinned[columns[0]] = columns[1]
	}
	for id, sum := range current {
		switch want, ok := pinned[id]; {
		case !ok:
			t.Errorf("row %q is new and has no pin", id)
		case want != sum:
			t.Errorf("row %q changed (pin %s, now %s): a profile row must not change", id, want, sum)
		}
	}
	for id := range pinned {
		if _, ok := current[id]; !ok {
			t.Errorf("row %q was removed", id)
		}
	}
	if len(pinned) < 290 {
		t.Fatalf("only %d pins", len(pinned))
	}
}

// (golden) the walk is recorded in ci/go_served_routes.tsv so the Python side of the
// differential (tests/test_endpoint_profiles_contract.py, which runs where Python exists)
// compares ci/discover_ops_routes.py with the SAME set this test executes. A route added
// to or removed from any Go service changes the walk and fails here until the file is
// re-recorded on purpose: UPDATE_SERVED_ROUTES=1 go test -run TestTheWalkIsTheRecordedServedRoutes.
func TestTheWalkIsTheRecordedServedRoutes(t *testing.T) {
	seen := map[string]bool{}
	var lines []string
	for _, route := range servedRoutes(t) {
		line := route.Plane + "\t" + route.Method + "\t" + route.Path
		if !seen[line] {
			seen[line] = true
			lines = append(lines, line)
		}
	}
	sort.Strings(lines)
	path := contractPath(t, "ci/go_served_routes.tsv")
	header := "# CHAOS-8305: every (plane, method, path) the Go services serve, walked from the production router constructors by\n" +
		"# internal/apiservice TestTheWalkIsTheRecordedServedRoutes (path parameters written {}). The Python half of the differential reads\n" +
		"# it. Re-record on purpose: UPDATE_SERVED_ROUTES=1 go test -run TestTheWalkIsTheRecordedServedRoutes ./internal/apiservice/\n"
	want := header + strings.Join(lines, "\n") + "\n"
	if os.Getenv("UPDATE_SERVED_ROUTES") == "1" {
		if err := os.WriteFile(path, []byte(want), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("ci/go_served_routes.tsv is not the walk (%d routes walked, %d bytes recorded): re-record it on purpose with UPDATE_SERVED_ROUTES=1", len(lines), len(got))
	}
}
