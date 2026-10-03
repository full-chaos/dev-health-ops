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
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
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
	return mountingDepsWith(t, nil)
}

// mountingDepsWith is mountingDeps with the users/memberships store behind the
// authenticator replaced (nil = the Postgres store over the unreachable pool).
func mountingDepsWith(t *testing.T, store policy.Store) (apiservice.Deps, *slog.Logger) {
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
	if store == nil {
		store = policy.PGStore{Pool: pool}
	}
	auth, err := policy.NewAuthenticator(verifier, store, logger)
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
	return goAPIHandlerFor(t, deps, logger)
}

func goAPIHandlerFor(t *testing.T, deps apiservice.Deps, logger *slog.Logger) http.Handler {
	t.Helper()
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
func classProbe(handler http.Handler, method, path string, headers ...[2]string) int {
	url := routeprofile.Parameter.ReplaceAllString(path, "x")
	request := httptest.NewRequest(method, url, strings.NewReader("{}"))
	request.Header.Set("Content-Type", "application/json")
	for _, header := range headers {
		request.Header.Set(header[0], header[1])
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder.Code
}

// A refusal is exactly 401 (or 403 once a credential is present): a 500 or 503 is a
// handler that failed before it could judge the credential, which is a measurement that
// did not happen, never a pass (D4551). The routes whose credential is looked up in
// Postgres cannot be judged without one and are the closed list pendingNeedsPostgres.
func refused(code int) bool {
	return code == http.StatusUnauthorized || code == http.StatusForbidden
}

// wrongCredential is the request a provider-secret route must refuse: the right headers
// with a signature or token that is wrong (the secrets are set to other values below).
// A route not listed here is probed with no credential at all.
var wrongCredential = map[string][][2]string{
	"/api/v1/webhooks/github": {{"X-GitHub-Event", "push"}, {"X-GitHub-Delivery", "d1"}, {"X-Hub-Signature-256", "sha256=00"}},
	"/api/v1/webhooks/gitlab": {{"X-Gitlab-Event", "Push Hook"}, {"X-Gitlab-Token", "wrong"}},
	"/api/v1/webhooks/jira":   {{"X-Hub-Signature", "sha256=00"}},
}

// pendingNeedsPostgres are the routes whose credential (a push token, a per-binding
// PagerDuty secret) is read from a Postgres row; with no database they answer 500, which
// is not a refusal. They are NOT counted as passed. The executed proof is the real-store
// harness of CHAOS-8323; the gate fails when a route enters or leaves this list.
var pendingNeedsPostgres = []string{
	"GET /api/v1/external-ingest/availability",
	"GET /api/v1/external-ingest/schemas",
	"GET /api/v1/external-ingest/schemas/{schema_version}",
	"GET /api/v1/external-ingest/batches",
	"GET /api/v1/external-ingest/batches/{ingestion_id}",
	"POST /api/v1/external-ingest/batches",
	"POST /api/v1/external-ingest/validate",
	"POST /api/v1/webhooks/pagerduty/{binding_id}",
}

// (class) every go-api route whose profile row says "protected" refuses an
// unauthenticated request; the rows the Go code does not enforce as the row says are the
// closed list ci/endpoint_profile_class_exceptions.tsv (each must still answer exactly its
// recorded status).
func TestEveryProtectedRowRefusesAnUnauthenticatedRequest(t *testing.T) {
	// Provider secrets that the wrong credentials above do not match: a webhook with a
	// wrong signature must be refused (401), not fail on a missing secret (500).
	t.Setenv("GITHUB_WEBHOOK_SECRET", "gate-secret-github")
	t.Setenv("GITLAB_WEBHOOK_TOKEN", "gate-secret-gitlab")
	t.Setenv("JIRA_WEBHOOK_SECRET", "gate-secret-jira")
	handler := goAPIHandler(t)
	pending := map[routeprofile.Pair]bool{}
	for _, entry := range pendingNeedsPostgres {
		method, path, _ := strings.Cut(entry, " ")
		pending[routeprofile.Pair{Method: method, Path: routeprofile.Normalize(path)}] = true
	}
	pendingSeen := map[routeprofile.Pair]bool{}
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
		code := classProbe(handler, route.Method, route.Path, wrongCredential[route.Path]...)
		seen[route.Pair] = true
		if pending[route.Pair] {
			pendingSeen[route.Pair] = true
			if code != http.StatusInternalServerError {
				t.Errorf("%s: listed as pending (needs Postgres) but answers %d: it is measurable now, take it off the list and assert its refusal", route.Pair, code)
			}
			continue
		}
		probed++
		if entry, ok := excepted[route.Pair]; ok {
			if code != entry.Status {
				t.Errorf("%s: listed in the class exceptions with status %d, answers %d (%s)", route.Pair, entry.Status, code, entry.Ref)
			}
			continue
		}
		if !refused(code) {
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
	for pair := range pending {
		if !pendingSeen[pair] {
			t.Errorf("pending route %s was not reached by the probe: stale", pair)
		}
	}
	if probed < 150 {
		t.Fatalf("only %d protected routes were probed: the walk or the rows shrank", probed)
	}
	t.Logf("probed %d protected routes (refusal executed), %d pending a real store, %d class exceptions", probed, len(pendingSeen), len(excepted))
}

// (wildcard proof, unit) a wildcard handler serves a literal path value by comparing it in
// code; a registered pattern cannot show that. Executed here, with no store: the literal
// reaches the route's handler (its guard or public handler answers, never the method
// refusal or a missing route) and a planted sibling literal on the SAME wildcard is
// refused (405 or 404). The wildcards whose literal is dispatched behind the credential
// ("integration" rows) need an authenticated request on real stores and are listed
// separately (their executed proof is CHAOS-8323): the gate fails if the list and the rows
// differ.
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
	// The rows whose literal is dispatched behind the credential. Their executed proof is
	// the real-store harness of CHAOS-8323 (the Python gate is not deleted while a row is
	// here); they are named so a row cannot drift into "integration" (and out of every
	// proof) without this list changing.
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

// A 500 or 503 is a handler that failed before it judged the credential: never a refusal.
func TestAServerErrorIsNotARefusal(t *testing.T) {
	for code, want := range map[int]bool{401: true, 403: true, 200: false, 404: false, 405: false, 422: false, 500: false, 503: false} {
		if refused(code) != want {
			t.Errorf("refused(%d) = %v, want %v", code, refused(code), want)
		}
	}
}

// memberStore is the fake users/memberships store behind policy.Authenticator for the
// member-credential probe: one active, non-superuser user who is a plain "member" of one
// organization.
type memberStore struct{}

func (memberStore) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	return policy.UserState{IsActive: true}, true, nil
}
func (memberStore) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	return "member", true, nil
}
func (memberStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

// adminDeclared says whether the row's primary validator is an admin or platform-role
// check (require_admin / require_platform_role): the rows a plain member must be refused.
// memberExceptions are the admin-declared rows where Go (matching Python) serves a signed-in
// plain member: the profile row over-claims, the handler's own dependency is
// get_current_user only (CHAOS-4780, true class written at the file merge CHAOS-8306).
// Closed: each must still answer exactly its recorded status. Evidence for the one entry:
// Python src/dev_health_ops/api/admin/impersonation.py:33 (its own APIRouter, no
// require_admin) and :233-242 (Depends(get_current_user) only; a non-superuser gets
// is_impersonating=False); Go internal/apiservice/admin/impersonation.go:32-33
// (guard.Wrap(policy.Authenticated)).
var memberExceptions = map[string]int{
	"GET /api/v1/admin/impersonate/status": http.StatusOK,
}

func adminDeclared(row routeprofile.Row) bool {
	var parsed struct {
		PrimaryValidator struct {
			Description string `json:"description"`
		} `json:"primary_validator"`
	}
	if err := json.Unmarshal(row.Raw, &parsed); err != nil {
		return false
	}
	description := parsed.PrimaryValidator.Description
	return strings.Contains(description, "require_admin") || strings.Contains(description, "require_platform_role")
}

// (class, member) every go-api route whose row declares an admin or platform-role check
// refuses a signed-in plain member with exactly 403, through a fake Store behind the real
// policy.Authenticator. A route that answers anything else is Go enforcing LESS than its
// declared class: a finding for the lead with this probe as the reproduction, not a
// list entry.
func TestEveryAdminRowRefusesAPlainMember(t *testing.T) {
	const secret = "route-profile-gate-secret-key-32-bytes-long!!"
	deps, logger := mountingDepsWith(t, memberStore{})
	handler := goAPIHandlerFor(t, deps, logger)
	signer, err := edgetoken.NewSigner(secret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	user, org := uuid.New().String(), uuid.New().String()
	token, err := signer.Access(edgetoken.AccessClaims{UserID: user, Email: "member@example.test", OrgID: org, Role: "member"}, time.Now(), "gate-jti")
	if err != nil {
		t.Fatal(err)
	}
	rowOf := map[routeprofile.Pair]routeprofile.Row{}
	for _, row := range profileRows(t) {
		if row.SurfaceKind == "rest" {
			for _, pair := range row.Pairs() {
				rowOf[pair] = row
			}
		}
	}
	probed := 0
	excepted := map[string]bool{}
	for _, route := range servedRoutes(t) {
		row, ok := rowOf[route.Pair]
		if route.Plane != "go-api" || !ok || row.Classification != "protected" || !adminDeclared(row) {
			continue
		}
		url := routeprofile.Parameter.ReplaceAllString(route.Path, "x")
		request := httptest.NewRequest(route.Method, url, strings.NewReader("{}"))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		probed++
		if want, ok := memberExceptions[route.Pair.String()]; ok {
			excepted[route.Pair.String()] = true
			if recorder.Code != want {
				t.Errorf("%s: member exception recorded as %d, answers %d", route.Pair, want, recorder.Code)
			}
			continue
		}
		if recorder.Code != http.StatusForbidden {
			t.Errorf("%s: declared admin (%s), a plain member answers %d %s", route.Pair, row.ID, recorder.Code, strings.TrimSpace(recorder.Body.String()))
		}
	}
	for entry := range memberExceptions {
		if !excepted[entry] {
			t.Errorf("member exception %s names a route the probe did not reach: stale", entry)
		}
	}
	if probed < 100 {
		t.Fatalf("only %d admin rows were probed: the walk or the rows shrank", probed)
	}
	t.Logf("probed %d admin-declared routes with a plain member credential", probed)
}

// The one member exception serves a plain member only the all-null shape: no field of
// another user (the route reports whether the CALLER's own session is an impersonation).
func TestTheMemberExceptionBodyCarriesNothingOfAnotherUser(t *testing.T) {
	const secret = "route-profile-gate-secret-key-32-bytes-long!!"
	deps, logger := mountingDepsWith(t, memberStore{})
	handler := goAPIHandlerFor(t, deps, logger)
	signer, err := edgetoken.NewSigner(secret, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.Access(edgetoken.AccessClaims{UserID: uuid.New().String(), Email: "member@example.test", OrgID: uuid.New().String(), Role: "member"}, time.Now(), "gate-jti-2")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/impersonate/status", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", recorder.Body.String(), err)
	}
	want := map[string]any{"is_impersonating": false, "target_user_id": nil, "target_email": nil, "target_org_id": nil, "expires_at": nil}
	if len(body) != len(want) {
		t.Fatalf("body %v, want exactly the all-null shape %v", body, want)
	}
	for key, value := range want {
		if got, ok := body[key]; !ok || got != value {
			t.Errorf("body[%q] = %v (present %v), want %v", key, got, ok, value)
		}
	}
}
