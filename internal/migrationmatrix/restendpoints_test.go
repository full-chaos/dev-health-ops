package migrationmatrix

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTempFile(t *testing.T, dir, rel, body string) string {
	t.Helper()
	path := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", rel, err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
	return path
}

const fixtureMainPy = `from fastapi import FastAPI

app = FastAPI()


@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    return {"ok": True}


@app.get("/api/v1/meta", response_model=MetaResponse)
async def meta():
    ...


@app.post("/api/v1/home", response_model=HomeResponse)
@limiter.limit("60/minute")
async def home_post(payload: HomeRequest):
    ...


@app.post(
    "/api/v1/work-units/{work_unit_id}/explain",
    response_model=WorkUnitExplanation,
)
@limiter.limit("20/minute")
async def work_unit_explain_endpoint(request: Request):
    ...
`

func TestLoadFastAPIRoutesParsesSingleAndMultilineDecoratorsAndMethodLists(t *testing.T) {
	path := writeTempFile(t, t.TempDir(), "main.py", fixtureMainPy)
	routes, err := LoadFastAPIRoutes(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []RESTRoute{
		{Method: "GET", Path: "/health"},
		{Method: "HEAD", Path: "/health"},
		{Method: "GET", Path: "/api/v1/meta"},
		{Method: "POST", Path: "/api/v1/home"},
		{Method: "POST", Path: "/api/v1/work-units/{work_unit_id}/explain"},
	}
	if len(routes) != len(want) {
		t.Fatalf("got %d routes, want %d: %+v", len(routes), len(want), routes)
	}
	for i, w := range want {
		if routes[i].Method != w.Method || routes[i].Path != w.Path {
			t.Fatalf("route %d = %+v, want method=%s path=%s", i, routes[i], w.Method, w.Path)
		}
		if routes[i].Line == 0 {
			t.Fatalf("route %d has no decorator line recorded: %+v", i, routes[i])
		}
	}
}

func TestLoadFastAPIRoutesRefusesARouteWithNoLiteralPath(t *testing.T) {
	path := writeTempFile(t, t.TempDir(), "main.py", "@app.get(SOME_VARIABLE_PATH)\nasync def x():\n    ...\n")
	if _, err := LoadFastAPIRoutes(path); err == nil {
		t.Fatal("expected an error for a route with no string literal path")
	}
}

func TestLoadFastAPIRoutesRefusesAnAPIRouteWithNoMethodsList(t *testing.T) {
	path := writeTempFile(t, t.TempDir(), "main.py", "@app.api_route(\"/x\")\nasync def x():\n    ...\n")
	if _, err := LoadFastAPIRoutes(path); err == nil {
		t.Fatal("expected an error for api_route with no methods=[...] list")
	}
}

func TestApiV1RoutesFiltersOutNonAPIRoutes(t *testing.T) {
	routes := []RESTRoute{
		{Method: "GET", Path: "/health"},
		{Method: "GET", Path: "/api/v1/meta"},
	}
	got := apiV1Routes(routes)
	if len(got) != 1 || got[0].Path != "/api/v1/meta" {
		t.Fatalf("got %+v, want only /api/v1/meta", got)
	}
}

const fixtureQueryAPIMain = `package main

import "net/http"

func main() {
	mux := http.NewServeMux()
	if quadrantHandler, quadrantCleanup, quadrantOK, quadrantErr := buildQuadrantRoute(); quadrantErr != nil {
		panic(quadrantErr)
	} else if quadrantOK {
		defer quadrantCleanup()
		mux.HandleFunc("/api/v1/quadrant", quadrantHandler)
	}
}
`

const fixtureQuadrantRoute = `package main

import "net/http"

func buildQuadrantRoute() (handler http.HandlerFunc, cleanup func(), ok bool, err error) {
	return nil, func() {}, true, nil
}
`

func TestLoadQueryAPIMuxRoutesResolvesTheBuilderFunctionDefinition(t *testing.T) {
	dir := t.TempDir()
	writeTempFile(t, dir, "main.go", fixtureQueryAPIMain)
	writeTempFile(t, dir, "quadrant_route.go", fixtureQuadrantRoute)

	routes, err := LoadQueryAPIMuxRoutes(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(routes) != 1 || routes[0].Path != "/api/v1/quadrant" {
		t.Fatalf("got %+v, want exactly one /api/v1/quadrant route", routes)
	}
	if !strings.HasSuffix(routes[0].HandlerLoc, "quadrant_route.go:5") {
		t.Fatalf("HandlerLoc = %q, want it to resolve to buildQuadrantRoute's definition (quadrant_route.go:5)", routes[0].HandlerLoc)
	}
}

func TestLoadQueryAPIMuxRoutesFallsBackToTheMountSiteWhenTheBuilderCannotBeResolved(t *testing.T) {
	dir := t.TempDir()
	writeTempFile(t, dir, "main.go", `package main

import "net/http"

func main() {
	mux := http.NewServeMux()
	handler := inlineHandler()
	mux.HandleFunc("/api/v1/quadrant", handler)
}

func inlineHandler() http.HandlerFunc { return nil }
`)
	routes, err := LoadQueryAPIMuxRoutes(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(routes) != 1 {
		t.Fatalf("got %+v, want exactly one route", routes)
	}
	if !strings.Contains(routes[0].HandlerLoc, "main.go:") {
		t.Fatalf("HandlerLoc = %q, want a main.go fallback location", routes[0].HandlerLoc)
	}
}

func TestLoadQueryAPIMuxRoutesIgnoresTestFiles(t *testing.T) {
	dir := t.TempDir()
	writeTempFile(t, dir, "main.go", `package main

func main() {}
`)
	writeTempFile(t, dir, "main_test.go", `package main

import "net/http"

func init() {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/from-a-test-file", nil)
}
`)
	routes, err := LoadQueryAPIMuxRoutes(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(routes) != 0 {
		t.Fatalf("expected _test.go files to be excluded, got %+v", routes)
	}
}

// TestLoadRESTEndpointsCatchesANewlyAddedPythonOnlyRoute is the red-first
// case the brief asks for: before this section existed, a route added to
// main.py with nothing on the Go side left no trace anywhere on the board.
// Seeding a fixture main.py with a route absent from query-api's mux must
// enumerate it and mark it python-only, not silently drop it.
func TestLoadRESTEndpointsCatchesANewlyAddedPythonOnlyRoute(t *testing.T) {
	dir := t.TempDir()
	mainPy := writeTempFile(t, dir, "main.py", `@app.get("/api/v1/brand-new-endpoint")
async def brand_new():
    ...
`)
	queryAPIDir := filepath.Join(dir, "query-api")
	writeTempFile(t, queryAPIDir, "main.go", `package main

func main() {}
`)

	rows, err := LoadRESTEndpoints(mainPy, queryAPIDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %+v, want exactly one enumerated row", rows)
	}
	if rows[0].Status != RESTPythonOnly {
		t.Fatalf("got status %q, want python-only for a route with no Go registration", rows[0].Status)
	}
}

// TestLoadRESTEndpointsFlipsToPythonOnlyWhenTheGoRouteDisappears is the gate
// test: a route previously ported loses its Go registration and the row
// must flip, not keep reporting ported from a stale memory.
func TestLoadRESTEndpointsFlipsToPythonOnlyWhenTheGoRouteDisappears(t *testing.T) {
	dir := t.TempDir()
	mainPy := writeTempFile(t, dir, "main.py", `@app.get("/api/v1/quadrant")
async def quadrant():
    ...
`)
	queryAPIDir := filepath.Join(dir, "query-api")

	writeTempFile(t, queryAPIDir, "main.go", `package main

import "net/http"

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/quadrant", nil)
}
`)
	rows, err := LoadRESTEndpoints(mainPy, queryAPIDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rows[0].Status != RESTPorted {
		t.Fatalf("setup check failed: got status %q, want ported before the route is removed", rows[0].Status)
	}

	// Now the Go route disappears -- the same file, re-registered with
	// nothing left in it.
	writeTempFile(t, queryAPIDir, "main.go", `package main

func main() {}
`)
	rows, err = LoadRESTEndpoints(mainPy, queryAPIDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rows[0].Status != RESTPythonOnly {
		t.Fatalf("got status %q, want python-only once the Go mux no longer registers the route", rows[0].Status)
	}
}

func TestLoadRESTEndpointsRejectsAStaleRESTDeadByDesignEntry(t *testing.T) {
	dir := t.TempDir()
	mainPy := writeTempFile(t, dir, "main.py", `@app.get("/api/v1/meta")
async def meta():
    ...
`)
	queryAPIDir := filepath.Join(dir, "query-api")
	writeTempFile(t, queryAPIDir, "main.go", "package main\n\nfunc main() {}\n")

	previous := RESTDeadByDesign
	RESTDeadByDesign = map[string]string{"GET /api/v1/a-route-that-does-not-exist": "CHAOS-0000: fake citation for this test"}
	defer func() { RESTDeadByDesign = previous }()

	if _, err := LoadRESTEndpoints(mainPy, queryAPIDir); err == nil {
		t.Fatal("expected an error for a RESTDeadByDesign entry naming a route main.py no longer declares")
	}
}

func TestLoadRESTEndpointsAppliesRESTDeadByDesign(t *testing.T) {
	dir := t.TempDir()
	mainPy := writeTempFile(t, dir, "main.py", `@app.get("/api/v1/meta")
async def meta():
    ...
`)
	queryAPIDir := filepath.Join(dir, "query-api")
	writeTempFile(t, queryAPIDir, "main.go", "package main\n\nfunc main() {}\n")

	previous := RESTDeadByDesign
	RESTDeadByDesign = map[string]string{"GET /api/v1/meta": "CHAOS-0000: chris ruled this stays Python (test fixture)"}
	defer func() { RESTDeadByDesign = previous }()

	rows, err := LoadRESTEndpoints(mainPy, queryAPIDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rows[0].Status != RESTDeadByDesignStatus {
		t.Fatalf("got status %q, want dead-by-design", rows[0].Status)
	}
	if rows[0].Note == "" {
		t.Fatal("expected the dead-by-design citation to be carried onto the row")
	}
}

func TestRenderRESTEndpointsBlockRendersCountsAndRowsGolden(t *testing.T) {
	rows := []RESTEndpointRow{
		{Method: "POST", Path: "/api/v1/investment/explain", Status: RESTPorted, GoHandler: "cmd/query-api/investment_explain_route.go:119"},
		{Method: "GET", Path: "/api/v1/meta", Status: RESTPythonOnly},
		{Method: "GET", Path: "/api/v1/opportunities", Status: RESTDeadByDesignStatus, Note: "CHAOS-0000"},
	}
	got := RenderRESTEndpointsBlock(rows)
	want := "_3 `/api/v1/*` routes in `src/dev_health_ops/api/main.py`: **1** ported, **1** python-only, **1** dead-by-design._\n\n" +
		"| Method | Path | Status | Go handler |\n" +
		"| --- | --- | --- | --- |\n" +
		"| POST | `/api/v1/investment/explain` | ported | `cmd/query-api/investment_explain_route.go:119` |\n" +
		"| GET | `/api/v1/meta` | python-only | -- |\n" +
		"| GET | `/api/v1/opportunities` | dead-by-design (CHAOS-0000) | -- |\n"
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

// TestLoadRESTEndpointsOnTheRealRepo pins today's real, mechanically-derived
// facts: /api/v1/investment/explain, /api/v1/quadrant, /api/v1/filters/options,
// /api/v1/drilldown/prs (both methods) and /api/v1/meta are ported so far,
// and a representative python-only route is still python-only. This is the
// same "byte-identity proof against the real repo" shape
// TestLoadDailyFinalizeCompatFamiliesIsEmptyOnTheRealRepo uses.
func TestLoadRESTEndpointsOnTheRealRepo(t *testing.T) {
	root := repoRootForTest(t)
	rows, err := LoadRESTEndpoints(
		filepath.Join(root, "src/dev_health_ops/api/main.py"),
		filepath.Join(root, "cmd/query-api"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	byKey := map[string]RESTEndpointRow{}
	for _, r := range rows {
		byKey[r.Method+" "+r.Path] = r
	}

	explain, ok := byKey["POST /api/v1/investment/explain"]
	if !ok {
		t.Fatal("expected POST /api/v1/investment/explain to be enumerated")
	}
	if explain.Status != RESTPorted || !strings.Contains(explain.GoHandler, "investment_explain_route.go") {
		t.Fatalf("POST /api/v1/investment/explain = %+v, want ported at investment_explain_route.go", explain)
	}

	quadrant, ok := byKey["GET /api/v1/quadrant"]
	if !ok {
		t.Fatal("expected GET /api/v1/quadrant to be enumerated")
	}
	if quadrant.Status != RESTPorted || !strings.Contains(quadrant.GoHandler, "quadrant_route.go") {
		t.Fatalf("GET /api/v1/quadrant = %+v, want ported at quadrant_route.go", quadrant)
	}

	filterOptions, ok := byKey["GET /api/v1/filters/options"]
	if !ok {
		t.Fatal("expected GET /api/v1/filters/options to be enumerated")
	}
	if filterOptions.Status != RESTPorted || !strings.Contains(filterOptions.GoHandler, "filter_options_route.go") {
		t.Fatalf("GET /api/v1/filters/options = %+v, want ported at filter_options_route.go", filterOptions)
	}

	meta, ok := byKey["GET /api/v1/meta"]
	if !ok {
		t.Fatal("expected GET /api/v1/meta to be enumerated")
	}
	if meta.Status != RESTPorted || !strings.Contains(meta.GoHandler, "meta_route.go") {
		t.Fatalf("GET /api/v1/meta = %+v, want ported at meta_route.go", meta)
	}

	homeGet, ok := byKey["GET /api/v1/home"]
	if !ok {
		t.Fatal("expected GET /api/v1/home to be enumerated")
	}
	if homeGet.Status != RESTPythonOnly {
		t.Fatalf("GET /api/v1/home = %+v, want python-only (query-api registers no such route)", homeGet)
	}

	drilldownPRsPost, ok := byKey["POST /api/v1/drilldown/prs"]
	if !ok {
		t.Fatal("expected POST /api/v1/drilldown/prs to be enumerated")
	}
	if drilldownPRsPost.Status != RESTPorted || !strings.Contains(drilldownPRsPost.GoHandler, "drilldown_prs_route.go") {
		t.Fatalf("POST /api/v1/drilldown/prs = %+v, want ported at drilldown_prs_route.go", drilldownPRsPost)
	}

	drilldownPRsGet, ok := byKey["GET /api/v1/drilldown/prs"]
	if !ok {
		t.Fatal("expected GET /api/v1/drilldown/prs to be enumerated")
	}
	// This package's own doc comment on matching-by-PATH (not (METHOD,
	// PATH)) applies here: query-api's mux registers ONE path serving
	// both methods, so both Python method rows render "ported" even
	// though this is the real, both-methods-actually-wired case that
	// comment flags as a known simplification for the OTHER (partial)
	// case.
	if drilldownPRsGet.Status != RESTPorted || !strings.Contains(drilldownPRsGet.GoHandler, "drilldown_prs_route.go") {
		t.Fatalf("GET /api/v1/drilldown/prs = %+v, want ported at drilldown_prs_route.go", drilldownPRsGet)
	}

	ported, _, _ := RESTEndpointCounts(rows)
	if ported != 6 {
		t.Fatalf("got %d ported routes, want exactly 6 (POST /api/v1/investment/explain, GET /api/v1/quadrant, "+
			"GET /api/v1/filters/options, POST+GET /api/v1/drilldown/prs, GET /api/v1/meta) -- if this changed, "+
			"a route was ported or un-ported; update this pin, it is not stale by accident", ported)
	}
}
