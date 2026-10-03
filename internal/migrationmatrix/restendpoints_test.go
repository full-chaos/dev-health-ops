package migrationmatrix

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/server"
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

// The query-api route set is the production table, executed (CHAOS-8307): these tests
// pin what the matrix reads from it.

func TestLoadQueryAPIMuxRoutesIsTheProductionTable(t *testing.T) {
	want := map[string]bool{}
	for _, route := range server.RESTRoutes() {
		want[route.Pattern] = true
	}
	routes := LoadQueryAPIMuxRoutes()
	if len(routes) != len(want) || len(routes) < 20 {
		t.Fatalf("got %d routes, the production table has %d distinct patterns", len(routes), len(want))
	}
	for _, route := range routes {
		if !want[route.Path] {
			t.Errorf("%s is not a pattern of the production route table", route.Path)
		}
	}
}

// The citation names the builder's symbol and never a line: an unrelated edit above
// the builder shifts every later line and, when the citation was a line number, failed
// the doc-drift check on PRs that did not touch a route (CHAOS-6633). A renamed builder
// changes the table row and so the citation, so the drift check keeps failing for a
// real mapping change.
func TestLoadQueryAPIMuxRoutesCitesTheBuilderSymbolAndNeverALine(t *testing.T) {
	for _, route := range LoadQueryAPIMuxRoutes() {
		prefix := "internal/queryapi/server/"
		if !strings.HasPrefix(route.HandlerLoc, prefix) || !strings.Contains(route.HandlerLoc, "_route.go#build") {
			t.Errorf("%s: HandlerLoc = %q, want %s<file>_route.go#build<X>Route", route.Path, route.HandlerLoc, prefix)
			continue
		}
		if strings.ContainsAny(route.HandlerLoc[strings.LastIndex(route.HandlerLoc, "#"):], "0123456789") {
			t.Errorf("%s: citation %q carries a digit after '#': a line number crept back in", route.Path, route.HandlerLoc)
		}
	}
	if got := queryAPILoc("quadrant_route.go", "buildQuadrantRoute"); got != "internal/queryapi/server/quadrant_route.go#buildQuadrantRoute" {
		t.Fatalf("queryAPILoc = %q", got)
	}
}

// TestLoadRESTEndpointsMatchesAPathParameterRoute is a fixture-only,
// isolated-from-the-real-repo proof for the people/{person_id} route
// pair's own concern (people_summary_route.go's package doc comment): a Go 1.22+
// "{person_id}"-shaped net/http.ServeMux pattern is registered as a
// PLAIN STRING LITERAL (mux.HandleFunc's own convention every route in
// this file follows), and LoadFastAPIRoutes reads main.py's matching
// decorator's own "{person_id}"-shaped literal the same way -- so
// LoadRESTEndpoints' path-equality match (byPath, no regex, no
// mux-pattern parsing) pairs the two and renders "ported" with NO
// matcher change, the same way TestLoadFastAPIRoutesParsesSingleAndMulti
// lineDecoratorsAndMethodLists already proves LoadFastAPIRoutes alone
// handles "{work_unit_id}". This end-to-end version closes the gap that
// fixture left open: pairing against an ACTUAL mux.HandleFunc
// registration, not just the Python side.
func TestLoadRESTEndpointsMatchesAPathParameterRoute(t *testing.T) {
	mainPy := writeFrozenRoutes(t, "GET /api/v1/people/{person_id}/summary")
	muxRoutes := []QueryAPIMuxRoute{{
		Path:       "/api/v1/people/{person_id}/summary",
		HandlerLoc: queryAPILoc("people_summary_route.go", "buildPeopleSummaryRoute"),
	}}

	rows, err := LoadRESTEndpoints(mainPy, muxRoutes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %+v, want exactly one row", rows)
	}
	row := rows[0]
	if row.Method != "GET" || row.Path != "/api/v1/people/{person_id}/summary" {
		t.Fatalf("row = %+v, want GET /api/v1/people/{person_id}/summary", row)
	}
	if row.Status != RESTPorted {
		t.Fatalf("row.Status = %q, want %q -- a {person_id}-shaped path must match its identically-spelled Go mux registration", row.Status, RESTPorted)
	}
	if want := "internal/queryapi/server/people_summary_route.go#buildPeopleSummaryRoute"; row.GoHandler != want {
		t.Fatalf("row.GoHandler = %q, want %q", row.GoHandler, want)
	}
}

// TestLoadRESTEndpointsCatchesANewlyAddedPythonOnlyRoute is the red-first
// case the brief asks for: before this section existed, a route added to
// main.py with nothing on the Go side left no trace anywhere on the board.
// Seeding a fixture main.py with a route absent from query-api's mux must
// enumerate it and mark it python-only, not silently drop it.
func TestLoadRESTEndpointsCatchesANewlyAddedPythonOnlyRoute(t *testing.T) {
	mainPy := writeFrozenRoutes(t, "GET /api/v1/brand-new-endpoint")
	rows, err := LoadRESTEndpoints(mainPy, nil)
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
	mainPy := writeFrozenRoutes(t, "GET /api/v1/quadrant")
	rows, err := LoadRESTEndpoints(mainPy, []QueryAPIMuxRoute{{Path: "/api/v1/quadrant", HandlerLoc: queryAPILoc("quadrant_route.go", "buildQuadrantRoute")}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rows[0].Status != RESTPorted {
		t.Fatalf("setup check failed: got status %q, want ported before the route is removed", rows[0].Status)
	}

	// Now the Go route disappears from the table.
	rows, err = LoadRESTEndpoints(mainPy, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rows[0].Status != RESTPythonOnly {
		t.Fatalf("got status %q, want python-only once the Go mux no longer registers the route", rows[0].Status)
	}
}

func TestLoadRESTEndpointsRejectsAStaleRESTDeadByDesignEntry(t *testing.T) {
	mainPy := writeFrozenRoutes(t, "GET /api/v1/meta")

	previous := RESTDeadByDesign
	RESTDeadByDesign = map[string]string{"GET /api/v1/a-route-that-does-not-exist": "CHAOS-0000: fake citation for this test"}
	defer func() { RESTDeadByDesign = previous }()

	if _, err := LoadRESTEndpoints(mainPy, nil); err == nil {
		t.Fatal("expected an error for a RESTDeadByDesign entry naming a route main.py no longer declares")
	}
}

func TestLoadRESTEndpointsAppliesRESTDeadByDesign(t *testing.T) {
	mainPy := writeFrozenRoutes(t, "GET /api/v1/meta")

	previous := RESTDeadByDesign
	RESTDeadByDesign = map[string]string{"GET /api/v1/meta": "CHAOS-0000: chris ruled this stays Python (test fixture)"}
	defer func() { RESTDeadByDesign = previous }()

	rows, err := LoadRESTEndpoints(mainPy, nil)
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
		{Method: "POST", Path: "/api/v1/investment/explain", Status: RESTPorted, GoHandler: "internal/queryapi/server/investment_explain_route.go:119"},
		{Method: "GET", Path: "/api/v1/meta", Status: RESTPythonOnly},
		{Method: "GET", Path: "/api/v1/opportunities", Status: RESTDeadByDesignStatus, Note: "CHAOS-0000"},
	}
	got := RenderRESTEndpointsBlock(rows)
	want := "_3 `/api/v1/*` routes in the frozen Python api route list (`contracts/migration-status/v1/python-rest-routes.json`): **1** ported, **1** python-only, **1** dead-by-design._\n\n" +
		"| Method | Path | Status | Go handler |\n" +
		"| --- | --- | --- | --- |\n" +
		"| POST | `/api/v1/investment/explain` | ported | `internal/queryapi/server/investment_explain_route.go:119` |\n" +
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
		filepath.Join(root, FrozenRESTRoutesRelative),
		LoadQueryAPIMuxRoutes(),
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
	if homeGet.Status != RESTPorted || !strings.Contains(homeGet.GoHandler, "home_route.go") {
		t.Fatalf("GET /api/v1/home = %+v, want ported at home_route.go", homeGet)
	}
	homePost, ok := byKey["POST /api/v1/home"]
	if !ok {
		t.Fatal("expected POST /api/v1/home to be enumerated")
	}
	if homePost.Status != RESTPorted || !strings.Contains(homePost.GoHandler, "home_route.go") {
		t.Fatalf("POST /api/v1/home = %+v, want ported at home_route.go", homePost)
	}

	opportunitiesGet, ok := byKey["GET /api/v1/opportunities"]
	if !ok {
		t.Fatal("expected GET /api/v1/opportunities to be enumerated")
	}
	if opportunitiesGet.Status != RESTPorted || !strings.Contains(opportunitiesGet.GoHandler, "opportunities_route.go") {
		t.Fatalf("GET /api/v1/opportunities = %+v, want ported at opportunities_route.go", opportunitiesGet)
	}
	opportunitiesPost, ok := byKey["POST /api/v1/opportunities"]
	if !ok {
		t.Fatal("expected POST /api/v1/opportunities to be enumerated")
	}
	if opportunitiesPost.Status != RESTPorted || !strings.Contains(opportunitiesPost.GoHandler, "opportunities_route.go") {
		t.Fatalf("POST /api/v1/opportunities = %+v, want ported at opportunities_route.go", opportunitiesPost)
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

	drilldownIssuesPost, ok := byKey["POST /api/v1/drilldown/issues"]
	if !ok {
		t.Fatal("expected POST /api/v1/drilldown/issues to be enumerated")
	}
	if drilldownIssuesPost.Status != RESTPorted || !strings.Contains(drilldownIssuesPost.GoHandler, "drilldown_issues_route.go") {
		t.Fatalf("POST /api/v1/drilldown/issues = %+v, want ported at drilldown_issues_route.go", drilldownIssuesPost)
	}

	drilldownIssuesGet, ok := byKey["GET /api/v1/drilldown/issues"]
	if !ok {
		t.Fatal("expected GET /api/v1/drilldown/issues to be enumerated")
	}
	// Same matching-by-PATH note as drilldownPRsGet's own comment above:
	// one mux registration serves both methods, so both Python method
	// rows render "ported".
	if drilldownIssuesGet.Status != RESTPorted || !strings.Contains(drilldownIssuesGet.GoHandler, "drilldown_issues_route.go") {
		t.Fatalf("GET /api/v1/drilldown/issues = %+v, want ported at drilldown_issues_route.go", drilldownIssuesGet)
	}

	explainPost, ok := byKey["POST /api/v1/explain"]
	if !ok {
		t.Fatal("expected POST /api/v1/explain to be enumerated")
	}
	if explainPost.Status != RESTPorted || !strings.Contains(explainPost.GoHandler, "explain_route.go") {
		t.Fatalf("POST /api/v1/explain = %+v, want ported at explain_route.go", explainPost)
	}

	explainGet, ok := byKey["GET /api/v1/explain"]
	if !ok {
		t.Fatal("expected GET /api/v1/explain to be enumerated")
	}
	// Same both-methods-actually-wired case as drilldown/prs above.
	if explainGet.Status != RESTPorted || !strings.Contains(explainGet.GoHandler, "explain_route.go") {
		t.Fatalf("GET /api/v1/explain = %+v, want ported at explain_route.go", explainGet)
	}

	peopleGet, ok := byKey["GET /api/v1/people"]
	if !ok {
		t.Fatal("expected GET /api/v1/people to be enumerated")
	}
	if peopleGet.Status != RESTPorted || !strings.Contains(peopleGet.GoHandler, "people_route.go") {
		t.Fatalf("GET /api/v1/people = %+v, want ported at people_route.go", peopleGet)
	}

	// The first PATH-PARAMETER routes this repo ports -- query-api's
	// mux registers "/api/v1/people/{person_id}/summary" as a
	// literal string (mux.HandleFunc's own registration convention every
	// route in this file follows), and LoadFastAPIRoutes reads main.py's
	// own "{person_id}"-shaped decorator literal verbatim too: both sides
	// are plain string equality on the SAME "{person_id}" text, so no
	// matcher change was needed for a "{...}" segment to render "ported".
	peopleSummaryGet, ok := byKey["GET /api/v1/people/{person_id}/summary"]
	if !ok {
		t.Fatal("expected GET /api/v1/people/{person_id}/summary to be enumerated")
	}
	if peopleSummaryGet.Status != RESTPorted || !strings.Contains(peopleSummaryGet.GoHandler, "people_summary_route.go") {
		t.Fatalf("GET /api/v1/people/{person_id}/summary = %+v, want ported at people_summary_route.go", peopleSummaryGet)
	}

	// Same path-parameter shape, sibling route.
	peopleMetricGet, ok := byKey["GET /api/v1/people/{person_id}/metric"]
	if !ok {
		t.Fatal("expected GET /api/v1/people/{person_id}/metric to be enumerated")
	}
	if peopleMetricGet.Status != RESTPorted || !strings.Contains(peopleMetricGet.GoHandler, "people_metric_route.go") {
		t.Fatalf("GET /api/v1/people/{person_id}/metric = %+v, want ported at people_metric_route.go", peopleMetricGet)
	}

	flameGet, ok := byKey["GET /api/v1/flame"]
	if !ok {
		t.Fatal("expected GET /api/v1/flame to be enumerated")
	}
	if flameGet.Status != RESTPorted || !strings.Contains(flameGet.GoHandler, "flame_route.go") {
		t.Fatalf("GET /api/v1/flame = %+v, want ported at flame_route.go", flameGet)
	}

	// Same path-parameter shape, sibling routes (own package doc comment:
	// api/services/people.py also backs these two drilldown routes,
	// sibling files of internal/people rather than one-off packages).
	peopleDrilldownPRsGet, ok := byKey["GET /api/v1/people/{person_id}/drilldown/prs"]
	if !ok {
		t.Fatal("expected GET /api/v1/people/{person_id}/drilldown/prs to be enumerated")
	}
	if peopleDrilldownPRsGet.Status != RESTPorted || !strings.Contains(peopleDrilldownPRsGet.GoHandler, "people_drilldown_prs_route.go") {
		t.Fatalf("GET /api/v1/people/{person_id}/drilldown/prs = %+v, want ported at people_drilldown_prs_route.go", peopleDrilldownPRsGet)
	}

	peopleDrilldownIssuesGet, ok := byKey["GET /api/v1/people/{person_id}/drilldown/issues"]
	if !ok {
		t.Fatal("expected GET /api/v1/people/{person_id}/drilldown/issues to be enumerated")
	}
	if peopleDrilldownIssuesGet.Status != RESTPorted || !strings.Contains(peopleDrilldownIssuesGet.GoHandler, "people_drilldown_issues_route.go") {
		t.Fatalf("GET /api/v1/people/{person_id}/drilldown/issues = %+v, want ported at people_drilldown_issues_route.go", peopleDrilldownIssuesGet)
	}

	flameAggregatedGet, ok := byKey["GET /api/v1/flame/aggregated"]
	if !ok {
		t.Fatal("expected GET /api/v1/flame/aggregated to be enumerated")
	}
	if flameAggregatedGet.Status != RESTPorted || !strings.Contains(flameAggregatedGet.GoHandler, "flame_aggregated_route.go") {
		t.Fatalf("GET /api/v1/flame/aggregated = %+v, want ported at flame_aggregated_route.go", flameAggregatedGet)
	}

	heatmapGet, ok := byKey["GET /api/v1/heatmap"]
	if !ok {
		t.Fatal("expected GET /api/v1/heatmap to be enumerated")
	}
	if heatmapGet.Status != RESTPorted || !strings.Contains(heatmapGet.GoHandler, "heatmap_route.go") {
		t.Fatalf("GET /api/v1/heatmap = %+v, want ported at heatmap_route.go", heatmapGet)
	}

	sankeyGet, ok := byKey["GET /api/v1/sankey"]
	if !ok {
		t.Fatal("expected GET /api/v1/sankey to be enumerated")
	}
	if sankeyGet.Status != RESTPorted || !strings.Contains(sankeyGet.GoHandler, "sankey_route.go") {
		t.Fatalf("GET /api/v1/sankey = %+v, want ported at sankey_route.go", sankeyGet)
	}
	sankeyPost, ok := byKey["POST /api/v1/sankey"]
	if !ok {
		t.Fatal("expected POST /api/v1/sankey to be enumerated")
	}
	if sankeyPost.Status != RESTPorted || !strings.Contains(sankeyPost.GoHandler, "sankey_route.go") {
		t.Fatalf("POST /api/v1/sankey = %+v, want ported at sankey_route.go", sankeyPost)
	}

	investmentFlowPost, ok := byKey["POST /api/v1/investment/flow"]
	if !ok {
		t.Fatal("expected POST /api/v1/investment/flow to be enumerated")
	}
	if investmentFlowPost.Status != RESTPorted || !strings.Contains(investmentFlowPost.GoHandler, "investment_flow_route.go") {
		t.Fatalf("POST /api/v1/investment/flow = %+v, want ported at investment_flow_route.go", investmentFlowPost)
	}
	investmentFlowRepoTeamPost, ok := byKey["POST /api/v1/investment/flow/repo-team"]
	if !ok {
		t.Fatal("expected POST /api/v1/investment/flow/repo-team to be enumerated")
	}
	if investmentFlowRepoTeamPost.Status != RESTPorted || !strings.Contains(investmentFlowRepoTeamPost.GoHandler, "investment_flow_route.go") {
		t.Fatalf("POST /api/v1/investment/flow/repo-team = %+v, want ported at investment_flow_route.go", investmentFlowRepoTeamPost)
	}

	investmentGet, ok := byKey["GET /api/v1/investment"]
	if !ok {
		t.Fatal("expected GET /api/v1/investment to be enumerated")
	}
	if investmentGet.Status != RESTPorted || !strings.Contains(investmentGet.GoHandler, "investment_route.go") {
		t.Fatalf("GET /api/v1/investment = %+v, want ported at investment_route.go", investmentGet)
	}
	investmentPost, ok := byKey["POST /api/v1/investment"]
	if !ok {
		t.Fatal("expected POST /api/v1/investment to be enumerated")
	}
	if investmentPost.Status != RESTPorted || !strings.Contains(investmentPost.GoHandler, "investment_route.go") {
		t.Fatalf("POST /api/v1/investment = %+v, want ported at investment_route.go", investmentPost)
	}
	investmentSunburstGet, ok := byKey["GET /api/v1/investment/sunburst"]
	if !ok {
		t.Fatal("expected GET /api/v1/investment/sunburst to be enumerated")
	}
	if investmentSunburstGet.Status != RESTPorted || !strings.Contains(investmentSunburstGet.GoHandler, "investment_route.go") {
		t.Fatalf("GET /api/v1/investment/sunburst = %+v, want ported at investment_route.go", investmentSunburstGet)
	}

	workUnitExplainPost, ok := byKey["POST /api/v1/work-units/{work_unit_id}/explain"]
	if !ok {
		t.Fatal("expected POST /api/v1/work-units/{work_unit_id}/explain to be enumerated")
	}
	if workUnitExplainPost.Status != RESTPorted || !strings.Contains(workUnitExplainPost.GoHandler, "workunit_explain_route.go") {
		t.Fatalf("POST /api/v1/work-units/{work_unit_id}/explain = %+v, want ported at workunit_explain_route.go", workUnitExplainPost)
	}

	ported, pythonOnly, _ := RESTEndpointCounts(rows)
	if ported != 32 {
		t.Fatalf("got %d ported routes, want exactly 32 (POST /api/v1/investment/explain, GET /api/v1/quadrant, "+
			"GET /api/v1/filters/options, POST+GET /api/v1/drilldown/prs, GET /api/v1/meta, "+
			"POST+GET /api/v1/drilldown/issues, POST+GET /api/v1/explain, GET /api/v1/people, "+
			"GET /api/v1/people/{person_id}/summary, GET /api/v1/people/{person_id}/metric, GET /api/v1/flame, "+
			"GET /api/v1/people/{person_id}/drilldown/prs, GET /api/v1/people/{person_id}/drilldown/issues, "+
			"GET /api/v1/flame/aggregated, GET /api/v1/heatmap, POST+GET /api/v1/sankey, "+
			"POST /api/v1/investment/flow, POST /api/v1/investment/flow/repo-team, "+
			"POST+GET /api/v1/investment, GET /api/v1/investment/sunburst, POST+GET /api/v1/home, "+
			"POST+GET /api/v1/work-units, POST /api/v1/work-units/{work_unit_id}/explain, "+
			"POST+GET /api/v1/opportunities) -- "+
			"if this changed, a route was ported or un-ported; update this pin, it is not stale by accident", ported)
	}
	// Every REST route main.py declares is served by a Go handler. A
	// python-only row reappearing means a route was added on the reference
	// plane with no Go counterpart, or a Go handler stopped registering its
	// path -- both of which this count catches where the ported pin above
	// cannot, since a route added on BOTH planes keeps the ported total
	// moving in step with the python-only one.
	if pythonOnly != 0 {
		t.Fatalf("got %d python-only routes, want 0: every /api/v1/* route main.py declares is registered by a Go handler", pythonOnly)
	}
}

// writeFrozenRoutes writes a frozen route list holding the given
// "METHOD /path" entries and returns its path.
func writeFrozenRoutes(t *testing.T, entries ...string) string {
	t.Helper()
	var frozen FrozenRESTRoutes
	for _, entry := range entries {
		method, path, _ := strings.Cut(entry, " ")
		frozen.Routes = append(frozen.Routes, FrozenRESTRoute{Method: method, Path: path})
	}
	raw, err := json.Marshal(frozen)
	if err != nil {
		t.Fatal(err)
	}
	return writeTempFile(t, t.TempDir(), "python-rest-routes.json", string(raw))
}
