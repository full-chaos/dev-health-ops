package migrationmatrix

// This file adds the ONE section the board never had: a per-endpoint row for
// the FastAPI REST surface (`src/dev_health_ops/api/main.py`'s `/api/v1/*`
// routes), cross-referenced against query-api's registered mux routes.
//
// Every other section on this page answers "which language executes this" for
// a family, a Go-API GraphQL operation, or a provider/dataset pair -- and the
// REST surface was simply absent, which is its own answer read the wrong way:
// a reader scanning the page for "is /api/v1/investment ported" found
// nothing, not a python-only row saying so. Chris, on this exact class:
// "WTF is the migration status board that keeps getting forgotten?"
//
// BOTH sides are read from the code on every render AND every check -- the
// Python side from committed source, query-api's side by EXECUTING its
// production route table (internal/queryapi/server.RESTRoutes, CHAOS-8307) --
// there is no curated per-row ledger to go stale, unlike status.json's family
// rows. The only hand-curated input is
// RESTDeadByDesign, and it stays empty until a route carries chris's word in
// an existing record; see its own doc comment for why "python-only" is the
// default a route earns by simply existing.
//
// MATCHING IS BY PATH, NOT (METHOD, PATH). query-api's `http.ServeMux`
// patterns here are plain paths (a route table row's Pattern, mounted with
// mux.HandleFunc), not Go 1.22 method-prefixed patterns -- the mux itself does not
// discriminate by method, each handler checks `r.Method` internally and
// answers 405 otherwise. Today every `/api/v1/*` path query-api registers
// has exactly one Python method, so path matching and method matching agree.
// If a future path carries two Python methods (a GET and a POST at the same
// URL) while only one is actually wired in the Go handler, this renders BOTH
// method rows "ported" -- a known simplification, flagged here rather than
// discovered later. The route table now DECLARES each pattern's methods
// (server.RESTRoutes, pinned against the handlers by the server package's own
// tests), so a method-aware match is possible; this page keeps the path-level
// match until it is changed on purpose.
import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/server"
)

// Marker pair bounding the new block. Same convention as every other
// generated block on the page.
const (
	RESTEndpointsBegin = "<!-- BEGIN GENERATED REST ENDPOINTS -->"
	RESTEndpointsEnd   = "<!-- END GENERATED REST ENDPOINTS -->"
)

// RESTEndpointStatus is the migration status of one FastAPI route.
type RESTEndpointStatus string

const (
	// RESTPorted -- query-api's mux registers this route's path.
	RESTPorted RESTEndpointStatus = "ported"
	// RESTPythonOnly -- the honest default. No Go route exists for this
	// path, and nothing has said it never will.
	RESTPythonOnly RESTEndpointStatus = "python-only"
	// RESTDeadByDesignStatus -- chris's word, on record, that this route is
	// staying Python. See RESTDeadByDesign.
	RESTDeadByDesignStatus RESTEndpointStatus = "dead-by-design"
	// RESTProven -- RESTPorted, AND an admissible go-api-rest-prove receipt
	// exists for this route at the candidate build ApplyRESTProof was given.
	// See restproven.go.
	RESTProven RESTEndpointStatus = "proven"
)

// RESTRoute is one route mechanically parsed from main.py: one (method,
// path) pair, as FastAPI itself would register it.
type RESTRoute struct {
	Method string
	Path   string
	// Line is the 1-indexed line of the route's decorator in main.py, for a
	// reader who wants to look at the source.
	Line int
}

// QueryAPIMuxRoute is one `/api/v1/*` path query-api's mux registers.
type QueryAPIMuxRoute struct {
	Path string
	// HandlerLoc is "<file>#<builder function>" of the route's builder function
	// when it could be resolved (see LoadQueryAPIMuxRoutes), or
	// "<file>#HandleFunc(<route>)" -- the mux registration, named by its route
	// -- as a fallback. It cites a SYMBOL, never a line number: a line moves
	// whenever anything above it in the file is edited, which failed the
	// doc-drift check on unrelated PRs (CHAOS-6633).
	HandlerLoc string
}

// RESTEndpointRow is one row of the "Per REST endpoint" table.
type RESTEndpointRow struct {
	Method    string
	Path      string
	Status    RESTEndpointStatus
	GoHandler string // set only for RESTPorted/RESTProven rows
	Note      string // dead-by-design citation, set only for that status
	// Proven is DERIVED, never asserted: the id of an admissible
	// go-api-rest-prove receipt for this row, or goapiproof-equivalent
	// NoProof. Empty (not NoProof) until ApplyRESTProof runs -- LoadRESTEndpoints
	// itself never sets it, matching OperationRow.Proven's own "read live,
	// never invented" discipline. See restproven.go.
	Proven string
}

// RESTDeadByDesign is CURATED: the only hand-maintained input to this
// section. A ("METHOD", "/path") pair keyed here renders "dead-by-design"
// instead of the honest default "python-only" -- and may be added ONLY when
// chris's word already exists in an existing record or ticket saying that
// route is staying Python on purpose. Nil today: no `/api/v1/*` route has
// that word on record yet. The next such ruling adds one entry here, cited
// by the record it came from -- never a guess at what "probably" won't be
// ported. Declared nil, not an empty map literal, and only ever read
// through restDeadByDesignKeys/restDeadByDesignCitation below, both of
// which guard on len() first -- so a range or index of this variable is
// never dead code reachable only through an always-empty map.
var RESTDeadByDesign map[string]string

// restDeadByDesignKeys returns RESTDeadByDesign's keys, or nil while the
// ledger is empty.
func restDeadByDesignKeys() []string {
	if len(RESTDeadByDesign) == 0 {
		return nil
	}
	keys := make([]string, 0, len(RESTDeadByDesign))
	for key := range RESTDeadByDesign {
		keys = append(keys, key)
	}
	return keys
}

// restDeadByDesignCitation looks up one ("METHOD", "/path") key's citation.
func restDeadByDesignCitation(key string) (string, bool) {
	if len(RESTDeadByDesign) == 0 {
		return "", false
	}
	citation, ok := RESTDeadByDesign[key]
	return citation, ok
}

var (
	restRouteDecoratorRe = regexp.MustCompile(`^@app\.(get|post|put|delete|patch|api_route)\(`)
	restStringLiteralRe  = regexp.MustCompile(`"([^"]*)"|'([^']*)'`)
	restMethodsListRe    = regexp.MustCompile(`methods\s*=\s*\[([^\]]*)\]`)
)

// LoadFastAPIRoutes mechanically parses every `@app.<verb>(...)` /
// `@app.api_route(...)` decorator in a FastAPI source file into its
// (method, path) pairs -- a decorator-shaped scan, not an AST walk, in the
// same spirit as extractPythonFunctionBody's line-based scan of
// job_daily.py: it finds every route registration exactly as FastAPI itself
// would read the decorator, without resolving anything reached only through
// an indirection (a route built from a variable path or a methods list
// assembled elsewhere is not a shape this codebase's `main.py` uses).
func LoadFastAPIRoutes(path string) ([]RESTRoute, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // repo-relative path
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	lines := strings.Split(string(raw), "\n")

	var routes []RESTRoute
	for i := 0; i < len(lines); i++ {
		verbMatch := restRouteDecoratorRe.FindStringSubmatch(lines[i])
		if verbMatch == nil {
			continue
		}
		declLine := i + 1
		verb := verbMatch[1]

		// The decorator call can span multiple lines (a `response_model=`
		// kwarg on its own line). Accumulate lines by paren balance, the
		// same "read until the construct actually ends" approach
		// extractPythonFunctionBody uses for the enclosing function -- here
		// applied to one call instead of one def.
		depth := 0
		var b strings.Builder
		end := i
		for j := i; j < len(lines); j++ {
			b.WriteString(lines[j])
			b.WriteString("\n")
			depth += strings.Count(lines[j], "(") - strings.Count(lines[j], ")")
			end = j
			if depth <= 0 {
				break
			}
		}
		i = end
		decorator := b.String()

		literal := restStringLiteralRe.FindStringSubmatch(decorator)
		if literal == nil {
			return nil, fmt.Errorf("%s:%d: @app.%s(...) has no string literal path argument -- "+
				"a route built from a variable or f-string cannot be enumerated mechanically; "+
				"give it a literal path or extend LoadFastAPIRoutes for this shape", path, declLine, verb)
		}
		route := literal[1]
		if route == "" {
			route = literal[2]
		}

		methods := []string{strings.ToUpper(verb)}
		if verb == "api_route" {
			listMatch := restMethodsListRe.FindStringSubmatch(decorator)
			if listMatch == nil {
				return nil, fmt.Errorf("%s:%d: @app.api_route(%q, ...) has no methods=[...] list -- "+
					"FastAPI itself would refuse this route too", path, declLine, route)
			}
			methods = nil
			for _, m := range restStringLiteralRe.FindAllStringSubmatch(listMatch[1], -1) {
				method := m[1]
				if method == "" {
					method = m[2]
				}
				methods = append(methods, strings.ToUpper(method))
			}
			if len(methods) == 0 {
				return nil, fmt.Errorf("%s:%d: @app.api_route(%q, ...) methods=[...] names no method", path, declLine, route)
			}
		}

		for _, method := range methods {
			routes = append(routes, RESTRoute{Method: method, Path: route, Line: declLine})
		}
	}
	return routes, nil
}

// apiV1Routes filters to the /api/v1/* surface this section reports on --
// the health/readiness probes above it in main.py answer a different
// question (process liveness, not migration status) and have no query-api
// counterpart to cross-reference against.
func apiV1Routes(routes []RESTRoute) []RESTRoute {
	var out []RESTRoute
	for _, r := range routes {
		if strings.HasPrefix(r.Path, "/api/v1/") {
			out = append(out, r)
		}
	}
	return out
}

// queryAPILoc renders a "Go handler location" citation for a file inside
// query-api's own directory. It names the file relative to
// internal/queryapi/server/ -- a fixed, repo-relative label. The symbol after
// '#' is the citation's anchor: the builder function's name. Never a line
// number (CHAOS-6633): the page is regenerated from the committed sources and
// compared, so a citation that moves with unrelated edits fails every PR that
// touches the file. A renamed or removed builder still changes the citation,
// so the drift check still fails for a real mapping change.
func queryAPILoc(file, symbol string) string {
	return fmt.Sprintf("internal/queryapi/server/%s#%s", file, symbol)
}

// LoadQueryAPIMuxRoutes returns every `/api/v1/*` pattern query-api's mux mounts,
// read by EXECUTING the production route table (server.RESTRoutes, CHAOS-8307):
// the table is the data BuildWithLookup iterates to mount the routes, so this is
// a walk of the production route set, not a parse of the source text. It
// replaces the earlier regex parse of server.go, whose `mux.HandleFunc(path,
// ident)` call form every route had to keep.
//
// The routes are path-level (one entry per pattern); the declared methods
// are in server.RESTRoutes. HandlerLoc cites the builder function that
// serves the pattern.
func LoadQueryAPIMuxRoutes() []QueryAPIMuxRoute {
	seen := map[string]bool{}
	var out []QueryAPIMuxRoute
	for _, route := range server.RESTRoutes() {
		if seen[route.Pattern] {
			continue
		}
		seen[route.Pattern] = true
		out = append(out, QueryAPIMuxRoute{Path: route.Pattern, HandlerLoc: queryAPILoc(route.File, route.Builder)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// FrozenRESTRoutesRelative is the checked-in, frozen copy of the Python api's
// `/api/v1/*` route list, relative to the repository root. The Python api
// source tree is being deleted, so this section no longer reads it: the list
// below is what `LoadFastAPIRoutes` parsed out of `src/dev_health_ops/api/main.py`
// at the commit recorded in the file. While main.py still exists, a test holds
// this file to a fresh parse of it; once main.py is gone the file is the source.
const FrozenRESTRoutesRelative = "contracts/migration-status/v1/python-rest-routes.json"

// FrozenRESTRoute is one (method, path) pair of the frozen list.
type FrozenRESTRoute struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

// FrozenRESTRoutes is the on-disk shape of FrozenRESTRoutesRelative.
type FrozenRESTRoutes struct {
	Source string            `json:"source"`
	Commit string            `json:"frozen_from_commit"`
	Routes []FrozenRESTRoute `json:"routes"`
}

// LoadFrozenRESTRoutes reads the frozen Python route list. An unreadable,
// malformed or empty file is an error, never an empty section.
func LoadFrozenRESTRoutes(path string) ([]RESTRoute, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // repo-relative path
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var frozen FrozenRESTRoutes
	if err := json.Unmarshal(raw, &frozen); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	routes := make([]RESTRoute, 0, len(frozen.Routes))
	for _, r := range frozen.Routes {
		if r.Method == "" || !strings.HasPrefix(r.Path, "/api/v1/") {
			return nil, fmt.Errorf("%s: route %+v is not a method plus an /api/v1/* path", path, r)
		}
		routes = append(routes, RESTRoute{Method: r.Method, Path: r.Path})
	}
	return routes, nil
}

// LoadRESTEndpoints builds the "Per REST endpoint" rows: every /api/v1/*
// route in the frozen Python route list (frozenRoutesPath), cross-referenced
// against query-api's mux. The mux side is read fresh here; the Python side is
// a frozen list, so only RESTDeadByDesign's citations are curated beside it.
func LoadRESTEndpoints(frozenRoutesPath string, muxRoutes []QueryAPIMuxRoute) ([]RESTEndpointRow, error) {
	pyRoutes, err := LoadFrozenRESTRoutes(frozenRoutesPath)
	if err != nil {
		return nil, err
	}
	apiRoutes := apiV1Routes(pyRoutes)
	if len(apiRoutes) == 0 {
		return nil, fmt.Errorf("%s: found no /api/v1/* routes -- the frozen route list is empty or the file moved", frozenRoutesPath)
	}

	byPath := map[string]QueryAPIMuxRoute{}
	for _, m := range muxRoutes {
		byPath[m.Path] = m
	}

	// Completeness guard, the same direction R1-unknown-family checks for
	// the family ledger: a RESTDeadByDesign entry naming a route main.py no
	// longer declares is a stale citation, not a current ruling.
	live := map[string]bool{}
	for _, r := range apiRoutes {
		live[r.Method+" "+r.Path] = true
	}
	for _, key := range restDeadByDesignKeys() {
		if !live[key] {
			return nil, fmt.Errorf("RESTDeadByDesign names %q, which the frozen Python route list no longer declares as a route -- "+
				"the route was renamed or removed; update or drop the entry", key)
		}
	}

	rows := make([]RESTEndpointRow, 0, len(apiRoutes))
	for _, r := range apiRoutes {
		key := r.Method + " " + r.Path
		if citation, ok := restDeadByDesignCitation(key); ok {
			rows = append(rows, RESTEndpointRow{Method: r.Method, Path: r.Path, Status: RESTDeadByDesignStatus, Note: citation})
			continue
		}
		if m, ok := byPath[r.Path]; ok {
			rows = append(rows, RESTEndpointRow{Method: r.Method, Path: r.Path, Status: RESTPorted, GoHandler: m.HandlerLoc})
			continue
		}
		rows = append(rows, RESTEndpointRow{Method: r.Method, Path: r.Path, Status: RESTPythonOnly})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Path != rows[j].Path {
			return rows[i].Path < rows[j].Path
		}
		return rows[i].Method < rows[j].Method
	})
	return rows, nil
}

// RESTEndpointCounts tallies rows by status, for the provenance line above
// the table -- the same "put the tally on the page" habit RenderOpsBlock
// uses, because the tally is exactly what nobody did before this section
// existed.
func RESTEndpointCounts(rows []RESTEndpointRow) (ported, pythonOnly, deadByDesign int) {
	for _, r := range rows {
		switch r.Status {
		case RESTPorted, RESTProven:
			// RESTProven is RESTPorted plus an admissible receipt --
			// tallied as ported here so this function's total
			// (ported+pythonOnly+deadByDesign) always equals len(rows)
			// regardless of whether ApplyRESTProof has run. The per-row
			// Status column still renders "proven" distinctly; only this
			// summary bucket treats the two as one.
			ported++
		case RESTPythonOnly:
			pythonOnly++
		case RESTDeadByDesignStatus:
			deadByDesign++
		}
	}
	return ported, pythonOnly, deadByDesign
}

// RenderRESTEndpointsBlock renders the "Per REST endpoint" table.
func RenderRESTEndpointsBlock(rows []RESTEndpointRow) string {
	ported, pythonOnly, deadByDesign := RESTEndpointCounts(rows)
	var b strings.Builder
	fmt.Fprintf(&b, "_%d `/api/v1/*` routes in the frozen Python api route list (`contracts/migration-status/v1/python-rest-routes.json`): **%d** ported, **%d** python-only, **%d** dead-by-design._\n\n",
		len(rows), ported, pythonOnly, deadByDesign)

	b.WriteString("| Method | Path | Status | Go handler |\n")
	b.WriteString("| --- | --- | --- | --- |\n")
	for _, r := range rows {
		handler := "--"
		if r.GoHandler != "" {
			handler = "`" + r.GoHandler + "`"
		}
		status := string(r.Status)
		if r.Note != "" {
			status = fmt.Sprintf("%s (%s)", status, r.Note)
		}
		fmt.Fprintf(&b, "| %s | `%s` | %s | %s |\n", r.Method, r.Path, status, handler)
	}
	return b.String()
}
