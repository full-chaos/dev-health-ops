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
// BOTH sides are read from committed Go/Python source on every render AND
// every check -- there is no curated per-row ledger to go stale, unlike
// status.json's family rows. The only hand-curated input is
// RESTDeadByDesign, and it stays empty until a route carries chris's word in
// an existing record; see its own doc comment for why "python-only" is the
// default a route earns by simply existing.
//
// MATCHING IS BY PATH, NOT (METHOD, PATH). query-api's `http.ServeMux`
// patterns here are plain paths (`mux.HandleFunc("/api/v1/quadrant", ...)`),
// not Go 1.22 method-prefixed patterns -- the mux itself does not
// discriminate by method, each handler checks `r.Method` internally and
// answers 405 otherwise. Today every `/api/v1/*` path query-api registers
// has exactly one Python method, so path matching and method matching agree.
// If a future path carries two Python methods (a GET and a POST at the same
// URL) while only one is actually wired in the Go handler, this renders BOTH
// method rows "ported" -- a known simplification, flagged here rather than
// discovered later, because resolving it needs parsing each handler's own
// `r.Method` guard, which is a materially bigger change than this section.
import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
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
	// HandlerLoc is "<file>:<line>" of the route's builder function
	// definition when it could be resolved (see LoadQueryAPIMuxRoutes), or
	// the mux registration's own "<file>:<line>" as a fallback.
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

var muxHandleFuncRe = regexp.MustCompile(`mux\.HandleFunc\(\s*"(/api/v1/[^"]+)"\s*,\s*([A-Za-z0-9_]+)\s*\)`)

// builderAssignRe finds `<handlerVar>, ... := <builderFunc>()` -- the shape
// every /api/v1/* route in query-api's main.go uses today
// (`explainHandler, explainCleanup, explainOK, explainErr :=
// buildInvestmentExplainRoute()`) to build its handler before mounting it.
func builderAssignRe(handlerVar string) *regexp.Regexp {
	return regexp.MustCompile(regexp.QuoteMeta(handlerVar) + `\s*,[^\n=]*:=\s*([A-Za-z0-9_]+)\(\)`)
}

func funcDefRe(name string) *regexp.Regexp {
	return regexp.MustCompile(`(?m)^func ` + regexp.QuoteMeta(name) + `\(`)
}

// LoadQueryAPIMuxRoutes mechanically parses every `/api/v1/*`
// `mux.HandleFunc` registration across query-api's own top-level source
// files (not `internal/`, not `tools/`, not `_test.go` -- the mux itself is
// built in `cmd/query-api`'s own package). Read it from the Go source, not
// from a registry: query-api has none for its REST surface, only for
// GraphQL operations.
//
// For each registration this also tries to resolve the handler's builder
// function (query-api's own `build<X>Route()` convention) to its definition
// site, so a "ported" row can point at the code that actually serves it
// rather than only the mount call. Falling back to the mount site itself is
// not a bug: it is a real, if less useful, "Go handler location".
// queryAPILoc renders a "Go handler location" citation for a file inside
// query-api's own directory. It names the file relative to
// cmd/query-api/ -- a fixed, repo-relative label, deliberately NOT derived
// from the caller-supplied queryAPIDir argument, which can be absolute
// (a test's t.TempDir(), or -root resolved to an absolute path) and would
// otherwise leak a build-machine path onto the rendered page.
func queryAPILoc(name string, line int) string {
	return fmt.Sprintf("cmd/query-api/%s:%d", filepath.Base(name), line)
}

func LoadQueryAPIMuxRoutes(queryAPIDir string) ([]QueryAPIMuxRoute, error) {
	matches, err := filepath.Glob(filepath.Join(queryAPIDir, "*.go"))
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", queryAPIDir, err)
	}
	sort.Strings(matches)

	type file struct {
		name string
		text string
	}
	var files []file
	for _, m := range matches {
		if strings.HasSuffix(m, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(m) //nolint:gosec // repo-relative path
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", m, err)
		}
		files = append(files, file{name: m, text: string(raw)})
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("%s: no non-test .go files found", queryAPIDir)
	}

	var out []QueryAPIMuxRoute
	for _, f := range files {
		for _, m := range muxHandleFuncRe.FindAllStringSubmatchIndex(f.text, -1) {
			route := f.text[m[2]:m[3]]
			handlerVar := f.text[m[4]:m[5]]
			mountLine := 1 + strings.Count(f.text[:m[0]], "\n")
			loc := queryAPILoc(f.name, mountLine)

			// Best-effort: if the builder cannot be resolved (a shape this
			// codebase does not use today), loc stays the mount site set
			// above -- a real, if less useful, "Go handler location".
			if assign := builderAssignRe(handlerVar).FindStringSubmatch(f.text); assign != nil {
				builder := assign[1]
				for _, f2 := range files {
					if defMatch := funcDefRe(builder).FindStringIndex(f2.text); defMatch != nil {
						defLine := 1 + strings.Count(f2.text[:defMatch[0]], "\n")
						loc = queryAPILoc(f2.name, defLine)
						break
					}
				}
			}

			out = append(out, QueryAPIMuxRoute{Path: route, HandlerLoc: loc})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// LoadRESTEndpoints builds the "Per REST endpoint" rows: every /api/v1/*
// route main.py declares, cross-referenced against query-api's mux. Both
// sources are read fresh here -- there is no ledger of routes to drift out
// of sync with either side, only RESTDeadByDesign's citations.
func LoadRESTEndpoints(mainPyPath, queryAPIDir string) ([]RESTEndpointRow, error) {
	pyRoutes, err := LoadFastAPIRoutes(mainPyPath)
	if err != nil {
		return nil, err
	}
	apiRoutes := apiV1Routes(pyRoutes)
	if len(apiRoutes) == 0 {
		return nil, fmt.Errorf("%s: found no /api/v1/* routes -- main.py's route decorators changed shape, "+
			"or the file moved; LoadFastAPIRoutes needs updating before this section can be trusted", mainPyPath)
	}

	muxRoutes, err := LoadQueryAPIMuxRoutes(queryAPIDir)
	if err != nil {
		return nil, err
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
			return nil, fmt.Errorf("RESTDeadByDesign names %q, which main.py no longer declares as a route -- "+
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
	fmt.Fprintf(&b, "_%d `/api/v1/*` routes in `src/dev_health_ops/api/main.py`: **%d** ported, **%d** python-only, **%d** dead-by-design._\n\n",
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
