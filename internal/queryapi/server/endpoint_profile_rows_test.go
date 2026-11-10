package server

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

// servedRoutes returns every "path@listener" the query-api binary registers,
// read from the registrations themselves rather than from a list: the string
// (or package-constant) first argument of every Handle/HandleFunc call on the
// route muxes of this package and of the operator listener, plus the REST
// table BuildWithLookup iterates. Listeners: query (public), internal, mcp,
// operator. A mux whose pattern is not a literal or a package constant is the
// REST mount loop, covered by RESTRoutes.
func servedRoutes(t *testing.T) map[string]bool {
	t.Helper()
	served := map[string]bool{}
	add := func(path string, listeners ...string) {
		for _, listener := range listeners {
			served[path+"@"+listener] = true
		}
	}
	scan := func(dir string, pick func(file, receiver, path string) []string) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		fset := token.NewFileSet()
		var files []*ast.File
		var names []string
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			files = append(files, file)
			names = append(names, name)
		}
		consts := map[string]string{}
		for _, file := range files {
			for _, decl := range file.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.CONST {
					continue
				}
				for _, spec := range gen.Specs {
					value := spec.(*ast.ValueSpec)
					for i, ident := range value.Names {
						if i < len(value.Values) {
							if lit, ok := value.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
								consts[ident.Name], _ = strconv.Unquote(lit.Value)
							}
						}
					}
				}
			}
		}
		for i, file := range files {
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || len(call.Args) == 0 {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || (selector.Sel.Name != "Handle" && selector.Sel.Name != "HandleFunc") {
					return true
				}
				receiver, ok := selector.X.(*ast.Ident)
				if !ok {
					return true
				}
				var path string
				switch arg := call.Args[0].(type) {
				case *ast.BasicLit:
					path, _ = strconv.Unquote(arg.Value)
				case *ast.Ident:
					path = consts[arg.Name]
				}
				if path == "" || path == "/" {
					return true
				}
				add(path, pick(names[i], receiver.Name, path)...)
				return true
			})
		}
	}

	scan(".", func(file, receiver, path string) []string {
		switch {
		case file == "listener.go":
			return nil
		case file == "operator_compat.go":
			if path == "/metrics" {
				return []string{"internal"}
			}
			return []string{"query", "internal"}
		case receiver == "mcpMux":
			return []string{"mcp"}
		case receiver == "internalMux":
			return []string{"internal"}
		case receiver == "mux":
			return []string{"query", "internal"}
		}
		return nil
	})
	scan("../../platform/health", func(file, receiver, path string) []string {
		if file == "server.go" && receiver == "mux" {
			return []string{"operator"}
		}
		return nil
	})
	for _, route := range RESTRoutes() {
		add(route.Pattern, "query", "internal")
	}
	return served
}

var rowListeners = regexp.MustCompile(`@([a-z,]+)\]$`)

func loadQueryAPIRows(t *testing.T) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile("../../../contracts/auth/v1/endpoint-profiles.query-api.json")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Rows []map[string]any `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	return document.Rows
}

// TestEveryServedRouteHasAnEndpointProfileRowAndNoRowIsStale: a route the
// binary serves with no row fails here, and so does a row for a route nothing
// serves, per listener. The REST methods of each row are RESTRoutes' methods.
func TestEveryServedRouteHasAnEndpointProfileRowAndNoRowIsStale(t *testing.T) {
	served := servedRoutes(t)
	rowTokens := map[string]bool{}
	restMethods := map[string][]string{}
	for _, route := range RESTRoutes() {
		restMethods[route.Pattern] = append(restMethods[route.Pattern], route.Method)
	}
	rowMethods := map[string][]string{}
	for _, row := range loadQueryAPIRows(t) {
		id, _ := row["id"].(string)
		route, _ := row["route"].(string)
		match := rowListeners.FindStringSubmatch(id)
		if match == nil {
			t.Fatalf("row id %q does not end in @listener[,listener]]", id)
		}
		for _, listener := range strings.Split(match[1], ",") {
			rowTokens[route+"@"+listener] = true
		}
		if _, isREST := restMethods[route]; isREST {
			rowMethods[route] = append(rowMethods[route], row["method"].(string))
		}
	}
	var missing, stale []string
	for token := range served {
		if !rowTokens[token] {
			missing = append(missing, token)
		}
	}
	for token := range rowTokens {
		if !served[token] {
			stale = append(stale, token)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 {
		t.Errorf("served with no endpoint-profile row (contracts/auth/v1/endpoint-profiles.query-api.json): %v", missing)
	}
	if len(stale) > 0 {
		t.Errorf("endpoint-profile rows for routes nothing serves: %v", stale)
	}
	for route, want := range restMethods {
		got := dedupSorted(rowMethods[route])
		if strings.Join(got, ",") != strings.Join(dedupSorted(want), ",") {
			t.Errorf("%s: rows declare methods %v, the REST table serves %v", route, got, dedupSorted(want))
		}
	}
	if len(served) < 30 {
		t.Fatalf("only %d served routes found: the registration scan stopped seeing the mux calls", len(served))
	}
}

func dedupSorted(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, item := range in {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	sort.Strings(out)
	return out
}

// TestEndpointProfileAnchorsStillPointAtTheirSymbols: a query-api row's source
// file holds the one registration of its route (the pattern, or the constant it
// is written as) and method, and every validator anchor's note resolves to one
// place; the rows' lines are citations and are not compared. The anchors of the
// envelope credential class carry a note and name a file that exists; their text
// is the credential-class pin check's (internal/auth/contracts).
func TestEndpointProfileAnchorsStillPointAtTheirSymbols(t *testing.T) {
	problems, checked, drift := queryRowAnchorProblems(t, "../../..", loadQueryAPIRows(t))
	t.Logf("citation note: %d query-api row sources and validator anchors cite a line other than the one they resolve to", drift)
	walk := anchorWalk(t, "../../..", &problems, &checked)
	raw, err := os.ReadFile("../../../contracts/auth/v1/credential-classes.json")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Classes []map[string]any `json:"classes"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	for _, class := range document.Classes {
		if class["class_id"] == "effective_principal_envelope" {
			walk(class, "effective_principal_envelope")
		}
	}
	for _, problem := range problems {
		t.Error(problem)
	}
	if checked < 30 {
		t.Fatalf("only %d anchors were checked: the walk stopped seeing them", checked)
	}
}

// anchorWalk checks every ops anchor under a node: it carries a note and its file exists. The anchors it walks are
// credential-classes.json anchors, whose text and symbol the pin check in internal/auth/contracts owns
// (TestCredentialClassAnchorsPointAtTheirPinnedLineText; TestEnvelopeAndLLMKeyAnchorsAllHaveAPin proves each has a pin),
// so the cited line is not read here.
func anchorWalk(t *testing.T, root string, problems *[]string, checked *int) func(node any, where string) {
	var walk func(node any, where string)
	walk = func(node any, where string) {
		switch value := node.(type) {
		case map[string]any:
			path, hasPath := value["path"].(string)
			line, hasLine := value["line"].(float64)
			if hasPath && hasLine && (value["repo"] == nil || value["repo"] == "ops") {
				note, _ := value["note"].(string)
				if note == "" {
					*problems = append(*problems, fmt.Sprintf("%s: anchor %s:%d carries no note naming the symbol its line holds", where, path, int(line)))
				} else if _, err := os.Stat(filepath.Join(root, path)); err != nil {
					*problems = append(*problems, fmt.Sprintf("%s: anchor file %s: %v", where, path, err))
				}
				*checked++
			}
			for key, child := range value {
				walk(child, where+"/"+key)
			}
		case []any:
			for index, child := range value {
				walk(child, where+"["+strconv.Itoa(index)+"]")
			}
		}
	}
	return walk
}

// muxListeners are the listeners each route mux of this package serves (servedRoutes reads the same mapping).
var muxListeners = map[string][]string{"mux": {"query", "internal"}, "internalMux": {"internal"}, "mcpMux": {"mcp"}}

// queryRowAnchorProblems resolves the query-api rows in the source under root, by symbol: a row's source file holds
// the one registration of its method and route, and each validator anchor's note resolves to one place. drift counts
// the cited lines that are not the resolved ones.
func queryRowAnchorProblems(t *testing.T, root string, rows []map[string]any) (problems []string, checked, drift int) {
	t.Helper()
	scanned := map[string][]routescan.Registration{}
	resolve := func(file, method, route string, listeners []string) (routescan.Registration, error) {
		dir := filepath.Join(root, filepath.Dir(file))
		if _, ok := scanned[dir]; !ok {
			regs, err := routescan.ScanWithMux(dir)
			if err != nil {
				t.Fatal(err)
			}
			scanned[dir] = regs
		}
		path := filepath.ToSlash(filepath.Join(root, file))
		reg, err := routescan.Resolve(scanned[dir], path, method, route)
		if err != nil && strings.Contains(err.Error(), "source anchor ambiguous") {
			// One path on two muxes (POST /query on mux and mcpMux) is two symbols: the row's listeners pick the mux.
			reg, err = routescan.ResolveWhere(scanned[dir], path, method, route, func(reg routescan.Registration) bool {
				for _, listener := range listeners {
					if !slices.Contains(muxListeners[reg.Mux], listener) {
						return false
					}
				}
				return true
			})
		}
		if err != nil {
			return reg, fmt.Errorf("%s", strings.ReplaceAll(err.Error(), filepath.ToSlash(root)+"/", ""))
		}
		return reg, nil
	}
	for _, row := range rows {
		id, _ := row["id"].(string)
		method, _ := row["method"].(string)
		route, _ := row["route"].(string)
		source := row["source"].(map[string]any)
		file, line := source["file"].(string), int(source["line"].(float64))
		var listeners []string
		if match := rowListeners.FindStringSubmatch(id); match != nil {
			listeners = strings.Split(match[1], ",")
		}
		own := func() (routescan.Registration, error) { return resolve(file, method, route, listeners) }
		if reg, err := own(); err != nil {
			problems = append(problems, fmt.Sprintf("row %q: %v", id, err))
		} else if reg.Line != line {
			drift++
		}
		checked++
		var walk func(node any, where string)
		walk = func(node any, where string) {
			switch value := node.(type) {
			case map[string]any:
				path, hasPath := value["path"].(string)
				anchorLine, hasLine := value["line"].(float64)
				if hasPath && hasLine && (value["repo"] == nil || value["repo"] == "ops") {
					checked++
					note, _ := value["note"].(string)
					if note == "" {
						problems = append(problems, fmt.Sprintf("%s: anchor %s:%d carries no note naming the symbol its line holds", where, path, int(anchorLine)))
					} else if raw, err := os.ReadFile(filepath.Join(root, path)); err != nil {
						t.Fatalf("anchor file %s: %v", path, err)
					} else if problem, drifted := routescan.ResolveNote(raw, path, note, int(anchorLine), file, own); problem != "" {
						problems = append(problems, where+": "+problem)
					} else if drifted {
						drift++
					}
				}
				for key, child := range value {
					walk(child, where+"/"+key)
				}
			case []any:
				for index, child := range value {
					walk(child, where+"["+strconv.Itoa(index)+"]")
				}
			}
		}
		walk(row["primary_validator"], id+"/primary_validator")
	}
	return problems, checked, drift
}

// TestClassConsumersNameEveryQueryAPIRouteThatAcceptsThem keeps the credential
// class registry honest about WHERE each class is accepted (CHAOS-5522). For
// EVERY class, every query-api profile row that lists the class in
// accepted_credential_classes must have its route named in the class's
// consumers in credential-classes.json. The /api/v1/ REST read routes are
// covered by the single token "/api/v1/*". The class is the defect, not one
// class: the first instance was the effective-principal envelope.
func TestClassConsumersNameEveryQueryAPIRouteThatAcceptsThem(t *testing.T) {
	raw, err := os.ReadFile("../../../contracts/auth/v1/credential-classes.json")
	if err != nil {
		t.Fatal(err)
	}
	var registry struct {
		Classes []struct {
			ClassID   string `json:"class_id"`
			Consumers []struct {
				Description string `json:"description"`
			} `json:"consumers"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(raw, &registry); err != nil {
		t.Fatal(err)
	}
	described := map[string]string{}
	for _, entry := range registry.Classes {
		for _, consumer := range entry.Consumers {
			described[entry.ClassID] += " " + consumer.Description + " "
		}
		if _, ok := described[entry.ClassID]; !ok {
			described[entry.ClassID] = " "
		}
	}
	checked := 0
	for _, row := range loadQueryAPIRows(t) {
		route, _ := row["route"].(string)
		token := route
		if strings.HasPrefix(route, "/api/v1/") {
			token = "/api/v1/*"
		}
		classes, _ := row["accepted_credential_classes"].([]any)
		for _, c := range classes {
			class, _ := c.(string)
			text, registered := described[class]
			if !registered {
				t.Errorf("route %s lists class %q, which the registry does not define", route, class)
				continue
			}
			checked++
			if !strings.Contains(text, " "+token+" ") && !strings.Contains(text, " "+token+",") && !strings.Contains(text, " "+token+".") {
				t.Errorf("route %s accepts %s but that class's consumers do not name it (token %q) in: %s", route, class, token, strings.TrimSpace(text))
			}
		}
	}
	if checked == 0 {
		t.Fatal("no (route, class) pair was checked: the test measured nothing")
	}
}
