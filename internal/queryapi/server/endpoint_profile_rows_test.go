package server

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
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
