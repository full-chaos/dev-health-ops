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

// TestEndpointProfileAnchorsStillPointAtTheirSymbols: every anchor of the
// query-api rows and of the envelope credential class carries the text its
// line must still hold, so a pinned file:line cannot drift silently. A row's
// source line holds a registration of its route (the pattern, or the constant
// it is written as).
func TestEndpointProfileAnchorsStillPointAtTheirSymbols(t *testing.T) {
	lineOf := func(path string, line int) string {
		raw, err := os.ReadFile(filepath.Join("../../..", path))
		if err != nil {
			t.Fatalf("anchor file %s: %v", path, err)
		}
		lines := strings.Split(string(raw), "\n")
		if line < 1 || line > len(lines) {
			t.Fatalf("anchor %s:%d is outside the file (%d lines)", path, line, len(lines))
		}
		return lines[line-1]
	}
	checked := 0
	var walk func(node any, where string)
	walk = func(node any, where string) {
		switch value := node.(type) {
		case map[string]any:
			path, hasPath := value["path"].(string)
			line, hasLine := value["line"].(float64)
			if hasPath && hasLine && (value["repo"] == nil || value["repo"] == "ops") {
				note, _ := value["note"].(string)
				if note == "" {
					t.Errorf("%s: anchor %s:%d carries no note naming the symbol its line holds", where, path, int(line))
				} else if !strings.Contains(lineOf(path, int(line)), note) {
					t.Errorf("%s: %s:%d no longer holds %q (it holds %q)", where, path, int(line), note, strings.TrimSpace(lineOf(path, int(line))))
				}
				checked++
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
	for _, row := range loadQueryAPIRows(t) {
		id, _ := row["id"].(string)
		route, _ := row["route"].(string)
		source := row["source"].(map[string]any)
		file, line := source["file"].(string), int(source["line"].(float64))
		text := lineOf(file, line)
		if !strings.Contains(text, route) && !strings.Contains(text, "Handle") && !strings.Contains(text, "Pattern:") {
			t.Errorf("row %q: %s:%d holds no registration of %s: %q", id, file, line, route, strings.TrimSpace(text))
		}
		walk(row["primary_validator"], id+"/primary_validator")
		checked++
	}
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
	if checked < 30 {
		t.Fatalf("only %d anchors were checked: the walk stopped seeing them", checked)
	}
}
