package server

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// sameLineRegistrations reports every file:line in dir (non-test .go files)
// that holds two or more route registrations: a Handle/HandleFunc call or a
// `Pattern:` field of a route table entry. Endpoint-profile rows address a
// surface as file:line, so a second registration on a line could never be
// profiled by a row of its own.
func sameLineRegistrations(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		perLine := map[int][]string{}
		isRegistrar := func(name string) bool { return name == "Handle" || name == "HandleFunc" }
		calleeName := func(fun ast.Expr) (string, ast.Node) {
			switch f := fun.(type) {
			case *ast.Ident:
				return f.Name, f
			case *ast.SelectorExpr:
				return f.Sel.Name, f
			}
			return "", nil
		}
		directCallees := map[ast.Node]bool{}
		declared := map[*ast.Ident]bool{}
		routeLits := map[*ast.CompositeLit]bool{}
		routeType := func(e ast.Expr) bool {
			id, ok := e.(*ast.Ident)
			return ok && (id.Name == "restMount" || id.Name == "RESTRoute")
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CallExpr:
				if fname, callee := calleeName(n.Fun); isRegistrar(fname) {
					directCallees[callee] = true
				}
			case *ast.FuncDecl:
				declared[n.Name] = true
			case *ast.Field:
				for _, id := range n.Names {
					declared[id] = true
				}
			case *ast.CompositeLit:
				if routeType(n.Type) {
					routeLits[n] = true
				}
				if arr, ok := n.Type.(*ast.ArrayType); ok && routeType(arr.Elt) {
					for _, elt := range n.Elts {
						if lit, ok := elt.(*ast.CompositeLit); ok {
							routeLits[lit] = true
						}
					}
				}
			}
			return true
		})
		skipIdent := map[*ast.Ident]bool{}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.SelectorExpr:
				skipIdent[n.Sel] = true
				if isRegistrar(n.Sel.Name) && !directCallees[n] {
					found = append(found, fmt.Sprintf("%s:%d: Handle/HandleFunc used as a value (alias): register each route by a direct call", name, fset.Position(n.Pos()).Line))
				}
			case *ast.Ident:
				if isRegistrar(n.Name) && !skipIdent[n] && !declared[n] && !directCallees[n] {
					found = append(found, fmt.Sprintf("%s:%d: Handle/HandleFunc used as a value (alias): register each route by a direct call", name, fset.Position(n.Pos()).Line))
				}
			case *ast.CallExpr:
				if fname, _ := calleeName(n.Fun); isRegistrar(fname) && len(n.Args) > 0 {
					// Handle(pattern, handler); a method expression takes the receiver first.
					route := n.Args[0]
					if len(n.Args) > 2 {
						route = n.Args[1]
					}
					line := fset.Position(n.Pos()).Line
					perLine[line] = append(perLine[line], routeArg(route))
				}
			case *ast.CompositeLit:
				if !routeLits[n] {
					return true
				}
				for _, elt := range n.Elts {
					if kv, ok := elt.(*ast.KeyValueExpr); ok {
						if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Pattern" {
							line := fset.Position(kv.Pos()).Line
							perLine[line] = append(perLine[line], routeArg(kv.Value))
						}
					}
				}
			}
			return true
		})
		for line, routes := range perLine {
			if len(routes) > 1 {
				sort.Strings(routes)
				found = append(found, fmt.Sprintf("%s:%d: MULTIPLE REGISTRATIONS ON ONE LINE: %s", name, line, strings.Join(routes, " AND ")))
			}
		}
	}
	sort.Strings(found)
	return found
}

func routeArg(expr ast.Expr) string {
	return types.ExprString(expr)
}

// TestNoTwoRouteRegistrationsShareALine: a profile row addresses its surface
// as file:line, so one registration per line is a rule of this package.
func TestNoTwoRouteRegistrationsShareALine(t *testing.T) {
	for _, dir := range []string{".", "../../platform/health"} {
		if found := sameLineRegistrations(t, dir); len(found) > 0 {
			t.Errorf("%s: %s\nsplit each registration onto its own line; see docs/reference/auth/endpoint-profiles.md (one registration per line)", dir, strings.Join(found, "\n"))
		}
	}
}

func TestSameLineRegistrationIsDetectedAndNamesBothRoutes(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("ok.go", "package x\nfunc f(mux M) {\n\tmux.Handle(\"/a\", h)\n\tmux.HandleFunc(\"/b\", h)\n}\n")
	if found := sameLineRegistrations(t, dir); len(found) != 0 {
		t.Fatalf("control flagged: %v", found)
	}
	write("unrelated.go", "package x\ntype T struct{ Pattern string }\nfunc (m M) Handle(p string, h H) {}\nfunc g() { _ = []T{{Pattern: \"a\"}, {Pattern: \"b\"}}; _ = T{Pattern: \"c\"} }\n")
	if found := sameLineRegistrations(t, dir); len(found) != 0 {
		t.Fatalf("control (unrelated Pattern fields, a Handle declaration) flagged: %v", found)
	}
	write("bad.go", "package x\nfunc f(mux M) {\n\tmux.Handle(\"/a\", h); mux.HandleFunc(\"/b\", h)\n\tr := []restMount{{Pattern: \"/c\"}, {Pattern: \"/d\"}}\n}\n")
	write("alias.go", "package x\nfunc f(mux M) {\n\tregister := mux.HandleFunc\n\tregister(\"/e\", h)\n}\n")
	write("expr.go", "package x\nfunc f(mux M) {\n\tmux.Handle(routePath, h); mux.Handle(prefix+\"/g\", h)\n}\n")
	write("dot.go", "package x\nfunc f() {\n\tHandleFunc(\"/h\", h); HandleFunc(\"/i\", h)\n\treg := HandleFunc\n}\n")
	write("methodexpr.go", "package x\nfunc f(mux *M) {\n\t(*M).Handle(mux, \"/j\", h); (*M).Handle(mux, \"/k\", h)\n}\n")
	found := strings.Join(sameLineRegistrations(t, dir), "\n")
	for _, want := range []string{"bad.go:3", `"/a" AND "/b"`, "bad.go:4", `"/c" AND "/d"`, "MULTIPLE REGISTRATIONS ON ONE LINE", `dot.go:3: MULTIPLE REGISTRATIONS ON ONE LINE: "/h" AND "/i"`, "dot.go:4: Handle/HandleFunc used as a value", `methodexpr.go:3: MULTIPLE REGISTRATIONS ON ONE LINE: "/j" AND "/k"`, "alias.go:3: Handle/HandleFunc used as a value", `expr.go:3: MULTIPLE REGISTRATIONS ON ONE LINE: prefix + "/g" AND routePath`} {
		if !strings.Contains(found, want) {
			t.Errorf("missing %q in:\n%s", want, found)
		}
	}
}
