package server

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
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
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CallExpr:
				selector, ok := n.Fun.(*ast.SelectorExpr)
				if ok && (selector.Sel.Name == "Handle" || selector.Sel.Name == "HandleFunc") && len(n.Args) > 0 {
					line := fset.Position(n.Pos()).Line
					perLine[line] = append(perLine[line], routeArg(n.Args[0]))
				}
			case *ast.KeyValueExpr:
				if key, ok := n.Key.(*ast.Ident); ok && key.Name == "Pattern" {
					line := fset.Position(n.Pos()).Line
					perLine[line] = append(perLine[line], routeArg(n.Value))
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
	switch v := expr.(type) {
	case *ast.BasicLit:
		return v.Value
	case *ast.Ident:
		return v.Name
	}
	return "<non-literal>"
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
	write("bad.go", "package x\nfunc f(mux M) {\n\tmux.Handle(\"/a\", h); mux.HandleFunc(\"/b\", h)\n\tr := []R{{Pattern: \"/c\"}, {Pattern: \"/d\"}}\n}\n")
	found := strings.Join(sameLineRegistrations(t, dir), "\n")
	for _, want := range []string{"bad.go:3", `"/a" AND "/b"`, "bad.go:4", `"/c" AND "/d"`, "MULTIPLE REGISTRATIONS ON ONE LINE"} {
		if !strings.Contains(found, want) {
			t.Errorf("missing %q in:\n%s", want, found)
		}
	}
}
