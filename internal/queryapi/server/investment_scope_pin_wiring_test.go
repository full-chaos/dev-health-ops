package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// investmentReaderConstructors are the package-main entry points that hand a
// ClickHouse client to a reader composing
// analytics.LatestWorkUnitInvestmentsSource (packages analytics via the
// GraphQL handler, investment, investmentexplain, investmentflow, sankey).
var investmentReaderConstructors = map[string]bool{
	"investment.NewReader":             true,
	"investmentexplain.NewReader":      true,
	"newInvestmentFlowHandler":         true,
	"newInvestmentFlowRepoTeamHandler": true,
	"newSankeyGetHandler":              true,
	"newSankeyPostHandler":             true,
	"newQueryHandler":                  true,
}

func calleeName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		if pkg, ok := fn.X.(*ast.Ident); ok {
			return pkg.Name + "." + fn.Sel.Name
		}
	}
	return ""
}

func parsePackageMainSources(t *testing.T) map[string]*ast.File {
	t.Helper()
	// The package directory, read through a constant (the same form
	// unrestricted_read_options_test.go uses): only Go sources are parsed.
	const dir = "."
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	files := map[string]*ast.File{}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files[name] = file
	}
	return files
}

// TestInvestmentReadersReceiveAPinnedScopeClient: every production call that
// builds an investment reader passes it a client wrapped by
// analytics.PinInvestmentMembershipScope, so each request's investment
// queries share one membership-scope resolution. Every listed constructor
// must still be called somewhere, so a rename cannot empty the check.
func TestInvestmentReadersReceiveAPinnedScopeClient(t *testing.T) {
	seen := map[string]int{}
	for name, file := range parsePackageMainSources(t) {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			callee := calleeName(call)
			if !investmentReaderConstructors[callee] {
				return true
			}
			seen[callee]++
			if len(call.Args) == 0 {
				t.Errorf("%s: %s called without a client", name, callee)
				return true
			}
			arg, ok := call.Args[0].(*ast.CallExpr)
			if !ok || calleeName(arg) != "analytics.PinInvestmentMembershipScope" {
				t.Errorf("%s: %s receives an unpinned client; wrap it with analytics.PinInvestmentMembershipScope", name, callee)
			}
			return true
		})
	}
	for constructor := range investmentReaderConstructors {
		if seen[constructor] == 0 {
			t.Errorf("%s is never called in package main; update investmentReaderConstructors", constructor)
		}
	}
}

// TestServerInstallsTheInvestmentScopeRequestMiddleware: the HTTP server's
// root handler gives every request its own scope resolution.
func TestServerInstallsTheInvestmentScopeRequestMiddleware(t *testing.T) {
	found := 0
	for _, file := range parsePackageMainSources(t) {
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			selector, ok := literal.Type.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Server" {
				return true
			}
			if pkg, ok := selector.X.(*ast.Ident); !ok || pkg.Name != "http" {
				return true
			}
			for _, element := range literal.Elts {
				field, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := field.Key.(*ast.Ident); !ok || key.Name != "Handler" {
					continue
				}
				found++
				call, ok := field.Value.(*ast.CallExpr)
				if !ok || calleeName(call) != "analytics.InvestmentMembershipScopeRequestMiddleware" {
					t.Errorf("http.Server Handler is not wrapped by analytics.InvestmentMembershipScopeRequestMiddleware")
				}
			}
			return true
		})
	}
	if found == 0 {
		t.Fatal("no http.Server literal with a Handler field found in package main")
	}
}
