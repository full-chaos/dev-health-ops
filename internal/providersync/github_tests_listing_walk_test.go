package providersync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// The walk and its replay list the same pages only while one function owns the paginator call of the workflow runs
// listings: githubTestsListingWalk.run is the only VisitGitHubLinkPages call of the package (CHAOS-14435).
func TestGitHubTestsRunsRouteListingHasOneWalker(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var sites []string
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fset, name, nil, 0)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			owner := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				if ident, isIdent := fn.Recv.List[0].Type.(*ast.Ident); isIdent {
					owner = ident.Name + "." + owner
				}
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, isCall := node.(*ast.CallExpr)
				if !isCall {
					return true
				}
				if selector, isSelector := call.Fun.(*ast.SelectorExpr); isSelector && selector.Sel.Name == "VisitGitHubLinkPages" {
					sites = append(sites, name+":"+owner)
				}
				return true
			})
		}
	}
	if len(sites) != 1 || sites[0] != "github_tests_listing_walk.go:githubTestsListingWalk.run" {
		t.Fatalf("VisitGitHubLinkPages call sites = %v, want only github_tests_listing_walk.go:githubTestsListingWalk.run", sites)
	}
}
