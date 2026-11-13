package scoperesolve

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// Every page entry that has a scope filter calls scoperesolve.Unresolved itself (CHAOS-13705): a page that skips the call answers
// the scope it was handed. The floor is the pages known today; a census that finds fewer measured nothing.
var pageEntries = []struct{ file, function string }{
	{"heatmap/heatmap.go", "BuildResponse"},
	{"sankey/sankey.go", "BuildResponse"},
	{"explain/response.go", "BuildExplainResponse"},
	{"home/scoperesolve.go", "ResolveScope"},
}

func TestEveryPageEntryCallsTheSharedResolver(t *testing.T) {
	found := 0
	for _, entry := range pageEntries {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", filepath.FromSlash(entry.file)), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != entry.function || fn.Body == nil {
				continue
			}
			calls := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					if x, ok := sel.X.(*ast.Ident); ok && x.Name == "scoperesolve" && sel.Sel.Name == "Unresolved" {
						calls = true
					}
				}
				return true
			})
			found++
			if !calls {
				t.Errorf("%s %s does not call scoperesolve.Unresolved", entry.file, entry.function)
			}
		}
	}
	if found != len(pageEntries) {
		t.Errorf("found %d of %d page entries: the census measured less than the floor", found, len(pageEntries))
	}
}
