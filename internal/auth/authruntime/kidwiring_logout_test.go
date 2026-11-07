package authruntime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The production builder installs the logout counter it builds: without the
// Set call the compat logout route counts nothing and the HS256 removal reads 0.
func TestTheProductionIssuerBuilderInstallsTheLogoutCounter(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "kidwiring.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var built, installed string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Name.Name != "newRecordingIssuer" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.AssignStmt:
				if call, ok := n.Rhs[0].(*ast.CallExpr); ok && selectorIs(call.Fun, "accessverify", "NewLogoutCounter") {
					if id, ok := n.Lhs[0].(*ast.Ident); ok {
						built = id.Name
					}
				}
			case *ast.CallExpr:
				if selectorIs(n.Fun, "authapi", "SetLogoutAccepted") && len(n.Args) == 1 {
					if id, ok := n.Args[0].(*ast.Ident); ok {
						installed = id.Name
					}
				}
			}
			return true
		})
	}
	if built == "" || installed != built {
		t.Fatalf("newRecordingIssuer builds the logout counter as %q and installs %q; want the built one installed", built, installed)
	}
}

func selectorIs(expr ast.Expr, pkg, name string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg
}
