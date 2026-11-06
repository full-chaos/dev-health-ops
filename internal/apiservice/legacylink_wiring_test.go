package apiservice

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

func parseFuncs(t *testing.T, file string) map[string]*ast.FuncDecl {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*ast.FuncDecl{}
	for _, decl := range parsed.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
			out[fn.Name.Name] = fn
		}
	}
	return out
}

func callsSelector(fn *ast.FuncDecl, pkg, name string) (args [][]ast.Expr) {
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == name {
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == pkg {
				args = append(args, call.Args)
			}
		}
		return true
	})
	return args
}

// The api installs the legacy-key link counter it builds, and its startup
// starts it: without either, the link flows count nothing and the HS256
// removal reads 0.
func TestStartupInstallsTheLegacyLinkCounter(t *testing.T) {
	start := parseFuncs(t, "invites.go")["startLegacyLinkCounter"]
	if start == nil {
		t.Fatal("invites.go has no startLegacyLinkCounter")
	}
	var built string
	ast.Inspect(start.Body, func(n ast.Node) bool {
		if assign, ok := n.(*ast.AssignStmt); ok {
			if call, ok := assign.Rhs[0].(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "NewLegacyLinkCounter" {
					built = assign.Lhs[0].(*ast.Ident).Name
				}
			}
		}
		return true
	})
	installs := callsSelector(start, "authmail", "SetLegacyLinkAccepted")
	if built == "" || len(installs) != 1 || len(installs[0]) != 1 {
		t.Fatalf("startLegacyLinkCounter builds %q and installs %d times; want one install of the built counter", built, len(installs))
	}
	if id, ok := installs[0][0].(*ast.Ident); !ok || id.Name != built {
		t.Fatalf("startLegacyLinkCounter installs %#v, want %q", installs[0][0], built)
	}
	var started int
	for _, fn := range parseFuncs(t, "service.go") {
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "startLegacyLinkCounter" {
					started++
				}
			}
			return true
		})
	}
	if started != 1 {
		t.Fatalf("service.go calls startLegacyLinkCounter %d times, want 1", started)
	}
}
