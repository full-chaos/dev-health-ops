package authruntime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// The authorize audit sink of authorizeRoutes is one audit.AppendAll call with every event of the call and
// returns its error: the rows of one decision call (a delegated high-risk decision writes two) go in one
// transaction, and a failed write reaches the handler (503). A static pin: this cell does not run the sink.
func TestTheAuthorizeAuditSinkIsOneAppendAllCall(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "authorize.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	auditName := ""
	for _, spec := range file.Imports {
		if path, _ := strconv.Unquote(spec.Path.Value); path == "github.com/full-chaos/dev-health-ops/internal/audit" {
			auditName = "audit"
			if spec.Name != nil {
				auditName = spec.Name.Name
			}
		}
	}
	if auditName == "" {
		t.Fatal("authorize.go does not import internal/audit")
	}
	var sinks []*ast.FuncLit
	var outside int
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CompositeLit:
				sel, ok := n.Type.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "AuthorizeDeps" {
					return true
				}
				for _, elt := range n.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if key, isIdent := kv.Key.(*ast.Ident); !isIdent || key.Name != "Audit" {
						continue
					}
					lit, isLit := kv.Value.(*ast.FuncLit)
					if !isLit || fn.Name.Name != "authorizeRoutes" {
						outside++
						continue
					}
					sinks = append(sinks, lit)
				}
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "Audit" {
						outside++
					}
				}
			}
			return true
		})
	}
	if len(sinks) != 1 || outside != 0 {
		t.Fatalf("found %d Audit closures in the authorizeRoutes AuthorizeDeps literal and %d other Audit settings, want exactly 1 and 0",
			len(sinks), outside)
	}
	sink := sinks[0]
	var params []string
	for _, field := range sink.Type.Params.List {
		for _, name := range field.Names {
			params = append(params, name.Name)
		}
	}
	if len(params) != 2 {
		t.Fatalf("the sink has params %v, want (ctx, events)", params)
	}
	at := fset.Position(sink.Pos())
	if len(sink.Body.List) != 1 {
		t.Fatalf("%s: the sink has %d statements, want one return of audit.AppendAll", at, len(sink.Body.List))
	}
	ret, ok := sink.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		t.Fatalf("%s: the sink statement is not a return of one value", at)
	}
	call, ok := ret.Results[0].(*ast.CallExpr)
	if !ok {
		t.Fatalf("%s: the sink returns no call", at)
	}
	fun, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		t.Fatalf("%s: the sink returns a call that is not %s.AppendAll", at, auditName)
	}
	if pkg, isIdent := fun.X.(*ast.Ident); !isIdent || pkg.Name != auditName || fun.Sel.Name != "AppendAll" {
		t.Fatalf("%s: the sink returns a call that is not %s.AppendAll", at, auditName)
	}
	want := []string{params[0], "pool", "schema", params[1]}
	if len(call.Args) != len(want) || !call.Ellipsis.IsValid() {
		t.Fatalf("%s: AppendAll gets %d args (ellipsis %v), want (%v...)", at, len(call.Args), call.Ellipsis.IsValid(), want)
	}
	for i, arg := range call.Args {
		if id, isIdent := arg.(*ast.Ident); !isIdent || id.Name != want[i] {
			t.Fatalf("%s: AppendAll arg %d is not %s", at, i, want[i])
		}
	}
}
