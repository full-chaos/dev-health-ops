package moduleroot

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// The dynamic-read scan (internal/cigates/affected) treats `_, file, _, _ := moduleroot.Caller(0)` as runtime.Caller(0).
// That holds only while Caller calls runtime.Caller(skip+1) once, takes its file from that call, and otherwise only
// calls load and absolute (which rewrite the path to an absolute one). A change to this shape must be reviewed with the scan.
func TestCallerIsRuntimeCallerOneFrameDeeper(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "moduleroot.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var fn *ast.FuncDecl
	for _, d := range file.Decls {
		if f, ok := d.(*ast.FuncDecl); ok && f.Recv == nil && f.Name.Name == "Caller" {
			fn = f
		}
	}
	if fn == nil {
		t.Fatal("no func Caller")
	}
	var rt []*ast.CallExpr
	var assigns []*ast.AssignStmt
	var returns []*ast.ReturnStmt
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			switch f := x.Fun.(type) {
			case *ast.SelectorExpr:
				id, ok := f.X.(*ast.Ident)
				if !ok || id.Name != "runtime" || f.Sel.Name != "Caller" {
					t.Errorf("Caller calls %s: only runtime.Caller is allowed", f.Sel.Name)
					return true
				}
				rt = append(rt, x)
			case *ast.Ident:
				if f.Name != "load" && f.Name != "absolute" {
					t.Errorf("Caller calls %s: only load and absolute are allowed", f.Name)
				}
			default:
				t.Errorf("Caller makes a call of an unexpected form")
			}
		case *ast.AssignStmt:
			assigns = append(assigns, x)
		case *ast.ReturnStmt:
			returns = append(returns, x)
		}
		return true
	})
	if len(rt) != 1 {
		t.Fatalf("Caller makes %d runtime.Caller calls; want exactly 1", len(rt))
	}
	arg, ok := rt[0].Args[0].(*ast.BinaryExpr)
	if len(rt[0].Args) != 1 || !ok || arg.Op != token.ADD || !isIdent(arg.X, "skip") || !isInt(arg.Y, "1") {
		t.Fatal("the runtime.Caller call is not runtime.Caller(skip + 1)")
	}
	if len(assigns) != 1 || len(assigns[0].Lhs) != 4 || assigns[0].Rhs[0] != ast.Expr(rt[0]) || !isIdent(assigns[0].Lhs[1], "file") {
		t.Fatal("the file result must be the second result of the runtime.Caller call, assigned once")
	}
	if len(returns) != 2 {
		t.Fatalf("Caller has %d returns; want 2", len(returns))
	}
	if r := returns[0].Results; len(r) != 4 || !isIdent(r[1], "file") || !isIdent(r[3], "false") {
		t.Fatal("the failure return must pass runtime's file through with ok false")
	}
	r := returns[1].Results
	if len(r) != 4 || !isIdent(r[3], "true") {
		t.Fatal("the success return must end in true")
	}
	abs, ok := r[1].(*ast.CallExpr)
	if !ok || !isIdent(abs.Fun, "absolute") || len(abs.Args) == 0 || !isIdent(abs.Args[0], "file") {
		t.Fatal("the success return must give absolute(file, ...), the runtime file made absolute")
	}
}

func isIdent(e ast.Expr, name string) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == name
}

func isInt(e ast.Expr, v string) bool {
	l, ok := e.(*ast.BasicLit)
	return ok && l.Kind == token.INT && l.Value == v
}
