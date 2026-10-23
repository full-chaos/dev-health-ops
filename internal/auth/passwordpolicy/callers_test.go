package passwordpolicy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// Every path that sets a password reaches passwordpolicy.Validate through
// one of these functions; a path that stops calling it, or a new copy of the
// length rule, shows up here.
func TestEveryPasswordSettingPathCallsTheSharedPolicy(t *testing.T) {
	paths := []struct{ file, fn, callee string }{
		{"../../api/session/register.go", "register", "passwordpolicy.Validate"},
		{"../../api/session/changepassword.go", "changePassword", "passwordpolicy.Validate"},
		{"../../api/session/resetpolicy.go", "validateResetPasswordPolicy", "passwordpolicy.ValidateForHash"},
		{"../../api/session/verify.go", "resetPassword", "validateResetPasswordPolicy"},
		{"../../apiservice/admin/passwordviolations.go", "refusePasswordPolicy", "passwordpolicy.ValidateForHash"},
		{"../../apiservice/admin/users.go", "createUser", "refusePasswordPolicy"},
		{"../../apiservice/admin/users.go", "setUserPassword", "passwordpolicy.Validate"},
		{"../identity/local/flows.go", "passwordRefusal", "passwordpolicy.ValidateForHash"},
		{"../identity/local/flows.go", "Register", "passwordRefusal"},
		{"../identity/local/flows.go", "Reset", "passwordRefusal"},
		{"../identity/local/flows.go", "Change", "passwordRefusal"},
	}
	for _, p := range paths {
		if !functionCalls(t, p.file, p.fn, p.callee) {
			t.Errorf("%s: %s does not call %s", p.file, p.fn, p.callee)
		}
	}
}

func functionCalls(t *testing.T, file, fn, callee string) bool {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	found := false
	for _, decl := range parsed.Decls {
		d, ok := decl.(*ast.FuncDecl)
		if !ok || d.Name.Name != fn || d.Body == nil {
			continue
		}
		ast.Inspect(d.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch f := call.Fun.(type) {
			case *ast.Ident:
				found = found || f.Name == callee
			case *ast.SelectorExpr:
				if x, ok := f.X.(*ast.Ident); ok {
					found = found || x.Name+"."+f.Sel.Name == callee
				}
			}
			return true
		})
	}
	return found
}
