package identity

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// The admin password set is a writer of auth.users.password_updated_at (the stamp that ends older sessions, the
// session rule's revoke). acp 13398 invariant: such a writer bumps the token pins in the SAME transaction. This reads
// AdminSetPassword's own source: its one users UPDATE sets the stamp AND legacy_token_version + 1 in one statement,
// and the same Apply then calls updatePublicPassword (public token_version + 1) inside the same audit.Commit.
func TestAdminSetPasswordBumpsBothPinsWithTheStamp(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "adminusers.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var fn *ast.FuncDecl
	for _, decl := range file.Decls {
		if d, ok := decl.(*ast.FuncDecl); ok && d.Name.Name == "AdminSetPassword" {
			fn = d
		}
	}
	if fn == nil {
		t.Fatal("no AdminSetPassword in adminusers.go")
	}
	var commits, stamps, pinned, public int
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Commit" {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "audit" {
					commits++
				}
			}
			if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "updatePublicPassword" {
				public++
			}
		case *ast.BasicLit:
			if x.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(x.Value)
			if err != nil || !strings.Contains(text, "password_updated_at = clock_timestamp()") {
				return true
			}
			stamps++
			if strings.Contains(text, "legacy_token_version = legacy_token_version + 1") {
				pinned++
			}
		}
		return true
	})
	if commits != 1 || stamps != 1 || pinned != 1 || public != 1 {
		t.Fatalf("AdminSetPassword: audit.Commit %d, stamp statements %d, with the legacy pin bump %d, updatePublicPassword %d; want 1 each in the one Commit",
			commits, stamps, pinned, public)
	}
}
