package serviceaccount

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// D6651: ResolveKey reads the organization's status from public.organizations;
// the auth.organizations copy is never a decision input. The service-account
// principal (p.disabled_at) and sa.status stay auth-native.
func TestResolveKeyReadsOrganizationStatusFromPublic(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "keys.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var literals []string
	for _, decl := range parsed.Decls {
		f, ok := decl.(*ast.FuncDecl)
		if !ok || f.Name.Name != "ResolveKey" {
			continue
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if text, err := strconv.Unquote(lit.Value); err == nil {
					literals = append(literals, text)
				}
			}
			return true
		})
	}
	sql := strings.Join(literals, "\n")
	if sql == "" {
		t.Fatal("ResolveKey holds no SQL literal")
	}
	if strings.Contains(sql, ".organizations") {
		t.Error("ResolveKey reads the auth.organizations copy: read public.organizations.is_active")
	}
	if !strings.Contains(sql, "public.organizations o") || !strings.Contains(sql, "o.is_active") {
		t.Error("ResolveKey does not read public.organizations.is_active")
	}
	if !strings.Contains(sql, "p.disabled_at") {
		t.Error("ResolveKey no longer reads the service-account principal's own status")
	}
}
