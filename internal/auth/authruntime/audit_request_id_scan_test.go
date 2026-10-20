package authruntime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestEveryAuditEventTheSessionRoutesReachSetsARequestID is the source half of
// the request-id class (the behavioural half drives each route in
// TestEveryAuditRowARouteWritesCarriesItsRequestID): every audit.AuditEvent
// literal in the packages the session routes reach names a RequestID, so a new
// audit write cannot be added without one.
func TestEveryAuditEventTheSessionRoutesReachSetsARequestID(t *testing.T) {
	checked := 0
	for _, dir := range []string{"../session", "../authapi"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			fset := token.NewFileSet()
			parsed, err := parser.ParseFile(fset, file, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(parsed, func(node ast.Node) bool {
				literal, ok := node.(*ast.CompositeLit)
				if !ok {
					return true
				}
				selector, ok := literal.Type.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "AuditEvent" {
					return true
				}
				if pkg, ok := selector.X.(*ast.Ident); !ok || pkg.Name != "audit" {
					return true
				}
				checked++
				for _, element := range literal.Elts {
					if kv, ok := element.(*ast.KeyValueExpr); ok {
						if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "RequestID" {
							return true
						}
					}
				}
				t.Errorf("%s: an audit.AuditEvent with no RequestID", fset.Position(literal.Pos()))
				return true
			})
		}
	}
	if checked == 0 {
		t.Fatal("no audit.AuditEvent literal found: the scan measured nothing")
	}
}
