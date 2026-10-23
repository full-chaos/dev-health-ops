package authruntime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestKeyStateSourceIsAssignedOnlyByTests: keyStateSource is unexported and
// no production file of this package assigns it, so no env var, flag or
// config value can swap the key state source of a production binary; only
// _test.go files replace it.
func TestKeyStateSourceIsAssignedOnlyByTests(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	declared, scanned := 0, 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(set, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.ValueSpec:
				for _, ident := range n.Names {
					if ident.Name == "keyStateSource" {
						declared++
					}
				}
			case *ast.AssignStmt:
				for _, lhs := range n.Lhs {
					if ident, ok := lhs.(*ast.Ident); ok && ident.Name == "keyStateSource" {
						t.Errorf("%s assigns keyStateSource; only tests may", set.Position(n.Pos()))
					}
				}
			case *ast.UnaryExpr:
				if ident, ok := n.X.(*ast.Ident); ok && n.Op == token.AND && ident.Name == "keyStateSource" {
					t.Errorf("%s takes the address of keyStateSource", set.Position(n.Pos()))
				}
			}
			return true
		})
	}
	if scanned == 0 || declared != 1 {
		t.Fatalf("scanned %d production files, keyStateSource declared %d times; want it declared once", scanned, declared)
	}
}
