package saml

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// GO-2026-6909 has no fixed crewjam/saml version: no non-test file of this
// package may call the library's ServiceProvider parsers.
func TestNoCrewjamServiceProviderParserCalls(t *testing.T) {
	banned := map[string]bool{
		"ParseResponse":            true,
		"ParseXMLResponse":         true,
		"ParseXMLArtifactResponse": true,
	}
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob: %v (%d files)", err, len(files))
	}
	fset := token.NewFileSet()
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		checked++
		ast.Inspect(f, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && banned[sel.Sel.Name] {
				t.Errorf("%s: %s uses %s (GO-2026-6909)", name, fset.Position(sel.Pos()), sel.Sel.Name)
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no non-test file checked")
	}
}
