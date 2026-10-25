package authruntime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newAccessIssuer builds an issuer with no kid recorder. Its one caller in
// this package must be newRecordingIssuer, so no runtime path can mint around
// the recorder.
func TestNewAccessIssuerHasOneCallerNewRecordingIssuer(t *testing.T) {
	callers := newAccessIssuerCallers(t, ".")
	if len(callers) != 1 || callers[0] != "newRecordingIssuer" {
		t.Fatalf("newAccessIssuer callers = %v; want exactly [newRecordingIssuer]", callers)
	}
}

func newAccessIssuerCallers(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var callers []string
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "newAccessIssuer" {
						callers = append(callers, fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	return callers
}
