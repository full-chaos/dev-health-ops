package frozengoldens

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The exemption's digest is the set of files the guard records while it runs; a read that does not go through the
// recorder (reads.go) would be a guard input outside the digest. So no other source of the package may read a file,
// list a directory, run git or touch the embedded manifest.
func TestEveryGuardReadGoesThroughTheRecorder(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	banned := map[string]bool{"os": true, "exec": true, "fs": true, "ioutil": true}
	bannedFilepath := map[string]bool{"Glob": true, "Walk": true, "WalkDir": true}
	checked := 0
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") || source == "reads.go" {
			continue
		}
		checked++
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "go:embed") {
			t.Errorf("%s embeds a file outside the recorder", source)
		}
		file, err := parser.ParseFile(token.NewFileSet(), source, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.SelectorExpr:
				if pkg, ok := n.X.(*ast.Ident); ok && (banned[pkg.Name] || pkg.Name == "filepath" && bannedFilepath[n.Sel.Name]) {
					t.Errorf("%s uses %s.%s: every guard read goes through the recorder in reads.go", source, pkg.Name, n.Sel.Name)
				}
			case *ast.Ident:
				if n.Name == "manifestFile" {
					t.Errorf("%s reads the embedded manifest outside the recorder", source)
				}
			}
			return true
		})
	}
	if checked == 0 {
		t.Fatal("no package source was checked")
	}
}
