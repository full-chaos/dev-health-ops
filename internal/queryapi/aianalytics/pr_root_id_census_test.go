package aianalytics

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-9225 census: no source of this package spells a pull request root id by hand. The id of a pull
// request in the edge tables has ONE constructor (internal/prrootid); the drilldown chips were served
// with "#" while the writers store ":". A string literal "#" or "#pr" joined into an id is refused.
func TestNoSourceBuildsAPullRequestRootIDByHand(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) < 10 {
		t.Fatalf("the census read %d files (%v): it did not measure the package", len(files), err)
	}
	scanned := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		ast.Inspect(parsed, func(node ast.Node) bool {
			lit, ok := node.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			if value, err := strconv.Unquote(lit.Value); err == nil && (value == "#" || value == "#pr") {
				t.Errorf("%s: the literal %s joins a pull request root id by hand: use internal/prrootid", file, lit.Value)
			}
			return true
		})
	}
	if scanned < 8 {
		t.Fatalf("the census scanned %d source files", scanned)
	}
}
