package providersync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strings"
	"testing"
)

// A list walk that feeds a snapshot close (the held-set census) may end only on
// a marker the provider states: a cursor, a Link or X-Next-Page, isLast or
// total. A page shorter than the size asked for is not one: providers serve
// short pages in the middle of a listing, and a catalog taken as complete on
// one closes the ownership of everything the walk did not list.
func TestSnapshotFeedingWalksDoNotTakeAShortPageAsTheEnd(t *testing.T) {
	files := map[string]bool{"internal/providersync/jira_team_catalog.go": true}
	for path := range heldSetWalks {
		files[path] = true
	}
	sizeName := regexp.MustCompile(`(?i)(per|page|size|max|limit)`)
	fset := token.NewFileSet()
	for path := range files {
		file, err := parser.ParseFile(fset, "../../"+path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			compare, ok := node.(*ast.BinaryExpr)
			if !ok || (compare.Op != token.LSS && compare.Op != token.LEQ) {
				return true
			}
			call, ok := compare.X.(*ast.CallExpr)
			if !ok {
				return true
			}
			if name, ok := call.Fun.(*ast.Ident); !ok || name.Name != "len" {
				return true
			}
			var size strings.Builder
			ast.Inspect(compare.Y, func(inner ast.Node) bool {
				if ident, ok := inner.(*ast.Ident); ok {
					size.WriteString(ident.Name + " ")
				}
				return true
			})
			if sizeName.MatchString(size.String()) {
				t.Errorf("%s: %s compares a length with a page size (%s): a short page is read as the end of a walk that feeds a snapshot close",
					path, fset.Position(compare.Pos()), strings.TrimSpace(size.String()))
			}
			return true
		})
	}
}
