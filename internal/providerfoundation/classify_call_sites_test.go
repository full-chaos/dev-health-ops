package providerfoundation

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// A rate limit is recorded with the budget dimension of its request only if
// the code that classified the response knew the request path. Every
// production caller outside http.go (where the generic classification lives)
// must therefore call ClassifyHTTPForRequest; a direct ClassifyHTTP or
// ClassifyHTTPWithMessage call elsewhere classifies without a dimension and the
// durable observation is silently not written.
func TestNoProductionCodeClassifiesAResponseWithoutItsRequestPath(t *testing.T) {
	root := filepath.Join("..", "..", "internal")
	fset := token.NewFileSet()
	var offenders []string
	scanned := 0
	err := filepath.WalkDir(filepath.Join(root), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if filepath.ToSlash(path) == filepath.ToSlash(filepath.Join(root, "providerfoundation", "http.go")) {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		scanned++
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := ""
			switch fun := call.Fun.(type) {
			case *ast.Ident:
				name = fun.Name
			case *ast.SelectorExpr:
				name = fun.Sel.Name
			}
			if name == "ClassifyHTTP" || name == "ClassifyHTTPWithMessage" {
				offenders = append(offenders, fset.Position(call.Pos()).String())
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 200 {
		t.Fatalf("only %d files scanned: the walk stopped seeing the tree", scanned)
	}
	if len(offenders) > 0 {
		t.Errorf("classify a response with ClassifyHTTPForRequest so its rate limit carries a dimension: %v", offenders)
	}
}
