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
	fset := token.NewFileSet()
	var offenders []string
	scanned := 0
	httpGo := filepath.ToSlash(filepath.Join("..", "..", "internal", "providerfoundation", "http.go"))
	// The fixture checker classifies recorded statuses and messages that carry
	// no request path; it is the one named exclusion.
	fixtureChecker := filepath.ToSlash(filepath.Join("..", "..", "cmd", "dev-health-provider-fixture", "main.go"))
	walk := func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		if filepath.ToSlash(path) == fixtureChecker {
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
			if name != "ClassifyHTTP" && name != "ClassifyHTTPWithMessage" {
				return true
			}
			// http.go holds the generic classifier: only its own definitions,
			// which chain ClassifyHTTP -> ClassifyHTTPWithMessage ->
			// ClassifyHTTPForRequest, may call the others. Do and
			// DoUnauthenticated included: they classify with their request path.
			if filepath.ToSlash(path) == httpGo && inClassifierDefinition(file, call) {
				return true
			}
			offenders = append(offenders, fset.Position(call.Pos()).String())
			return true
		})
		return nil
	}
	for _, root := range []string{filepath.Join("..", "..", "internal"), filepath.Join("..", "..", "cmd")} {
		if err := filepath.WalkDir(root, walk); err != nil {
			t.Fatal(err)
		}
	}
	if scanned < 200 {
		t.Fatalf("only %d files scanned: the walk stopped seeing the tree", scanned)
	}
	if len(offenders) > 0 {
		t.Errorf("classify a response with ClassifyHTTPForRequest so its rate limit carries a dimension: %v", offenders)
	}
}

// inClassifierDefinition reports whether call sits inside one of the three
// functions that define the classification chain (ClassifyHTTP,
// ClassifyHTTPWithMessage, ClassifyHTTPForRequest).
func inClassifierDefinition(file *ast.File, call *ast.CallExpr) bool {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Pos() > call.Pos() || call.End() > function.End() {
			continue
		}
		switch function.Name.Name {
		case "ClassifyHTTP", "ClassifyHTTPWithMessage", "ClassifyHTTPForRequest":
			return true
		}
	}
	return false
}
