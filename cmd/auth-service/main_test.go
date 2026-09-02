package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestMainStaysThin enforces CHAOS-4881's binding constraint:
// cmd/auth-service/main.go is a thin entrypoint only.
//
// A prose constraint in a doc comment is a constraint nothing checks. This
// parses the file and asserts the shape structurally: exactly one function,
// named main, whose body is a single call, importing only the runtime
// package. Adding configuration parsing, wiring, or a domain call here fails
// this test and says why, instead of drifting one commit at a time the way
// cmd/query-api's internal/ tree did.
func TestMainStaysThin(t *testing.T) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "main.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}

	const runtimePackage = `"github.com/full-chaos/dev-health-ops/internal/auth/authruntime"`
	if len(file.Imports) != 1 || file.Imports[0].Path.Value != runtimePackage {
		var paths []string
		for _, imported := range file.Imports {
			paths = append(paths, imported.Path.Value)
		}
		t.Fatalf(
			"main.go imports %v; a thin entrypoint imports only %s, so any other import "+
				"means logic moved into main that belongs behind an internal/ interface",
			paths, runtimePackage,
		)
	}

	var functions []*ast.FuncDecl
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok {
			functions = append(functions, function)
		}
	}
	if len(functions) != 1 || functions[0].Name.Name != "main" {
		t.Fatalf("main.go declares %d function(s); a thin entrypoint declares exactly one, main", len(functions))
	}

	body := functions[0].Body
	if body == nil || len(body.List) != 1 {
		t.Fatalf("func main has %d statement(s); a thin entrypoint has exactly one", len(body.List))
	}
	if _, ok := body.List[0].(*ast.ExprStmt); !ok {
		t.Fatalf("func main's only statement is %T; a thin entrypoint's is a single call", body.List[0])
	}

	// No package-level state either: a var or const here is configuration or
	// wiring that belongs in the runtime package.
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		if general.Tok == token.VAR || general.Tok == token.CONST || general.Tok == token.TYPE {
			t.Fatalf("main.go declares a package-level %s; a thin entrypoint declares none", general.Tok)
		}
	}
}
