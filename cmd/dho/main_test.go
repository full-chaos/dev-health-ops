package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

const (
	modulePrefix = "github.com/full-chaos/dev-health-ops/internal/"
	cliPackage   = modulePrefix + "cli"
)

// TestMainStaysThin pins main.go's shape structurally: exactly two
// functions; main is one call, cli.Main("dho", commands()); commands returns
// one composite literal whose every element is a call <pkg>.Command() with no
// arguments; every import is internal/cli or a vertical package that
// commands uses. Logic added to main.go (flag parsing, wiring, a conditional,
// a loop) fails here and says why.
func TestMainStaysThin(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}

	imports := map[string]string{} // local name -> path
	for _, spec := range file.Imports {
		path, _ := strconv.Unquote(spec.Path.Value)
		if !strings.HasPrefix(path, modulePrefix) {
			t.Fatalf("main.go imports %s; only internal/cli and vertical packages belong here", path)
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if spec.Name != nil {
			t.Fatalf("main.go renames import %s; keep the package name", path)
		}
		imports[name] = path
	}
	if imports["cli"] != cliPackage {
		t.Fatalf("main.go must import %s", cliPackage)
	}

	functions := map[string]*ast.FuncDecl{}
	for _, declaration := range file.Decls {
		switch declaration := declaration.(type) {
		case *ast.FuncDecl:
			functions[declaration.Name.Name] = declaration
		case *ast.GenDecl:
			if declaration.Tok != token.IMPORT {
				t.Fatalf("main.go declares a %s; a thin entrypoint declares only main and commands", declaration.Tok)
			}
		}
	}
	if len(functions) != 2 || functions["main"] == nil || functions["commands"] == nil {
		t.Fatalf("main.go declares %d functions; want exactly main and commands", len(functions))
	}

	// main: one statement, cli.Main("dho", commands()).
	body := functions["main"].Body.List
	if len(body) != 1 {
		t.Fatalf("main has %d statements, want 1", len(body))
	}
	call := expressionCall(t, body[0])
	if selector(call.Fun) != "cli.Main" || len(call.Args) != 2 {
		t.Fatalf("main must call cli.Main(name, commands())")
	}
	if literal, ok := call.Args[0].(*ast.BasicLit); !ok || literal.Value != `"dho"` {
		t.Fatalf(`main must name the binary "dho"`)
	}
	if inner, ok := call.Args[1].(*ast.CallExpr); !ok || len(inner.Args) != 0 {
		t.Fatalf("main must pass commands()")
	} else if ident, ok := inner.Fun.(*ast.Ident); !ok || ident.Name != "commands" {
		t.Fatalf("main must pass commands()")
	}

	// commands: return []cli.Command{ <pkg>.Command(), ... }.
	statements := functions["commands"].Body.List
	if len(statements) != 1 {
		t.Fatalf("commands has %d statements, want one return", len(statements))
	}
	ret, ok := statements[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		t.Fatalf("commands must be a single return")
	}
	literal, ok := ret.Results[0].(*ast.CompositeLit)
	if !ok || len(literal.Elts) == 0 {
		t.Fatalf("commands must return a non-empty []cli.Command literal")
	}
	used := map[string]bool{"cli": true}
	for index, element := range literal.Elts {
		element, ok := element.(*ast.CallExpr)
		if !ok || len(element.Args) != 0 {
			t.Fatalf("commands element %d must be <pkg>.Command()", index)
		}
		name := selector(element.Fun)
		pkg, fn, found := strings.Cut(name, ".")
		if !found || fn != "Command" || imports[pkg] == "" {
			t.Fatalf("commands element %d is %q; want <imported pkg>.Command()", index, name)
		}
		used[pkg] = true
	}
	for name := range imports {
		if !used[name] {
			t.Fatalf("main.go imports %s but commands does not use it", name)
		}
	}
}

// TestCommandTreeIsValid runs the real tree through the same validation
// Execute applies before dispatch.
func TestCommandTreeIsValid(t *testing.T) {
	if err := cli.Validate(commands()); err != nil {
		t.Fatal(err)
	}
}

func expressionCall(t *testing.T, statement ast.Stmt) *ast.CallExpr {
	t.Helper()
	expression, ok := statement.(*ast.ExprStmt)
	if !ok {
		t.Fatalf("main's statement is not a call")
	}
	call, ok := expression.X.(*ast.CallExpr)
	if !ok {
		t.Fatalf("main's statement is not a call")
	}
	return call
}

func selector(expression ast.Expr) string {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	ident, ok := selector.X.(*ast.Ident)
	if !ok {
		return ""
	}
	return ident.Name + "." + selector.Sel.Name
}
