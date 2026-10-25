//go:build ignore

// gotestfuncs prints, for each Go test file named on the command line, the top-level test functions and hooks that
// ci/check_go.sh needs to plan the integration shards. It parses the files (go/parser) instead of matching text, so a
// comment or a string literal can neither hide nor invent a function. One record per line, fields split by U+001F:
//
//	T <path> <name>   a top-level test by the rule cmd/go applies (load.isTest + isTestFunc): no receiver, no type
//	                  parameters, named "Test" or "Test" followed by a rune that is not lower case, one parameter of
//	                  type *T or *x.T, no results; TestMain is a hook, not a test. A Test-named function with another
//	                  signature is no test (go test refuses to build such a package: "wrong signature").
//	H <path> <func>   the first top-level `func init` or `func TestMain` of the file
//
// It is run with `go run` / `go build` on this file alone (build tag ignore keeps it out of ./...).
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"
)

func isTest(name string) bool {
	if !strings.HasPrefix(name, "Test") {
		return false
	}
	if len(name) == len("Test") {
		return true
	}
	r, _ := utf8.DecodeRuneInString(name[len("Test"):])
	return !unicode.IsLower(r)
}

// isTestFunc is cmd/go's check: one parameter of type *T or *x.T (it cannot know the import name of testing), no
// results, no type parameters.
func isTestFunc(fn *ast.FuncDecl) bool {
	if fn.Type.TypeParams != nil && fn.Type.TypeParams.NumFields() > 0 {
		return false
	}
	if fn.Type.Results != nil && len(fn.Type.Results.List) > 0 || fn.Type.Params == nil || len(fn.Type.Params.List) != 1 || len(fn.Type.Params.List[0].Names) > 1 {
		return false
	}
	ptr, ok := fn.Type.Params.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	if name, ok := ptr.X.(*ast.Ident); ok && name.Name == "T" {
		return true
	}
	sel, ok := ptr.X.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "T"
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: gotestfuncs FILE...")
		os.Exit(2)
	}
	out := &strings.Builder{}
	for _, path := range os.Args[1:] {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gotestfuncs: %v\n", err)
			os.Exit(1)
		}
		hooked := false
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			name := fn.Name.Name
			switch {
			case name == "init" || name == "TestMain":
				if !hooked {
					hooked = true
					fmt.Fprintf(out, "H\x1f%s\x1f%s\n", path, name)
				}
			case isTest(name) && isTestFunc(fn):
				fmt.Fprintf(out, "T\x1f%s\x1f%s\n", path, name)
			}
		}
	}
	fmt.Print(out.String())
}
