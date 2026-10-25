//go:build ignore

// gotestfuncs prints, for each Go test file named on the command line, the top-level test functions and hooks that
// ci/check_go.sh needs to plan the integration shards. It parses the files (go/parser) instead of matching text, so a
// comment or a string literal can neither hide nor invent a function. One record per line, fields split by U+001F:
//
//	T <path> <name>   a top-level test: no receiver, named "Test" or "Test" followed by a rune that is not lower case
//	                  (the rule of the testing package); TestMain is a hook, not a test
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
			case isTest(name):
				fmt.Fprintf(out, "T\x1f%s\x1f%s\n", path, name)
			}
		}
	}
	fmt.Print(out.String())
}
