package policy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-13596: go-api emits an unjudged reason only from the closed set, and the set holds no reason the enforcer's
// removed fallback returns counted (hs256, no_org, served_org). The census reads the production source of this package:
// every countEnforceUnjudged call site, its reason resolved to the constant's value, against the production
// enforceUnjudgedReasons (also the metric description's list). No copy of the set is written here.
func TestEveryUnjudgedReasonGoAPICanEmitIsInTheClosedSet(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	consts := map[string]string{}
	var files []*ast.File
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value := spec.(*ast.ValueSpec)
				for i, ident := range value.Names {
					if i >= len(value.Values) {
						continue
					}
					if lit, ok := value.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						if s, err := strconv.Unquote(lit.Value); err == nil {
							consts[ident.Name] = s
						}
					}
				}
			}
		}
	}
	var emitted []string
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if fn, ok := call.Fun.(*ast.Ident); !ok || fn.Name != "countEnforceUnjudged" {
				return true
			}
			if len(call.Args) != 3 {
				t.Errorf("%s: countEnforceUnjudged with %d arguments", fset.Position(call.Pos()), len(call.Args))
				return true
			}
			switch reason := call.Args[2].(type) {
			case *ast.Ident:
				value, ok := consts[reason.Name]
				if !ok {
					t.Errorf("%s: the reason %s is no string constant of this package", fset.Position(call.Pos()), reason.Name)
					return true
				}
				emitted = append(emitted, value)
			case *ast.BasicLit:
				value, _ := strconv.Unquote(reason.Value)
				emitted = append(emitted, value)
			default:
				t.Errorf("%s: a reason that is not a constant: it could be any value", fset.Position(call.Pos()))
			}
			return true
		})
	}
	if len(emitted) == 0 {
		t.Fatal("no countEnforceUnjudged call site found: the census measured nothing")
	}
	for _, reason := range emitted {
		if !slices.Contains(enforceUnjudgedReasons, reason) {
			t.Errorf("go-api can emit the unjudged reason %q, which is outside the closed set %v", reason, enforceUnjudgedReasons)
		}
	}
	for _, retired := range []string{"hs256", "no_org", "served_org"} {
		if slices.Contains(enforceUnjudgedReasons, retired) || slices.Contains(emitted, retired) {
			t.Errorf("the retired unjudged reason %q is still in the set %v or emitted %v", retired, enforceUnjudgedReasons, emitted)
		}
	}
	for _, reason := range enforceUnjudgedReasons {
		if !slices.Contains(emitted, reason) {
			t.Errorf("the closed set names %q, which no call site emits", reason)
		}
	}
}
