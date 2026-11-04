package identity

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// delegates names the one function that returns the IdentityBySubject result to its callers instead of reading
// ServiceProvider itself; each of its callers is held to the same rule.
var delegates = map[string]string{"../identity/social/service.go": "lookup"}

func readsServiceProvider(n ast.Node) []token.Pos {
	var at []token.Pos
	ast.Inspect(n, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.SelectorExpr:
			if v.Sel.Name == "ServiceProvider" {
				at = append(at, v.Pos())
			}
		case *ast.KeyValueExpr:
			if k, ok := v.Key.(*ast.Ident); ok && k.Name == "ServiceProvider" {
				at = append(at, v.Pos())
			}
		case *ast.CallExpr:
			if f, ok := v.Fun.(*ast.Ident); ok && f.Name == "signInTo" {
				at = append(at, v.Pos())
			}
		}
		return true
	})
	return at
}

func gapsFor(fset *token.FileSet, file *ast.File, callee string, skipFunc string) (gaps []string) {
	for _, d := range file.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Name.Name == skipFunc {
			continue
		}
		reads := readsServiceProvider(fn.Body)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != callee {
				return true
			}
			for _, r := range reads {
				if r > call.End() {
					return true
				}
			}
			gaps = append(gaps, fmt.Sprintf("%s: %s in %s has no ServiceProvider read after it", fset.Position(call.Pos()), callee, fn.Name.Name))
			return true
		})
	}
	return gaps
}

// callSiteGaps lists each IdentityBySubject call in src that no ServiceProvider read follows in its function.
func callSiteGaps(path string, src string) []string {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, 0)
	if err != nil {
		return []string{err.Error()}
	}
	if d, ok := delegates[path]; ok {
		return append(gapsFor(fset, file, "IdentityBySubject", d), gapsFor(fset, file, d, d)...)
	}
	return gapsFor(fset, file, "IdentityBySubject", "")
}

func TestCallSiteGapsFindsALookupWithNoLaterServiceProviderRead(t *testing.T) {
	const head = "package p\nfunc f(s S) (Q, error) {\n"
	cases := []struct {
		name string
		src  string
		gaps int
	}{
		{"read after call", head + "p, _, _ := s.IdentityBySubject(1)\nreturn Q{ServiceProvider: p.ServiceProvider}, nil\n}\n", 0},
		{"selector read after call", head + "p, _, _ := s.IdentityBySubject(1)\nif p.ServiceProvider {\nreturn Q{}, nil\n}\nreturn Q{}, nil\n}\n", 0},
		{"no read at all", head + "p, _, _ := s.IdentityBySubject(1)\nreturn Q{ID: p.ID}, nil\n}\n", 1},
		{"read only before the call", head + "x := T{ServiceProvider: true}\n_ = x\np, _, _ := s.IdentityBySubject(1)\nreturn Q{ID: p.ID}, nil\n}\n", 1},
		{"second lookup in same file without read", head + "p, _, _ := s.IdentityBySubject(1)\nreturn Q{ServiceProvider: p.ServiceProvider}, nil\n}\nfunc g(s S) Q {\np, _, _ := s.IdentityBySubject(2)\nreturn Q{ID: p.ID}\n}\n", 1},
		{"two lookups one read after the first only", head + "p, _, _ := s.IdentityBySubject(1)\nq := Q{ServiceProvider: p.ServiceProvider}\nr, _, _ := s.IdentityBySubject(2)\nreturn Q{ID: r.ID}, nil\n}\n", 1},
		{"comment and string do not count", head + "p, _, _ := s.IdentityBySubject(1)\n// ServiceProvider\n_ = \"ServiceProvider\"\nreturn Q{ID: p.ID}, nil\n}\n", 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := callSiteGaps("x.go", tc.src); len(got) != tc.gaps {
				t.Fatalf("gaps = %v, want %d", got, tc.gaps)
			}
		})
	}
}

func TestCallSiteGapsHoldsTheDelegateCallersToTheSameRule(t *testing.T) {
	const path = "../identity/social/service.go"
	const lookup = "package p\nfunc (s S) lookup() (Q, bool, error) {\nreturn s.store.IdentityBySubject(1)\n}\n"
	if got := callSiteGaps(path, lookup+"func (s S) ok() error {\np, _, _ := s.lookup()\nreturn signInTo(p)\n}\n"); len(got) != 0 {
		t.Fatalf("gaps = %v, want none", got)
	}
	if got := callSiteGaps(path, lookup+"func (s S) bad() error {\np, _, _ := s.lookup()\n_ = p\nreturn nil\n}\n"); len(got) != 1 {
		t.Fatalf("gaps = %v, want 1", got)
	}
	if got := callSiteGaps("../other.go", lookup); len(got) != 1 {
		t.Fatalf("gaps = %v, want 1 outside the delegate path", got)
	}
}
