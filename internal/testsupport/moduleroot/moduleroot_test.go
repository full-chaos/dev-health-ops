package moduleroot

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAbsoluteMapsTheModulePathFormToTheModuleRoot(t *testing.T) {
	got := absolute("example.com/mod/internal/x/x_test.go", filepath.FromSlash("/work/mod"), "example.com/mod")
	want := filepath.FromSlash("/work/mod/internal/x/x_test.go")
	if got != want {
		t.Fatalf("absolute = %q; want %q", got, want)
	}
}

func TestAbsoluteKeepsAnAbsolutePathAndAnotherModule(t *testing.T) {
	abs := filepath.FromSlash("/src/mod/internal/x/x_test.go")
	if got := absolute(abs, "/work/mod", "example.com/mod"); got != abs {
		t.Fatalf("an absolute path changed: %q", got)
	}
	other := "example.org/other/x.go"
	if got := absolute(other, "/work/mod", "example.com/mod"); got != other {
		t.Fatalf("another module's path changed: %q", got)
	}
	// a module path that only SHARES A PREFIX with the real one is another module
	lookalike := "example.com/module/x.go"
	if got := absolute(lookalike, "/work/mod", "example.com/mod"); got != lookalike {
		t.Fatalf("a prefix lookalike was mapped: %q", got)
	}
}

func TestFindWalksUpToTheNearestGoMod(t *testing.T) {
	top := t.TempDir()
	if err := os.WriteFile(filepath.Join(top, "go.mod"), []byte("// c\nmodule \"example.com/top\"\n\ngo 1.27\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(top, "a", "b")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	root, path, err := find(nested)
	if err != nil || root != top || path != "example.com/top" {
		t.Fatalf("find = %q, %q, %v", root, path, err)
	}
	if _, _, err := find(filepath.Join(t.TempDir(), "x")); err == nil {
		t.Log("a temp dir without go.mod found one above it (acceptable only if an ancestor has go.mod)")
	}
}

// callerOf calls Caller from a helper frame: skip 1 must report the function that called the helper, i.e. the
// skip accounts for Caller's own frame exactly once.
func callerOf() (string, bool) {
	_, file, _, ok := Caller(1)
	return file, ok
}

func TestCallerReportsAnExistingAbsoluteSourceFileWithSkipCounted(t *testing.T) {
	_, here, _, ok := Caller(0)
	if !ok || !filepath.IsAbs(here) || filepath.Base(here) != "moduleroot_test.go" {
		t.Fatalf("Caller(0) = %q, %v; want the absolute path of moduleroot_test.go", here, ok)
	}
	if _, err := os.Stat(here); err != nil {
		t.Fatalf("Caller(0) names a file that is not on disk (-trimpath form not mapped): %v", err)
	}
	viaHelper, ok := callerOf()
	if !ok || viaHelper != here {
		t.Fatalf("Caller(1) from a helper = %q, %v; want the calling test file %q", viaHelper, ok, here)
	}
}

func TestRootIsTheModuleOfTheTestedPackage(t *testing.T) {
	root, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("Root %q has no go.mod: %v", root, err)
	}
	_, here, _, _ := Caller(0)
	if !strings.HasPrefix(here, root+string(filepath.Separator)) {
		t.Fatalf("this test file %q is not under Root %q", here, root)
	}
}

// permittedRuntimeCaller names every file in scope of the guard below that may still call runtime.Caller directly,
// with the reason. It is empty today. An entry must still exist and still call runtime.Caller (a stale entry
// fails), so the list cannot rot into a silent exemption.
var permittedRuntimeCaller = map[string]string{}

// inGuardScope: the guard covers test files and test support, where runtime.Caller is used to find files on disk
// and breaks under -trimpath. Product code may use runtime.Caller for caller info in logs or stacks; it is not
// covered (a file-lookup there would have to be fixed in its own change).
func inGuardScope(rel string) bool {
	return strings.HasSuffix(rel, "_test.go") || strings.HasPrefix(rel, "internal/testsupport/")
}

// No test or test-support file may call runtime.Caller directly: it breaks under -trimpath. The walk is derived
// from the module itself; finding no in-scope Go file at all would prove nothing, so it fails.
func TestNoRuntimeCallerInTestsOrTestSupportOutsideThisPackage(t *testing.T) {
	root, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	inScope := 0
	var offenders []string
	used := map[string]bool{}
	walkErr := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "node_modules", "third_party", "moduleroot":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if !inGuardScope(rel) {
			return nil
		}
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return nil // a file that does not parse is another gate's finding
		}
		inScope++
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "runtime" && selector.Sel.Name == "Caller" {
				if _, permitted := permittedRuntimeCaller[rel]; permitted {
					used[rel] = true
					return true
				}
				offenders = append(offenders, fmt.Sprintf("%s:%d", rel, fset.Position(call.Pos()).Line))
			}
			return true
		})
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk: %v", walkErr)
	}
	if inScope < 100 {
		t.Fatalf("only %d in-scope Go files under %s: the derivation is empty or broken", inScope, root)
	}
	for rel, reason := range permittedRuntimeCaller {
		if !used[rel] {
			t.Errorf("permittedRuntimeCaller names %s (%s) but it no longer calls runtime.Caller: delete the entry", rel, reason)
		}
	}
	if len(offenders) != 0 {
		t.Fatalf("%d runtime.Caller call(s) in test code or test support (use moduleroot.Caller; -trimpath breaks the runtime form): %v", len(offenders), offenders)
	}
}
