package pepgate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const importPath = "github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"

// constructorSites are the only production files that may build a scope, by constructor. An enforced scope
// (Organization, Allowlist) comes from the PDP decision alone; a Legacy scope from the gate's not-enforced answer and
// the exempt credential classes. The census checks both ways: a call outside the list fails, and an entry no call
// matches fails.
var constructorSites = map[string][]string{
	"Organization": {"internal/queryapi/pdp/pdp.go"},
	"Allowlist":    {"internal/queryapi/pdp/pdp.go"},
	"Legacy": {
		"internal/queryapi/pdp/pdp.go",
		"internal/queryapi/server/rest_access.go",
		"internal/queryapi/server/rest_error_response.go",
	},
}

func TestOnlyTheListedFilesBuildAScope(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("git", "-C", root, "ls-files", "-z", "--", "*.go").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	found := map[string]map[string]bool{}
	fset := token.NewFileSet()
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel == "" || strings.HasSuffix(rel, "_test.go") || strings.HasPrefix(rel, "internal/queryapi/pepgate/") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(root, rel), nil, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		local := ""
		for _, spec := range file.Imports {
			if path, _ := strconv.Unquote(spec.Path.Value); path == importPath {
				local = "pepgate"
				if spec.Name != nil {
					local = spec.Name.Name
				}
			}
		}
		if local == "" {
			continue
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == local {
				if found[sel.Sel.Name] == nil {
					found[sel.Sel.Name] = map[string]bool{}
				}
				found[sel.Sel.Name][rel] = true
			}
			return true
		})
	}
	for constructor, files := range found {
		for file := range files {
			if !contains(constructorSites[constructor], file) {
				t.Errorf("%s calls pepgate.%s: only %v may", file, constructor, constructorSites[constructor])
			}
		}
	}
	var stale []string
	for constructor, files := range constructorSites {
		for _, file := range files {
			if !found[constructor][file] {
				stale = append(stale, constructor+" "+file)
			}
		}
	}
	sort.Strings(stale)
	if len(stale) != 0 {
		t.Errorf("stale constructor sites (no call matches): %v", stale)
	}
}

// The package is a leaf: it imports the standard library only, so every layer can carry a Scope.
func TestPackageImportsTheStandardLibraryOnly(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range file.Imports {
			path, _ := strconv.Unquote(spec.Path.Value)
			if first, _, _ := strings.Cut(path, "/"); strings.Contains(first, ".") {
				t.Errorf("%s imports %s", name, path)
			}
		}
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
