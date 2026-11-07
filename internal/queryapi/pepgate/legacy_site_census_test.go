package pepgate

import (
	"fmt"
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

// legacySites is the closed list of production call sites of pepgate.Legacy, one entry per enclosing function with
// its call count. Gate.Authorize builds it when a route is not enforced (two returns: nil gate, cell off) and for an
// enforced row asked with no organization (one return: only a superuser passes with none, product rule 1);
// legacyClaims builds it for an accepted caller the PDP does not decide (HS256 and edge tokens). A new site, or a
// second call in a listed function, fails here.
var legacySites = map[string]int{
	"internal/queryapi/pdp/pdp.go Gate.Authorize":          3,
	"internal/queryapi/server/rest_access.go legacyClaims": 1,
}

func TestLegacyScopeIsBuiltAtTheListedSitesOnly(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("git", "-C", root, "ls-files", "-z", "--", "internal/*.go").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	got := map[string]int{}
	fset := token.NewFileSet()
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel == "" || strings.HasSuffix(rel, "_test.go") || strings.HasPrefix(rel, "internal/queryapi/pepgate/") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(root, rel), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
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
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				recv := fn.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if id, ok := recv.(*ast.Ident); ok {
					name = id.Name + "." + name
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Legacy" {
					if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == local {
						got[rel+" "+name]++
					}
				}
				return true
			})
		}
	}
	keys := map[string]bool{}
	for k := range got {
		keys[k] = true
	}
	for k := range legacySites {
		keys[k] = true
	}
	var sorted []string
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		if got[k] != legacySites[k] {
			t.Errorf("pepgate.Legacy calls at %s = %s, listed %s", k, fmt.Sprint(got[k]), fmt.Sprint(legacySites[k]))
		}
	}
}
