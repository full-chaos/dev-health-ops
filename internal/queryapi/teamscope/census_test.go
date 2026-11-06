package teamscope

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

// C4 (CHAOS-13090): teamscope.NoCeiling() is the explicit "read every repository" of a read that is not narrowed. It
// may be called only from the listed files: the mixed (M) route packages, whose whole route stays M and so refuses an
// allowlist at the gate, and the dead heatmap helper. A new call, or a listed file that no longer calls it, fails.
var noCeilingSites = []string{
	"internal/queryapi/explain/response.go",
	"internal/queryapi/heatmap/scopefilter.go",
	"internal/queryapi/home/scopefilter.go",
	"internal/queryapi/quadrant/selection.go",
	"internal/queryapi/sankey/scopefilter.go",
}

func TestC4_NoCeilingOnlyInTheListedFiles(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("git", "-C", root, "ls-files", "-z", "--", "internal/*.go").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	const importPath = "github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	found := map[string]bool{}
	fset := token.NewFileSet()
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel == "" || strings.HasSuffix(rel, "_test.go") || strings.HasPrefix(rel, "internal/queryapi/teamscope/") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(root, rel), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", rel, err)
		}
		local := ""
		for _, spec := range file.Imports {
			if path, _ := strconv.Unquote(spec.Path.Value); path == importPath {
				local = "teamscope"
				if spec.Name != nil {
					local = spec.Name.Name
				}
			}
		}
		if local == "" {
			continue
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "NoCeiling" {
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == local {
					found[rel] = true
				}
			}
			return true
		})
	}
	listed := map[string]bool{}
	for _, file := range noCeilingSites {
		listed[file] = true
	}
	var problems []string
	for file := range found {
		if !listed[file] {
			problems = append(problems, file+" calls teamscope.NoCeiling: not on the list")
		}
	}
	for file := range listed {
		if !found[file] {
			problems = append(problems, file+" is listed but does not call teamscope.NoCeiling")
		}
	}
	sort.Strings(problems)
	if len(problems) != 0 {
		t.Fatalf("NoCeiling census:\n%s", strings.Join(problems, "\n"))
	}
}
