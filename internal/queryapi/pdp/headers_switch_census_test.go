package pdp

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// S-e (D6721): the header carrier's cells are read in one place: this package's gate builds the switch, and only the
// declared-settings row names its variable. A second reader would be a switch the gate's caller class does not pick.
func TestOnlyThePDPGateReadsTheHeaderCarrierCells(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("git", "-C", root, "ls-files", "-z", "--", "*.go").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	want := map[string]bool{
		"internal/platform/config/queryapi_options.go": true,
		"internal/queryapi/pdp/pdp.go":                 true,
	}
	got := map[string]bool{}
	fset := token.NewFileSet()
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel == "" || strings.HasSuffix(rel, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(root, rel), nil, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "QueryAPIHeaders" {
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "authzenforce" {
					got[rel] = true
				}
			}
			return true
		})
	}
	for rel := range got {
		if !want[rel] {
			t.Errorf("%s reads authzenforce.QueryAPIHeaders; only the pdp gate and the settings row may", rel)
		}
	}
	for rel := range want {
		if !got[rel] {
			t.Errorf("listed reader %s reads no authzenforce.QueryAPIHeaders (stale list)", rel)
		}
	}
}
