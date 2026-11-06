package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
)

// C3 (CHAOS-13089): the route-class registry (pdp/routeclass.go) is the ONE gate point of an allowlist decision.
// The code ties to it both ways, so a flip of one class (CHAOS-13083) cannot happen without its code, nor code
// without its flip.

// restClassesOfFile is the set of labels a route file passes to authenticateRESTRequest.
func restClassesOfFile(t *testing.T, name string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	var labels []string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "authenticateRESTRequest" || len(call.Args) == 0 {
			return true
		}
		if lit, ok := call.Args[len(call.Args)-1].(*ast.BasicLit); ok && lit.Kind == token.STRING {
			if label, err := strconv.Unquote(lit.Value); err == nil {
				labels = append(labels, label)
			}
		}
		return true
	})
	return labels
}

func TestC3_RouteClassNamesAreExactlyTheAuthLabels(t *testing.T) {
	got := map[string]bool{}
	for _, name := range restScopeCensusFiles {
		for _, label := range restClassesOfFile(t, name) {
			got[label] = true
		}
	}
	want := map[string]bool{}
	for _, class := range pdp.RouteClassNames() {
		want[class] = true
	}
	var problems []string
	for label := range got {
		if !want[label] {
			problems = append(problems, "auth label "+label+" has no route class")
		}
	}
	for class := range want {
		if !got[class] {
			problems = append(problems, "route class "+class+" is no auth label of a route file")
		}
	}
	sort.Strings(problems)
	if len(got) != 21 || len(want) != 21 || len(problems) != 0 {
		t.Fatalf("%d auth labels, %d route classes:\n%s", len(got), len(want), strings.Join(problems, "\n"))
	}
}

// restCeilingUse reports whether the file calls teamscope.ResolveCeiling, the call that hands a ceiling to its reads.
func restCeilingUse(t *testing.T, name string) bool {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	found := false
	ast.Inspect(file, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "ResolveCeiling" {
			found = true
		}
		return true
	})
	return found
}

func TestC3_CeilingIffNarrowed(t *testing.T) {
	for _, name := range restScopeCensusFiles {
		labels := restClassesOfFile(t, name)
		if len(labels) == 0 {
			t.Fatalf("%s: no authenticateRESTRequest label", name)
		}
		narrowed := false
		for _, label := range labels {
			narrowed = narrowed || pdp.AllowlistNarrowed(label)
		}
		if used := restCeilingUse(t, name); used != narrowed {
			t.Errorf("%s: ResolveCeiling used=%t, but its class(es) %v narrowed=%t", name, used, labels, narrowed)
		}
	}
}

// Q9: one gate point. No handler or other server file refuses an allowlist caller by itself.
func TestC3_NoSecondRefusalInTheServer(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, banned := range []string{"allowlist_route_unsupported", "ReasonAllowlistRouteUnsupported"} {
			if strings.Contains(string(raw), banned) {
				t.Errorf("%s mentions %q: the registry in pdp is the only gate of an allowlist decision", name, banned)
			}
		}
	}
}
