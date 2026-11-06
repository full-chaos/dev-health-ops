package server

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// C1 (CHAOS-10864 PR-1): a REST handler must read its organization from claims.Scope, the scope the enforced PDP
// decision allowed, never from claims.OrgID. This census walks the REST route files and lists every use of
// claims.OrgID that is not an error, response or log sink, by file and the call (or statement) it feeds. Every
// handler is cut over (CHAOS-13089), so the list of allowed uses is empty.

var restScopeCensusFiles = []string{
	"quadrant_route.go", "heatmap_route.go", "sankey_route.go", "home_route.go", "opportunities_route.go",
	"filter_options_route.go", "investment_route.go", "investment_explain_route.go", "drilldown_prs_route.go",
	"drilldown_issues_route.go", "workunits_route.go", "workunit_explain_route.go", "people_route.go",
	"people_summary_route.go", "people_metric_route.go", "people_drilldown_prs_route.go",
	"people_drilldown_issues_route.go", "explain_route.go", "flame_route.go", "flame_aggregated_route.go",
}

// restScopeNotCutOver is every non-sink use of claims.OrgID: "file: target" -> count. CHAOS-13089 cut every handler
// over, so it is empty; a new non-sink use fails the census.
var restScopeNotCutOver = map[string]int{}

// restScopeSink reports a callee that only answers, logs or admits: it serves no data of the organization.
func restScopeSink(callee string) bool {
	switch {
	case strings.HasPrefix(callee, "write"), callee == "admitModel",
		strings.HasPrefix(callee, "log."), strings.HasPrefix(callee, "slog."):
		return true
	}
	return false
}

func TestRESTHandlersReadTheOrganizationFromTheScope(t *testing.T) {
	got := map[string]int{}
	fset := token.NewFileSet()
	for _, name := range restScopeCensusFiles {
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var stack []ast.Node
		ast.Inspect(file, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, n)
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "OrgID" {
				return true
			}
			if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "claims" {
				return true
			}
			if target := restScopeTarget(stack); target != "" {
				got[name+": "+target]++
			}
			return true
		})
	}
	var problems []string
	for key, n := range got {
		if want := restScopeNotCutOver[key]; n != want {
			problems = append(problems, fmt.Sprintf("%s: %d use(s), listed %d", key, n, want))
		}
	}
	for key, want := range restScopeNotCutOver {
		if _, seen := got[key]; !seen {
			problems = append(problems, fmt.Sprintf("%s: listed %d, no use (stale entry)", key, want))
		}
	}
	sort.Strings(problems)
	if len(problems) != 0 {
		t.Fatalf("claims.OrgID reaches a read outside the not-cut-over list:\n%s", strings.Join(problems, "\n"))
	}
}

// restScopeTarget names what a claims.OrgID use feeds: the nearest enclosing call, or the statement that holds it.
// "" is a sink.
func restScopeTarget(stack []ast.Node) string {
	for i := len(stack) - 2; i >= 0; i-- {
		switch node := stack[i].(type) {
		case *ast.CallExpr:
			callee := restScopeCallee(node.Fun)
			if restScopeSink(callee) {
				return ""
			}
			return callee
		case *ast.AssignStmt:
			return "assign"
		case *ast.ValueSpec:
			return "var"
		case *ast.ReturnStmt:
			return "return"
		case *ast.BinaryExpr:
			return "compare"
		case ast.Stmt:
			return fmt.Sprintf("%T", node)
		}
	}
	return "file"
}

func restScopeCallee(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		return restScopeCallee(f.X) + "." + f.Sel.Name
	case *ast.CallExpr:
		return restScopeCallee(f.Fun) + "()"
	case *ast.IndexExpr:
		return restScopeCallee(f.X)
	case *ast.FuncLit:
		return "func"
	}
	return fmt.Sprintf("%T", fun)
}
