package graph

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// notImplementedResolverNames names every resolver whose body is a gqlgen
// "not implemented" stub. A resolver that gains a real body must drop its
// line here; a stub that is not listed fails the test, so an unbuilt field
// can never reach a router unnoticed.
var notImplementedResolverNames = []string{
	"mutationResolver.CloneSavedReport",
	"mutationResolver.CreateSavedReport",
	"mutationResolver.DeleteSavedReport",
	"mutationResolver.TriggerReport",
	"mutationResolver.UpdateSavedReport",
	"queryResolver.AiAttributedPrs",
	"queryResolver.AiAttributionOverview",
	"queryResolver.AiComparison",
	"queryResolver.AiGovernanceSummary",
	"queryResolver.AiImpactSummary",
	"queryResolver.AiOpportunities",
	"queryResolver.AiReviewLoad",
	"queryResolver.AiRiskBreakdown",
	"queryResolver.AiWorkflowDrilldown",
	"queryResolver.CompoundingRisk",
	"queryResolver.DevChangeSummary",
	"queryResolver.DevDataHealth",
	"queryResolver.DevEvidenceSearch",
	"queryResolver.DevMetric",
	"queryResolver.DevMetricCatalog",
	"queryResolver.DevScopeSearch",
	"queryResolver.DevStatusSnapshot",
	"queryResolver.DevWorkGraphNeighbors",
	"queryResolver.Experiments",
	"queryResolver.Home",
	"queryResolver.ImproveOpportunities",
	"queryResolver.ProductTelemetryDashboard",
	"queryResolver.ProductTelemetryPlatformDashboard",
	"queryResolver.Recommendations",
	"queryResolver.ReportRuns",
	"queryResolver.SavedReport",
	"queryResolver.SavedReports",
	"queryResolver.TestopsRisk",
	"queryResolver.WorkItemTeamAttributions",
	"subscriptionResolver.MetricsUpdated",
	"subscriptionResolver.SyncProgress",
	"subscriptionResolver.TaskStatus",
}

// stubResolvers returns "Receiver.Method" for every method in
// schema.resolvers.go whose body is a single panic(...) call carrying a
// "not implemented" message.
func stubResolvers(t *testing.T) map[string]struct{} {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "schema.resolvers.go", nil, 0)
	if err != nil {
		t.Fatalf("parse schema.resolvers.go: %v", err)
	}
	stubs := map[string]struct{}{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Body == nil || len(fn.Recv.List) != 1 {
			continue
		}
		if len(fn.Body.List) != 1 {
			continue
		}
		stmt, ok := fn.Body.List[0].(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := stmt.X.(*ast.CallExpr)
		if !ok {
			continue
		}
		if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "panic" {
			continue
		}
		if !callMentionsNotImplemented(call) {
			continue
		}
		recv := fn.Recv.List[0].Type
		if star, ok := recv.(*ast.StarExpr); ok {
			recv = star.X
		}
		recvIdent, ok := recv.(*ast.Ident)
		if !ok {
			continue
		}
		stubs[recvIdent.Name+"."+fn.Name.Name] = struct{}{}
	}
	return stubs
}

func callMentionsNotImplemented(call *ast.CallExpr) bool {
	found := false
	ast.Inspect(call, func(n ast.Node) bool {
		if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING && strings.Contains(lit.Value, "not implemented") {
			found = true
		}
		return !found
	})
	return found
}

func TestNotImplementedResolversMatchAllowlist(t *testing.T) {
	notImplementedResolvers := make(map[string]struct{}, len(notImplementedResolverNames))
	for _, name := range notImplementedResolverNames {
		notImplementedResolvers[name] = struct{}{}
	}
	stubs := stubResolvers(t)
	if len(stubs) == 0 && len(notImplementedResolvers) > 0 {
		t.Fatal("parsed no stub resolvers; the detector no longer recognises the stub shape")
	}

	var unlisted, built []string
	for name := range stubs {
		if _, ok := notImplementedResolvers[name]; !ok {
			unlisted = append(unlisted, name)
		}
	}
	for name := range notImplementedResolvers {
		if _, ok := stubs[name]; !ok {
			built = append(built, name)
		}
	}
	sort.Strings(unlisted)
	sort.Strings(built)
	if len(unlisted) > 0 {
		t.Errorf("resolvers with a not-implemented body that are not in the allowlist: %v", unlisted)
	}
	if len(built) > 0 {
		t.Errorf("allowlist names resolvers that are built or absent; remove them: %v", built)
	}
}
