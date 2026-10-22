package graph

import (
	"context"
	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/analytics"
)

// A call that is not the resolution of a GraphQL field has no selection to
// read. Everything is then selected: a part is left out only on the positive
// knowledge that the operation did not ask for it.
func TestAnAnalyticsCallWithNoSelectionToReadSelectsEverything(t *testing.T) {
	if got := analyticsSelection(context.Background()); got != analytics.EverythingSelected() {
		t.Fatalf("selection with no field on the context = %+v, want everything selected", got)
	}
	everything := analytics.EverythingSelected()
	if !everything.Breakdowns || !everything.EvidenceQualityStats || !everything.EvidenceQualityByGroup || !everything.SankeyCoverage || !everything.SankeyFlow {
		t.Fatalf("EverythingSelected leaves a part out: %+v", everything)
	}
	if (analytics.Selection{}) == everything {
		t.Fatal("the zero selection selects nothing")
	}
}

func sankeySelectionOf(t *testing.T, inner ...string) analytics.Selection {
	t.Helper()
	var innerSet ast.SelectionSet
	for _, name := range inner {
		innerSet = append(innerSet, &ast.Field{Name: name, Alias: name})
	}
	sankey := &ast.Field{Name: "sankey", Alias: "sankey", SelectionSet: innerSet}
	analyticsField := &ast.Field{Name: "analytics", Alias: "analytics", SelectionSet: ast.SelectionSet{sankey}}
	opCtx := &graphql.OperationContext{Variables: map[string]any{}}
	ctx := graphql.WithOperationContext(context.Background(), opCtx)
	ctx = graphql.WithFieldContext(ctx, &graphql.FieldContext{
		Field: graphql.CollectedField{Field: analyticsField, Selections: analyticsField.SelectionSet},
	})
	return analyticsSelection(ctx)
}

func TestAnalyticsSelectionSankeyFlowFollowsNodesAndEdges(t *testing.T) {
	cases := []struct {
		name         string
		inner        []string
		coverage, fl bool
	}{
		{"coverage only", []string{"coverage"}, true, false},
		{"unit only", []string{"unit"}, false, false},
		{"nodes", []string{"nodes"}, false, true},
		{"edges", []string{"edges"}, false, true},
		{"coverage and nodes", []string{"coverage", "nodes"}, true, true},
	}
	for _, c := range cases {
		got := sankeySelectionOf(t, c.inner...)
		if got.SankeyCoverage != c.coverage || got.SankeyFlow != c.fl {
			t.Errorf("%s: coverage=%v flow=%v, want coverage=%v flow=%v", c.name, got.SankeyCoverage, got.SankeyFlow, c.coverage, c.fl)
		}
	}
}
