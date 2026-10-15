package analytics

import (
	"fmt"
	"strings"
	"testing"
)

func TestExactSumsRewritesEverySumAndNothingElse(t *testing.T) {
	exact := func(inner string) string { return "toFloat64(sum(toDecimal128(toFloat64(" + inner + "), 18)))" }
	cases := []struct{ in, want string }{
		{"SUM(subcategory_kv.2)", exact("subcategory_kv.2")},
		{"SUM(subcategory_kv.2 * allocation_weight * if(ut.owner_matched = 1, ut.owner_share, 1.0))",
			exact("subcategory_kv.2 * allocation_weight * if(ut.owner_matched = 1, ut.owner_share, 1.0)")},
		{"SUM(dateDiff('hour', from_ts, to_ts) * w) / NULLIF(SUM(w), 0)",
			exact("dateDiff('hour', from_ts, to_ts) * w") + " / NULLIF(" + exact("w") + ", 0)"},
		{"SUM(if(label = ')', 1, 0))", exact("if(label = ')', 1, 0)")},
		{"AVG(dateDiff('hour', from_ts, to_ts))", "AVG(dateDiff('hour', from_ts, to_ts))"},
		{"mySUM(x)", "mySUM(x)"},
	}
	for _, c := range cases {
		got, err := exactSums(c.in)
		if err != nil {
			t.Fatalf("exactSums(%q) error = %v", c.in, err)
		}
		if got != c.want {
			t.Errorf("exactSums(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
	if _, err := exactSums("SUM(x"); err == nil {
		t.Error("exactSums on an unbalanced SUM( = nil error, want an error")
	}
}

// Every flow-matrix query that sums a Float64 measure takes the sum in
// Decimal128, so its value and its ORDER BY ... LIMIT cut do not depend on the
// engine's merge order. The fixed TEAM, REPO and WORK_TYPE templates count
// distinct items (an integer) and are left as they are.
func TestFlowMatrixSumsAreExactOnEveryInvestmentPath(t *testing.T) {
	yes := true
	for _, dimension := range []Dimension{DimensionTeam, DimensionRepo, DimensionTheme, DimensionSubcategory} {
		for _, measure := range []Measure{MeasureCount, MeasureThroughput, MeasureCycleTimeHours} {
			nodes, edges, err := CompileFlowMatrix(FlowMatrixRequest{
				Dimension: dimension, Measure: measure, UseInvestment: &yes,
				StartDate: mustDate(t, "2026-06-01"), EndDate: mustDate(t, "2026-09-01"),
				MaxNodes: 50, MaxEdges: 200,
			}, "org", 30, nil)
			if err != nil {
				t.Fatalf("%s/%s: CompileFlowMatrix error = %v", dimension, measure, err)
			}
			for kind, q := range map[string]compiledQuery{"nodes": nodes, "edges": edges} {
				name := fmt.Sprintf("%s/%s %s", dimension, measure, kind)
				if strings.Contains(q.sql, "SUM(") {
					t.Errorf("%s: a Float64 SUM( is left in the query", name)
				}
				if measure != MeasureCycleTimeHours || dimension == DimensionTeam || dimension == DimensionRepo {
					if !strings.Contains(q.sql, "sum(toDecimal128(toFloat64(") {
						t.Errorf("%s: no exact Decimal128 sum in the query", name)
					}
				}
			}
		}
	}
}

func TestFlowMatrixFixedTemplatesAreUnchanged(t *testing.T) {
	no := false
	for _, c := range []struct {
		dimension            Dimension
		nodesTmpl, edgesTmpl string
	}{
		{DimensionWorkType, flowMatrixWorkTypeNodesTemplate, flowMatrixWorkTypeEdgesTemplate},
		{DimensionTeam, flowMatrixTeamNodesTemplate, flowMatrixTeamEdgesTemplate},
		{DimensionRepo, flowMatrixRepoNodesTemplate, flowMatrixRepoEdgesTemplate},
	} {
		nodes, edges, err := CompileFlowMatrix(FlowMatrixRequest{
			Dimension: c.dimension, Measure: MeasureCount, UseInvestment: &no,
			StartDate: mustDate(t, "2026-06-01"), EndDate: mustDate(t, "2026-09-01"),
			MaxNodes: 50, MaxEdges: 200,
		}, "org", 30, nil)
		if err != nil {
			t.Fatalf("%s: CompileFlowMatrix error = %v", c.dimension, err)
		}
		if want := fmt.Sprintf(c.nodesTmpl, settingsMaxExecutionTime(30)); nodes.sql != want {
			t.Errorf("%s nodes SQL changed:\n%s", c.dimension, nodes.sql)
		}
		if want := fmt.Sprintf(c.edgesTmpl, settingsMaxExecutionTime(30)); edges.sql != want {
			t.Errorf("%s edges SQL changed:\n%s", c.dimension, edges.sql)
		}
	}
}
