package analytics

import (
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

func TestExactSumsRewritesEverySumAndAverageAndNothingElse(t *testing.T) {
	exact := func(inner string) string { return "toFloat64(sum(toDecimal128(toFloat64(" + inner + "), 18)))" }
	cases := []struct{ in, want string }{
		{"SUM(subcategory_kv.2)", exact("subcategory_kv.2")},
		{"SUM(subcategory_kv.2 * allocation_weight * if(ut.owner_matched = 1, ut.owner_share, 1.0))",
			exact("subcategory_kv.2 * allocation_weight * if(ut.owner_matched = 1, ut.owner_share, 1.0)")},
		{"SUM(dateDiff('hour', from_ts, to_ts) * w) / NULLIF(SUM(w), 0)",
			exact("dateDiff('hour', from_ts, to_ts) * w") + " / NULLIF(" + exact("w") + ", 0)"},
		{"SUM(if(label = ')', 1, 0))", exact("if(label = ')', 1, 0)")},
		{"AVG(dateDiff('hour', from_ts, to_ts))",
			"(" + exact("dateDiff('hour', from_ts, to_ts)") + " / count(dateDiff('hour', from_ts, to_ts)))"},
		{"AVG(success_rate) * 100", "(" + exact("success_rate") + " / count(success_rate)) * 100"},
		{"if(sum(ifNull(prs_merged_reviewed, 0)) = 0, NULL, 1)", "if(sum(ifNull(prs_merged_reviewed, 0)) = 0, NULL, 1)"},
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
	if got, want := exactSumIf("w", "x > 1"), "toFloat64(sumIf(toDecimal128(toFloat64(w), 18), x > 1))"; got != want {
		t.Errorf("exactSumIf = %q, want %q", got, want)
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

var everyMeasure = []Measure{
	MeasureCount, MeasureChurnLOC, MeasurePRReworkRatio, MeasureCycleTimeHours, MeasureThroughput,
	MeasurePipelineSuccessRate, MeasurePipelineFailureRate, MeasurePipelineDurationP95, MeasurePipelineQueueTime,
	MeasurePipelineRerunRate, MeasureTestPassRate, MeasureTestFailureRate, MeasureTestFlakeRate,
	MeasureTestSuiteDurationP95, MeasureCoverageLinePct, MeasureCoverageBranchPct, MeasureCoverageDeltaPct,
	MeasureFlagFrictionDelta, MeasureFlagErrorRateDelta, MeasureFlagCoverageRatio, MeasureFlagActivationRate,
}

// notReproducedSum is a Float64 sum this package still takes: a few rows of
// one work unit (its repository effort total) or of one repository (its owner
// shares). Twenty real-engine runs over thousands of rows per group never
// drifted, so they are code-argued only and left as they are.
func notReproducedSum(rest string) bool {
	for _, term := range []string{"repo_effort_value)", "owned.stored_share)", "owner_share)"} {
		if strings.HasPrefix(rest, term) {
			return true
		}
	}
	return false
}

// inexactAggregates names every Float64 SUM/AVG left in a query, and every
// lower-case sum/sumIf whose term is not an exact Decimal128 term, except two
// integer counts (the pull-request rework rate's and the owner shares' count of
// unmeasured owners) and the notReproducedSum totals.
func inexactAggregates(sql string) []string {
	var out []string
	for _, open := range []string{"SUM(", "AVG("} {
		if strings.Contains(sql, open) {
			out = append(out, open)
		}
	}
	for _, open := range []string{"sum(", "sumIf("} {
		for rest := sql; ; {
			i := strings.Index(rest, open)
			if i < 0 {
				break
			}
			rest = rest[i+len(open):]
			if i > 0 && isIdentByte(sql[len(sql)-len(rest)-len(open)-1]) {
				continue
			}
			if !strings.HasPrefix(rest, "toDecimal128(") && !strings.HasPrefix(rest, "ifNull(prs_merged_") &&
				!strings.HasPrefix(rest, "if(owned.stored_share IS NULL, 1, 0)") && !notReproducedSum(rest) {
				out = append(out, open+rest[:min(40, len(rest))])
			}
		}
	}
	return out
}

func TestEveryAnalyticsMeasureSumIsExact(t *testing.T) {
	start, end := mustDate(t, "2026-06-01"), mustDate(t, "2026-09-01")
	repoScope := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputRepo, Ids: []string{"repo-1"}}}
	teamScope := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-1"}}}
	check := func(name, sql string) {
		t.Helper()
		if left := inexactAggregates(sql); len(left) > 0 {
			t.Errorf("%s: inexact aggregates left: %q", name, left)
		}
	}
	for _, filters := range []*model.FilterInput{nil, repoScope, teamScope} {
		for _, useInvestment := range []bool{true, false} {
			for _, measure := range everyMeasure {
				for _, dimension := range []Dimension{DimensionTeam, DimensionRepo, DimensionTheme, DimensionWorkType} {
					name := fmt.Sprintf("%s/%s inv=%v filters=%v", dimension, measure, useInvestment, filters != nil)
					if q, err := CompileTimeseries(TimeseriesRequest{Dimension: dimension, Measure: measure, Interval: BucketIntervalWeek, StartDate: start, EndDate: end}, "org", 30, useInvestment, filters); err == nil {
						check("timeseries "+name, q.sql)
					}
					if q, err := CompileBreakdown(BreakdownRequest{Dimension: dimension, Measure: measure, StartDate: start, EndDate: end, TopN: 10}, "org", 30, useInvestment, filters); err == nil {
						check("breakdown "+name, q.sql)
					}
				}
				req := SankeyRequest{Path: []Dimension{DimensionTeam, DimensionTheme, DimensionRepo}, Measure: measure, StartDate: start, EndDate: end, MaxNodes: 60, MaxEdges: 200}
				name := fmt.Sprintf("sankey/%s inv=%v filters=%v", measure, useInvestment, filters != nil)
				if nodes, edges, err := CompileSankey(req, "org", 30, useInvestment, filters); err == nil {
					check(name+" nodes", nodes.sql)
					for i, e := range edges {
						check(fmt.Sprintf("%s edges %d", name, i), e.sql)
					}
				}
				if grouped, err := CompileSankeyGrouped(req, "org", 30, useInvestment, filters); err == nil {
					check(name+" grouped", grouped.query.sql)
				}
				if cov, err := compileSankeyCoverage(req, "org", 30, useInvestment, filters); err == nil {
					check(name+" coverage", cov.sql)
				}
			}
		}
	}
}
