package analytics

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
)

// CHAOS-9629 (D5988): a team scope weighs every non-TEAM investment read by the share of each repository the teams in
// view hold, through the same expression the explanation uses, with no join (so no row fan-out); the TEAM dimension
// keeps its ownership join and its share, once.
func TestInvestmentContextWeighsATeamScopeByTheRepoShareWithoutAJoin(t *testing.T) {
	scoped := investmentContextFor("org-1", []Dimension{DimensionTheme}, false, true, false, []string{"team-co"})
	if scoped.ScopeShare == "" || scoped.OwnershipTeam {
		t.Fatalf("THEME under a team scope: ScopeShare %q OwnershipTeam %v, want the scope share and no ut join", scoped.ScopeShare, scoped.OwnershipTeam)
	}
	if strings.Contains(scoped.ExtraClauses, "AS ut") {
		t.Errorf("THEME under a team scope joins the ownership rows (row fan-out):\n%s", scoped.ExtraClauses)
	}
	if !scoped.UseRepoAllocation {
		t.Error("a team scope must read the per-repository allocation rows")
	}
	if len(scoped.Bindings) == 0 {
		t.Error("the scope share carries its bindings")
	}
	expr, err := scoped.measure(MeasureCount)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(expr, "allocation_weight * "+scoped.ScopeShare) {
		t.Errorf("COUNT is not weighed by the scope share: %s", expr)
	}
	cycle, err := scoped.measure(MeasureCycleTimeHours)
	if err != nil {
		t.Fatal(err)
	}
	// An intensive measure is the share-weighted mean, sum(value x share) / sum(share), never value x share (D5998).
	if !strings.Contains(cycle, "/ NULLIF(SUM(allocation_weight * ") || !strings.Contains(cycle, "dateDiff('hour', from_ts, to_ts) * allocation_weight * ") {
		t.Errorf("cycle time is not a share-weighted mean: %s", cycle)
	}

	team := investmentContextFor("org-1", []Dimension{DimensionTeam}, false, true, false, []string{"team-co"})
	if !team.OwnershipTeam || team.ScopeShare != "" {
		t.Errorf("TEAM under a team scope: OwnershipTeam %v ScopeShare %q, want the ut join alone (the share is applied once)", team.OwnershipTeam, team.ScopeShare)
	}

	unscoped := investmentContextFor("org-1", []Dimension{DimensionTheme}, false, false, false, nil)
	if unscoped.ScopeShare != "" || unscoped.OwnershipTeam || len(unscoped.Bindings) != 0 {
		t.Errorf("no team scope must change nothing: %+v", unscoped)
	}
	count, err := unscoped.measure(MeasureCount)
	if err != nil || count != "SUM(subcategory_kv.2)" {
		t.Errorf("unscoped COUNT = %q (%v), want SUM(subcategory_kv.2)", count, err)
	}
}

// Every named parameter of a compiled team-scoped statement has a binding (CHAOS-9629): the REPO flow matrix embeds the
// node context's measure in its edge statement, and the scope share's bindings must travel with it. A statement with
// an unbound name is refused by ClickHouse and the page degrades to an empty matrix.
func TestTeamScopedFlowMatrixStatementsBindEveryName(t *testing.T) {
	scoped := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-co", "team-main"}}}
	start := graphqldate.New(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	end := graphqldate.New(time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC))
	useInvestment := true
	names := regexp.MustCompile(`\{([a-z_0-9]+):`)
	for _, dimension := range []Dimension{DimensionRepo, DimensionTeam, DimensionTheme} {
		nodes, edges, err := CompileFlowMatrix(FlowMatrixRequest{Dimension: dimension, Measure: MeasureCount, StartDate: start, EndDate: end, MaxNodes: 10, MaxEdges: 10, UseInvestment: &useInvestment}, "org-1", 30, scoped)
		if err != nil {
			t.Fatalf("%s: %v", dimension, err)
		}
		for label, query := range map[string]compiledQuery{"nodes": nodes, "edges": edges} {
			if query.sql == "" {
				continue
			}
			bound := map[string]int{}
			for _, binding := range query.bindings {
				bound[binding.Name]++
			}
			for _, match := range names.FindAllStringSubmatch(query.sql, -1) {
				if bound[match[1]] == 0 {
					t.Errorf("%s %s: parameter %q has no binding", dimension, label, match[1])
				}
			}
			for name, count := range bound {
				if count > 1 {
					t.Errorf("%s %s: parameter %q is bound %d times", dimension, label, name, count)
				}
			}
		}
	}
}
