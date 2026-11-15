package analytics

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// The owner join of a TestOps TEAM read exists only off the investment path, for the three TestOps tables and the team
// dimension, in every compiler that reads them (the flow matrix reads no TestOps table for the team dimension: fixed templates); every other read keeps its own source and its team column.
func TestTestopsOwnerJoinIsOffTheInvestmentPath(t *testing.T) {
	req := TimeseriesRequest{Dimension: DimensionTeam, Measure: MeasureTestPassRate, Interval: BucketIntervalDay,
		StartDate: mustDate(t, "2026-01-01"), EndDate: mustDate(t, "2026-01-07")}
	bd := BreakdownRequest{Dimension: DimensionTeam, Measure: MeasureTestPassRate, StartDate: req.StartDate, EndDate: req.EndDate, TopN: 5}
	sankey := SankeyRequest{Path: []Dimension{DimensionTeam, DimensionRepo}, Measure: MeasureTestPassRate, StartDate: req.StartDate, EndDate: req.EndDate, MaxNodes: 10, MaxEdges: 10}

	compile := map[string]func(useInvestment bool) (string, error){
		"timeseries": func(flag bool) (string, error) {
			q, err := CompileTimeseries(req, "org-1", 30, flag, nil)
			return q.sql, err
		},
		"breakdown": func(flag bool) (string, error) {
			q, err := CompileBreakdown(bd, "org-1", 30, flag, nil)
			return q.sql, err
		},
		"sankey_grouped": func(flag bool) (string, error) {
			grouped, err := CompileSankeyGrouped(sankey, "org-1", 30, flag, nil)
			return grouped.query.sql, err
		},
		"sankey": func(flag bool) (string, error) {
			nodes, edges, err := CompileSankey(sankey, "org-1", 30, flag, nil)
			all := nodes.sql
			for _, e := range edges {
				all += e.sql
			}
			return all, err
		},
	}
	for name, build := range compile {
		off, err := build(false)
		if err != nil {
			t.Fatalf("%s off the investment path: %v", name, err)
		}
		if !strings.Contains(off, "team_repo_ownership") || !strings.Contains(off, ownerTeamColumn) {
			t.Fatalf("%s: a TestOps TEAM read off the investment path does not read the owners:\n%s", name, off)
		}
		if on, err := build(true); err == nil && strings.Contains(on, ownerTeamColumn) {
			t.Fatalf("%s: the owner join leaked onto the investment path:\n%s", name, on)
		}
	}
	// A TestOps measure on the REPOSITORY dimension, and a non-TestOps measure on the team dimension, keep their own reads.
	repoReq := req
	repoReq.Dimension = DimensionRepo
	if q, err := CompileTimeseries(repoReq, "org-1", 30, false, nil); err != nil || strings.Contains(q.sql, ownerTeamColumn) {
		t.Fatalf("a repository series reads the owner join (err %v):\n%s", err, q.sql)
	}
	other := req
	other.Measure = MeasureCount
	if q, err := CompileTimeseries(other, "org-1", 30, false, nil); err != nil || strings.Contains(q.sql, ownerTeamColumn) {
		t.Fatalf("a work-item measure reads the owner join (err %v):\n%s", err, q.sql)
	}
	// The team scope of a TestOps read is the repositories the team owns, never the stored team column.
	scoped := &model.FilterInput{Scope: &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-b"}}}
	repoQ, err := CompileTimeseries(repoReq, "org-1", 30, false, scoped)
	if err != nil || strings.Contains(repoQ.sql, "team_id IN {scope_ids") || !strings.Contains(repoQ.sql, "team_repo_ownership") {
		t.Fatalf("the team scope of a TestOps read must be the ownership rule (err %v):\n%s", err, repoQ.sql)
	}
}
