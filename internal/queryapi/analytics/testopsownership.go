package analytics

import (
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// ownerTeamColumn is the team of a TestOps series: the owner of the repository as of the request, from
// team_repo_ownership, not the team stored on the daily row.
const ownerTeamColumn = "own.owner_team_id"

// isTestopsMeasure reports whether the measure reads one of the three TestOps daily tables.
func isTestopsMeasure(measure Measure) bool {
	switch measureSourceTable(measure) {
	case "testops_pipeline_metrics_daily", "testops_test_metrics_daily", "testops_coverage_metrics_daily":
		return true
	}
	return false
}

// testopsOwner carries the ownership join of a TestOps TEAM series. The zero value is inactive: every other measure
// and the investment path keep their own source and columns.
type testopsOwner struct {
	measure  Measure
	join     string
	bindings []clickhouse.Binding
}

// newTestopsOwner decides, for one compiled read, whether its team comes from ownership. It is active for a TestOps
// measure off the investment path whose dimensions include the team; the join is written once and every dimension
// column of the read goes through column.
func newTestopsOwner(orgID string, measure Measure, dims []Dimension, useInvestment bool, filters *model.FilterInput) testopsOwner {
	owner := testopsOwner{measure: measure}
	if useInvestment || !isTestopsMeasure(measure) {
		return owner
	}
	hasTeam := false
	for _, dim := range dims {
		if dim == DimensionTeam {
			hasTeam = true
		}
	}
	if !hasTeam {
		return owner
	}
	asOf := time.Now().UTC()
	fromWhere, bindings := teamscope.OwnershipFromWhere(orgID, asOf)
	scopeFilter := ""
	if ids := scopedTeamIDs(filters); len(ids) > 0 {
		scopeFilter = fmt.Sprintf("\n      AND o.team_id IN {%s:Array(String)}", teamscope.BindingTeamIDs)
		bindings = append(bindings, clickhouse.Binding{Name: teamscope.BindingTeamIDs, Value: ids})
	}
	owner.join = fmt.Sprintf(`
INNER JOIN (
    SELECT DISTINCT o.team_id AS owner_team_id, coalesce(toString(o.repo_id), toString(r.id)) AS owned_repo_id
%s%s
) AS own ON toString(repo_id) = own.owned_repo_id`, fromWhere, scopeFilter)
	owner.bindings = bindings
	return owner
}

func (owner testopsOwner) active() bool { return owner.join != "" }

// column is the SQL of a dimension: the owner team for the team dimension of an active read, dbCol otherwise.
func (owner testopsOwner) column(dim Dimension, dbCol string) string {
	if owner.active() && dim == DimensionTeam {
		return ownerTeamColumn
	}
	return dbCol
}

// filterColumns are the filter columns of a TestOps read: the team scope is the repositories the team owns
// (teamscope.RepoCondition), never the stored team column. On a read whose own join already restricts to the scoped
// owners the scope adds no clause of its own.
func (owner testopsOwner) filterColumns(measure Measure, useInvestment bool) filterColumns {
	cols := defaultFilterColumns()
	if useInvestment || !isTestopsMeasure(measure) {
		return cols
	}
	cols.TeamByOwnership = true
	cols.TeamRepo = "toString(repo_id)"
	cols.TeamScopeOnJoin = owner.active()
	return cols
}

func scopedTeamIDs(filters *model.FilterInput) []string {
	if filters == nil || filters.Scope == nil || filters.Scope.Level != model.ScopeLevelInputTeam {
		return nil
	}
	var ids []string
	for _, id := range filters.Scope.Ids {
		if id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}
