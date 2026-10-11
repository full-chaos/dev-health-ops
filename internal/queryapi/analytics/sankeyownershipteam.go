package analytics

import (
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// The Investment flow's TEAM node is the team that OWNS the repository of the effort row (team_repo_ownership, the
// ownership rule of the team scope and of the coverage), never the work-item attribution vote. A work unit's effort
// is already split per repository (work_unit_repo_effort); each repository row names its owning teams.
//
//   - A repository with no owner is "unassigned" (true data, not a failure to attribute).
//   - A repository owned by several teams is shared equally among them (teamscope.OwnerShares): the effort of the
//     unit is counted once across the TEAM nodes, so THEME and REPO nodes keep their totals. Under a team scope the
//     owners are narrowed to the teams in view, so one selected team holds its repositories whole.
//   - The vote is read by none of the statements of this flow. It stays where the attribution TEAM dimension itself
//     is shown (breakdown, timeseries, catalog, flow matrix: investmentContextFor).
const (
	ownershipTeamColumn = "ifNull(nullIf(ot.owner_team_label, ''), 'unassigned')"
	ownershipShareExpr  = "if(ot.owner_matched = 1, ot.owner_share, 1.0)"
)

// ownershipTeamContext is investmentContextFor for a path that holds TEAM and reads the team from ownership. The
// per-repository allocation source is always used: ownership is a property of the repository row, and the unit's own
// scalar repo_id is NULL for a unit that touches several repositories.
func ownershipTeamContext(orgID string, path []Dimension, filters *model.FilterInput) investmentContext {
	var dims []Dimension
	for _, d := range path {
		if d != DimensionTeam {
			dims = append(dims, d)
		}
	}
	ictx := investmentContextFor(dims, false, true, needsAuthorJoin(filters))

	var scoped []string
	if filters != nil && filters.Scope != nil && filters.Scope.Level == model.ScopeLevelInputTeam {
		scoped = filters.Scope.Ids
	}
	shares, bindings := teamscope.OwnerShares(orgID, scoped, time.Now().UTC())
	ictx.ExtraClauses = strings.Join([]string{
		ictx.ExtraClauses,
		fmt.Sprintf("LEFT JOIN %s AS ot ON ot.owner_repo_id = toString(%s.repo_id)", shares, ictx.Alias),
	}, "\n")
	ictx.Bindings = bindings
	return ictx
}

// ownershipDbExpression is dbExpression(measure, true, true) weighted by the owner's share of the repository.
func ownershipDbExpression(measure Measure) (string, error) {
	switch measure {
	case MeasureCount, MeasureThroughput:
		return "SUM(subcategory_kv.2 * allocation_weight * " + ownershipShareExpr + ")", nil
	case MeasureChurnLOC:
		return "SUM(if(effort_metric = 'churn_loc', subcategory_kv.2 * effort_value, 0) * " + ownershipShareExpr + ")", nil
	case MeasureCycleTimeHours:
		return "SUM(dateDiff('hour', from_ts, to_ts) * allocation_weight * " + ownershipShareExpr + ") / NULLIF(SUM(allocation_weight * " + ownershipShareExpr + "), 0)", nil
	}
	return dbExpression(measure, true, true)
}
