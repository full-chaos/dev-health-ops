package analytics

import (
	"github.com/full-chaos/dev-health-go/readers"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// The investment-path team (the `ut` join of investmentContextFor: the TEAM dimension of the sankey, breakdown,
// timeseries and flow matrix, and the flow matrix's team bridge) is the team that OWNS the repository of the effort
// row (team_repo_ownership, the ownership rule of the team scope and of the coverage), never the work-item
// attribution vote. A work unit's effort is already split per repository (work_unit_repo_effort); each repository
// row names its owning teams.
//
//   - A repository with no owner is "unassigned" (true data, not a failure to attribute).
//   - A repository owned by several teams is shared among them by linked share, the main owner by its share and each
//     lesser owner by its own lesser share, never equally (teamscope.OwnerShares; D5988): the effort of the
//     unit is counted once across the TEAM groups, so every other dimension keeps its totals. Under a team scope the
//     owner rows are narrowed to the teams in view AFTER the shares are computed over every owner, so a selected
//     co-owner keeps its own share of the repository, never the whole of it (CHAOS-9413).
const ownershipShareExpr = "if(ut.owner_matched = 1, ut.owner_share, 1.0)"

// ownershipTeamSource is the derived table the `ut` alias names on the investment path. Its columns keep the names
// the consumers already use (ut.team_label, ut.team_id) next to the owner share.
func ownershipTeamSource(orgID string, scopedTeams []string) (string, []clickhouse.Binding) {
	shares, bindings := teamscope.OwnerShares(orgID, scopedTeams, time.Now().UTC())
	return `(
        SELECT
            owner_repo_id,
            owner_team_id AS team_id,
            owner_team_label AS team_label,
            owner_share,
            owner_matched
        FROM ` + shares + `
    )`, bindings
}

// teamScopeIDs are the team ids of a team-level scope filter, nil when the request has none.
func teamScopeIDs(filters *model.FilterInput) []string {
	if filters != nil && filters.Scope != nil && filters.Scope.Level == model.ScopeLevelInputTeam {
		return filters.Scope.Ids
	}
	return nil
}

// ownershipDbExpression is dbExpression(measure, true, true) weighted by shareExpr, the share of the repository the
// row stands for: the `ut` join's owner share (ownershipShareExpr: the TEAM dimension, each owning team its own
// share) or the team scope's RepoShare (every other dimension under a team scope, the owners in view together
// never more than the repository once; CHAOS-9629, D5988). One weighting, two sources of the share, the same
// arithmetic as investmentexplain's measure for the same rows.
func ownershipDbExpression(measure Measure, shareExpr string) (string, error) {
	switch measure {
	case MeasureCount, MeasureThroughput:
		return "SUM(subcategory_kv.2 * allocation_weight * " + shareExpr + ")", nil
	case MeasureIssueSize:
		return "SUM(subcategory_kv.2 * allocation_weight * " + readers.UnitSizeWeightColumn + " * " + shareExpr + ")", nil
	case MeasureCycleTimeHours:
		return "SUM(dateDiff('hour', from_ts, to_ts) * allocation_weight * " + shareExpr + ") / NULLIF(SUM(allocation_weight * " + shareExpr + "), 0)", nil
	}
	return dbExpression(measure, true, true)
}
