package quadrant

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// selection is the selected teams and repositories of a request (CHAOS-9159) as the
// conditions the metric reads take, and the two facts the response serves about them.
type selection struct {
	// scope is the condition of the plain metric reads (team_id or repository grain).
	scope extraScope
	// attributedTeams is the team condition of the work-item attribution read
	// (cycle_throughput at the team grain), keyed on its own alias.
	attributedTeams extraScope
	// teams are the selected teams at the team grain: an OwnerTeam read narrows
	// the repository owners to them (teamscope.OwnerShares).
	teams []string
	// repoFilterApplied is null when no repository is named, true when the
	// repository grain was narrowed by it, false at the person grain (it takes no filter).
	repoFilterApplied *bool
	emptyReason       *string
	// linked is the named repositories at the team grain (the items linked to their
	// pull requests); nil when no repository is named there. A named repository that
	// leaves no repository (unresolved, or not held by the selected teams) is a linked
	// scope with no ids: every read of it answers nothing.
	linked *linkedRepoScope
}

// resolveSelection builds the selection. The filters AND and never widen (D5844): at
// the repository grain a repository is kept only when it is named AND, with a team
// selected, owned by that team (teamscope.NarrowRepoScope); a named repository that
// resolves to nothing keeps no row (D5841). At the team grain the selected teams
// narrow the points; a named repository narrows them too, through the repository's
// own work: the reviews of its files (user_metrics_daily.repo_id) and the work items
// linked to its pull requests (work_graph_issue_pr, linkeditems), never an item's own
// repository column. With a team selected only the repositories the team holds
// count. The person grain plots the person's own team cohort and takes neither filter.
func resolveSelection(ctx context.Context, client QueryClient, orgID, scope string, params Params) (selection, error) {
	filter, err := teamscope.ResolveRouteFilter(ctx, client, orgID, params.TeamIDs, params.RepoRefs, time.Now().UTC(),
		fmt.Sprintf("SETTINGS max_execution_time = %d", 30), "quadrant: ")
	if err != nil {
		return selection{}, err
	}
	var out selection
	if filter.Named {
		applied := scope == "repo" || scope == "team"
		out.repoFilterApplied = &applied
	}
	switch scope {
	case "team":
		if filter.Named {
			held, err := teamscope.HeldRepoIDs(ctx, client, orgID, filter.RepoIDs, filter.TeamIDs, time.Now().UTC())
			if err != nil {
				return selection{}, err
			}
			out.linked = &linkedRepoScope{repoIDs: held}
			out.emptyReason = teamscope.EmptyReasonText(filter.EmptyReason)
		}
		if len(filter.TeamIDs) > 0 {
			out.teams = filter.TeamIDs
			bindings := []dhclickhouse.Binding{{Name: "scope_team_ids", Value: filter.TeamIDs}}
			out.scope = extraScope{SQL: "\nAND m.team_id IN {scope_team_ids:Array(String)}", Bindings: bindings}
			out.attributedTeams = extraScope{SQL: "\n              AND t.team_id IN {scope_team_ids:Array(String)}", Bindings: bindings}
		}
		if out.linked != nil {
			out.attributedTeams = joinScopes(out.attributedTeams, out.linked.linkedItemCondition("wct.work_item_id"))
		}
	case "repo":
		var explicitSQL string
		var explicitBindings []dhclickhouse.Binding
		if len(filter.RepoIDs) > 0 {
			explicitSQL = " AND repos.id IN {scope_repo_ids:Array(String)}"
			explicitBindings = []dhclickhouse.Binding{{Name: "scope_repo_ids", Value: filter.RepoIDs}}
		}
		var teamCondition string
		var teamBindings []dhclickhouse.Binding
		if len(filter.TeamIDs) > 0 {
			teamCondition, teamBindings = teamscope.RepoCondition(orgID, "repos.id", filter.TeamIDs, time.Now().UTC())
		}
		sql, bindings := teamscope.NarrowRepoScope(filter.Named, explicitSQL, explicitBindings, teamCondition, teamBindings)
		if sql != "" {
			out.scope = extraScope{SQL: "\n" + sql, Bindings: bindings}
		}
		out.emptyReason = teamscope.EmptyReasonText(filter.EmptyReason)
	}
	return out, nil
}
