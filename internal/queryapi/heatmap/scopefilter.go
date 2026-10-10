// Repo-scope resolution for the three "repo" scope() heatmap metrics
// (review_wait_density, repo_touchpoints, hotspot_risk) -- ports:
//   - api/services/filtering.py's scope_filter_for_metric/
//     resolve_repo_filter_ids
//   - api/queries/scopes.py's resolve_repo_id/resolve_repo_ids/
//     resolve_repo_ids_for_teams/build_scope_filter_multi
//
// Duplicated from explain/repofilter.go rather than imported -- same
// Python source, same "repeat, don't couple" convention that package's
// own doc comment already establishes for this exact helper set.
//
// heatmap.py always calls scope_filter_for_metric with team_column=
// "team_id"/repo_column="repo_id" (services/heatmap.py's three call
// sites never pass either kwarg), so scopeClauseTeam/scopeClauseRepo are
// hardcoded to those two column names, matching explain's own copy.
package heatmap

import (
	"context"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// resolveRepoID ports resolve_repo_id (api/queries/scopes.py:19-50).
// repos is ReplacingMergeTree(last_synced): FINAL dedups before the
// id/repo equality and org_id filters apply, all inside this same read
// (class ruling (a)+(b)).
func resolveRepoID(ctx context.Context, client QueryClient, repoRef, orgID string) (string, bool, error) {
	if client == nil {
		return "", false, ErrUnavailable
	}
	return teamscope.ResolveRepoRef(ctx, client, repoRef, orgID, settingsMaxExecutionTime(), "heatmap: ")
}

// resolveRepoIDs ports resolve_repo_ids (api/queries/scopes.py:53-69).
func resolveRepoIDs(ctx context.Context, client QueryClient, repoRefs []string, orgID string) ([]string, error) {
	if client == nil && len(repoRefs) > 0 {
		for _, ref := range repoRefs {
			if ref != "" {
				return nil, ErrUnavailable
			}
		}
	}
	return teamscope.ResolveRepoRefs(ctx, client, repoRefs, orgID, settingsMaxExecutionTime(), "heatmap: ")
}

// resolveRepoFilterIDs resolves the EXPLICIT repo refs a request names.
// whatRepos is always nil for this route (heatmap's MetricFilter never sets
// filters.what.repos -- build_heatmap_response constructs MetricFilter with
// only time/scope set), kept as a parameter for parity with the ported
// Python signature. A team scope resolves nowhere here: its repositories
// come from team_repo_ownership, pushed into SQL by teamscope.RepoCondition.
func resolveRepoFilterIDs(ctx context.Context, client QueryClient, scopeLevel string, scopeIDs, whatRepos []string, orgID string) ([]string, error) {
	repoRefs := teamscope.NamedRepoRefs(scopeLevel, scopeIDs, whatRepos)
	return resolveRepoIDs(ctx, client, repoRefs, orgID)
}

func scopeClauseRepo(repoIDs []string) (filterSQL string, bindings []dhclickhouse.Binding) {
	if len(repoIDs) == 0 {
		return "", nil
	}
	return " AND repo_id IN {scope_ids:Array(String)}", []dhclickhouse.Binding{
		{Name: "scope_ids", Value: repoIDs},
	}
}

func scopeClauseTeam(teamIDs []string) (filterSQL string, bindings []dhclickhouse.Binding) {
	if len(teamIDs) == 0 {
		return "", nil
	}
	return " AND team_id IN {scope_ids:Array(String)}", []dhclickhouse.Binding{
		{Name: "scope_ids", Value: teamIDs},
	}
}

// scopeFilterForMetric ports scope_filter_for_metric (api/services/
// filtering.py:129-147). Every heatmap.py call site passes org_id=org_id
// (services/heatmap.py's three scope_filter_for_metric calls), so --
// unlike explain.py's own call site (explain/response.go's own declared
// org_id-omission defect) -- there is no org_id-drop defect to declare
// here: the real org id is threaded through on both planes.
func scopeFilterForMetric(ctx context.Context, client QueryClient, metricScope, scopeLevel string, scopeIDs, whatRepos []string, orgID string, asOf time.Time) (filterSQL string, bindings []dhclickhouse.Binding, err error) {
	if metricScope == "team" && scopeLevel == "team" {
		filterSQL, bindings = scopeClauseTeam(scopeIDs)
		return filterSQL, bindings, nil
	}
	if metricScope == "repo" {
		repoIDs, resolveErr := resolveRepoFilterIDs(ctx, client, scopeLevel, scopeIDs, whatRepos, orgID)
		if resolveErr != nil {
			return "", nil, resolveErr
		}
		explicitSQL, explicitBindings := scopeClauseRepo(repoIDs)

		var teamCondition string
		var teamBindings []dhclickhouse.Binding
		if scopeLevel == "team" && len(scopeIDs) > 0 {
			teamCondition, teamBindings = teamscope.RepoCondition(orgID, "repo_id", scopeIDs, asOf)
		}

		filter, bindings := teamscope.NarrowRepoScope(len(teamscope.NamedRepoRefs(scopeLevel, scopeIDs, whatRepos)) > 0, explicitSQL, explicitBindings, teamCondition, teamBindings)
		return filter, bindings, nil
	}
	return "", nil, nil
}

// selectedFilter is the resolved selected teams and repositories of a request
// (CHAOS-9159).
type selectedFilter struct {
	filter teamscope.RouteFilter
}

// served is the two facts the response carries: repo_filter_applied (null when no
// repository is named, false when the metric is not repository-keyed) and the empty
// reason (only when the filter applied and nothing matched).
func (selected selectedFilter) served(repositoryKeyed bool) (applied *bool, reason *string) {
	if !selected.filter.Named {
		return nil, nil
	}
	applied = &repositoryKeyed
	if !repositoryKeyed {
		return applied, nil
	}
	return applied, teamscope.EmptyReasonText(selected.filter.EmptyReason)
}

// selectedScopeFilter resolves the effective teams and repositories of a request and
// builds the one " AND ..." condition of the repository-keyed reads. Teams: scope_id
// of a team scope plus team_ids; repositories: scope_id of a repo scope plus repo_ids;
// the two dimensions AND (teamscope.NarrowRepoScope), a named repository that resolves
// to nothing leaves nothing (D5841). A request that selects neither reads unfiltered,
// as before.
func selectedScopeFilter(ctx context.Context, client QueryClient, orgID, scopeType string, params Params, asOf time.Time) (filterSQL string, bindings []dhclickhouse.Binding, selected selectedFilter, err error) {
	teamIDs := append([]string(nil), params.TeamIDs...)
	repoRefs := append([]string(nil), params.RepoRefs...)
	switch scopeType {
	case "team":
		teamIDs = append(teamIDs, params.ScopeID)
	case "repo":
		repoRefs = append(repoRefs, params.ScopeID)
	}
	filter, err := teamscope.ResolveRouteFilter(ctx, client, orgID, teamIDs, repoRefs, asOf, settingsMaxExecutionTime(), "heatmap: ")
	if err != nil {
		return "", nil, selectedFilter{}, err
	}
	explicitSQL, explicitBindings := scopeClauseRepo(filter.RepoIDs)
	conditionTeams := filter.TeamIDs
	if scopeType == "team" && len(conditionTeams) == 0 {
		// A team scope with no id: the condition of the empty id, as the route has
		// always built it (the answer of a request that selects nothing new is unchanged).
		conditionTeams = []string{""}
	}
	// With both selected, the two conditions compare the repository as text: a UUID
	// column against a typed list AND against the ownership subquery's strings is a
	// type error in ClickHouse (code 169) on the second comparison.
	column := "repo_id"
	if len(conditionTeams) > 0 && explicitSQL != "" {
		column = "toString(repo_id)"
		explicitSQL = " AND toString(repo_id) IN {scope_ids:Array(String)}"
	}
	var teamCondition string
	var teamBindings []dhclickhouse.Binding
	if len(conditionTeams) > 0 {
		teamCondition, teamBindings = teamscope.RepoCondition(orgID, column, conditionTeams, asOf)
	}
	filterSQL, bindings = teamscope.NarrowRepoScope(filter.Named, explicitSQL, explicitBindings, teamCondition, teamBindings)
	return filterSQL, bindings, selectedFilter{filter: filter}, nil
}
