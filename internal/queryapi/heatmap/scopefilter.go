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
