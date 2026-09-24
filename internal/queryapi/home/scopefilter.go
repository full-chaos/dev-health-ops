// Repo/team-scope resolution -- ports:
//   - api/services/filtering.py's resolve_repo_filter_ids/scope_filter_for_metric
//   - api/queries/scopes.py's resolve_repo_id/resolve_repo_ids/
//     build_scope_filter_multi
//
// A team scope is the exception: its repositories come from
// team_repo_ownership through the one shared
// internal/queryapi/teamscope.RepoCondition, not from a per-package
// copy. The rule is identical on every route, and a route resolving it its
// own way answers a different question under the same word.
//
// DEDUP: repos is ReplacingMergeTree(last_synced) and is read FINAL here,
// org_id filtered in the same statement -- api/queries/scopes.py's
// resolve_repo_id/resolve_repo_ids read it with no dedup at all, a declared
// Python-plane defect (see internal/goapiproof/restcorpus.go's home
// declarations), matching sankey/heatmap's own precedent of always reading
// this table FINAL regardless of whether a given aggregate happens to be
// dedup-invariant.
package home

import (
	"context"
	"fmt"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

func settingsMaxExecutionTime() string {
	return "SETTINGS max_execution_time = 30"
}

// resolveRepoID ports resolve_repo_id (api/queries/scopes.py:19-50).
func resolveRepoID(ctx context.Context, client QueryClient, repoRef, orgID string) (string, bool, error) {
	var query string
	var bindings []dhclickhouse.Binding
	if parsed, err := pythonparity.ParseUUID(repoRef); err == nil {
		query = fmt.Sprintf(`
SELECT toString(id) AS id
FROM repos FINAL
WHERE toString(id) = {repo_id:String}
  AND org_id = {org_id:String}
LIMIT 1
%s
`, settingsMaxExecutionTime())
		bindings = []dhclickhouse.Binding{
			{Name: "repo_id", Value: parsed.String()},
			{Name: "org_id", Value: orgID},
		}
	} else {
		query = fmt.Sprintf(`
SELECT toString(id) AS id
FROM repos FINAL
WHERE repo = {repo_name:String}
  AND org_id = {org_id:String}
LIMIT 1
%s
`, settingsMaxExecutionTime())
		bindings = []dhclickhouse.Binding{
			{Name: "repo_name", Value: repoRef},
			{Name: "org_id", Value: orgID},
		}
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return "", false, fmt.Errorf("home: resolve repo id: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", false, fmt.Errorf("home: iterate resolve repo id rows: %w", err)
		}
		return "", false, nil
	}
	var id string
	if err := rows.Scan(&id); err != nil {
		return "", false, fmt.Errorf("home: scan resolve repo id row: %w", err)
	}
	return id, true, nil
}

// resolveRepoIDs ports resolve_repo_ids (api/queries/scopes.py:53-69).
func resolveRepoIDs(ctx context.Context, client QueryClient, repoRefs []string, orgID string) ([]string, error) {
	var resolved []string
	for _, ref := range repoRefs {
		if ref == "" {
			continue
		}
		id, ok, err := resolveRepoID(ctx, client, ref, orgID)
		if err != nil {
			return nil, err
		}
		if ok {
			resolved = append(resolved, id)
		}
	}
	return resolved, nil
}

// repoScopeFilter narrows a repo-keyed metric to the repositories a request
// names. Explicit repo refs (scope.IDs at scope="repo", or what.repos) are
// resolved and verified one at a time through resolveRepoIDs -- that list is
// bounded by what the caller named, not by organization scale, so it stays a
// plain array binding. A team scope contributes the repositories that team
// OWNS, as teamscope.RepoCondition resolves them from team_repo_ownership.
// The two are ORed when both are present, so a request naming a team and
// explicit repos sees the union.
//
// asOf is the response's own instant, so every metric in one response
// resolves the same team membership.
func repoScopeFilter(ctx context.Context, client QueryClient, f Filters, orgID, repoColumn string, asOf time.Time) (string, []dhclickhouse.Binding, error) {
	var repoRefs []string
	if f.Scope.Level == "repo" {
		repoRefs = append(repoRefs, f.Scope.IDs...)
	}
	repoRefs = append(repoRefs, f.What.Repos...)

	explicitIDs, err := resolveRepoIDs(ctx, client, repoRefs, orgID)
	if err != nil {
		return "", nil, err
	}

	var teamCondition string
	var teamBindings []dhclickhouse.Binding
	if f.Scope.Level == "team" && len(f.Scope.IDs) > 0 {
		teamCondition, teamBindings = teamscope.RepoCondition(orgID, repoColumn, f.Scope.IDs, asOf)
	}

	switch {
	case len(explicitIDs) > 0 && teamCondition != "":
		condition := fmt.Sprintf(" AND (%s IN {scope_ids:Array(String)} OR %s)", repoColumn, teamCondition)
		return condition, append(scopeBindingsMulti(explicitIDs), teamBindings...), nil
	case len(explicitIDs) > 0:
		return scopeClauseMulti(explicitIDs, repoColumn), scopeBindingsMulti(explicitIDs), nil
	case teamCondition != "":
		return " AND " + teamCondition, teamBindings, nil
	default:
		return "", nil, nil
	}
}

// scopeFilterForMetric ports scope_filter_for_metric (api/services/
// filtering.py:129-147).
func scopeFilterForMetric(ctx context.Context, client QueryClient, metricScope string, f Filters, orgID, teamColumn, repoColumn string, asOf time.Time) (string, []dhclickhouse.Binding, error) {
	if metricScope == "team" && f.Scope.Level == "team" {
		return scopeClauseMulti(f.Scope.IDs, teamColumn), scopeBindingsMulti(f.Scope.IDs), nil
	}
	if metricScope == "repo" {
		return repoScopeFilter(ctx, client, f, orgID, repoColumn, asOf)
	}
	return "", nil, nil
}

// scopeClauseMulti/scopeBindingsMulti port build_scope_filter_multi
// (api/queries/scopes.py:105-117).
func scopeClauseMulti(ids []string, column string) string {
	if len(ids) == 0 {
		return ""
	}
	return fmt.Sprintf(" AND %s IN {scope_ids:Array(String)}", column)
}

func scopeBindingsMulti(ids []string) []dhclickhouse.Binding {
	if len(ids) == 0 {
		return nil
	}
	return []dhclickhouse.Binding{{Name: "scope_ids", Value: ids}}
}

// workCategoryFilter ports work_category_filter (api/services/
// filtering.py:113-126).
func workCategoryFilter(f Filters) (string, []dhclickhouse.Binding) {
	var categories []string
	for _, c := range f.Why.WorkCategory {
		if trimmed := strings.TrimSpace(c); trimmed != "" {
			categories = append(categories, trimmed)
		}
	}
	if len(categories) == 0 {
		return "", nil
	}
	return " AND investment_area IN {work_categories:Array(String)}", []dhclickhouse.Binding{
		{Name: "work_categories", Value: categories},
	}
}
