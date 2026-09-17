// Repo/team-scope resolution -- ports:
//   - api/services/filtering.py's resolve_repo_filter_ids/scope_filter_for_metric
//   - api/queries/scopes.py's resolve_repo_id/resolve_repo_ids/
//     resolve_repo_ids_for_teams/build_scope_filter_multi
//
// Duplicated from cmd/query-api/internal/sankey/scopefilter.go rather
// than imported -- same Python source, same "repeat, don't couple"
// convention that package's own doc comment establishes.
//
// DEDUP: repos and user_metrics_daily are
// both ReplacingMergeTree tables (last_synced / computed_at
// respectively, confirmed against prod system.tables). This package
// reads both FINAL, org_id filtered in the same statement --
// api/queries/scopes.py's resolve_repo_id/resolve_repo_ids read repos
// with no dedup at all, and resolve_repo_ids_for_teams reads
// user_metrics_daily the same way; both are declared Python-plane
// defects (see internal/goapiproof/restcorpus.go's home declarations),
// matching sankey/heatmap's own precedent of always reading these two
// tables FINAL regardless of whether a given aggregate happens to be
// dedup-invariant.
package home

import (
	"context"
	"fmt"
	"strings"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
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

// resolveRepoIDsForTeams ports resolve_repo_ids_for_teams (api/queries/
// scopes.py:72-89), reading user_metrics_daily FINAL (see this file's
// own package doc comment for the dedup fix).
func resolveRepoIDsForTeams(ctx context.Context, client QueryClient, teamIDs []string, orgID string) ([]string, error) {
	var teamList []string
	for _, id := range teamIDs {
		if id != "" {
			teamList = append(teamList, id)
		}
	}
	if len(teamList) == 0 {
		return nil, nil
	}

	query := fmt.Sprintf(`
SELECT DISTINCT toString(repo_id) AS id
FROM user_metrics_daily FINAL
WHERE org_id = {org_id:String}
  AND team_id IN {team_ids:Array(String)}
%s
`, settingsMaxExecutionTime())
	bindings := []dhclickhouse.Binding{
		{Name: "team_ids", Value: teamList},
		{Name: "org_id", Value: orgID},
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("home: resolve repo ids for teams: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("home: scan resolve repo ids for teams row: %w", err)
		}
		if id != "" {
			out = append(out, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("home: iterate resolve repo ids for teams rows: %w", err)
	}
	return out, nil
}

// resolveRepoFilterIDs ports resolve_repo_filter_ids (api/services/
// filtering.py:95-110).
func resolveRepoFilterIDs(ctx context.Context, client QueryClient, f Filters, orgID string) ([]string, error) {
	var repoRefs []string
	if f.Scope.Level == "repo" {
		repoRefs = append(repoRefs, f.Scope.IDs...)
	}
	repoRefs = append(repoRefs, f.What.Repos...)
	if f.Scope.Level == "team" && len(f.Scope.IDs) > 0 {
		teamRepoIDs, err := resolveRepoIDsForTeams(ctx, client, f.Scope.IDs, orgID)
		if err != nil {
			return nil, err
		}
		repoRefs = append(repoRefs, teamRepoIDs...)
	}
	return resolveRepoIDs(ctx, client, repoRefs, orgID)
}

// scopeFilterForMetric ports scope_filter_for_metric (api/services/
// filtering.py:129-147).
func scopeFilterForMetric(ctx context.Context, client QueryClient, metricScope string, f Filters, orgID, teamColumn, repoColumn string) (string, []dhclickhouse.Binding, error) {
	if metricScope == "team" && f.Scope.Level == "team" {
		return scopeClauseMulti(f.Scope.IDs, teamColumn), scopeBindingsMulti(f.Scope.IDs), nil
	}
	if metricScope == "repo" {
		repoIDs, err := resolveRepoFilterIDs(ctx, client, f, orgID)
		if err != nil {
			return "", nil, err
		}
		return scopeClauseMulti(repoIDs, repoColumn), scopeBindingsMulti(repoIDs), nil
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
