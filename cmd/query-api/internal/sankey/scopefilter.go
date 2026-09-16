// Repo/team-scope resolution -- ports:
//   - api/services/filtering.py's resolve_repo_filter_ids
//   - api/queries/scopes.py's resolve_repo_id/resolve_repo_ids/
//     resolve_repo_ids_for_teams/build_scope_filter_multi
//
// Duplicated from cmd/query-api/internal/heatmap/scopefilter.go rather
// than imported -- same Python source, same "repeat, don't couple"
// convention that package's own doc comment establishes. Unlike
// heatmap.py's three call sites (which always pass team_column="team_id"/
// repo_column="repo_id"), services/sankey.py's callers pass VARYING column
// expressions (a raw "repo_id", the qualified "metrics.repo_id" for the
// hotspot reader, and the ifNull(nullIf(team_id, ”), 'unassigned') fallback
// expression for expense/state's team scope), so the clause builders below
// take the column expression as a parameter instead of hardcoding it.
package sankey

import (
	"context"
	"fmt"
	"strings"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// resolveRepoID ports resolve_repo_id (api/queries/scopes.py:19-50). repos
// is ReplacingMergeTree(last_synced): FINAL dedups before the id/repo
// equality and org_id filters apply, all inside this same read (class
// ruling). api/queries/scopes.py's own resolve_repo_id reads `FROM repos`
// with no FINAL at all -- a declared Python-plane defect (see
// internal/goapiproof/restcorpus.go's sankeyRepoDedupParity).
func resolveRepoID(ctx context.Context, client QueryClient, repoRef, orgID string) (string, bool, error) {
	if client == nil {
		return "", false, ErrUnavailable
	}

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
		return "", false, fmt.Errorf("sankey: resolve repo id: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", false, fmt.Errorf("sankey: iterate resolve repo id rows: %w", err)
		}
		return "", false, nil
	}
	var id string
	if err := rows.Scan(&id); err != nil {
		return "", false, fmt.Errorf("sankey: scan resolve repo id row: %w", err)
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
// scopes.py:72-89). user_metrics_daily is read as a plain (undeduped) scan
// by Python here too; this route's own class ruling scopes that to a
// separate declared defect only if a live divergence is ever observed --
// left as a plain read here, unchanged from Python, matching
// heatmap/scopefilter.go's own copy of this exact function (its own doc
// comment reads it FINAL; this package matches that precedent).
func resolveRepoIDsForTeams(ctx context.Context, client QueryClient, teamIDs []string, orgID string) ([]string, error) {
	if client == nil {
		return nil, ErrUnavailable
	}

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
		return nil, fmt.Errorf("sankey: resolve repo ids for teams: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("sankey: scan resolve repo ids for teams row: %w", err)
		}
		if id != "" {
			out = append(out, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sankey: iterate resolve repo ids for teams rows: %w", err)
	}
	return out, nil
}

// resolveRepoFilterIDs ports resolve_repo_filter_ids (api/services/
// filtering.py:95-110).
func resolveRepoFilterIDs(ctx context.Context, client QueryClient, scopeLevel string, scopeIDs, whatRepos []string, orgID string) ([]string, error) {
	var repoRefs []string
	if scopeLevel == "repo" {
		repoRefs = append(repoRefs, scopeIDs...)
	}
	repoRefs = append(repoRefs, whatRepos...)
	if scopeLevel == "team" && len(scopeIDs) > 0 {
		teamRepoIDs, err := resolveRepoIDsForTeams(ctx, client, scopeIDs, orgID)
		if err != nil {
			return nil, err
		}
		repoRefs = append(repoRefs, teamRepoIDs...)
	}
	return resolveRepoIDs(ctx, client, repoRefs, orgID)
}

// scopeClauseRepo ports build_scope_filter_multi's "repo" branch
// (api/queries/scopes.py:105-117), repoColumn standing in for that
// function's own repo_column parameter.
func scopeClauseRepo(repoIDs []string, repoColumn string) (filterSQL string, bindings []dhclickhouse.Binding) {
	if len(repoIDs) == 0 {
		return "", nil
	}
	return fmt.Sprintf(" AND %s IN {scope_ids:Array(String)}", repoColumn), []dhclickhouse.Binding{
		{Name: "scope_ids", Value: repoIDs},
	}
}

// scopeClauseTeam ports build_scope_filter_multi's "team" branch.
func scopeClauseTeam(teamIDs []string, teamColumn string) (filterSQL string, bindings []dhclickhouse.Binding) {
	if len(teamIDs) == 0 {
		return "", nil
	}
	return fmt.Sprintf(" AND %s IN {scope_ids:Array(String)}", teamColumn), []dhclickhouse.Binding{
		{Name: "scope_ids", Value: teamIDs},
	}
}

// repoScopeFilter ports _repo_scope_filter (services/sankey.py:180-189):
// resolve the request's scope down to concrete repo ids, then build an
// " AND <repoColumn> IN (...)" clause -- empty when no repo id resolves.
func repoScopeFilter(ctx context.Context, client QueryClient, scopeLevel string, scopeIDs, whatRepos []string, orgID, repoColumn string) (string, []dhclickhouse.Binding, error) {
	repoIDs, err := resolveRepoFilterIDs(ctx, client, scopeLevel, scopeIDs, whatRepos, orgID)
	if err != nil {
		return "", nil, err
	}
	if len(repoIDs) == 0 {
		return "", nil, nil
	}
	filterSQL, bindings := scopeClauseRepo(repoIDs, repoColumn)
	return filterSQL, bindings, nil
}

// teamScopeFilter ports _team_scope_filter (services/sankey.py:192-198):
// only meaningful at team scope, with a scope id present.
func teamScopeFilter(scopeLevel string, scopeIDs []string, teamColumn string) (string, []dhclickhouse.Binding) {
	if scopeLevel != "team" || len(scopeIDs) == 0 {
		return "", nil
	}
	return scopeClauseTeam(scopeIDs, teamColumn)
}

// workScopeFilter ports _work_scope_filter (services/sankey.py:219-233):
// scope.ids at repo scope, plus filters.what.repos, deduped by the plain
// membership Python's own list concatenation already produces (no
// dedup -- an id present in both collapses to one IN-list entry anyway).
func workScopeFilter(scopeLevel string, scopeIDs, whatRepos []string, workScopeColumn string) (string, []dhclickhouse.Binding) {
	var ids []string
	if scopeLevel == "repo" {
		ids = append(ids, scopeIDs...)
	}
	ids = append(ids, whatRepos...)
	var filtered []string
	for _, id := range ids {
		if id != "" {
			filtered = append(filtered, id)
		}
	}
	if len(filtered) == 0 {
		return "", nil
	}
	return fmt.Sprintf(" AND %s IN {work_scope_ids:Array(String)}", workScopeColumn), []dhclickhouse.Binding{
		{Name: "work_scope_ids", Value: filtered},
	}
}

// categoryThemeFilters ports _category_theme_filters (services/sankey.py:
// 201-216): each why.work_category entry's theme prefix (everything
// before its first '.', or the whole string when there is none),
// deduplicated while preserving first-seen order (Python's
// dict.fromkeys idiom).
func categoryThemeFilters(workCategory []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, category := range workCategory {
		trimmed := strings.TrimSpace(category)
		if trimmed == "" {
			continue
		}
		theme := trimmed
		for i := 0; i < len(trimmed); i++ {
			if trimmed[i] == '.' {
				theme = trimmed[:i]
				break
			}
		}
		if theme == "" || seen[theme] {
			continue
		}
		seen[theme] = true
		out = append(out, theme)
	}
	return out
}
