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
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/teamscope"
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

// resolveRepoFilterIDs resolves the EXPLICIT repo refs a request names. A
// team scope resolves nowhere here: its repositories come from
// team_repo_ownership, pushed into SQL by teamscope.RepoCondition.
// Mirrors the explicit-ref half of resolve_repo_filter_ids (api/services/
// filtering.py:95-110).
func resolveRepoFilterIDs(ctx context.Context, client QueryClient, scopeLevel string, scopeIDs, whatRepos []string, orgID string) ([]string, error) {
	var repoRefs []string
	if scopeLevel == "repo" {
		repoRefs = append(repoRefs, scopeIDs...)
	}
	repoRefs = append(repoRefs, whatRepos...)
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

// repoScopeFilter narrows a repo-keyed read to the repositories a request
// names: the explicit refs resolved to concrete ids, ORed with the
// repositories a team scope OWNS (teamscope.RepoCondition, resolved inside
// the statement rather than as a result of its own). Empty when the request
// names neither.
func repoScopeFilter(ctx context.Context, client QueryClient, scopeLevel string, scopeIDs, whatRepos []string, orgID, repoColumn string, asOf time.Time) (string, []dhclickhouse.Binding, error) {
	repoIDs, err := resolveRepoFilterIDs(ctx, client, scopeLevel, scopeIDs, whatRepos, orgID)
	if err != nil {
		return "", nil, err
	}
	explicitSQL, explicitBindings := scopeClauseRepo(repoIDs, repoColumn)

	var teamCondition string
	var teamBindings []dhclickhouse.Binding
	if scopeLevel == "team" && len(scopeIDs) > 0 {
		teamCondition, teamBindings = teamscope.RepoCondition(orgID, repoColumn, scopeIDs, asOf)
	}

	switch {
	case explicitSQL != "" && teamCondition != "":
		return fmt.Sprintf(" AND (%s IN {scope_ids:Array(String)} OR %s)", repoColumn, teamCondition),
			append(append([]dhclickhouse.Binding{}, explicitBindings...), teamBindings...), nil
	case explicitSQL != "":
		return explicitSQL, explicitBindings, nil
	case teamCondition != "":
		return " AND " + teamCondition, teamBindings, nil
	}
	return "", nil, nil
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
