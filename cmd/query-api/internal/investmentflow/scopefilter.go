// Repo/team-scope resolution -- ports:
//   - api/services/filtering.py's resolve_repo_filter_ids
//   - api/queries/scopes.py's resolve_repo_id/resolve_repo_ids/
//     resolve_repo_ids_for_teams/build_scope_filter_multi
//
// Duplicated from cmd/query-api/internal/sankey/scopefilter.go rather
// than imported -- same Python source, same "repeat, don't couple"
// convention that package's own doc comment establishes (itself
// duplicated from cmd/query-api/internal/heatmap/scopefilter.go). This
// package's own callers (the six investment_flow/investment_flow_repo_
// team fetchers) always pass repoColumn="repo_id" -- unlike sankey.py's
// varying column expressions -- but the column is still taken as a
// parameter to keep the shape identical to its siblings rather than
// hardcoding a value that happens to be constant today.
package investmentflow

import (
	"context"
	"fmt"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// resolveRepoID ports resolve_repo_id (api/queries/scopes.py:19-50). repos
// is ReplacingMergeTree(last_synced): FINAL dedups before the id/repo
// equality and org_id filters apply, all inside this same read (class
// ruling). api/queries/scopes.py's own resolve_repo_id reads `FROM repos`
// with no FINAL at all -- a declared Python-plane defect, same class as
// sankey's own sankeyRepoDedupParity (see internal/goapiproof/
// restcorpus.go's investmentFlowRepoDedupParity declaration).
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
		return "", false, fmt.Errorf("investmentflow: resolve repo id: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", false, fmt.Errorf("investmentflow: iterate resolve repo id rows: %w", err)
		}
		return "", false, nil
	}
	var id string
	if err := rows.Scan(&id); err != nil {
		return "", false, fmt.Errorf("investmentflow: scan resolve repo id row: %w", err)
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
// by Python here too; this port matches that unchanged, same precedent as
// sankey/scopefilter.go's own copy of this exact function.
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
		return nil, fmt.Errorf("investmentflow: resolve repo ids for teams: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("investmentflow: scan resolve repo ids for teams row: %w", err)
		}
		if id != "" {
			out = append(out, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("investmentflow: iterate resolve repo ids for teams rows: %w", err)
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
// function's own repo_column parameter. Every fetcher in this package
// calls resolveRepoFilterIDs and then this with repoColumn="repo_id",
// mirroring investment_flow.py's own
// `build_scope_filter_multi("repo", repo_ids, repo_column="repo_id")`
// call sites (the scope level passed there is always the literal "repo",
// never filters.scope.level -- both builders resolve every scope down to
// concrete repo ids first, so the clause itself is always the repo
// branch).
func scopeClauseRepo(repoIDs []string, repoColumn string) (filterSQL string, bindings []dhclickhouse.Binding) {
	if len(repoIDs) == 0 {
		return "", nil
	}
	return fmt.Sprintf(" AND %s IN {scope_ids:Array(String)}", repoColumn), []dhclickhouse.Binding{
		{Name: "scope_ids", Value: repoIDs},
	}
}
