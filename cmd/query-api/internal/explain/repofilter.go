package explain

import (
	"context"
	"fmt"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// resolveRepoID ports resolve_repo_id (api/queries/scopes.py:19-50) --
// see drilldown/repofilter.go's own copy of this exact port for the full
// rationale (repos is ReplacingMergeTree(last_synced): FINAL dedups
// before the id/repo equality and org_id filters apply, all inside this
// same read, class ruling (a)+(b)). Duplicated here rather than imported
// from drilldown, matching this binary's own "repeat, don't couple"
// convention for this narrow a helper (drilldown.go/investmentexplain's
// own doc comments establish the same choice for their own copies).
func (reader *Reader) resolveRepoID(ctx context.Context, repoRef, orgID string) (string, bool, error) {
	if reader == nil || reader.client == nil {
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

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return "", false, fmt.Errorf("resolve repo id: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", false, fmt.Errorf("iterate resolve repo id rows: %w", err)
		}
		return "", false, nil
	}
	var id string
	if err := rows.Scan(&id); err != nil {
		return "", false, fmt.Errorf("scan resolve repo id row: %w", err)
	}
	return id, true, nil
}

// resolveRepoIDs ports resolve_repo_ids (api/queries/scopes.py:53-69).
func (reader *Reader) resolveRepoIDs(ctx context.Context, repoRefs []string, orgID string) ([]string, error) {
	var resolved []string
	for _, ref := range repoRefs {
		if ref == "" {
			continue
		}
		id, ok, err := reader.resolveRepoID(ctx, ref, orgID)
		if err != nil {
			return nil, err
		}
		if ok {
			resolved = append(resolved, id)
		}
	}
	return resolved, nil
}

// resolveRepoIDsForTeams ports resolve_repo_ids_for_teams
// (api/queries/scopes.py:72-89) -- see drilldown/repofilter.go's own copy
// for the full ReplacingMergeTree/FINAL rationale (migration 096 converts
// user_metrics_daily to ReplacingMergeTree(computed_at) with team_id
// OUTSIDE the sorting key).
func (reader *Reader) resolveRepoIDsForTeams(ctx context.Context, teamIDs []string, orgID string) ([]string, error) {
	if reader == nil || reader.client == nil {
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

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("resolve repo ids for teams: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan resolve repo ids for teams row: %w", err)
		}
		if id != "" {
			out = append(out, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate resolve repo ids for teams rows: %w", err)
	}
	return out, nil
}

// ResolveRepoFilterIDs ports resolve_repo_filter_ids
// (api/services/filtering.py:95-110) exactly, including the "team"
// branch -- scopeIDs are filters.scope.ids; whatRepos are
// filters.what.repos. orgID here is ALWAYS the caller's real org id: see
// response.go's scopeFilterForMetric doc comment for a declared Python-
// plane defect this diverges from on purpose (explain.py's OWN call site
// for this chain passes org_id="", every other caller in the Python
// source passes the real one).
func (reader *Reader) ResolveRepoFilterIDs(ctx context.Context, scopeLevel string, scopeIDs, whatRepos []string, orgID string) ([]string, error) {
	var repoRefs []string
	if scopeLevel == "repo" {
		repoRefs = append(repoRefs, scopeIDs...)
	}
	repoRefs = append(repoRefs, whatRepos...)
	if scopeLevel == "team" && len(scopeIDs) > 0 {
		teamRepoIDs, err := reader.resolveRepoIDsForTeams(ctx, scopeIDs, orgID)
		if err != nil {
			return nil, err
		}
		repoRefs = append(repoRefs, teamRepoIDs...)
	}
	return reader.resolveRepoIDs(ctx, repoRefs, orgID)
}
