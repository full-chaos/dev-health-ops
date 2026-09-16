package drilldown

import (
	"context"
	"fmt"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// resolveRepoID ports resolve_repo_id (api/queries/scopes.py:19-50):
// a repoRef that parses as a UUID (pythonparity.ParseUUID, matching
// Python's own parse_uuid -> uuid.UUID(str(value)) accept set) resolves
// by repos.id; anything else resolves by repos.repo verbatim --
// case-SENSITIVE, no lower(). Not found is ("", false, nil), never an
// error.
//
// repos is ReplacingMergeTree(last_synced): FINAL dedups by version
// before the id/repo equality and org_id filters apply, all inside this
// same read -- never a raw scan of a ReplacingMergeTree table, and the
// org bound never sits outside the table's own read (the class ruling
// this package's reads follow throughout).
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

// resolveRepoIDs ports resolve_repo_ids (api/queries/scopes.py:53-69):
// resolves each non-empty ref in order via resolveRepoID, appending only
// the ones that resolve. An unresolved ref is silently skipped, not an
// error and not a sentinel -- matching Python exactly, and deliberately
// NOT deduped (Python's own list.append here never dedupes either).
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
// (api/queries/scopes.py:72-89): every distinct repo_id worked on by any
// user_metrics_daily row carrying one of teamIDs in this org, within the
// table's own retention -- there is no time-window filter in the Python
// source, so none is added here either.
//
// user_metrics_daily was plain MergeTree at 001_metrics_v2.sql, but
// migration 096 (daily_family_tables_replacing_merge_tree.py) converts
// it to ReplacingMergeTree(computed_at), sorting key (org_id, repo_id,
// author_email, day) -- team_id is NOT part of that key, so an
// un-merged older version of a (org_id, repo_id, author_email, day) row
// can still carry a team_id the current version no longer has (a team
// reassignment). A raw read of team_id IN (...) can therefore add a
// stale repo to this set even though its LATEST version no longer
// belongs to that team. FINAL dedups to the current version before
// team_id is read; org_id -- the sorting key's own first column -- stays
// in the same WHERE, so the read is both correctly deduped and bounded
// to this tenant before team_id is evaluated.
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
// (api/services/filtering.py:95-110) exactly, including the "team" branch.
// scopeIDs are filters.scope.ids; whatRepos are filters.what.repos.
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
