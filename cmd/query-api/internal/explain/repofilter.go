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

// teamRepoScopeCondition returns the repo_id membership test for a
// team-level scope as a standalone boolean SQL condition (no leading
// "AND", no trailing statement) -- the team's matching repo set is
// resolved and checked against repos' own org-scoped rows entirely
// inside this condition, so the surrounding statement's own result set
// is the only thing that ever crosses back to the caller. A team whose
// true matching-repo count is large produces exactly the same handful of
// metric rows the caller already reads today, not a separate, large,
// client-visible row set of its own -- unlike resolving that id set as
// its own standalone query first and re-injecting it as a literal array,
// which asks the read-only client to hand back every matching id as the
// query's OWN result before the caller can even use it as a filter.
// Empty/blank team ids are dropped, matching resolveRepoIDs' own
// blank-ref skip.
func teamRepoScopeCondition(orgID string, teamIDs []string) (condition string, bindings []dhclickhouse.Binding) {
	var teamList []string
	for _, id := range teamIDs {
		if id != "" {
			teamList = append(teamList, id)
		}
	}
	if len(teamList) == 0 {
		return "", nil
	}
	return `repo_id IN (
    SELECT toString(id) AS id
    FROM repos FINAL
    WHERE org_id = {team_repo_scope_org_id:String}
      AND toString(id) IN (
          SELECT DISTINCT toString(repo_id) AS id
          FROM user_metrics_daily FINAL
          WHERE org_id = {team_repo_scope_org_id:String}
            AND team_id IN {team_repo_scope_ids:Array(String)}
      )
)`, []dhclickhouse.Binding{
		{Name: "team_repo_scope_ids", Value: teamList},
		{Name: "team_repo_scope_org_id", Value: orgID},
	}
}
