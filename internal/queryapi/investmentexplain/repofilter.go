package investmentexplain

import (
	"context"
	"fmt"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// resolveRepoID ports resolve_repo_id (api/queries/scopes.py:19-50)
// exactly: a repoRef that parses as a UUID (pythonparity.ParseUUID,
// matching Python's own parse_uuid -> uuid.UUID(str(value)) accept set)
// resolves by repos.id; anything else resolves by repos.repo verbatim
// -- case-SENSITIVE, no lower(), unlike analytics/repofilters.go's
// resolveRepoFilterRefs (a DIFFERENT Python function, analytics.py's
// _resolve_repo_filter_refs, batched and case-folded on purpose for a
// different call site -- do not conflate the two or "simplify" one into
// the other). Not found is (\"\", false, nil), never an error.
//
// repos is ReplacingMergeTree(last_synced) (migrations/clickhouse/
// 000_raw_tables.sql, org_id added by 024, sorting key (org_id, id) since
// 027) -- both branches read FINAL so a rename/resync in flight resolves
// to the current row, not a stale un-merged copy, with org_id filtered in
// the same WHERE as the FINAL source.
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

// ResolveRepoFilterIDs resolves the EXPLICIT repo refs a request names:
// filters.scope.ids at scope="repo", unioned with filters.what.repos, each
// verified individually via resolveRepoIDs (api/services/filtering.py:95-110's
// own explicit-ref branch). This list is bounded by what the caller named, not
// by organization scale, so it stays a materialized id list.
//
// A team scope resolves nowhere in this function. A team's repositories come
// from team_repo_ownership, pushed into SQL as a condition by
// internal/queryapi/teamscope.RepoCondition, and callers OR the two
// together. See that package's doc comment for the source and the semantics.
func (reader *Reader) ResolveRepoFilterIDs(ctx context.Context, scopeLevel string, scopeIDs, whatRepos []string, orgID string) ([]string, error) {
	var repoRefs []string
	if scopeLevel == "repo" {
		repoRefs = append(repoRefs, scopeIDs...)
	}
	repoRefs = append(repoRefs, whatRepos...)
	return reader.resolveRepoIDs(ctx, repoRefs, orgID)
}
