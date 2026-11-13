package home

import (
	"context"
	"fmt"
	"strings"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// An unresolved scope filter answers EMPTY (CHAOS-6119, R335): a repository, service or developer scope whose ids name nothing that
// exists for this organization matches nothing. The Python plane dropped such a filter and answered the whole organization.
//
// "Known" is read from the source of the page's filter options (filteroptions): repos for a repository, user_metrics_daily
// author_email plus the people-search identity union (user_metrics_daily.identity_id, work_item_user_metrics_daily.user_identity)
// for a developer, no time window, org-scoped inside every branch. No picker lists services (filter options always answer
// services: []), so no service id is known.

const reposExistQuery = `
SELECT toString(id) AS hit
FROM repos FINAL
WHERE org_id = {org_id:String}
  AND (toString(id) IN {scope_ids:Array(String)} OR repo IN {scope_ids:Array(String)})
LIMIT {id_count:UInt64}
`

var developersExistQuery = `
SELECT hit FROM (
    (SELECT author_email AS hit
     FROM user_metrics_daily FINAL
     WHERE org_id = {org_id:String}
       AND (author_email IN {scope_ids:Array(String)} OR identity_id IN {scope_ids:Array(String)})
       AND ` + liverow.Predicate("user_metrics_daily", "") + `
     LIMIT {id_count:UInt64})
    UNION ALL
    (SELECT user_identity AS hit
     FROM work_item_user_metrics_daily FINAL
     WHERE org_id = {org_id:String}
       AND user_identity IN {scope_ids:Array(String)}
       AND ` + liverow.Predicate("work_item_user_metrics_daily", "") + `
     LIMIT {id_count:UInt64})
)
LIMIT {id_count:UInt64}
`

// Unresolved reports that the request names scope ids at a repository, service or developer level and none of them exists for the
// organization. Only meaningful after ResolveScope.
func (s ScopeFilter) Unresolved() bool { return s.unresolved }

// ResolveScope reads, once per request and one query per level, whether the named scope ids exist for the organization. Team scopes
// and requests that name no id are untouched.
func ResolveScope(ctx context.Context, client QueryClient, orgID string, f Filters) (Filters, error) {
	if f.Scope.checked {
		return f, nil
	}
	f.Scope.checked = true
	ids := uniqueIDs(f.Scope.IDs)
	if len(ids) == 0 {
		return f, nil
	}
	var query string
	switch f.Scope.Level {
	case "service":
		f.Scope.unresolved = true
		return f, nil
	case "repo":
		query = reposExistQuery
		for i, id := range ids {
			if parsed, err := pythonparity.ParseUUID(id); err == nil {
				ids[i] = parsed.String()
			}
		}
	case "developer":
		query = developersExistQuery
	default:
		return f, nil
	}
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "scope_ids", Value: ids},
		{Name: "id_count", Value: uint64(len(ids))},
	})
	if err != nil {
		return f, fmt.Errorf("home: resolve %s scope: %w", f.Scope.Level, err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var hit string
		if err := rows.Scan(&hit); err != nil {
			return f, fmt.Errorf("home: scan %s scope: %w", f.Scope.Level, err)
		}
		if strings.TrimSpace(hit) != "" {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		return f, fmt.Errorf("home: iterate %s scope: %w", f.Scope.Level, err)
	}
	f.Scope.unresolved = !found
	return f, nil
}

const unresolvedScopeFilter = " AND 1 = 0"
