// Package scoperesolve says whether the scope ids a request names exist for the organization (CHAOS-6119, CHAOS-13705, R335): a
// repository, service or developer scope whose ids name nothing answers EMPTY on every page that has the filter. The Python
// reference dropped such a filter and answered the whole organization.
//
// "Known" is read from the source of the page's filter options (filteroptions): repos for a repository, user_metrics_daily
// author_email plus the people-search identity union (user_metrics_daily.identity_id, work_item_user_metrics_daily.user_identity)
// for a developer, no time window, org-scoped inside every branch. No picker lists services (filter options always answer
// services: []), so no service id is known.
package scoperesolve

import (
	"context"
	"fmt"
	"strings"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// RowQuerier is the read boundary of the existence read.
type RowQuerier interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

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

// Unresolved reports that ids are named at a repository, service or developer level and none of them exists for the organization.
// One query per call, bounded by the number of ids; a service scope reads nothing (no service source exists: when one ships it
// becomes the known set). Team and org scopes, and a request naming no id, are never unresolved.
func Unresolved(ctx context.Context, client RowQuerier, orgID, level string, ids []string) (bool, error) {
	unique := uniqueNonEmpty(ids)
	if len(unique) == 0 {
		return false, nil
	}
	var query string
	switch level {
	case "service":
		return true, nil
	case "repo":
		query = reposExistQuery
		for i, id := range unique {
			if parsed, err := pythonparity.ParseUUID(id); err == nil {
				unique[i] = parsed.String()
			}
		}
	case "developer":
		query = developersExistQuery
	default:
		return false, nil
	}
	rows, err := client.Query(ctx, query, []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "scope_ids", Value: unique},
		{Name: "id_count", Value: uint64(len(unique))},
	})
	if err != nil {
		return false, fmt.Errorf("resolve %s scope: %w", level, err)
	}
	defer rows.Close()
	for rows.Next() {
		var hit string
		if err := rows.Scan(&hit); err != nil {
			return false, fmt.Errorf("scan %s scope: %w", level, err)
		}
		if strings.TrimSpace(hit) != "" {
			return false, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate %s scope: %w", level, err)
	}
	return true, nil
}

func uniqueNonEmpty(ids []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}
