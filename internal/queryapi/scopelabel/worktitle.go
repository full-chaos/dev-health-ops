package scopelabel

import (
	"context"
	"log"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

const workItemTitlesQuery = `
SELECT work_item_id, argMax(title, last_synced) AS title
FROM work_items FINAL
WHERE org_id = {org_id:String}
  AND work_item_id IN {work_item_ids:Array(String)}
GROUP BY work_item_id
%s
`

// ResolveWorkItemTitles returns {work_item_id: title} read from work_items
// inside the caller's organisation, so another organisation's title for the
// same id is never served. Best effort: a failure degrades to an empty map
// and is logged, so the caller keeps its rows. An id with no displayable
// title is absent from the map.
func ResolveWorkItemTitles(ctx context.Context, client Querier, orgID string, ids []string, opts Options) map[string]string {
	unique := UniqueSortedNonEmpty(ids)
	if len(unique) == 0 {
		return map[string]string{}
	}
	if client == nil {
		log.Printf("%s: resolve work item titles: client unavailable, ids=%d", opts.Log, len(unique))
		return map[string]string{}
	}
	query := strings.Replace(workItemTitlesQuery, "%s", opts.Suffix, 1)
	rows, err := client.Query(ctx, query, []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "work_item_ids", Value: unique},
	})
	if err != nil {
		log.Printf("%s: could not resolve work item titles for ids=%d: %v", opts.Log, len(unique), err)
		return map[string]string{}
	}
	defer rows.Close()
	resolved := map[string]string{}
	for rows.Next() {
		var id, title string
		if err := rows.Scan(&id, &title); err != nil {
			log.Printf("%s: could not resolve work item titles for ids=%d: scan: %v", opts.Log, len(unique), err)
			return map[string]string{}
		}
		if clean, ok := CleanName(title); id != "" && ok {
			resolved[id] = clean
		}
	}
	if err := rows.Err(); err != nil {
		log.Printf("%s: could not resolve work item titles for ids=%d: iterate: %v", opts.Log, len(unique), err)
		return map[string]string{}
	}
	return resolved
}
