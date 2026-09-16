package explain

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// bareUUIDRe ports identity.py's _BARE_UUID_RE verbatim (case-insensitive,
// anchored both ends).
var bareUUIDRe = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// looksLikeUUID ports identity.py's looks_like_uuid.
func looksLikeUUID(value string) bool {
	v := strings.TrimSpace(value)
	if v == "" {
		return false
	}
	return bareUUIDRe.MatchString(v)
}

// scopeKindForGroupBy ports identity.py's scope_kind_for_group_by. Every
// metricConfigs entry's GroupBy is "team_id" or "repo_id" (metricconfig.go),
// so the "" fallback is unreached in practice, ported anyway for fidelity
// with the generic Python function.
func scopeKindForGroupBy(groupBy string) (string, bool) {
	switch groupBy {
	case "team_id":
		return "team", true
	case "repo_id":
		return "repo", true
	default:
		return "", false
	}
}

// resolveScopeDisplayNames ports resolve_scope_display_names
// (api/services/identity.py:45-99) -- best-effort: ANY failure (a bad
// query, a scan error, a row-iteration error) degrades to an empty map,
// never propagated as a route error, matching Python's own
// `except Exception: ... return {}`. Logged, not silently dropped.
//
// DEDUP: repos is ReplacingMergeTree(last_synced) (000_raw_tables.sql);
// teams is ReplacingMergeTree(updated_at) (002_teams.sql) -- BOTH since
// their original creation, not a later conversion. Python's own query
// here (identity.py:63-78) reads NEITHER table with FINAL or any argMax
// dedup -- a genuine ReplacingMergeTree-raw-read defect under the class
// ruling (an unmerged part can return more than one physical row for the
// same id, and/or a stale display_name for an id whose name changed).
// This port adds FINAL to both reads -- the Go answer is canonical (class
// ruling (b)); the difference from Python's raw read is a declared
// Python-plane defect, not a data-semantics choice.
func (reader *Reader) resolveScopeDisplayNames(ctx context.Context, orgID, scopeKind string, ids []string) map[string]string {
	unique := uniqueSortedNonEmpty(ids)
	if len(unique) == 0 {
		return map[string]string{}
	}

	var query string
	switch scopeKind {
	case "repo":
		query = fmt.Sprintf(`
SELECT toString(id) AS scope_id, repo AS display_name
FROM repos FINAL
WHERE org_id = {org_id:String}
  AND toString(id) IN {scope_ids:Array(String)}
%s
`, settingsMaxExecutionTime())
	case "team":
		query = fmt.Sprintf(`
SELECT toString(id) AS scope_id, name AS display_name
FROM teams FINAL
WHERE org_id = {org_id:String}
  AND toString(id) IN {scope_ids:Array(String)}
%s
`, settingsMaxExecutionTime())
	default:
		return map[string]string{}
	}

	if reader == nil || reader.client == nil {
		log.Printf("explain: resolve scope display names: client unavailable, scope=%s ids=%v", scopeKind, unique)
		return map[string]string{}
	}

	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "scope_ids", Value: unique},
	}
	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		log.Printf("explain: could not resolve %s display names for ids=%v: %v", scopeKind, unique, err)
		return map[string]string{}
	}
	defer rows.Close()

	resolved := map[string]string{}
	for rows.Next() {
		var scopeID, displayName string
		if err := rows.Scan(&scopeID, &displayName); err != nil {
			log.Printf("explain: could not resolve %s display names for ids=%v: scan: %v", scopeKind, unique, err)
			return map[string]string{}
		}
		displayName = strings.TrimSpace(displayName)
		// A8: a bare UUID is not a human label -- omit so the caller
		// falls back to shortToken.
		if scopeID == "" || displayName == "" || looksLikeUUID(displayName) {
			continue
		}
		resolved[scopeID] = displayName
	}
	if err := rows.Err(); err != nil {
		log.Printf("explain: could not resolve %s display names for ids=%v: iterate: %v", scopeKind, unique, err)
		return map[string]string{}
	}
	return resolved
}

func uniqueSortedNonEmpty(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	var out []string
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
