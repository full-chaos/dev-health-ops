// Package scopelabel resolves repository and team ids to display names for
// the routes that must never show a bare id as a label. It is the one
// implementation of that lookup: the explain route and the analytics
// breakdowns both call it.
package scopelabel

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// Querier is the narrow ClickHouse boundary this package needs.
type Querier interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// Options shapes the lookup statement.
type Options struct {
	// Final reads repos and teams with FINAL (the merged view); without it the
	// tables are read as stored, one row per physical version.
	Final bool
	// Suffix is appended to the statement (a SETTINGS clause), or empty.
	Suffix string
	// Log prefixes the failure log lines.
	Log string
}

var bareUUIDRe = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// LooksLikeUUID reports whether value is a bare UUID, which must never be
// shown as a label.
func LooksLikeUUID(value string) bool {
	v := strings.TrimSpace(value)
	if v == "" {
		return false
	}
	return bareUUIDRe.MatchString(v)
}

// UniqueSortedNonEmpty returns the distinct non-empty ids in ascending order.
func UniqueSortedNonEmpty(ids []string) []string {
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

// Resolve returns {id: display name} for the ids of one scope kind ("repo"
// reads the repository name, "team" the team name). Best effort: any failure
// degrades to an empty map and is logged. A name that is a bare UUID is
// omitted so the caller applies its own fallback.
func Resolve(ctx context.Context, client Querier, orgID, kind string, ids []string, opts Options) map[string]string {
	unique := UniqueSortedNonEmpty(ids)
	if len(unique) == 0 {
		return map[string]string{}
	}
	final := ""
	if opts.Final {
		final = " FINAL"
	}
	var query string
	switch kind {
	case "repo":
		query = fmt.Sprintf(`
SELECT toString(id) AS scope_id, repo AS display_name
FROM repos%s
WHERE org_id = {org_id:String}
  AND toString(id) IN {scope_ids:Array(String)}
%s
`, final, opts.Suffix)
	case "team":
		query = fmt.Sprintf(`
SELECT toString(id) AS scope_id, name AS display_name
FROM teams%s
WHERE org_id = {org_id:String}
  AND toString(id) IN {scope_ids:Array(String)}
%s
`, final, opts.Suffix)
	default:
		return map[string]string{}
	}
	if client == nil {
		log.Printf("%s: resolve scope display names: client unavailable, scope=%s ids=%v", opts.Log, kind, unique)
		return map[string]string{}
	}
	rows, err := client.Query(ctx, query, []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "scope_ids", Value: unique},
	})
	if err != nil {
		log.Printf("%s: could not resolve %s display names for ids=%v: %v", opts.Log, kind, unique, err)
		return map[string]string{}
	}
	defer rows.Close()
	resolved := map[string]string{}
	for rows.Next() {
		var scopeID, displayName string
		if err := rows.Scan(&scopeID, &displayName); err != nil {
			log.Printf("%s: could not resolve %s display names for ids=%v: scan: %v", opts.Log, kind, unique, err)
			return map[string]string{}
		}
		displayName = strings.TrimSpace(displayName)
		if scopeID == "" || displayName == "" || LooksLikeUUID(displayName) {
			continue
		}
		resolved[scopeID] = displayName
	}
	if err := rows.Err(); err != nil {
		log.Printf("%s: could not resolve %s display names for ids=%v: iterate: %v", opts.Log, kind, unique, err)
		return map[string]string{}
	}
	return resolved
}
