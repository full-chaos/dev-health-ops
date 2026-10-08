// Package scopelabel resolves repository and team ids to display names for
// the routes that must never show a bare id as a label. It is the one
// implementation of that lookup: the explain route and the analytics
// breakdowns both call it.
package scopelabel

import (
	"context"
	"errors"
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

// CleanName is the one name-or-nothing rule: the trimmed name, and false when
// it is empty or a bare UUID (an id is never a label).
func CleanName(raw string) (string, bool) {
	name := strings.TrimSpace(raw)
	if name == "" || LooksLikeUUID(name) {
		return "", false
	}
	return name, true
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

// Resolve is the best-effort form of ResolveStrict: any failure degrades to
// an empty map and is logged. The explain and analytics-breakdown labels use
// it because they apply their own fallback for an id without a name.
func Resolve(ctx context.Context, client Querier, orgID, kind string, ids []string, opts Options) map[string]string {
	resolved, err := ResolveStrict(ctx, client, orgID, kind, ids, opts)
	if err != nil {
		log.Printf("%s: could not resolve %s display names for ids=%v: %v", opts.Log, kind, UniqueSortedNonEmpty(ids), err)
		return map[string]string{}
	}
	return resolved
}

// ResolveStrict returns {id: display name} for the ids of one scope kind
// ("repo" reads the repository name, "team" the team name). A failed read
// (no client, query, scan or iterate) is an error, so a caller that serves the
// name as data never shows a failure as "no name". A name that is a bare UUID
// is omitted so the caller applies its own fallback.
func ResolveStrict(ctx context.Context, client Querier, orgID, kind string, ids []string, opts Options) (map[string]string, error) {
	unique := UniqueSortedNonEmpty(ids)
	if len(unique) == 0 {
		return map[string]string{}, nil
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
		return map[string]string{}, nil
	}
	if client == nil {
		return nil, errors.New("client unavailable")
	}
	rows, err := client.Query(ctx, query, []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "scope_ids", Value: unique},
	})
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	resolved := map[string]string{}
	for rows.Next() {
		var scopeID, displayName string
		if err := rows.Scan(&scopeID, &displayName); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		name, ok := CleanName(displayName)
		if scopeID == "" || !ok {
			continue
		}
		resolved[scopeID] = name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate: %w", err)
	}
	return resolved, nil
}
