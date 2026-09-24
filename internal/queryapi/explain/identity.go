package explain

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/scopelabel"
)

// looksLikeUUID ports identity.py's looks_like_uuid.
func looksLikeUUID(value string) bool { return scopelabel.LooksLikeUUID(value) }

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

// resolveScopeDisplayNames resolves repo/team ids to display names through
// the shared lookup (internal/scopelabel), reading repos and teams with FINAL:
// both are ReplacingMergeTree tables, and Python's own read here
// (api/services/identity.py:63-78) takes neither FINAL nor an argMax dedup,
// so an unmerged part can return more than one physical row for an id or a
// stale name. The Go answer is the canonical one (class ruling (b)); the
// difference from Python's raw read is a declared Python-plane defect.
// Best-effort: any failure degrades to an empty map, never a route error.
func (reader *Reader) resolveScopeDisplayNames(ctx context.Context, orgID, scopeKind string, ids []string) map[string]string {
	var client scopelabel.Querier
	if reader != nil && reader.client != nil {
		client = reader.client
	}
	return scopelabel.Resolve(ctx, client, orgID, scopeKind, ids, scopelabel.Options{
		Final:  true,
		Suffix: settingsMaxExecutionTime(),
		Log:    "explain",
	})
}

func uniqueSortedNonEmpty(ids []string) []string { return scopelabel.UniqueSortedNonEmpty(ids) }
