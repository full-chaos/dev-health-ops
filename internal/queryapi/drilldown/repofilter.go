package drilldown

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
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
	return teamscope.ResolveRepoRef(ctx, reader.client, repoRef, orgID, settingsMaxExecutionTime(), "")
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
// scopeIDs at scope="repo", unioned with filters.what.repos. A team scope
// resolves nowhere here -- its repositories come from team_repo_ownership,
// pushed into SQL by teamscope.RepoCondition, which the caller ORs with the
// clause built from this list.
func (reader *Reader) ResolveRepoFilterIDs(ctx context.Context, scopeLevel string, scopeIDs, whatRepos []string, orgID string) ([]string, error) {
	repoRefs := teamscope.NamedRepoRefs(scopeLevel, scopeIDs, whatRepos)
	return reader.resolveRepoIDs(ctx, repoRefs, orgID)
}
