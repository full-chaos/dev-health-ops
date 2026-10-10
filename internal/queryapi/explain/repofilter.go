package explain

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
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
	return teamscope.ResolveRepoRef(ctx, reader.client, repoRef, orgID, settingsMaxExecutionTime(), "")
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
