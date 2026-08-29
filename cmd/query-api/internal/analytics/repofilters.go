package analytics

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// unmatchedRepoFilterID ports analytics.py's _UNMATCHED_REPO_FILTER_ID
// (analytics.py:70) -- an all-zero UUID sentinel substituted for a repo
// name that does not resolve, so an unmatched filter honestly excludes
// everything (matches no real repo_id) rather than silently matching
// none of the filter (which would read as "no filter applied").
const unmatchedRepoFilterID = "00000000-0000-0000-0000-000000000000"

// asUUIDString ports analytics.py's _as_uuid_string (analytics.py:73-77):
// returns the canonical (lowercase, hyphenated) UUID string form if value
// parses as a UUID, else "" with ok=false.
func asUUIDString(value string) (string, bool) {
	id, err := uuid.Parse(value)
	if err != nil {
		return "", false
	}
	return id.String(), true
}

// dedupePreservingOrder ports analytics.py's _dedupe_preserving_order
// (analytics.py:80-88).
func dedupePreservingOrder(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

// resolveRepoFilterRefs ports _resolve_repo_filter_refs (analytics.py:91-135):
// resolves a mix of repo UUIDs and repo-name slugs to canonical repo_id
// UUIDs, in ONE query for every name in the batch (not N queries), falling
// back to unmatchedRepoFilterID for a name with no match.
func resolveRepoFilterRefs(ctx context.Context, client QueryClient, orgID string, repoRefs []string) ([]string, error) {
	cleanedRefs := make([]string, 0, len(repoRefs))
	for _, ref := range repoRefs {
		trimmed := strings.TrimSpace(ref)
		if trimmed != "" {
			cleanedRefs = append(cleanedRefs, trimmed)
		}
	}

	var repoNames []string
	for _, ref := range cleanedRefs {
		if _, ok := asUUIDString(ref); !ok {
			repoNames = append(repoNames, ref)
		}
	}

	repoIDsByName := make(map[string]string)
	if len(repoNames) > 0 {
		lowered := make([]string, len(repoNames))
		for i, n := range repoNames {
			lowered[i] = strings.ToLower(n)
		}
		lowered = dedupePreservingOrder(lowered)

		rows, err := client.Query(ctx, `
            SELECT
                toString(id) AS repo_id,
                repo
            FROM repos
            WHERE org_id = {org_id:String}
              AND lower(repo) IN {repo_names:Array(String)}
            `, []clickhouse.Binding{
			{Name: "org_id", Value: orgID},
			{Name: "repo_names", Value: lowered},
		})
		if err != nil {
			return nil, fmt.Errorf("analytics: resolveRepoFilterRefs: query: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var repoID, repo string
			if scanErr := rows.Scan(&repoID, &repo); scanErr != nil {
				return nil, fmt.Errorf("analytics: resolveRepoFilterRefs: scan: %w", scanErr)
			}
			repoName := strings.ToLower(repo)
			if canonical, ok := asUUIDString(repoID); ok && repoName != "" {
				repoIDsByName[repoName] = canonical
			}
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("analytics: resolveRepoFilterRefs: rows: %w", err)
		}
	}

	resolved := make([]string, 0, len(cleanedRefs))
	for _, ref := range cleanedRefs {
		if repoID, ok := asUUIDString(ref); ok {
			resolved = append(resolved, repoID)
			continue
		}
		if id, ok := repoIDsByName[strings.ToLower(ref)]; ok {
			resolved = append(resolved, id)
		} else {
			resolved = append(resolved, unmatchedRepoFilterID)
		}
	}
	return dedupePreservingOrder(resolved), nil
}

// ResolveAnalyticsRepoFilters ports _resolve_analytics_repo_filters
// (analytics.py:138-172): rewrites FilterInput.scope.ids (when
// scope.level == REPO) and FilterInput.what.repos through
// resolveRepoFilterRefs, leaving every other field untouched. Returns
// nil unchanged if filters is nil, matching Python's early return.
//
// This is resolve_analytics's Phase 0 -- the ONE sequential query before
// the timeseries+breakdowns concurrent gather (Phase 1). No try/except
// wraps it on the Python side (analytics.py:555-557), so an error here
// is FATAL, same class as Phase 1/4 -- this function returns a real
// error, never swallows.
func ResolveAnalyticsRepoFilters(ctx context.Context, client QueryClient, orgID string, filters *model.FilterInput) (*model.FilterInput, error) {
	if filters == nil {
		return nil, nil
	}

	out := &model.FilterInput{
		Scope: filters.Scope,
		Who:   filters.Who,
		What:  filters.What,
		Why:   filters.Why,
		How:   filters.How,
	}

	if filters.Scope != nil && filters.Scope.Level == model.ScopeLevelInputRepo && len(filters.Scope.Ids) > 0 {
		resolved, err := resolveRepoFilterRefs(ctx, client, orgID, filters.Scope.Ids)
		if err != nil {
			return nil, err
		}
		out.Scope = &model.ScopeFilterInput{Level: filters.Scope.Level, Ids: resolved}
	}

	if filters.What != nil && len(filters.What.Repos) > 0 {
		resolved, err := resolveRepoFilterRefs(ctx, client, orgID, filters.What.Repos)
		if err != nil {
			return nil, err
		}
		out.What = &model.WhatFilterInput{Repos: resolved, Services: filters.What.Services}
	}

	return out, nil
}
