// Repo/team-scope resolution -- ports:
//   - api/services/filtering.py's resolve_repo_filter_ids/scope_filter_for_metric
//   - api/queries/scopes.py's resolve_repo_id/resolve_repo_ids/
//     build_scope_filter_multi
//
// A team scope is the exception: its repositories come from
// team_repo_ownership through the one shared
// internal/queryapi/teamscope.RepoCondition, not from a per-package
// copy. The rule is identical on every route, and a route resolving it its
// own way answers a different question under the same word.
//
// DEDUP: repos is ReplacingMergeTree(last_synced) and is read FINAL here,
// org_id filtered in the same statement -- api/queries/scopes.py's
// resolve_repo_id/resolve_repo_ids read it with no dedup at all, a declared
// Python-plane defect (see internal/goapiproof/restcorpus.go's home
// declarations), matching sankey/heatmap's own precedent of always reading
// this table FINAL regardless of whether a given aggregate happens to be
// dedup-invariant.
package home

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

func settingsMaxExecutionTime() string {
	return "SETTINGS max_execution_time = 30"
}

// resolveRepoID ports resolve_repo_id (api/queries/scopes.py:19-50).
func resolveRepoID(ctx context.Context, client QueryClient, repoRef, orgID string) (string, bool, error) {
	return teamscope.ResolveRepoRef(ctx, client, repoRef, orgID, settingsMaxExecutionTime(), "home: ")
}

// resolveRepoIDs ports resolve_repo_ids (api/queries/scopes.py:53-69).
func resolveRepoIDs(ctx context.Context, client QueryClient, repoRefs []string, orgID string) ([]string, error) {
	return teamscope.ResolveRepoRefs(ctx, client, repoRefs, orgID, settingsMaxExecutionTime(), "home: ")
}

// repoScopeFilter narrows a repo-keyed metric to the repositories a request
// names. Explicit repo refs (scope.IDs at scope="repo", or what.repos) are
// resolved and verified one at a time through resolveRepoIDs -- that list is
// bounded by what the caller named, not by organization scale, so it stays a
// plain array binding. A team scope contributes the repositories that team
// OWNS, as teamscope.RepoCondition resolves them from team_repo_ownership.
// The two are ANDed when both are present (teamscope.NarrowRepoScope): a request
// naming a team and explicit repos sees the repositories that are both, and a
// filter whose named repositories resolve to nothing matches nothing.
//
// asOf is the response's own instant, so every metric in one response
// resolves the same team membership.
func repoScopeFilter(ctx context.Context, client QueryClient, f Filters, orgID, repoColumn string, asOf time.Time) (string, []dhclickhouse.Binding, error) {
	repoRefs := teamscope.NamedRepoRefs(f.Scope.Level, f.Scope.IDs, f.What.Repos)

	explicitIDs, err := resolveRepoIDs(ctx, client, repoRefs, orgID)
	if err != nil {
		return "", nil, err
	}

	teamScoped := f.Scope.Level == "team" && len(f.Scope.IDs) > 0
	var teamCondition string
	var teamBindings []dhclickhouse.Binding
	if teamScoped {
		teamCondition, teamBindings = teamscope.RepoCondition(orgID, repoColumn, f.Scope.IDs, asOf)
	}

	filter, bindings := teamscope.NarrowRepoScope(len(repoRefs) > 0, scopeClauseMulti(explicitIDs, repoColumn), scopeBindingsMulti(explicitIDs), teamCondition, teamBindings)
	return filter, bindings, nil
}

// repoFilterRequested reports whether the request names repositories: the ids of
// a repo-level scope, or what.repos. A team scope alone is not a repository
// filter.
func repoFilterRequested(f Filters) bool {
	return len(teamscope.NamedRepoRefs(f.Scope.Level, f.Scope.IDs, f.What.Repos)) > 0
}

// filterEmptyReason is Response.FilterEmptyReason (CHAOS-9098): why the
// repositories a request names matched nothing, by the one rule of
// teamscope.ClassifyEmptyFilter. nil when the request names no repository, and
// whenever something matched.
func filterEmptyReason(ctx context.Context, client QueryClient, f Filters, orgID string, asOf time.Time) (*teamscope.EmptyReason, error) {
	refs := teamscope.NamedRepoRefs(f.Scope.Level, f.Scope.IDs, f.What.Repos)
	if len(refs) == 0 {
		return nil, nil
	}
	resolved, err := resolveRepoIDs(ctx, client, refs, orgID)
	if err != nil {
		return nil, err
	}
	var teamIDs []string
	if f.Scope.Level == "team" {
		for _, id := range f.Scope.IDs {
			if id != "" {
				teamIDs = append(teamIDs, id)
			}
		}
	}
	held := 0
	if len(teamIDs) > 0 {
		if held, err = teamscope.CountTeamHeldRepos(ctx, client, orgID, resolved, teamIDs, asOf); err != nil {
			return nil, err
		}
	}
	return teamscope.ClassifyEmptyFilter(len(refs), len(resolved), len(teamIDs) > 0, held), nil
}

// repoFilterApplied is MetricDelta.RepoFilterApplied: nil when the request names
// no repository; otherwise true: a repository-keyed metric is narrowed by its
// repo_id (and has no data when the named repositories resolved to nothing), a
// work-item metric through the items linked to the repositories' pull requests
// (CHAOS-9094).
func repoFilterApplied(f Filters, metricScope string) *bool {
	if !repoFilterRequested(f) {
		return nil
	}
	// A work-item metric is narrowed through the items linked to the
	// repositories' pull requests (CHAOS-9094), a repository metric through
	// its repo_id.
	applied := metricScope == "repo" || metricScope == "team"
	return &applied
}

// scopeFilterForMetric ports scope_filter_for_metric (api/services/
// filtering.py:129-147).
func scopeFilterForMetric(ctx context.Context, client QueryClient, metricScope string, f Filters, orgID, teamColumn, repoColumn string, asOf time.Time) (string, []dhclickhouse.Binding, error) {
	if metricScope == "team" && f.Scope.Level == "team" {
		return scopeClauseMulti(f.Scope.IDs, teamColumn), scopeBindingsMulti(f.Scope.IDs), nil
	}
	if metricScope == "repo" {
		return repoScopeFilter(ctx, client, f, orgID, repoColumn, asOf)
	}
	return "", nil, nil
}

// scopeShareForMetric is the weight of a summed repository metric under a team scope: the share of each repository the
// team holds (teamscope.RepoShare, CHAOS-9413, D5988), so a co-owner reads its own share of the repository's total and
// the teams of one repository together read it once. Only a sum is weighed: an average, a median or a ratio is a
// property of the repository, not a quantity split among its owners, and keeps the membership test alone. ("", nil)
// when the metric is not a repository sum or the request has no team scope.
func scopeShareForMetric(spec metricSpec, f Filters, orgID, repoColumn string, asOf time.Time) (string, []dhclickhouse.Binding) {
	if spec.Scope != "repo" || spec.Aggregator != "sum" || f.Scope.Level != "team" || len(f.Scope.IDs) == 0 {
		return "", nil
	}
	return teamscope.RepoShare(orgID, repoColumn, f.Scope.IDs, asOf)
}

// scopeClauseMulti/scopeBindingsMulti port build_scope_filter_multi
// (api/queries/scopes.py:105-117).
func scopeClauseMulti(ids []string, column string) string {
	if len(ids) == 0 {
		return ""
	}
	return fmt.Sprintf(" AND %s IN {scope_ids:Array(String)}", column)
}

func scopeBindingsMulti(ids []string) []dhclickhouse.Binding {
	if len(ids) == 0 {
		return nil
	}
	return []dhclickhouse.Binding{{Name: "scope_ids", Value: ids}}
}
