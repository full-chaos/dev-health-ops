package home

import (
	"context"
	"slices"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/scoperesolve"
)

// An unresolved scope filter answers EMPTY (CHAOS-6119, R335); what "known" means is in package scoperesolve.

// developerNamed: the request scopes to developer ids. No metric of Home has a developer dimension, so such a scope answers
// no data whether or not the ids are known.
func (s ScopeFilter) developerNamed() bool {
	return s.Level == "developer" && slices.ContainsFunc(s.IDs, func(id string) bool { return id != "" })
}

// DeveloperNamed is developerNamed for the callers that build on Home.
func (s ScopeFilter) DeveloperNamed() bool { return s.developerNamed() }

// Unresolved reports that the request names scope ids at a repository, service or developer level and none of them exists for the
// organization. Only meaningful after ResolveScope.
func (s ScopeFilter) Unresolved() bool { return s.unresolved }

// ResolveScope reads, once per request and one query per level, whether the named scope ids exist for the organization. Team scopes
// and requests that name no id are untouched.
func ResolveScope(ctx context.Context, client QueryClient, orgID string, f Filters) (Filters, error) {
	if f.Scope.checked {
		return f, nil
	}
	f.Scope.checked = true
	// The scope ids are a SET (CHAOS-14379): each id once, in the order of its first use, compared as the scope
	// predicates compare ids (exact strings). Every read of the request takes that set, so [t, t] is the one team t
	// and [t, u, t] the team set [t, u].
	f.Scope.IDs = distinctIDs(f.Scope.IDs)
	unresolved, err := scoperesolve.Unresolved(ctx, client, orgID, f.Scope.Level, f.Scope.IDs)
	if err != nil {
		return f, err
	}
	f.Scope.unresolved = unresolved
	return f, nil
}

const unresolvedScopeFilter = " AND 1 = 0"

// distinctIDs is a list of ids as a set: each id once, in the order of its first use.
func distinctIDs(ids []string) []string {
	var out []string
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, repeated := seen[id]; repeated {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
