package home

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/scoperesolve"
)

// An unresolved scope filter answers EMPTY (CHAOS-6119, R335); what "known" means is in package scoperesolve.

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
	unresolved, err := scoperesolve.Unresolved(ctx, client, orgID, f.Scope.Level, f.Scope.IDs)
	if err != nil {
		return f, err
	}
	f.Scope.unresolved = unresolved
	return f, nil
}

const unresolvedScopeFilter = " AND 1 = 0"
