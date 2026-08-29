package analytics

import "github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"

// hasActiveFilters ports sql/compiler.py's _has_active_filters
// (compiler.py:268-288) exactly, including its own comment's reasoning:
// any non-org scope is treated as active even for dimensions/templates
// that apply no scope predicate of their own (service scope no-ops
// through translate_scope_filter too), so an unsupported-filter rejection
// path fails honestly instead of silently returning org-wide data
// (CHAOS-2487).
func hasActiveFilters(filters *model.FilterInput) bool {
	if filters == nil {
		return false
	}

	if filters.Scope != nil && len(filters.Scope.Ids) > 0 {
		if filters.Scope.Level != model.ScopeLevelInputOrg {
			return true
		}
	}

	if filters.Who != nil && (len(filters.Who.Developers) > 0 || len(filters.Who.Roles) > 0) {
		return true
	}
	if filters.What != nil && (len(filters.What.Repos) > 0 || len(filters.What.Services) > 0) {
		return true
	}
	if filters.Why != nil && (len(filters.Why.WorkCategory) > 0 || len(filters.Why.IssueType) > 0) {
		return true
	}
	if filters.How != nil && len(filters.How.FlowStage) > 0 {
		return true
	}
	return false
}
