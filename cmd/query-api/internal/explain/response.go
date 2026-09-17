package explain

import (
	"context"
	"fmt"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/teamscope"
)

// scopeClauseRepo/scopeClauseTeam port build_scope_filter_multi
// (api/queries/scopes.py:101-113) for the two branches this route
// reaches, team_column/repo_column always "team_id"/"repo_id" (this
// route's own metricConfigs never overrides either default). Empty ids
// -> no filter, matching build_scope_filter_multi's own
// `if not scope_ids: return "", {}`.
func scopeClauseRepo(repoIDs []string) (filterSQL string, bindings []dhclickhouse.Binding) {
	if len(repoIDs) == 0 {
		return "", nil
	}
	return " AND repo_id IN {scope_ids:Array(String)}", []dhclickhouse.Binding{
		{Name: "scope_ids", Value: repoIDs},
	}
}

// combineRepoScopeConditions ORs an explicit repo-id membership clause
// (scopeClauseRepo's own " AND ..." fragment) with a team-derived
// condition (teamscope.RepoCondition's bare boolean, no "AND"/"OR" of its
// own) into ONE "AND (...)" fragment -- resolve_repo_filter_ids' own
// Python shape unions explicit refs and team-resolved ids into a SINGLE
// id list before filtering, so a repo matches when it is named directly
// OR reachable through a scoped team; this keeps that same union
// semantics across the two different condition shapes. Either side may
// be empty; the result is "" only when both are.
func combineRepoScopeConditions(explicitSQL string, explicitBindings []dhclickhouse.Binding, teamCondition string, teamBindings []dhclickhouse.Binding) (filterSQL string, bindings []dhclickhouse.Binding) {
	explicitCondition := strings.TrimPrefix(explicitSQL, " AND ")
	var conditions []string
	if explicitCondition != "" {
		conditions = append(conditions, explicitCondition)
		bindings = append(bindings, explicitBindings...)
	}
	if teamCondition != "" {
		conditions = append(conditions, teamCondition)
		bindings = append(bindings, teamBindings...)
	}
	if len(conditions) == 0 {
		return "", nil
	}
	if len(conditions) == 1 {
		return " AND " + conditions[0], bindings
	}
	return " AND (" + strings.Join(conditions, " OR ") + ")", bindings
}

func scopeClauseTeam(teamIDs []string) (filterSQL string, bindings []dhclickhouse.Binding) {
	if len(teamIDs) == 0 {
		return "", nil
	}
	return " AND team_id IN {scope_ids:Array(String)}", []dhclickhouse.Binding{
		{Name: "scope_ids", Value: teamIDs},
	}
}

// metricStatusFilterSQL returns the WHERE-clause fragment restricting a
// metric's read to its own config.StatusFilter status ("" when the metric
// carries none). Appended to scopeFilterSQL so it lands inside
// metricFromClause's own dedup subquery, at the SAME nesting depth as
// org_id and any team/repo scope filter. status is a fixed metricConfigs
// value, never caller input, so a literal is safe here the same way the
// day-window/tenancy literals elsewhere in this package are.
func metricStatusFilterSQL(status string) string {
	if status == "" {
		return ""
	}
	return fmt.Sprintf(" AND status = '%s'", status)
}

// scopeFilterForMetric ports scope_filter_for_metric (api/services/
// filtering.py:129-147).
//
// DECLARED PYTHON-PLANE DEFECT (org_id): explain.py's own call site for
// this chain (api/services/explain.py:150-152) is the ONLY caller of
// scope_filter_for_metric or resolve_repo_filter_ids anywhere in the
// Python source (services/{investment,work_units,heatmap,home,
// investment_segments,sankey,investment_flow}.py all pass org_id=org_id)
// that OMITS the org_id keyword entirely -- it silently defaults to "".
// For a "repo" metricScope (review_latency/deploy_freq/churn/
// change_failure_rate), that flows into resolve_repo_id's own
// `org_id = %(org_id)s` filter (api/queries/scopes.py:26,42), which no
// real org's repos row ever matches -- every UUID/name ref fails to
// resolve, repo_ids ends up [], and build_scope_filter_multi returns
// ("", {}): the user's repo/team/what.repos scope is SILENTLY DROPPED,
// and the metric aggregates over the WHOLE ORG instead (still correctly
// org-bounded by the metric query's own separate `org_id = {org_id}`
// filter -- this is a scope-filter defect, not a cross-tenant leak). This
// is a genuine Python-plane defect, not a data-semantics choice: it does
// not match any other call site's behavior, and nothing in the source
// suggests it is intentional. This port passes the REAL orgID here --
// the Go answer is canonical (parallel to the class ruling's own org-
// filter-placement defect class); a repo/team-scoped GET/POST to
// /api/v1/explain for review_latency/deploy_freq/churn/
// change_failure_rate now actually narrows to the requested scope, where
// Python's real endpoint today does not. Flagged in RISK-NOTES for a
// follow-up ticket against the Python route while it still exists.
func (reader *Reader) scopeFilterForMetric(ctx context.Context, metricScope string, scopeLevel string, scopeIDs, whatRepos []string, orgID string, asOf time.Time) (filterSQL string, bindings []dhclickhouse.Binding, err error) {
	if metricScope == "team" && scopeLevel == "team" {
		filterSQL, bindings = scopeClauseTeam(scopeIDs)
		return filterSQL, bindings, nil
	}
	if metricScope == "repo" {
		var repoRefs []string
		if scopeLevel == "repo" {
			repoRefs = append(repoRefs, scopeIDs...)
		}
		repoRefs = append(repoRefs, whatRepos...)
		repoIDs, resolveErr := reader.resolveRepoIDs(ctx, repoRefs, orgID)
		if resolveErr != nil {
			return "", nil, resolveErr
		}
		explicitSQL, explicitBindings := scopeClauseRepo(repoIDs)

		var teamCondition string
		var teamBindings []dhclickhouse.Binding
		if scopeLevel == "team" {
			teamCondition, teamBindings = teamscope.RepoCondition(orgID, "repo_id", scopeIDs, asOf)
		}

		filterSQL, bindings = combineRepoScopeConditions(explicitSQL, explicitBindings, teamCondition, teamBindings)
		return filterSQL, bindings, nil
	}
	return "", nil, nil
}

// BuildExplainResponse is the Go port of build_explain_response
// (api/services/explain.py:121-261), minus the EXPLAIN_CACHE wrapper --
// see explain.go's own package doc comment for that declared, no-op
// divergence. Auth (current_user), the outer try/except -> 503 fallback,
// and the GET route's X-DevHealth-Deprecated response header are the
// CALLER's job (route file), matching drilldown.BuildPRsResponse's own
// division of labor: this function returns a plain Go error for any
// failure, never an HTTP status.
func BuildExplainResponse(ctx context.Context, reader *Reader, orgID string, params Params) (*Response, error) {
	if reader == nil {
		return nil, ErrUnavailable
	}

	config := resolveMetricConfig(params.Metric)

	// One instant for the whole response: the current and comparison windows
	// below share one scope filter, so they must share one team membership.
	scopeFilterSQL, scopeBindings, err := reader.scopeFilterForMetric(ctx, config.Scope, params.ScopeLevel, params.ScopeIDs, params.WhatRepos, orgID, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	scopeFilterSQL += metricStatusFilterSQL(config.StatusFilter)

	currentRaw, err := reader.fetchMetricValue(ctx, config.Table, config.Column, config.Aggregator, params.StartDay, params.EndDay, scopeFilterSQL, scopeBindings, orgID)
	if err != nil {
		return nil, err
	}
	previousRaw, err := reader.fetchMetricValue(ctx, config.Table, config.Column, config.Aggregator, params.CompareStart, params.CompareEnd, scopeFilterSQL, scopeBindings, orgID)
	if err != nil {
		return nil, err
	}
	currentValue := safeFloat(currentRaw)
	previousValue := safeFloat(previousRaw)
	pctChange := safeFloat(deltaPct(currentValue, previousValue))

	drivers, err := reader.fetchMetricDriverDelta(ctx, config.Table, config.Column, config.GroupBy, config.Aggregator, params.StartDay, params.EndDay, params.CompareStart, params.CompareEnd, scopeFilterSQL, scopeBindings, orgID)
	if err != nil {
		return nil, err
	}
	contributors, err := reader.fetchMetricContributors(ctx, config.Table, config.Column, config.GroupBy, config.Aggregator, params.StartDay, params.EndDay, scopeFilterSQL, scopeBindings, orgID)
	if err != nil {
		return nil, err
	}

	// Resolve scope ids -> display names server-side so labels never
	// carry a bare UUID (Framework A7/A8). group_by is repo_id or
	// team_id (explain.py:205-219).
	scopeKind, hasScopeKind := scopeKindForGroupBy(config.GroupBy)
	var displayNames map[string]string
	if hasScopeKind {
		allIDs := collectRowIDs(drivers, contributors)
		if len(allIDs) > 0 {
			displayNames = reader.resolveScopeDisplayNames(ctx, orgID, scopeKind, allIDs)
		}
	}
	if displayNames == nil {
		displayNames = map[string]string{}
	}

	primaryID := primaryScopeID(params.ScopeIDs)

	driverModels := make([]Contributor, 0, len(drivers))
	for _, row := range drivers {
		driverModels = append(driverModels, buildContributor(row, params.Metric, params.ScopeLevel, primaryID, config.Transform, displayNames, row.DeltaPct))
	}

	contributorModels := make([]Contributor, 0, len(contributors))
	for _, row := range contributors {
		// explain.py:240: delta_value=0.0 literal -- a contributor row
		// never carries its own delta (only a driver row does).
		contributorModels = append(contributorModels, buildContributor(row, params.Metric, params.ScopeLevel, primaryID, config.Transform, displayNames, 0.0))
	}

	return &Response{
		Metric:       params.Metric,
		Label:        config.Label,
		Unit:         config.Unit,
		Value:        safeTransform(config.Transform, currentValue),
		DeltaPct:     pctChange,
		Drivers:      driverModels,
		Contributors: contributorModels,
		DrilldownLinks: map[string]string{
			"prs":    fmt.Sprintf("/api/v1/drilldown/prs?metric=%s", params.Metric),
			"issues": fmt.Sprintf("/api/v1/drilldown/issues?metric=%s", params.Metric),
		},
	}, nil
}

// collectRowIDs ports explain.py:208-210's
// `{str(r.get("id") or "") for r in (*drivers, *contributors)} - {""}`.
func collectRowIDs(drivers, contributors []metricRow) []string {
	seen := make(map[string]struct{}, len(drivers)+len(contributors))
	var out []string
	add := func(id string) {
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, row := range drivers {
		add(row.ID)
	}
	for _, row := range contributors {
		add(row.ID)
	}
	return out
}

// buildContributor ports explain.py's _build_contributor (282-312).
func buildContributor(row metricRow, metric, scopeLevel, primaryID string, transform func(float64) float64, displayNames map[string]string, deltaValue float64) Contributor {
	scopeID := row.ID
	resolved, hasResolved := displayNames[scopeID]
	var label string
	var displayName *string
	if hasResolved && resolved != "" && !looksLikeUUID(resolved) {
		label = resolved
		resolvedCopy := resolved
		displayName = &resolvedCopy
	} else {
		label = shortToken(scopeID)
		displayName = nil
	}
	rawValue := safeFloat(row.Value)
	return Contributor{
		ID:           scopeID,
		Label:        label,
		DisplayName:  displayName,
		Value:        safeTransform(transform, rawValue),
		DeltaPct:     deltaValue,
		EvidenceLink: fmt.Sprintf("/api/v1/drilldown/prs?metric=%s&scope_type=%s&scope_id=%s", metric, scopeLevel, primaryID),
	}
}
