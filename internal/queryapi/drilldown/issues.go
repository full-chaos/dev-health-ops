package drilldown

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// IssueParams is the already-resolved request shape both GET and POST
// /api/v1/drilldown/issues reduce to before calling BuildIssuesResponse --
// same division of labor as PRParams (route file owns the HTTP-shape
// parsing, this package always gets a fully-resolved value).
type IssueParams struct {
	// StartDay/EndDay are the half-open [StartDay, EndDay) UTC-midnight
	// window time_window (api/services/filtering.py:78-92) computes, bound
	// against work_item_cycle_times.day (a ClickHouse `Date` column, unlike
	// drilldown/prs' DateTime64 `created_at`) via formatDay.
	StartDay time.Time
	EndDay   time.Time
	// ScopeLevel/ScopeIDs are filters.scope.level/filters.scope.ids
	// verbatim, fed to scopeClauseTeam. Unlike drilldown/prs there is no
	// WhatRepos field: scope_filter_for_metric's metric_scope="team" branch
	// (api/main.py:987-993, 1026-1032) never calls resolve_repo_filter_ids
	// or reads filters.what.repos at all -- that field is validated as
	// part of the POST body's MetricFilter shape but has no effect on this
	// route's query, matching Python exactly.
	ScopeLevel string
	ScopeIDs   []string
	// Limit is the EFFECTIVE limit already resolved by the caller -- same
	// "payload.limit or 50" / GET-hardcoded-50 contract PRParams.Limit
	// documents.
	Limit int
}

// IssueItem ports one dict fetch_issues (api/queries/drilldown.py:60-93)
// returns, in the same column order as its SELECT list.
//
// DATETIME WIRE FORM (started_at/completed_at): RFC 3339 (Go's default
// time.Time JSON marshaling), not Python's naive isoformat -- the same
// class-level direction already established for the sibling "pr" field
// family (createdAt/mergedAt/closedAt/firstReviewAt/firstCommentAt):
// clickhouse_connect returns a tzinfo-less datetime for a
// Nullable(DateTime('UTC')) column, so Pydantic's Any-typed `items` list
// serializes it with no "Z"/offset suffix -- confirmed live via a
// DrilldownResponse construction with a naive datetime, not assumed.
// Python is the declared baseline defect for this field pair too; Go's
// RFC 3339 form is canonical. A sibling declaration for started_at/
// completed_at, matching the shape of the `pr` field family's own
// declaration, lives in internal/goapiproof/restcorpus.go's
// drilldownIssuesParity.
type IssueItem struct {
	WorkItemID     string     `json:"work_item_id"`
	Provider       string     `json:"provider"`
	Status         string     `json:"status"`
	TeamID         *string    `json:"team_id"`
	CycleTimeHours *float64   `json:"cycle_time_hours"`
	LeadTimeHours  *float64   `json:"lead_time_hours"`
	StartedAt      *time.Time `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at"`
}

// IssuesResponse ports DrilldownResponse(items=...) for the issues route.
type IssuesResponse struct {
	Items []IssueItem `json:"items"`
}

// formatDay renders t as the plain YYYY-MM-DD form a ClickHouse `Date`
// column binding needs -- work_item_cycle_times.day is `Date`, not the
// DateTime64 column drilldown/prs' created_at binds directly as a
// time.Time, so this package needs its own copy of the same helper
// quadrant.go's formatDay already establishes (repeat, don't couple across
// unrelated packages -- this package's own doc comment already states that
// convention for QueryClient).
func formatDay(t time.Time) string {
	return t.Format("2006-01-02")
}

// scopeClauseTeam ports scope_filter_for_metric's only reachable branch for
// drilldown/issues (api/main.py:987-993, 1026-1032: every call site hardcodes
// metric_scope="team", team_column="t.team_id") as build_scope_filter_multi's
// own "team" branch renders it (api/services/filtering.py:138-141,
// api/queries/scopes.py:113-114). scope_filter_for_metric's OWN branch list
// (filtering.py:129-147) means this filter applies ONLY when
// filters.scope.level == "team" AND scope.ids is non-empty: an "org" or
// "repo" scope_type/level applies NO filter at all on this route -- every
// other combination falls through to scope_filter_for_metric's own final
// `return "", {}` (filtering.py:147), confirmed by reading its full branch
// list, not assumed. This is a real behavioural difference from
// drilldown/prs (whose metric_scope="repo" always resolves a repo filter
// regardless of scope.level via resolve_repo_filter_ids) -- not a port
// defect, Python's own dispatch is asymmetric between the two routes.
// t.team_id (the primaryWorkItemTeamAttributionSource join alias) is the
// SAME column this route's SELECT projects as "team_id", so a team-scoped
// drilldown filters on the resolved attribution, never
// work_item_cycle_times' own raw team_id column.
func scopeClauseTeam(scopeLevel string, teamIDs []string) (filterSQL string, bindings []dhclickhouse.Binding) {
	if scopeLevel != "team" || len(teamIDs) == 0 {
		return "", nil
	}
	return " AND t.team_id IN {scope_ids:Array(String)}", []dhclickhouse.Binding{
		{Name: "scope_ids", Value: teamIDs},
	}
}

// primaryWorkItemTeamAttributionSource inlines
// api/queries/investment.py's PRIMARY_WORK_ITEM_TEAM_ATTRIBUTION_SOURCE
// verbatim (bound-parameter form). This is the THIRD copy in the tree
// (internal/queryapi/quadrant/quadrant.go,
// internal/queryapi/investmentexplain/workunitreader.go) -- each a
// package-local inline, matching this package's own "repeat, don't couple"
// convention for a narrow, single-caller subquery rather than a shared
// package. It is already correctly scoped per the class ruling this PR's
// own read follows: FINAL dedups work_item_team_attributions, is_primary=1
// and the latest-computed_at subquery pick the precedence-resolved
// winner, and org_id sits INSIDE this same subquery -- so the LEFT JOIN
// below can never merge or scan another tenant's attribution rows into this
// route's result.
const primaryWorkItemTeamAttributionSource = `(
    SELECT
        work_item_id,
        team_id,
        team_name
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String}
      AND is_primary = 1
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = {org_id:String}
          GROUP BY work_item_id
      )
)`

// fetchIssuesQuery is the Go port of fetch_issues' SELECT
// (api/queries/drilldown.py:60-93).
//
// DEDUP: Python's own query already reads work_item_cycle_times WITH
// FINAL ("FROM work_item_cycle_times AS wct FINAL", drilldown.py:81) --
// unlike fetch_pull_requests, there is no Python-plane dedup defect to fix
// here. This port keeps FINAL on wct (a real ReplacingMergeTree(computed_at)
// read, migration 001/027) and adds FINAL on work_item_team_attributions
// inside primaryWorkItemTeamAttributionSource, the same dedup shape
// quadrant.go's sibling reader already establishes for that table.
//
// ORG SCOPE: wct.org_id sits directly in this query's own WHERE, on the
// SAME top-level read FINAL dedups (wct IS the FROM target, not a joined
// side table merged before an outer filter narrows it) -- satisfying the
// class ruling's placement requirement trivially, the same way
// fetch_pull_requests' fixed shape binds git_pull_requests.repo_id to an
// org-scoped repos subquery. Unlike git_pull_requests.org_id (avoided in
// prs.go in favor of repos.org_id), work_item_cycle_times has no
// alternative, independently-verified org source to resolve through
// instead: the only join here is a LEFT JOIN against the (already
// org-scoped) team-attribution subquery, and filtering the OUTER query on
// THAT subquery's org_id would silently drop every unattributed work item
// (a LEFT JOIN's unmatched right side is NULL, and Python's own
// nullIf(t.team_id, ”) deliberately keeps those rows) -- changing the
// result set, not merely the scoping mechanism. wct.org_id is therefore
// the only correct column, matching Python's own query exactly, carrying
// the SAME migration-024 DEFAULT 'default' backfill caveat prs.go's doc
// comment already flags for git_pull_requests.org_id (no alternative
// exists here to route around it).
//
// ORDER BY: `wct.completed_at DESC, wct.work_item_id ASC`. Python's own
// fetch_issues carries only `ORDER BY wct.completed_at DESC`, with no
// secondary sort key -- ClickHouse gives no ordering guarantee among rows
// sharing one completed_at value, so a tie can put a different subset of
// those rows on each side of this statement's own LIMIT. Appending
// wct.work_item_id ASC makes THIS statement's own tie order, and
// therefore this plane's own cursor pagination across repeated calls,
// stable and reproducible -- it does not, and cannot, make this
// statement agree with Python's own unordered tie, which carries no
// secondary key to match against.
const fetchIssuesQuery = `
SELECT
    wct.work_item_id AS work_item_id,
    wct.provider AS provider,
    wct.status AS status,
    nullIf(t.team_id, '') AS team_id,
    wct.cycle_time_hours AS cycle_time_hours,
    wct.lead_time_hours AS lead_time_hours,
    wct.started_at AS started_at,
    wct.completed_at AS completed_at
FROM work_item_cycle_times AS wct FINAL
LEFT JOIN %s AS t
  ON t.work_item_id = wct.work_item_id
WHERE wct.day >= {start_day:Date} AND wct.day < {end_day:Date}
  AND wct.org_id = {org_id:String}
%s
ORDER BY wct.completed_at DESC, wct.work_item_id ASC
LIMIT {limit:UInt64}
%s
`

// BuildIssuesResponse is the Go port of drilldown_issues/
// drilldown_issues_post's shared body (api/main.py:984-1005, 1020-1045):
// resolve the team scope, run fetch_issues, wrap the rows as items. Auth
// (current_user), the outer try/except -> 503 fallback, and the GET
// route's X-DevHealth-Deprecated response header are the CALLER's job
// (route file), matching BuildPRsResponse's own division -- this function
// returns a plain Go error for any failure, never an HTTP status.
func BuildIssuesResponse(ctx context.Context, reader *Reader, orgID string, params IssueParams) (*IssuesResponse, error) {
	if reader == nil {
		return nil, ErrUnavailable
	}

	scopeSQL, scopeBindings := scopeClauseTeam(params.ScopeLevel, params.ScopeIDs)

	query := fmt.Sprintf(fetchIssuesQuery, primaryWorkItemTeamAttributionSource, scopeSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(params.StartDay)},
		{Name: "end_day", Value: formatDay(params.EndDay)},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: params.Limit},
	}, scopeBindings...)

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("fetch issues: %w", err)
	}
	defer rows.Close()

	items := make([]IssueItem, 0)
	for rows.Next() {
		var item IssueItem
		if err := rows.Scan(
			&item.WorkItemID, &item.Provider, &item.Status, &item.TeamID,
			&item.CycleTimeHours, &item.LeadTimeHours,
			&item.StartedAt, &item.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan issue row: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate issue rows: %w", err)
	}

	return &IssuesResponse{Items: items}, nil
}
