package drilldown

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/scopelabel"
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
	// BlockedOnly selects the persisted per-item blocked-duration source.
	// It is true only for a POST body whose validated filters.how.blocked
	// value is true. False and absent retain the established drilldown
	// response from work_item_cycle_times.
	BlockedOnly bool
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
//
// team_name is Go-only (CHAOS-8749): the attribution row already carries the
// team's display name, null when the row has none.
//
// title is Go-only (CHAOS-8959) and set on blocked-only items alone: the
// work item's stored title, or an explicit null when there is none. A nil
// Title leaves the key out, so the ordinary drilldown keeps its payload.
type IssueItem struct {
	WorkItemID     string     `json:"work_item_id"`
	Provider       string     `json:"provider"`
	Status         string     `json:"status"`
	TeamID         *string    `json:"team_id"`
	TeamName       *string    `json:"team_name"`
	CycleTimeHours *float64   `json:"cycle_time_hours"`
	LeadTimeHours  *float64   `json:"lead_time_hours"`
	StartedAt      *time.Time `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at"`
	Title          *ItemTitle `json:"title,omitempty"`
}

// ItemTitle is a served name that is always written, as the title or null.
type ItemTitle struct {
	Value *string
}

// MarshalJSON writes the title, or null when there is none.
func (t ItemTitle) MarshalJSON() ([]byte, error) {
	if t.Value == nil {
		return []byte("null"), nil
	}
	return json.Marshal(*t.Value)
}

// IssuesResponse ports DrilldownResponse(items=...) for ordinary issue
// drilldowns. BlockedOnly requests add Count: it is the full count before the
// request limit, so it remains useful when Items is a truncated page.
type IssuesResponse struct {
	Items []IssueItem `json:"items"`
	Count *uint64     `json:"count,omitempty"`
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

// teamScopedWorkItemTeamAttributionSource is the same read for a TEAM-scoped
// query: it also takes the co-owner rows (is_primary = 2, teamattribution.
// AttributionCoOwner), so an item of a project with several owning teams is
// in each of those teams' views. Only a query that filters t.team_id may use
// it; without a team filter it would count such an item once per team.
const teamScopedWorkItemTeamAttributionSource = `(
    SELECT
        work_item_id,
        team_id,
        team_name
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String}
      AND is_primary IN (1, 2)
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
    nullIf(t.team_name, '') AS team_name,
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

// latestBlockedItemDaysSource first takes the latest snapshot for each
// (day, provider, work item) identity that migration 104 defines. Its outer
// predicate then removes zero snapshots. The order is required: filtering
// positive stored rows before argMax would make a stale positive reappear
// after a recompute writes zero for that item-day.
//
// work_scope_id and team values are part of the tuple, so the selected team
// belongs to the same snapshot as the selected duration. The later window
// source groups those kept item-days to the one work item a result-table row
// represents.
const latestBlockedItemDaysSource = `(
    SELECT
        day,
        provider,
        work_item_id,
        latest_snapshot.1 AS work_scope_id,
        latest_snapshot.2 AS team_id,
        latest_snapshot.3 AS team_name,
        latest_snapshot.4 AS duration_hours
    FROM (
        SELECT
            day,
            provider,
            work_item_id,
            argMax(tuple(work_scope_id, team_id, team_name, duration_hours), computed_at) AS latest_snapshot
        FROM work_item_blocked_durations_daily
        WHERE org_id = {org_id:String}
          AND day >= {start_day:Date} AND day < {end_day:Date}
        GROUP BY day, provider, work_item_id
    )
    WHERE latest_snapshot.4 > 0
)`

// blockedIssueWindowSource reduces the latest positive item-day snapshots to
// one list row per provider/work-item over the requested window. A team scope
// narrows the snapshots before that reduction: blocked time recorded while an
// item belonged to the requested team counts for that team's evidence even if
// a later day records a different team.
const blockedIssueWindowSource = `(
    SELECT
        provider,
        work_item_id,
        argMax(team_id, day) AS team_id,
        argMax(team_name, day) AS team_name
    FROM %s AS b
    WHERE 1 = 1%s
    GROUP BY provider, work_item_id
)`

func blockedScopeClauseTeam(scopeLevel string, teamIDs []string) (filterSQL string, bindings []dhclickhouse.Binding) {
	if scopeLevel != "team" || len(teamIDs) == 0 {
		return "", nil
	}
	return " AND b.team_id IN {scope_ids:Array(String)}", []dhclickhouse.Binding{
		{Name: "scope_ids", Value: teamIDs},
	}
}

func renderBlockedIssueWindowSource(scopeLevel string, teamIDs []string) (string, []dhclickhouse.Binding) {
	scopeSQL, scopeBindings := blockedScopeClauseTeam(scopeLevel, teamIDs)
	return fmt.Sprintf(blockedIssueWindowSource, latestBlockedItemDaysSource, scopeSQL), scopeBindings
}

// fetchBlockedIssuesQuery returns the page and its full, unpaginated count
// from one ClickHouse statement. count() OVER () runs before LIMIT, so Count
// describes all matching work items and is measured on the same snapshot as
// Items. This avoids a second read racing a recompute that appends a zero row.
const fetchBlockedIssuesQuery = `
SELECT
    b.work_item_id AS work_item_id,
    b.provider AS provider,
    nullIf(b.team_id, '') AS team_id,
    nullIf(b.team_name, '') AS team_name,
    count() OVER () AS total_count
FROM %s AS b
ORDER BY b.work_item_id ASC, b.provider ASC
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
	if params.BlockedOnly {
		return buildBlockedIssuesResponse(ctx, reader, orgID, params)
	}

	scopeSQL, scopeBindings := scopeClauseTeam(params.ScopeLevel, params.ScopeIDs)
	attributionSource := primaryWorkItemTeamAttributionSource
	if scopeSQL != "" {
		attributionSource = teamScopedWorkItemTeamAttributionSource
	}

	query := fmt.Sprintf(fetchIssuesQuery, attributionSource, scopeSQL, settingsMaxExecutionTime())
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
			&item.WorkItemID, &item.Provider, &item.Status, &item.TeamID, &item.TeamName,
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

func buildBlockedIssuesResponse(ctx context.Context, reader *Reader, orgID string, params IssueParams) (*IssuesResponse, error) {
	source, scopeBindings := renderBlockedIssueWindowSource(params.ScopeLevel, params.ScopeIDs)
	query := fmt.Sprintf(fetchBlockedIssuesQuery, source, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_day", Value: formatDay(params.StartDay)},
		{Name: "end_day", Value: formatDay(params.EndDay)},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: params.Limit},
	}, scopeBindings...)

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("fetch blocked issues: %w", err)
	}
	defer rows.Close()

	items := make([]IssueItem, 0)
	var count uint64
	for rows.Next() {
		var (
			item     IssueItem
			rowCount uint64
		)
		if err := rows.Scan(&item.WorkItemID, &item.Provider, &item.TeamID, &item.TeamName, &rowCount); err != nil {
			return nil, fmt.Errorf("scan blocked issue row: %w", err)
		}
		if len(items) > 0 && rowCount != count {
			return nil, fmt.Errorf("scan blocked issue row: inconsistent window count %d, want %d", rowCount, count)
		}
		count = rowCount
		item.Status = "blocked"
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate blocked issue rows: %w", err)
	}

	attachBlockedTitles(ctx, reader, orgID, items)
	return &IssuesResponse{Items: items, Count: &count}, nil
}

// attachBlockedTitles sets Title on every blocked item from work_items inside
// the caller's organisation. A failed lookup is logged by scopelabel and the
// rows stay, each with a null title; an id is never served as a name.
func attachBlockedTitles(ctx context.Context, reader *Reader, orgID string, items []IssueItem) {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.WorkItemID)
	}
	titles := scopelabel.ResolveWorkItemTitles(ctx, reader.client, orgID, ids, scopelabel.Options{
		Suffix: settingsMaxExecutionTime(),
		Log:    "query-api: drilldown blocked issues",
	})
	for i := range items {
		title := ItemTitle{}
		if value, ok := titles[items[i].WorkItemID]; ok {
			title.Value = &value
		}
		items[i].Title = &title
	}
}
