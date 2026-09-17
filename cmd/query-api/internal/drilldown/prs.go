package drilldown

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/teamscope"
)

// PRParams is the already-resolved request shape both
// GET and POST /api/v1/drilldown/prs reduce to before calling
// BuildPRsResponse -- the route file does the HTTP-shape-specific work
// (query-param defaults for GET, JSON-body parsing for POST) and always
// hands this package a fully-resolved value, matching
// investment_explain_route.go's own division of labor between its route
// file and internal/investmentexplain.
type PRParams struct {
	// StartDay/EndDay are the half-open [StartDay, EndDay) UTC-midnight
	// window time_window (api/services/filtering.py:78-92) computes --
	// the route file owns that computation (it is IDENTICAL to
	// investment_explain_route.go's own timeWindow, reused as-is from
	// cmd/query-api's package-level helper, not re-derived here).
	StartDay time.Time
	EndDay   time.Time
	// ScopeLevel/ScopeIDs/WhatRepos are filters.scope.level, filters.scope.ids
	// and filters.what.repos verbatim -- fed to ResolveRepoFilterIDs.
	ScopeLevel string
	ScopeIDs   []string
	WhatRepos  []string
	// Limit is the EFFECTIVE limit already resolved by the caller:
	// POST's `payload.limit or 50` (a JSON body limit of 0 or absent both
	// fall back to 50, matching Python's falsy-or semantics) or GET's
	// hardcoded 50 (fetch_pull_requests' own keyword default -- the GET
	// route has no limit query param at all, api/main.py:938-949).
	Limit int
}

// PRItem ports one dict fetch_pull_requests (api/queries/drilldown.py:
// 20-57) returns, in the same column order as its SELECT list.
//
// DATETIME WIRE FORM (created_at/merged_at/first_review_at): RFC 3339
// (Go's default time.Time JSON marshaling), not Python's naive
// isoformat -- the same already-established, class-level direction this
// "pr" field family (createdAt/mergedAt/firstReviewAt) already carries on
// its GraphQL side: clickhouse_connect returns a tzinfo-less datetime for
// a DateTime64(3,'UTC') column, so Pydantic's Any-typed `items` list
// serializes it with NO "Z"/offset suffix -- confirmed live via
// `uv run python3` against DrilldownResponse(items=[...]) with a naive
// datetime, not assumed. Python's wire form is the declared baseline
// defect; Go's RFC 3339 form is canonical. This is a DIFFERENT case from
// the one precedent-test-pinned byte-parity exception elsewhere in this
// service, which covers a different, String-typed GraphQL field and does
// not extend here.
type PRItem struct {
	RepoID             string     `json:"repo_id"`
	Number             uint32     `json:"number"`
	Title              *string    `json:"title"`
	AuthorName         *string    `json:"author_name"`
	CreatedAt          time.Time  `json:"created_at"`
	MergedAt           *time.Time `json:"merged_at"`
	FirstReviewAt      *time.Time `json:"first_review_at"`
	ReviewLatencyHours *int64     `json:"review_latency_hours"`
}

// PRsResponse ports DrilldownResponse(items=...) for the prs route.
type PRsResponse struct {
	Items []PRItem `json:"items"`
}

// scopeClauseRepo ports scope_filter_for_metric's "repo" branch
// (api/services/filtering.py:128-146) as build_scope_filter_multi("repo",
// repoIDs) would render it -- the ONLY branch drilldown_prs/
// drilldown_prs_post ever reach: both call sites hardcode
// metric_scope="repo" (api/main.py:918-919, 956-957), so the "team"
// branch of scope_filter_for_metric is dead code on this call path,
// exactly like investmentexplain/reader.go's BreakdownFilters.scopeClause
// doc comment already establishes for its own, sibling, always-"repo"
// call path. Empty repoIDs -> no filter, matching build_scope_filter_multi's
// own `if not scope_ids: return "", {}`.
func scopeClauseRepo(repoIDs []string) (filterSQL string, bindings []dhclickhouse.Binding) {
	if len(repoIDs) == 0 {
		return "", nil
	}
	return " AND pr.repo_id IN {scope_ids:Array(String)}", []dhclickhouse.Binding{
		{Name: "scope_ids", Value: repoIDs},
	}
}

// fetchPullRequestsQuery is the Go port of fetch_pull_requests'
// SELECT (api/queries/drilldown.py:20-57).
//
// DEDUP: Python query modules are not correct exemplars for
// ReplacingMergeTree reads. git_pull_requests and repos are BOTH
// ReplacingMergeTree(last_synced) (000_raw_tables.sql), with org_id
// prepended to both sorting keys by migration 027
// (add_org_id_to_sorting_keys.py) -- Python's own query
// (api/queries/drilldown.py:31-49) reads NEITHER table with FINAL or any
// argMax dedup, so an unmerged part can return more than one physical
// row for the same (repo_id, number)/(id). This port uses `FINAL` on
// both, matching workgraph/pr.go's FetchPRCoreRow (a real, reviewed Go
// port of a sibling Python PR reader) on the same table pair: FINAL
// performs a true merge, with no per-column NULL-skip footgun an
// argMax(col, version) would carry.
//
// ORG SCOPE sits INSIDE git_pull_requests' own read, not behind a join
// evaluated after it: pr.repo_id is bound to `(SELECT id FROM repos
// FINAL WHERE org_id = ...)`, a properly deduped, org-filtered-in-place
// subquery, so git_pull_requests is never merged or scanned across
// tenants before the org boundary applies -- an INNER JOIN whose ON
// clause carries no org predicate, with the org filter only in the
// join's OUTER WHERE, does not give that guarantee: the FINAL merge on
// the joined side runs over every org's rows before the WHERE prunes
// them. NOT git_pull_requests' own org_id column, added by the same
// migration 027: that column is backfilled `DEFAULT 'default'` for
// every row written before the migration ran, and nothing read in this
// port's source files confirms those historical rows also carry a real
// org_id there (as opposed to the literal string "default") the way
// repos.org_id was already correctly populated by migration 024. Taking
// the git_pull_requests.org_id shortcut risks silently dropping or
// misbucketing pre-migration PR rows for a real org; resolving through
// repos.org_id carries no such risk. workgraph/pr.go's FetchPRCoreRow
// DOES take the pr.org_id shortcut for a single-PR-by-id lookup -- a
// different route, already shipped, out of this port's scope to
// relitigate.
//
// review_latency_hours is computed in SQL, not Go, deliberately: it is
// the same `dateDiff('hour', created_at, first_review_at)` expression
// Python uses, applied to the FINAL-deduped row -- reimplementing
// dateDiff's calendar-boundary semantics in Go would risk a subtly
// different answer for a value never otherwise read out of ClickHouse.
const fetchPullRequestsQuery = `
SELECT
    toString(pr.repo_id) AS repo_id,
    pr.number AS number,
    pr.title AS title,
    pr.author_name AS author_name,
    pr.created_at AS created_at,
    pr.merged_at AS merged_at,
    pr.first_review_at AS first_review_at,
    if(pr.first_review_at IS NULL, NULL,
       dateDiff('hour', pr.created_at, pr.first_review_at)) AS review_latency_hours
FROM git_pull_requests AS pr FINAL
WHERE pr.created_at >= {start_ts:DateTime64(3, 'UTC')}
  AND pr.created_at < {end_ts:DateTime64(3, 'UTC')}
  AND pr.repo_id IN (
      SELECT id FROM repos FINAL WHERE org_id = {org_id:String}
  )
%s
ORDER BY pr.created_at DESC
LIMIT {limit:UInt64}
%s
`

// BuildPRsResponse is the Go port of drilldown_prs/drilldown_prs_post's
// shared body (api/main.py:915-935, 950-974): resolve the repo scope,
// run fetch_pull_requests, wrap the rows as items. Auth (current_user),
// the outer try/except -> 503 fallback, and the GET route's
// X-DevHealth-Deprecated response header are the CALLER's job (route
// file), matching quadrant.BuildResponse's own division -- this function
// returns a plain Go error for any failure, never an HTTP status.
func BuildPRsResponse(ctx context.Context, reader *Reader, orgID string, params PRParams) (*PRsResponse, error) {
	if reader == nil {
		return nil, ErrUnavailable
	}

	repoIDs, err := reader.ResolveRepoFilterIDs(ctx, params.ScopeLevel, params.ScopeIDs, params.WhatRepos, orgID)
	if err != nil {
		return nil, err
	}
	scopeSQL, scopeBindings := scopeClauseRepo(repoIDs)
	if params.ScopeLevel == "team" && len(params.ScopeIDs) > 0 {
		teamCondition, teamBindings := teamscope.RepoCondition(orgID, "pr.repo_id", params.ScopeIDs, time.Now().UTC())
		if scopeSQL != "" {
			scopeSQL = " AND (pr.repo_id IN {scope_ids:Array(String)} OR " + teamCondition + ")"
		} else {
			scopeSQL = " AND " + teamCondition
		}
		scopeBindings = append(scopeBindings, teamBindings...)
	}

	query := fmt.Sprintf(fetchPullRequestsQuery, scopeSQL, settingsMaxExecutionTime())
	bindings := append([]dhclickhouse.Binding{
		{Name: "start_ts", Value: params.StartDay},
		{Name: "end_ts", Value: params.EndDay},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: params.Limit},
	}, scopeBindings...)

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("fetch pull requests: %w", err)
	}
	defer rows.Close()

	items := make([]PRItem, 0)
	for rows.Next() {
		var item PRItem
		if err := rows.Scan(
			&item.RepoID, &item.Number, &item.Title, &item.AuthorName,
			&item.CreatedAt, &item.MergedAt, &item.FirstReviewAt,
			&item.ReviewLatencyHours,
		); err != nil {
			return nil, fmt.Errorf("scan pull request row: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pull request rows: %w", err)
	}

	return &PRsResponse{Items: items}, nil
}
