// GET /api/v1/people/{person_id}/drilldown/prs -- ports
// people_drilldown_prs (api/main.py:1123-1149) and
// build_person_drilldown_prs_response (services/people.py:734-790), backed
// by fetch_person_pull_requests (queries/people.py:235-259) and
// sql/people/person_drilldown_prs.sql.
//
// Identity resolution reuses resolveIdentityContext/identityVariants
// (resolve.go/identity.go) -- the SAME functions people_summary_route.go/
// people_metric_route.go already exercise for the same person_id path
// segment, so this file adds no second identity-resolution copy.
//
// DEDUP: Python query modules are not correct exemplars for
// ReplacingMergeTree reads. git_pull_requests and repos are BOTH
// ReplacingMergeTree(last_synced), with org_id prepended to both sorting
// keys by migration 027 -- see internal/drilldown/prs.go's own
// fetchPullRequestsQuery doc comment for the full citation trail (the SAME
// two tables, the SAME fix). This port reads both FINAL, matching that
// sibling reader's already-reviewed shape.
//
// ORG SCOPE sits INSIDE git_pull_requests' own read, not behind a join
// evaluated after it, for the identical reason internal/drilldown/prs.go's
// doc comment states: pr.repo_id is bound to `(SELECT id FROM repos FINAL
// WHERE org_id = ...)`, an org-filtered-in-place subquery, rather than the
// reference's own `INNER JOIN repos ON ... WHERE repos.org_id = ...` shape
// (person_drilldown_prs.sql), which lets FINAL merge every org's rows on
// the joined side before the WHERE prunes them.
package people

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// maxDrilldownLimit ports _MAX_DRILLDOWN_LIMIT (services/people.py:67).
const maxDrilldownLimit = 200

// boundedDrilldownLimit ports _bounded_limit_param(limit, 200)
// (main.py:211-214) and _bounded_limit(limit, _MAX_DRILLDOWN_LIMIT)
// (services/people.py:267-271) collapsed into one clamp -- both call sites
// share the SAME max (200), so the second application is a no-op given the
// first already ran, the identical reasoning boundedSearchLimit's own doc
// comment already establishes for search_people_response's own double
// clamp.
func boundedDrilldownLimit(limit int) int {
	if limit <= 0 {
		return 50
	}
	if limit > maxDrilldownLimit {
		return maxDrilldownLimit
	}
	return limit
}

// DrilldownPRsParams is the already-resolved request shape GET
// /api/v1/people/{person_id}/drilldown/prs's route file reduces to before
// calling BuildDrilldownPRsResponse -- same division of labor as
// SearchParams/SummaryParams/MetricParams (route file owns query-param
// parsing/validation, this package owns the business logic).
type DrilldownPRsParams struct {
	PersonID  string
	RangeDays int
	// Limit is the route's own boundedDrilldownLimit(limit) result
	// (main.py:1142's `_bounded_limit_param(limit, 200)`).
	Limit int
	// Cursor ports the optional `cursor: datetime | None` query param
	// (main.py:1133): nil means no cursor filter, matching
	// fetch_person_pull_requests' own `if cursor is not None` branch
	// (queries/people.py:247-248).
	Cursor *time.Time
	// Now stands in for utc_today() the same way SearchParams.Now/
	// SummaryParams.Now already do.
	Now time.Time
}

// PullRequestRow ports one PullRequestRow the response wraps (api/models/
// schemas.py:169-178), in the same field order.
//
// DATETIME WIRE FORM (created_at/merged_at/first_review_at): RFC 3339
// (Go's default time.Time JSON marshaling), not Python's naive isoformat --
// the same already-established, class-level direction this "pr"
// field family already carries on the GraphQL side and on
// internal/drilldown/prs.go's own PRItem for this exact field trio.
// Python's naive-isoformat wire form is the declared baseline defect; Go's
// RFC 3339 form is canonical.
type PullRequestRow struct {
	RepoID             string     `json:"repo_id"`
	Number             uint32     `json:"number"`
	Title              *string    `json:"title"`
	Author             *string    `json:"author"`
	CreatedAt          time.Time  `json:"created_at"`
	MergedAt           *time.Time `json:"merged_at"`
	FirstReviewAt      *time.Time `json:"first_review_at"`
	ReviewLatencyHours *float64   `json:"review_latency_hours"`
	// Link is always null -- build_person_drilldown_prs_response never
	// sets it (services/people.py:785's `link=None`, no computed value
	// anywhere in that function).
	Link *string `json:"link"`
}

// DrilldownPRsResponse ports PersonDrilldownResponse(items=..., next_cursor=...)
// for this route (api/models/schemas.py:459-461).
type DrilldownPRsResponse struct {
	Items      []PullRequestRow `json:"items"`
	NextCursor *time.Time       `json:"next_cursor"`
}

// fetchPersonPullRequestsQuery is the Go port of fetch_person_pull_requests'
// SELECT (queries/people.py:235-259, sql/people/person_drilldown_prs.sql).
const fetchPersonPullRequestsQuery = `
SELECT
    toString(pr.repo_id) AS repo_id,
    pr.number AS number,
    pr.title AS title,
    pr.author_name AS author_name,
    pr.author_email AS author_email,
    pr.created_at AS created_at,
    pr.merged_at AS merged_at,
    pr.first_review_at AS first_review_at,
    if(pr.first_review_at IS NULL, NULL,
       toFloat64(dateDiff('hour', pr.created_at, pr.first_review_at))) AS review_latency_hours
FROM git_pull_requests AS pr FINAL
WHERE pr.created_at >= {start_ts:DateTime64(3, 'UTC')}
  AND pr.created_at < {end_ts:DateTime64(3, 'UTC')}
  AND (pr.author_email IN {identities:Array(String)} OR pr.author_name IN {identities:Array(String)})
  AND pr.repo_id IN (
      SELECT id FROM repos FINAL WHERE org_id = {org_id:String}
  )
  %s
ORDER BY pr.created_at DESC
LIMIT {limit:UInt64}
%s
`

// fetchPersonPullRequests runs fetchPersonPullRequestsQuery and scans every
// row, in SELECT order.
func fetchPersonPullRequests(ctx context.Context, client QueryClient, identities []string, startDay, endDay time.Time, limit int, cursor *time.Time, orgID string) ([]PullRequestRow, error) {
	cursorFilter := ""
	bindings := []dhclickhouse.Binding{
		{Name: "start_ts", Value: startDay},
		{Name: "end_ts", Value: endDay},
		{Name: "identities", Value: identities},
		{Name: "org_id", Value: orgID},
		{Name: "limit", Value: limit},
	}
	if cursor != nil {
		cursorFilter = "AND pr.created_at < {cursor:DateTime64(3, 'UTC')}"
		bindings = append(bindings, dhclickhouse.Binding{Name: "cursor", Value: *cursor})
	}

	query := fmt.Sprintf(fetchPersonPullRequestsQuery, cursorFilter, settingsMaxExecutionTime())
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("people: fetch_person_pull_requests query: %w", err)
	}
	defer rows.Close()

	items := make([]PullRequestRow, 0)
	for rows.Next() {
		var (
			repoID             string
			number             uint32
			title              *string
			authorName         *string
			authorEmail        *string
			createdAt          time.Time
			mergedAt           *time.Time
			firstReviewAt      *time.Time
			reviewLatencyHours *float64
		)
		if err := rows.Scan(&repoID, &number, &title, &authorName, &authorEmail, &createdAt, &mergedAt, &firstReviewAt, &reviewLatencyHours); err != nil {
			return nil, fmt.Errorf("people: scan person pull request row: %w", err)
		}
		// author = row.get("author") or row.get("author_name")
		// (services/people.py:778): author_email (aliased "author" in
		// Python's own SQL) wins when non-empty, otherwise author_name --
		// whatever ITS value is, even nil or "".
		author := authorEmail
		if author == nil || *author == "" {
			author = authorName
		}
		items = append(items, PullRequestRow{
			RepoID:             repoID,
			Number:             number,
			Title:              title,
			Author:             author,
			CreatedAt:          createdAt,
			MergedAt:           mergedAt,
			FirstReviewAt:      firstReviewAt,
			ReviewLatencyHours: reviewLatencyHours,
			Link:               nil,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("people: iterate person pull request rows: %w", err)
	}
	return items, nil
}

// BuildDrilldownPRsResponse is the Go port of build_person_drilldown_prs_response
// (services/people.py:734-790): resolve the person identity, run
// fetch_person_pull_requests, wrap the rows. Returns *RequestError(404,
// "person not found") for an unresolved person_id (ValueError's own 404
// mapping is the CALLER's job in Python -- main.py:1146-1147 -- but this
// port raises the typed error here, matching BuildSummaryResponse/
// BuildMetricResponse's own division so every route file shares ONE
// AsRequestError translation, never a route-local ValueError check).
func BuildDrilldownPRsResponse(ctx context.Context, reader *Reader, orgID string, params DrilldownPRsParams) (*DrilldownPRsResponse, error) {
	if reader == nil {
		return nil, ErrUnavailable
	}

	startDay, endDay, _, _ := timeWindow(params.Now, params.RangeDays, params.RangeDays)
	limit := boundedDrilldownLimit(params.Limit)

	canonical, aliasList, err := resolveIdentityContext(ctx, reader.client, params.PersonID, orgID)
	if err != nil {
		return nil, err
	}
	if canonical == "" {
		return nil, notFound("Person not found")
	}
	identities := identityVariants(canonical, aliasList)

	rows, err := fetchPersonPullRequests(ctx, reader.client, identities, startDay, endDay, limit, params.Cursor, orgID)
	if err != nil {
		return nil, err
	}

	var nextCursor *time.Time
	if len(rows) > 0 {
		last := rows[len(rows)-1].CreatedAt
		nextCursor = &last
	}

	return &DrilldownPRsResponse{Items: rows, NextCursor: nextCursor}, nil
}
