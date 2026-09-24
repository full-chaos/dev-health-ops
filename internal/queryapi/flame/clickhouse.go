package flame

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// pullRequestRow is one row fetch_pull_request (api/queries/flame.py:9-41)
// returns, in the same column order as its SELECT list. created_at is
// DateTime64(3, 'UTC') NOT NULL (000_raw_tables.sql) -- always present for
// a row that was actually found, so it is a plain time.Time, not a
// pointer, matching what the schema guarantees rather than reproducing
// Python's defensive `if start is None` for a column that can never be
// NULL.
type pullRequestRow struct {
	Title         *string
	State         *string
	CreatedAt     time.Time
	FirstReviewAt *time.Time
	MergedAt      *time.Time
	ClosedAt      *time.Time
}

// reviewRow is one row fetch_pull_request_reviews (api/queries/flame.py:
// 44-71) returns.
type reviewRow struct {
	State       string
	SubmittedAt time.Time
}

// issueRow is one row fetch_issue (api/queries/flame.py:74-102) returns --
// restricted to the columns _build_issue_flame_response actually reads
// (see flame.go's package doc comment on the dropped team-attribution
// join).
type issueRow struct {
	WorkItemID  string
	Provider    *string
	Type        *string
	Status      *string
	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
}

// deploymentRow is one row fetch_deployment (api/queries/flame.py:105-143)
// returns.
type deploymentRow struct {
	Status      *string
	Environment *string
	StartedAt   *time.Time
	FinishedAt  *time.Time
	DeployedAt  *time.Time
	MergedAt    *time.Time
}

// canonicalRepoID re-derives the canonical (lowercase, hyphenated) UUID
// string form parseRepoEntity already validated repoID as -- the same
// asUUIDString/toString(...) convention internal/analytics/repofilters.go
// and internal/drilldown's own readers use for a UUID column, rather than
// comparing the caller-supplied string form directly against
// `toString(repo_id)`, whose canonicalisation is ClickHouse's, not the
// caller's.
func canonicalRepoID(repoID string) (string, error) {
	parsed, err := pythonparity.ParseUUID(repoID)
	if err != nil {
		return "", fmt.Errorf("flame: repoID %q is not a valid UUID after parseRepoEntity already accepted it: %w", repoID, err)
	}
	return parsed.String(), nil
}

// fetchPullRequestQuery is fetch_pull_request's SQL (api/queries/flame.py:
// 20-35), FINAL-dedup added -- see flame.go's package doc comment on the
// DATA-LAYER NOTE for why.
const fetchPullRequestQuery = `
        SELECT
            title,
            state,
            created_at,
            first_review_at,
            merged_at,
            closed_at
        FROM git_pull_requests FINAL
        WHERE org_id = {org_id:String}
          AND toString(repo_id) = {repo_id:String}
          AND number = {number:UInt32}
        LIMIT 1
    `

// fetchPullRequest ports fetch_pull_request (api/queries/flame.py:9-41).
// nil, nil means "not found" (Python's `rows[0] if rows else None`).
func fetchPullRequest(ctx context.Context, client QueryClient, repoID string, number int, orgID string) (*pullRequestRow, error) {
	canonicalID, err := canonicalRepoID(repoID)
	if err != nil {
		return nil, err
	}
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_id", Value: canonicalID},
		{Name: "number", Value: number},
	}
	rows, err := client.Query(ctx, fetchPullRequestQuery, bindings)
	if err != nil {
		return nil, fmt.Errorf("flame: fetch_pull_request query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, rows.Err()
	}
	var row pullRequestRow
	if err := rows.Scan(&row.Title, &row.State, &row.CreatedAt, &row.FirstReviewAt, &row.MergedAt, &row.ClosedAt); err != nil {
		return nil, fmt.Errorf("flame: fetch_pull_request scan: %w", err)
	}
	return &row, rows.Err()
}

// fetchPullRequestReviewsQuery is fetch_pull_request_reviews's SQL
// (api/queries/flame.py:54-66), FINAL-dedup added -- see flame.go's
// package doc comment.
const fetchPullRequestReviewsQuery = `
        SELECT
            state,
            submitted_at
        FROM git_pull_request_reviews FINAL
        WHERE org_id = {org_id:String}
          AND toString(repo_id) = {repo_id:String}
          AND number = {number:UInt32}
          AND submitted_at IS NOT NULL
        ORDER BY submitted_at
    `

// fetchPullRequestReviews ports fetch_pull_request_reviews (api/queries/
// flame.py:44-71). review_id/reviewer are read by Python but never used by
// _rework_windows/_submitted_reviews (services/flame.py:89-129), so this
// port's reviewRow omits them -- the same "drop a column nothing
// downstream reads" simplification flame.go's package doc comment already
// makes for fetch_issue's team-attribution join.
func fetchPullRequestReviews(ctx context.Context, client QueryClient, repoID string, number int, orgID string) ([]reviewRow, error) {
	canonicalID, err := canonicalRepoID(repoID)
	if err != nil {
		return nil, err
	}
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_id", Value: canonicalID},
		{Name: "number", Value: number},
	}
	rows, err := client.Query(ctx, fetchPullRequestReviewsQuery, bindings)
	if err != nil {
		return nil, fmt.Errorf("flame: fetch_pull_request_reviews query: %w", err)
	}
	defer rows.Close()

	var out []reviewRow
	for rows.Next() {
		var row reviewRow
		if err := rows.Scan(&row.State, &row.SubmittedAt); err != nil {
			return nil, fmt.Errorf("flame: fetch_pull_request_reviews scan: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// fetchIssueQuery is fetch_issue's SQL (api/queries/flame.py:80-98),
// restricted to the columns actually used -- see flame.go's package doc
// comment on the dropped team-attribution join. work_item_cycle_times is
// already read FINAL by Python (no divergence to declare here); ORDER BY
// day DESC LIMIT 1 matches Python's own selection of the most recent daily
// snapshot for this work item.
const fetchIssueQuery = `
        SELECT
            provider,
            type,
            status,
            created_at,
            started_at,
            completed_at
        FROM work_item_cycle_times FINAL
        WHERE work_item_id = {work_item_id:String}
          AND org_id = {org_id:String}
        ORDER BY day DESC
        LIMIT 1
    `

// fetchIssue ports fetch_issue (api/queries/flame.py:74-102).
func fetchIssue(ctx context.Context, client QueryClient, workItemID, orgID string) (*issueRow, error) {
	bindings := []dhclickhouse.Binding{
		{Name: "work_item_id", Value: workItemID},
		{Name: "org_id", Value: orgID},
	}
	rows, err := client.Query(ctx, fetchIssueQuery, bindings)
	if err != nil {
		return nil, fmt.Errorf("flame: fetch_issue query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, rows.Err()
	}
	var row issueRow
	if err := rows.Scan(&row.Provider, &row.Type, &row.Status, &row.CreatedAt, &row.StartedAt, &row.CompletedAt); err != nil {
		return nil, fmt.Errorf("flame: fetch_issue scan: %w", err)
	}
	row.WorkItemID = workItemID
	return &row, rows.Err()
}

// fetchDeploymentQuery is fetch_deployment's SQL (api/queries/flame.py:
// 120-135). Python reads this table with `ORDER BY last_synced DESC
// LIMIT 1` and no FINAL; this port uses FINAL instead -- see flame.go's
// package doc comment for why the two are equivalent here (the WHERE
// clause already narrows to exactly one ReplacingMergeTree sort-key
// group) and this is a style choice, not a declared divergence.
const fetchDeploymentQuery = `
        SELECT
            status,
            environment,
            started_at,
            finished_at,
            deployed_at,
            merged_at
        FROM deployments FINAL
        WHERE org_id = {org_id:String}
          AND toString(repo_id) = {repo_id:String}
          AND deployment_id = {deployment_id:String}
        LIMIT 1
    `

// fetchDeployment ports fetch_deployment (api/queries/flame.py:105-143).
func fetchDeployment(ctx context.Context, client QueryClient, repoID, deploymentID, orgID string) (*deploymentRow, error) {
	canonicalID, err := canonicalRepoID(repoID)
	if err != nil {
		return nil, err
	}
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_id", Value: canonicalID},
		{Name: "deployment_id", Value: deploymentID},
	}
	rows, err := client.Query(ctx, fetchDeploymentQuery, bindings)
	if err != nil {
		return nil, fmt.Errorf("flame: fetch_deployment query: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, rows.Err()
	}
	var row deploymentRow
	if err := rows.Scan(&row.Status, &row.Environment, &row.StartedAt, &row.FinishedAt, &row.DeployedAt, &row.MergedAt); err != nil {
		return nil, fmt.Errorf("flame: fetch_deployment scan: %w", err)
	}
	return &row, rows.Err()
}
