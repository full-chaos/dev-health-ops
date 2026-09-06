package issuepredges

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

// ErrUnavailable reports a missing ClickHouse dependency.
var ErrUnavailable = errors.New("issuepredges: clickhouse connection is required")

// ErrScopeRequired reports a caller trying to run unscoped.
//
// Python does not refuse this: an empty config.org_id skips every org filter
// both sub-builders carry (builder.py:753-754, :1495-1496) and reads/writes
// across every tenant. This port refuses instead, matching the precedent
// CHAOS-5264/CHAOS-4924 already set for every sibling producer in this family
// (prcommit.ErrScopeRequired, operationaledges.RequireOrganizationScope,
// edges.requireEdgeScope) -- a deliberate divergence, not an oversight.
var ErrScopeRequired = errors.New("issuepredges: organization id is required")

// conn is the narrow ClickHouse capability the Loader needs. Mirrors
// prcommit/issueprlinks' identical interface so tests can drive it without a
// container.
type conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// Window is the derivation's scope: the from/to/repo filters BuildConfig
// carries. A nil bound means unbounded.
//
// From/To apply to BOTH reads this package performs: the fast-path join
// filters on the joined PR's created_at (builder.py:1502-1508), and the
// text-parse PR read filters on its own created_at (builder.py:741-747) --
// unlike prcommit.Window, where the bounds apply only to the commit read, not
// the "known PR" read.
type Window struct {
	From   *time.Time
	To     *time.Time
	RepoID *uuid.UUID
	// HeuristicDaysWindow and HeuristicConfidence are only consumed by
	// ProduceHeuristicEdges (BuildConfig's own heuristic_days_window/
	// heuristic_confidence, builder.py:118-123) -- carried on the same
	// Window as From/To/RepoID rather than a second type, since all three
	// Produce* methods parse the identical scope shape via one
	// issuePREdgesWindowFor call in the caller.
	HeuristicDaysWindow int
	HeuristicConfidence float32
}

// truncateBoundToSecond mirrors prcommit's and issueprlinks' identical helper:
// Python's `_format_datetime_for_clickhouse` (builder.py:57-60) discards
// sub-second precision and rejects a non-UTC offset outright.
func truncateBoundToSecond(bound time.Time) (time.Time, error) {
	if _, offset := bound.Zone(); offset != 0 {
		return time.Time{}, fmt.Errorf("issuepredges: window bounds must be UTC, got %s", bound.Format(time.RFC3339))
	}
	return bound.UTC().Truncate(time.Second), nil
}

// Loader reads the inputs both Produce methods need, scoped to one org.
type Loader struct {
	conn conn
}

// NewLoader builds a Loader over an established ClickHouse connection.
func NewLoader(connection conn) (*Loader, error) {
	if connection == nil {
		return nil, ErrUnavailable
	}
	return &Loader{conn: connection}, nil
}

// fastPathQuery ports the join _build_issue_pr_edges_from_fast_path issues
// (builder.py:1488-1499). toString(...) on all three join predicates is
// Python's own choice, kept verbatim rather than typed UUID equality -- same
// reasoning as prcommit's identical join.
const fastPathQueryBase = `
SELECT
    p.repo_id,
    p.work_item_id,
    p.pr_number,
    p.confidence,
    p.provenance,
    p.evidence,
    pr.created_at
FROM work_graph_issue_pr AS p FINAL
INNER JOIN git_pull_requests AS pr FINAL ON (
    toString(p.repo_id) = toString(pr.repo_id)
    AND p.pr_number = pr.number
    AND toString(p.org_id) = toString(pr.org_id)
)
WHERE p.org_id = {org_id:String}`

// LoadFastPath reads the fast-path join for DeriveFastPathEdges. See
// ErrScopeRequired for why orgID is mandatory here even though Python's own
// equivalent read does not require it.
func (loader *Loader) LoadFastPath(ctx context.Context, orgID string, window Window) ([]FastPathRow, error) {
	if loader == nil || loader.conn == nil {
		return nil, ErrUnavailable
	}
	if orgID == "" {
		return nil, ErrScopeRequired
	}
	query := fastPathQueryBase
	args := []any{clickhouse.Named("org_id", orgID)}
	if window.RepoID != nil {
		query += ` AND p.repo_id = {repo_id:UUID}`
		args = append(args, clickhouse.Named("repo_id", window.RepoID.String()))
	}
	if window.From != nil {
		bound, err := truncateBoundToSecond(*window.From)
		if err != nil {
			return nil, fmt.Errorf("window from: %w", err)
		}
		query += ` AND pr.created_at >= {from_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("from_ts", remaining.DateTime64Argument(bound, remaining.DateTime64MillisecondPrecision)))
	}
	if window.To != nil {
		bound, err := truncateBoundToSecond(*window.To)
		if err != nil {
			return nil, fmt.Errorf("window to: %w", err)
		}
		query += ` AND pr.created_at <= {to_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("to_ts", remaining.DateTime64Argument(bound, remaining.DateTime64MillisecondPrecision)))
	}

	rows, err := loader.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []FastPathRow
	for rows.Next() {
		var (
			row        FastPathRow
			prNumber   uint32
			confidence float32
		)
		if err := rows.Scan(
			&row.RepoID, &row.WorkItemID, &prNumber, &confidence,
			&row.Provenance, &row.Evidence, &row.PRCreatedAt,
		); err != nil {
			return nil, err
		}
		row.PRNumber = int(prNumber)
		row.Confidence = confidence
		out = append(out, row)
	}
	return out, rows.Err()
}

// LoadTextParseInputs reads the PR and work-item rows DeriveTextParseEdges
// needs (builder.py:730-772).
func (loader *Loader) LoadTextParseInputs(ctx context.Context, orgID string, window Window) ([]PullRequestRow, []WorkItemRow, error) {
	if loader == nil || loader.conn == nil {
		return nil, nil, ErrUnavailable
	}
	if orgID == "" {
		return nil, nil, ErrScopeRequired
	}
	prs, err := loader.loadPullRequests(ctx, orgID, window)
	if err != nil {
		return nil, nil, fmt.Errorf("load git_pull_requests: %w", err)
	}
	workItems, err := loader.loadWorkItems(ctx, orgID)
	if err != nil {
		return nil, nil, fmt.Errorf("load work_items: %w", err)
	}
	return prs, workItems, nil
}

func (loader *Loader) loadPullRequests(ctx context.Context, orgID string, window Window) ([]PullRequestRow, error) {
	query := `
SELECT repo_id, number, title, body, head_branch, created_at
FROM git_pull_requests
WHERE org_id = {org_id:String}`
	args := []any{clickhouse.Named("org_id", orgID)}
	if window.From != nil {
		bound, err := truncateBoundToSecond(*window.From)
		if err != nil {
			return nil, fmt.Errorf("window from: %w", err)
		}
		query += ` AND created_at >= {from_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("from_ts", remaining.DateTime64Argument(bound, remaining.DateTime64MillisecondPrecision)))
	}
	if window.To != nil {
		bound, err := truncateBoundToSecond(*window.To)
		if err != nil {
			return nil, fmt.Errorf("window to: %w", err)
		}
		query += ` AND created_at <= {to_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("to_ts", remaining.DateTime64Argument(bound, remaining.DateTime64MillisecondPrecision)))
	}
	if window.RepoID != nil {
		query += ` AND repo_id = {repo_id:UUID}`
		args = append(args, clickhouse.Named("repo_id", window.RepoID.String()))
	}

	rows, err := loader.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []PullRequestRow
	for rows.Next() {
		var (
			row        PullRequestRow
			number     uint32
			title      *string
			body       *string
			headBranch *string
		)
		if err := rows.Scan(&row.RepoID, &number, &title, &body, &headBranch, &row.CreatedAt); err != nil {
			return nil, err
		}
		row.Number = int(number)
		if title != nil {
			row.Title = *title
		}
		if body != nil {
			row.Body = *body
		}
		if headBranch != nil {
			row.HeadBranch = *headBranch
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (loader *Loader) loadWorkItems(ctx context.Context, orgID string) ([]WorkItemRow, error) {
	query := `SELECT repo_id, work_item_id, provider FROM work_items FINAL WHERE org_id = {org_id:String}`
	rows, err := loader.conn.Query(ctx, query, clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []WorkItemRow
	for rows.Next() {
		var row WorkItemRow
		if err := rows.Scan(&row.RepoID, &row.WorkItemID, &row.Provider); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// LoadHeuristicInputs reads the three inputs DeriveHeuristicEdges needs
// (builder.py:1022-1074, plus a fresh explicit-links read -- see
// ExplicitLink's doc comment for why this package reads that set from
// ClickHouse rather than carrying it in memory from an earlier step).
func (loader *Loader) LoadHeuristicInputs(ctx context.Context, orgID string, window Window) ([]HeuristicWorkItemRow, []HeuristicPullRequestRow, []ExplicitLink, error) {
	if loader == nil || loader.conn == nil {
		return nil, nil, nil, ErrUnavailable
	}
	if orgID == "" {
		return nil, nil, nil, ErrScopeRequired
	}
	workItems, err := loader.loadHeuristicWorkItems(ctx, orgID, window)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load work_items: %w", err)
	}
	pullRequests, err := loader.loadHeuristicPullRequests(ctx, orgID, window)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load git_pull_requests: %w", err)
	}
	explicitLinks, err := loader.loadExplicitLinks(ctx, orgID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load work_graph_issue_pr: %w", err)
	}
	return workItems, pullRequests, explicitLinks, nil
}

func (loader *Loader) loadHeuristicWorkItems(ctx context.Context, orgID string, window Window) ([]HeuristicWorkItemRow, error) {
	query := `
SELECT repo_id, work_item_id, updated_at
FROM work_items FINAL
WHERE repo_id IS NOT NULL AND org_id = {org_id:String}`
	args := []any{clickhouse.Named("org_id", orgID)}
	if window.From != nil {
		bound, err := truncateBoundToSecond(*window.From)
		if err != nil {
			return nil, fmt.Errorf("window from: %w", err)
		}
		query += ` AND updated_at >= {from_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("from_ts", remaining.DateTime64Argument(bound, remaining.DateTime64MillisecondPrecision)))
	}
	if window.To != nil {
		bound, err := truncateBoundToSecond(*window.To)
		if err != nil {
			return nil, fmt.Errorf("window to: %w", err)
		}
		query += ` AND updated_at <= {to_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("to_ts", remaining.DateTime64Argument(bound, remaining.DateTime64MillisecondPrecision)))
	}
	if window.RepoID != nil {
		query += ` AND repo_id = {repo_id:UUID}`
		args = append(args, clickhouse.Named("repo_id", window.RepoID.String()))
	}

	rows, err := loader.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []HeuristicWorkItemRow
	for rows.Next() {
		var (
			row       HeuristicWorkItemRow
			updatedAt *time.Time
		)
		if err := rows.Scan(&row.RepoID, &row.WorkItemID, &updatedAt); err != nil {
			return nil, err
		}
		if updatedAt != nil {
			row.UpdatedAt = *updatedAt
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (loader *Loader) loadHeuristicPullRequests(ctx context.Context, orgID string, window Window) ([]HeuristicPullRequestRow, error) {
	query := `
SELECT repo_id, number, created_at
FROM git_pull_requests
WHERE org_id = {org_id:String}`
	args := []any{clickhouse.Named("org_id", orgID)}
	if window.From != nil {
		bound, err := truncateBoundToSecond(*window.From)
		if err != nil {
			return nil, fmt.Errorf("window from: %w", err)
		}
		query += ` AND created_at >= {from_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("from_ts", remaining.DateTime64Argument(bound, remaining.DateTime64MillisecondPrecision)))
	}
	if window.To != nil {
		bound, err := truncateBoundToSecond(*window.To)
		if err != nil {
			return nil, fmt.Errorf("window to: %w", err)
		}
		query += ` AND created_at <= {to_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("to_ts", remaining.DateTime64Argument(bound, remaining.DateTime64MillisecondPrecision)))
	}
	if window.RepoID != nil {
		query += ` AND repo_id = {repo_id:UUID}`
		args = append(args, clickhouse.Named("repo_id", window.RepoID.String()))
	}

	rows, err := loader.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []HeuristicPullRequestRow
	for rows.Next() {
		var (
			row       HeuristicPullRequestRow
			number    uint32
			createdAt *time.Time
		)
		if err := rows.Scan(&row.RepoID, &number, &createdAt); err != nil {
			return nil, err
		}
		row.Number = int(number)
		if createdAt != nil {
			row.CreatedAt = *createdAt
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// loadExplicitLinks reads the SAME (work_item_id, pr_number) pairs Python
// used to hold in memory as `explicit_links` -- see ExplicitLink's doc
// comment. No date window: work_graph_issue_pr carries no window semantics
// of its own, and Python's original in-memory set was never date-filtered
// either (it was every link either upstream sub-builder produced in the
// build, not a windowed subset of them).
func (loader *Loader) loadExplicitLinks(ctx context.Context, orgID string) ([]ExplicitLink, error) {
	query := `SELECT work_item_id, pr_number FROM work_graph_issue_pr FINAL WHERE org_id = {org_id:String}`
	rows, err := loader.conn.Query(ctx, query, clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []ExplicitLink
	for rows.Next() {
		var (
			workItemID string
			prNumber   uint32
		)
		if err := rows.Scan(&workItemID, &prNumber); err != nil {
			return nil, err
		}
		out = append(out, ExplicitLink{WorkItemID: workItemID, PRNumber: int(prNumber)})
	}
	return out, rows.Err()
}
