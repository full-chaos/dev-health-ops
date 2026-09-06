package issuecommitedges

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
var ErrUnavailable = errors.New("issuecommitedges: clickhouse connection is required")

// ErrScopeRequired reports a caller trying to run unscoped.
//
// Python does not refuse this: an empty config.org_id skips every org filter
// in both queries (builder.py:1063, :1077) and reads across every tenant.
// This port refuses instead, matching the precedent CHAOS-4441 set for
// issueprlinks.Loader.Load and prcommit.Loader.Load -- a deliberate
// divergence, not an oversight, for the identical reason: an unscoped read
// here would derive edges from every org's commits and work items
// indiscriminately.
var ErrScopeRequired = errors.New("issuecommitedges: organization id is required")

// conn is the narrow ClickHouse capability this package needs. Mirrors
// prcommit's conn interface so tests can drive Loader without a container.
type conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// Window is the derivation's commit scope: the from/to/repo filters
// BuildConfig carries (builder.py:1053-1061). A nil bound means unbounded.
// Unlike work_items (unfiltered by date), From/To/RepoID apply ONLY to the
// commit read, matching Python exactly.
type Window struct {
	From   *time.Time
	To     *time.Time
	RepoID *uuid.UUID
}

// truncateBoundToSecond mirrors prcommit.truncateBoundToSecond /
// issueprlinks.truncateBoundToSecond exactly: Python's
// `_format_datetime_for_clickhouse` (builder.py:57-60) discards sub-second
// precision and rejects a non-UTC offset outright, for the identical reasons
// documented there.
func truncateBoundToSecond(bound time.Time) (time.Time, error) {
	if _, offset := bound.Zone(); offset != 0 {
		return time.Time{}, fmt.Errorf("issuecommitedges: window bounds must be UTC, got %s", bound.Format(time.RFC3339))
	}
	return bound.UTC().Truncate(time.Second), nil
}

// Loader reads the two inputs Derive needs, scoped to one org.
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

// Load reads both inputs for one org and window. See ErrScopeRequired for why
// an empty orgID is refused rather than replicating Python's unscoped read.
func (loader *Loader) Load(ctx context.Context, orgID string, window Window) (Inputs, error) {
	if loader == nil || loader.conn == nil {
		return Inputs{}, ErrUnavailable
	}
	if orgID == "" {
		return Inputs{}, ErrScopeRequired
	}

	commits, err := loader.loadCommits(ctx, orgID, window)
	if err != nil {
		return Inputs{}, fmt.Errorf("load git_commits: %w", err)
	}

	workItems, err := loader.loadWorkItems(ctx, orgID)
	if err != nil {
		return Inputs{}, fmt.Errorf("load work_items: %w", err)
	}

	return Inputs{Commits: commits, WorkItems: workItems}, nil
}

// loadCommits ports the commit-message read (builder.py:1038-1063). message
// is Nullable(String) in the live schema -- the query's own non-null,
// non-empty message filter means an empty or absent message never reaches
// this read at all; the Scan below still guards against nil defensively,
// same reasoning prcommit.loadCommits gives for the identical column.
func (loader *Loader) loadCommits(ctx context.Context, orgID string, window Window) ([]CommitRow, error) {
	query := `
SELECT repo_id, hash, message, author_when
FROM git_commits
WHERE org_id = {org_id:String} AND message IS NOT NULL AND message != ''`
	args := []any{clickhouse.Named("org_id", orgID)}
	if window.From != nil {
		bound, err := truncateBoundToSecond(*window.From)
		if err != nil {
			return nil, fmt.Errorf("window from: %w", err)
		}
		query += ` AND author_when >= {from_ts:DateTime64(3)}`
		// Bind the RENDERED literal, never the raw time.Time -- see
		// prcommit.loadCommits's identical comment on why a bound time.Time
		// cannot be bound directly against a DateTime64(3) column
		// (clickhouse-go renders it as toDateTime(...), which ClickHouse
		// refuses to parse against that placeholder type, code 457).
		args = append(args, clickhouse.Named("from_ts", remaining.DateTime64Argument(bound, remaining.DateTime64MillisecondPrecision)))
	}
	if window.To != nil {
		bound, err := truncateBoundToSecond(*window.To)
		if err != nil {
			return nil, fmt.Errorf("window to: %w", err)
		}
		query += ` AND author_when <= {to_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("to_ts", remaining.DateTime64Argument(bound, remaining.DateTime64MillisecondPrecision)))
	}
	if window.RepoID != nil {
		query += ` AND repo_id = {repo_id:UUID}`
		args = append(args, clickhouse.Named("repo_id", *window.RepoID))
	}

	rows, err := loader.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []CommitRow
	for rows.Next() {
		var row CommitRow
		var message *string
		if err := rows.Scan(&row.RepoID, &row.Hash, &message, &row.AuthorWhen); err != nil {
			return nil, err
		}
		if message != nil {
			row.Message = *message
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// loadWorkItems ports the provider-lookup read (builder.py:1065-1077). No
// date filter -- Python's own read carries none either.
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

// CountCommitFileEdges ports _count_commit_file_edges (builder.py:1569-1583,
// CHAOS-5306, folded into this PR per team-lead's ruling since it is a
// trivial readback with no compute to golden-test).
//
// UNLIKE Python's try/except-warn-return-0, this propagates the error. That
// swallow was defensive against a table that might not exist in an older
// deployment (the same generic worry issueprlinks/prcommit's own
// ErrScopeRequired commentary calls out as NOT applying to this fleet's
// current, single-schema-version deployment model) -- and this fleet's
// standing rule for a touched failure path is to surface it, not
// re-introduce a silent swallow while porting. A caller wanting Python's old
// best-effort telemetry behavior can catch and log this error itself; this
// package does not make that choice for it.
func CountCommitFileEdges(ctx context.Context, connection conn, orgID string) (int, error) {
	if connection == nil {
		return 0, ErrUnavailable
	}
	query := "SELECT count(*) AS total FROM git_commit_stats"
	var args []any
	if orgID != "" {
		query += " WHERE org_id = {org_id:String}"
		args = append(args, clickhouse.Named("org_id", orgID))
	}
	rows, err := connection.Query(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	var count uint64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
	}
	return int(count), rows.Err()
}
