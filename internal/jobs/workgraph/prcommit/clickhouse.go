package prcommit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

// ErrUnavailable reports a missing ClickHouse dependency.
var ErrUnavailable = errors.New("prcommit: clickhouse connection is required")

// ErrScopeRequired reports a caller trying to run unscoped.
//
// Python does not refuse this: an empty config.org_id skips BOTH org filters
// (builder.py:1782-1783, :1822-1823) and reads/writes across every tenant. This
// port refuses instead, matching the precedent CHAOS-4441 already set for the
// sibling issue<->PR mapping (issueprlinks.Loader.Load) and the issue-issue edge
// producer (edges.ReadDependencies's requireEdgeScope) -- a deliberate
// divergence, not an oversight, ruled the same way for the same reason: an
// unscoped read here would derive links from every org's PRs and commits
// indiscriminately, and an unscoped write would stamp them all under one empty
// org_id, untargetable by any later scoped delete.
var ErrScopeRequired = errors.New("prcommit: organization id is required")

// conn is the narrow ClickHouse capability this package needs. Mirrors
// issueprlinks' conn interface so tests can drive Loader/Writer without a
// container.
type conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// Window is the derivation's commit scope: the from/to/repo filters
// BuildConfig carries. A nil bound means unbounded.
//
// Unlike issueprlinks.Window, From/To apply ONLY to the commit read
// (builder.py:1811-1825) -- the known-PR read carries no date filter at all
// (builder.py:1772-1786), matching Python exactly: a commit outside the window
// is never scanned, but a PR outside it is still a valid corroboration target
// if referenced by an in-window commit.
type Window struct {
	From   *time.Time
	To     *time.Time
	RepoID *uuid.UUID
}

// truncateBoundToSecond mirrors issueprlinks.truncateBoundToSecond exactly:
// Python's `_format_datetime_for_clickhouse` (builder.py:57-60) discards
// sub-second precision and rejects a non-UTC offset outright, for the identical
// reasons documented there.
func truncateBoundToSecond(bound time.Time) (time.Time, error) {
	if _, offset := bound.Zone(); offset != 0 {
		return time.Time{}, fmt.Errorf("prcommit: window bounds must be UTC, got %s", bound.Format(time.RFC3339))
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
	inputs := Inputs{OrgID: orgID}

	pullRequests, err := loader.loadPullRequests(ctx, orgID, window.RepoID)
	if err != nil {
		return Inputs{}, fmt.Errorf("load git_pull_requests: %w", err)
	}
	inputs.PullRequests = pullRequests

	commits, err := loader.loadCommits(ctx, orgID, window)
	if err != nil {
		return Inputs{}, fmt.Errorf("load git_commits: %w", err)
	}
	inputs.Commits = commits
	return inputs, nil
}

// loadPullRequests ports the known-PR read (builder.py:1772-1786). No date
// filter -- see Window's doc comment.
func (loader *Loader) loadPullRequests(ctx context.Context, orgID string, repoID *uuid.UUID) ([]PullRequestRow, error) {
	query := `SELECT org_id, repo_id, number FROM git_pull_requests WHERE org_id = {org_id:String}`
	args := []any{clickhouse.Named("org_id", orgID)}
	if repoID != nil {
		query += ` AND repo_id = {repo_id:UUID}`
		args = append(args, clickhouse.Named("repo_id", *repoID))
	}

	rows, err := loader.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []PullRequestRow
	for rows.Next() {
		var row PullRequestRow
		var number uint32
		if err := rows.Scan(&row.OrgID, &row.RepoID, &number); err != nil {
			return nil, err
		}
		row.Number = int(number)
		out = append(out, row)
	}
	return out, rows.Err()
}

// loadCommits ports the commit-message read (builder.py:1801-1827). message
// is Nullable(String) in the live schema -- Python coerces None to "" (`message
// = commit_row.get("message") or ""`, builder.py:1855), which this mirrors by
// scanning into *string and treating nil as "".
func (loader *Loader) loadCommits(ctx context.Context, orgID string, window Window) ([]CommitRow, error) {
	query := `
SELECT org_id, repo_id, hash, message
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
		// issueprlinks.loadPullRequests's identical comment on why a bound
		// time.Time cannot be bound directly against a DateTime64(3) column
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
		if err := rows.Scan(&row.OrgID, &row.RepoID, &row.Hash, &message); err != nil {
			return nil, err
		}
		if message != nil {
			row.Message = *message
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// insertStatement names every column explicitly, in the live table's own
// order -- same reasoning as issueprlinks' insertStatement: a positional
// insert against a ReplacingMergeTree is how a future ALTER ... ADD COLUMN
// silently shifts values into the wrong columns.
const insertStatement = `INSERT INTO work_graph_pr_commit
 (repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id)`

// Writer inserts fast-path PR<->commit rows.
type Writer struct {
	conn conn
}

// NewWriter builds a Writer over an established ClickHouse connection.
func NewWriter(connection conn) (*Writer, error) {
	if connection == nil {
		return nil, ErrUnavailable
	}
	return &Writer{conn: connection}, nil
}

// Write inserts the derived links. An empty slice writes nothing and is not an
// error, matching Python's `if not links: return` (builder.py:237-240).
func (writer *Writer) Write(ctx context.Context, orgID string, links []Link) error {
	if writer == nil || writer.conn == nil {
		return ErrUnavailable
	}
	if orgID == "" {
		return ErrScopeRequired
	}
	if len(links) == 0 {
		return nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, insertStatement)
	if err != nil {
		return fmt.Errorf("prepare work_graph_pr_commit batch: %w", err)
	}
	for _, link := range links {
		if err := batch.Append(
			link.RepoID,
			uint32(link.PRNumber),
			link.CommitHash,
			link.Confidence,
			link.Provenance,
			link.Evidence,
			link.LastSynced,
			orgID,
		); err != nil {
			return fmt.Errorf("append work_graph_pr_commit row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send work_graph_pr_commit batch: %w", err)
	}
	return nil
}

// fastPathQuery ports the join _build_pr_commit_edges_from_fast_path issues
// (builder.py:1906-1922). toString(...) on both join sides is Python's own
// choice (:1918/:1920), kept verbatim rather than typed UUID/UUID equality,
// since ClickHouse UUID comparison and string comparison of its canonical form
// are not guaranteed identical in every edge case (e.g. case folding) and this
// is a parity port, not an improvement.
const fastPathQueryBase = `
SELECT
    p.repo_id,
    p.pr_number,
    p.commit_hash,
    p.confidence,
    p.provenance,
    p.evidence,
    c.author_when
FROM work_graph_pr_commit AS p FINAL
INNER JOIN git_commits AS c FINAL ON (
    toString(p.repo_id) = toString(c.repo_id)
    AND p.commit_hash = c.hash
    AND toString(p.org_id) = toString(c.org_id)
)
WHERE p.org_id = {org_id:String}`

// FastPathRow is one joined row consumed by BuildFastPathEdges.
type FastPathRow struct {
	RepoID     uuid.UUID
	PRNumber   int
	CommitHash string
	Confidence float32
	Provenance string
	Evidence   string
	AuthorWhen time.Time
}

// LoadFastPath reads the join for the fast-path edge builder. See
// ErrScopeRequired for why orgID is mandatory here (Python does not require
// it: an empty config.org_id skips the `p.org_id = ...` filter it ALSO carries
// at builder.py:1926-1927, on top of the join's own unconditional
// toString(...)=toString(...) equality -- so an unscoped Python run still
// narrows to rows whose commit and PR-commit sides share an org, just not to
// any PARTICULAR one).
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
		args = append(args, clickhouse.Named("repo_id", *window.RepoID))
	}
	if window.From != nil {
		bound, err := truncateBoundToSecond(*window.From)
		if err != nil {
			return nil, fmt.Errorf("window from: %w", err)
		}
		query += ` AND c.author_when >= {from_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("from_ts", remaining.DateTime64Argument(bound, remaining.DateTime64MillisecondPrecision)))
	}
	if window.To != nil {
		bound, err := truncateBoundToSecond(*window.To)
		if err != nil {
			return nil, fmt.Errorf("window to: %w", err)
		}
		query += ` AND c.author_when <= {to_ts:DateTime64(3)}`
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
			&row.RepoID, &prNumber, &row.CommitHash,
			&confidence, &row.Provenance, &row.Evidence, &row.AuthorWhen,
		); err != nil {
			return nil, err
		}
		row.PRNumber = int(prNumber)
		row.Confidence = confidence
		out = append(out, row)
	}
	return out, rows.Err()
}

// WriteFastPathEdges inserts the built CONTAINS edges via the shared edges
// package writer -- the fast-path edge builder needs no writer of its own.
func WriteFastPathEdges(ctx context.Context, connection driver.Conn, orgID string, rows []edges.Row) (int, error) {
	return edges.WriteEdges(ctx, connection, orgID, rows)
}

// parseProvenance mirrors WorkGraphBuilder._parse_provenance (builder.py:242-253)
// exactly, including its unconditional NATIVE fallback: ANY value that is not
// one of the three exact (trimmed, lower-cased) literals -- empty or not --
// becomes NATIVE, not an error and not "unknown".
func parseProvenance(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case edges.ProvenanceNative:
		return edges.ProvenanceNative
	case edges.ProvenanceExplicitText:
		return edges.ProvenanceExplicitText
	case edges.ProvenanceHeuristic:
		return edges.ProvenanceHeuristic
	default:
		return edges.ProvenanceNative
	}
}
