package issueprlinks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// ErrUnavailable reports a missing ClickHouse dependency.
var ErrUnavailable = errors.New("issueprlinks: clickhouse connection is required")

// conn is the narrow ClickHouse capability this package needs -- query plus
// batch insert. driver.Conn satisfies it directly; the interface exists so
// tests can drive Loader/Writer without a container.
type conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// Window is the build's PR scope: the from/to/repo filters BuildConfig carries
// (work_graph/builder.py:124-133). A nil bound means unbounded, matching
// Python's `if self.config.from_date:` guards (:679-684).
//
// Both bounds are INCLUSIVE. That is Python's shape verbatim
// (`created_at >= from` AND `created_at <= to`, :680/:682) and not a typo for a
// half-open interval: a PR created exactly at `to` is inside the window on both
// planes.
type Window struct {
	From   *time.Time
	To     *time.Time
	RepoID *uuid.UUID
}

// ErrNonUTCWindowBound reports a window bound carrying a non-UTC offset.
var ErrNonUTCWindowBound = errors.New("issueprlinks: window bounds must be UTC")

// truncateBoundToSecond drops sub-second precision from a window bound, because
// Python does and this is a parity port.
//
// Python builds the clause by STRING FORMATTING the bound through
// `_format_datetime_for_clickhouse` (builder.py:57-60) --
// `strftime("%Y-%m-%d %H:%M:%S")` -- which silently discards everything below
// a second. The bounds reaching it routinely carry microseconds:
// `run_work_graph_build` defaults them to `datetime.now(timezone.utc)`
// (runner.py:61-69).
//
// `git_pull_requests.created_at` is `DateTime64(3, 'UTC')` in the live schema,
// so this is not cosmetic. Binding the untruncated instant moves the boundary
// by up to a second in BOTH directions: with `to = 00:00:00.750Z`, a PR created
// at `00:00:00.500Z` is outside Python's window (`<= '...00:00:00'`) and inside
// an untruncated Go one, so Go would write a native mapping row Python does not;
// at the `from` bound the asymmetry drops rows instead (codex round 1, F1).
//
// # Why a non-UTC bound is REFUSED rather than converted
//
// strftime renders the bound's WALL-CLOCK FIELDS and discards the offset
// entirely, so Python turns `2026-01-01T00:00:00.750+05:00` into the literal
// `2026-01-01 00:00:00`, which ClickHouse reads as UTC -- a five-hour shift
// from the instant the caller named. Converting to UTC first (the
// instant-preserving reading) gives `2025-12-31T19:00:00Z`. Both are
// defensible; they are not the same query, and a PR created at
// `2025-12-31T20:00Z` falls on opposite sides of them (codex round 3).
//
// Neither semantic is chosen here. Python's is arguably a latent bug -- it
// reinterprets an offset-bearing instant as UTC -- and copying it would carry
// that bug into the plane meant to outlive Python, while silently diverging
// from it would be a parity hole no oracle covers (the golden freezes no window
// at all). The live Python path cannot produce a non-UTC bound in the first
// place: `run_work_graph_build` tags every branch with `timezone.utc`
// (runner.py:61-69). So this refuses the input instead, loudly, rather than
// guessing on a path no caller exercises -- a measurement that cannot honour
// its contract must not report.
//
// A zero UTC offset is accepted whatever the Location is named, since a fixed
// +00:00 zone and time.UTC denote the same wall clock.
func truncateBoundToSecond(bound time.Time) (time.Time, error) {
	if _, offset := bound.Zone(); offset != 0 {
		return time.Time{}, fmt.Errorf("%w: got %s", ErrNonUTCWindowBound, bound.Format(time.RFC3339))
	}
	return bound.UTC().Truncate(time.Second), nil
}

// Loader reads the four inputs Derive needs, scoped to one org.
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

// dependencyQuery ports builder.py:648-663.
//
// The ORDER BY is the ONE deliberate addition. Python iterates the rows in
// whatever order ClickHouse returned and takes first-wins on a duplicate
// identity (:764-767); an unordered read makes which duplicate wins a property
// of the storage layout rather than of the data. Ordering on the full identity
// key -- plus last_synced and the raw kind, so the tuple is total -- makes the
// choice reproducible on both planes and lets the golden assert an exact row
// order. Where no duplicate exists, the order cannot change the output at all,
// which is the case for every row in the proof org.
const dependencyQuery = `
SELECT
  org_id,
  source_work_item_id,
  target_work_item_id,
  relationship_type_raw,
  last_synced
FROM work_item_dependencies FINAL
WHERE org_id = {org_id:String}
ORDER BY target_work_item_id, source_work_item_id, relationship_type_raw, last_synced`

const repoQuery = `
SELECT org_id, id, repo
FROM repos FINAL
WHERE org_id = {org_id:String}`

const workItemQuery = `
SELECT org_id, work_item_id
FROM work_items FINAL
WHERE org_id = {org_id:String}`

// Load reads all four inputs for one org and window.
func (loader *Loader) Load(ctx context.Context, orgID string, window Window) (Inputs, error) {
	if loader == nil || loader.conn == nil {
		return Inputs{}, ErrUnavailable
	}
	if orgID == "" {
		return Inputs{}, fmt.Errorf("issueprlinks: org id is required")
	}
	inputs := Inputs{OrgID: orgID}

	dependencies, err := loader.loadDependencies(ctx, orgID)
	if err != nil {
		return Inputs{}, fmt.Errorf("load work_item_dependencies: %w", err)
	}
	// Python returns early on an empty dependency read (builder.py:706-707) and
	// never issues the other three queries. Same short-circuit, so an org with
	// no dependencies costs one read on both planes.
	if len(dependencies) == 0 {
		return inputs, nil
	}
	inputs.Dependencies = dependencies

	if inputs.Repos, err = loader.loadRepos(ctx, orgID); err != nil {
		return Inputs{}, fmt.Errorf("load repos: %w", err)
	}
	if inputs.PullRequests, err = loader.loadPullRequests(ctx, orgID, window); err != nil {
		return Inputs{}, fmt.Errorf("load git_pull_requests: %w", err)
	}
	if inputs.WorkItems, err = loader.loadWorkItems(ctx, orgID); err != nil {
		return Inputs{}, fmt.Errorf("load work_items: %w", err)
	}
	return inputs, nil
}

func (loader *Loader) loadDependencies(ctx context.Context, orgID string) ([]DependencyRow, error) {
	rows, err := loader.conn.Query(ctx, dependencyQuery, clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []DependencyRow
	for rows.Next() {
		var row DependencyRow
		if err := rows.Scan(
			&row.OrgID, &row.SourceWorkItemID, &row.TargetWorkItemID,
			&row.RelationshipTypeRaw, &row.LastSynced,
		); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (loader *Loader) loadRepos(ctx context.Context, orgID string) ([]RepoRow, error) {
	rows, err := loader.conn.Query(ctx, repoQuery, clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []RepoRow
	for rows.Next() {
		var row RepoRow
		if err := rows.Scan(&row.OrgID, &row.ID, &row.Repo); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// loadPullRequests ports builder.py:665-684 including the window and repo
// filters. Only (org_id, repo_id, number) is read -- the derivation uses the
// row's EXISTENCE, never its columns, so created_at stays in the WHERE clause
// and never crosses into Go where a timezone could distort it.
func (loader *Loader) loadPullRequests(ctx context.Context, orgID string, window Window) ([]PullRequestRow, error) {
	query := `
SELECT org_id, repo_id, number
FROM git_pull_requests FINAL
WHERE org_id = {org_id:String}`
	args := []any{clickhouse.Named("org_id", orgID)}
	if window.From != nil {
		bound, err := truncateBoundToSecond(*window.From)
		if err != nil {
			return nil, fmt.Errorf("window from: %w", err)
		}
		query += ` AND created_at >= {from_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("from_ts", bound))
	}
	if window.To != nil {
		bound, err := truncateBoundToSecond(*window.To)
		if err != nil {
			return nil, fmt.Errorf("window to: %w", err)
		}
		query += ` AND created_at <= {to_ts:DateTime64(3)}`
		args = append(args, clickhouse.Named("to_ts", bound))
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

	var out []PullRequestRow
	for rows.Next() {
		var row PullRequestRow
		if err := rows.Scan(&row.OrgID, &row.RepoID, &row.Number); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (loader *Loader) loadWorkItems(ctx context.Context, orgID string) ([]WorkItemRow, error) {
	rows, err := loader.conn.Query(ctx, workItemQuery, clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []WorkItemRow
	for rows.Next() {
		var row WorkItemRow
		if err := rows.Scan(&row.OrgID, &row.WorkItemID); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// insertStatement names every column explicitly and in the live table's own
// order (system.columns, db `default`, 2026-09-01). Positional inserts against
// a ReplacingMergeTree are how a future ALTER ... ADD COLUMN silently shifts
// values into the wrong columns.
const insertStatement = `INSERT INTO work_graph_issue_pr
 (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)`

// Writer inserts mapping rows.
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

// Write inserts the derived rows. An empty slice writes nothing and is not an
// error -- Python's _write_issue_pr_links returns early on an empty list
// (builder.py:228-233), and an org with no provider-attached links is a normal
// state, not a failure.
func (writer *Writer) Write(ctx context.Context, links []Link) error {
	if writer == nil || writer.conn == nil {
		return ErrUnavailable
	}
	if len(links) == 0 {
		return nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, insertStatement)
	if err != nil {
		return fmt.Errorf("prepare work_graph_issue_pr batch: %w", err)
	}
	for _, link := range links {
		if err := batch.Append(
			link.RepoID,
			link.WorkItemID,
			link.PRNumber,
			link.Confidence,
			link.Provenance,
			link.Evidence,
			link.LastSynced,
			link.OrgID,
		); err != nil {
			return fmt.Errorf("append work_graph_issue_pr row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send work_graph_issue_pr batch: %w", err)
	}
	return nil
}
