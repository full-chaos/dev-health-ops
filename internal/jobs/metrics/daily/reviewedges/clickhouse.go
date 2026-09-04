package reviewedges

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
)

// conn is the narrow ClickHouse capability this package needs, matching
// internal/jobs/metrics/daily/cicd's conn interface shape.
type conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// ClickHouseLoader reads the two raw sync tables this family consumes.
type ClickHouseLoader struct {
	conn conn
}

func NewClickHouseLoader(connection conn) (*ClickHouseLoader, error) {
	if connection == nil {
		return nil, fmt.Errorf("reviewedges: clickhouse connection is required")
	}
	return &ClickHouseLoader{conn: connection}, nil
}

// DEDUP, AND WHY IT CHANGES NUMBERS (read before comparing against Python).
//
// Both source tables are ReplacingMergeTree(last_synced) -- git_pull_requests
// and git_pull_request_reviews, 000_raw_tables.sql:61 and :86, rekeyed by
// migration 027 to lead with org_id. Python's load_git_rows queries them RAW:
// no FINAL, no argMax, no ORDER BY (loaders/clickhouse.py:283-320). That has
// two consequences Python lives with today:
//
//   - PR rows: `pr_author_map[(repo_id, number)] = ...` is a last-write-wins
//     dict assignment over an UNORDERED result set, so on a re-synced PR the
//     author Python picks is arbitrary.
//   - REVIEW rows: every raw row is counted, so a re-synced review is counted
//     TWICE and inflates reviews_count.
//
// # WHY FINAL AND NOT argMax
//
// Python reads raw, so the dedup below is something this port ADDS rather than
// mirrors. An ADDED dedup on a ReplacingMergeTree table uses FINAL, not argMax
// (fleet rule, three arms: mirror Python's FINAL; an ADDED dedup is FINAL; a
// plain-MergeTree table or a Python argMax is mirrored as-is and disclosed).
//
// The reason is determinism on a version TIE, and ties are not exotic here: a
// re-sync that writes several columns in one batch stamps them with the same
// last_synced BY CONSTRUCTION. On such a tie:
//
//   - argMax picks nondeterministically -- and several argMax aggregates over
//     one GROUP BY each pick INDEPENDENTLY, so they can assemble a
//     "Frankenstein" row that never existed in the table (an author_email from
//     one snapshot beside an author_name from another). Collapsing them into a
//     single argMax(tuple(...)) fixes the Frankenstein half but leaves WHICH
//     row wins undefined.
//   - FINAL is deterministic: the last-inserted row wins, and it wins for the
//     whole row at once, so the Frankenstein class cannot arise either.
//
// FINAL is also what the table's own background merge will eventually do, so
// this read agrees with the table's settled state instead of racing it.
//
// This does NOT weaken the double-count fix: FINAL collapses a re-synced review
// to one row exactly as the argMax form did, and
// TestReviewEdgesComputeFamilyDeduplicatesResyncedRows still pins it, now
// alongside an identical-last_synced tie case with a positive control.
//
// The behaviour change against Python stands and is deliberate: for an org with
// re-ingested reviews, native reviews_count is LOWER than Python's, and
// correct. Recorded in RISK-NOTES rather than smuggled in.
//
// The window predicate stays AFTER the dedup -- with FINAL the WHERE applies to
// the collapsed row, so a PR is admitted on its WINNING snapshot's timestamps
// and reported with that same snapshot's author. Filtering pre-dedup could
// admit a row on a stale snapshot's created_at.

const pullRequestsQuery = `
SELECT
    repo_id,
    number,
    author_email,
    author_name
FROM git_pull_requests FINAL
WHERE repo_id IN {repo_ids:Array(UUID)}
  AND org_id = {org_id:String}
  AND ((created_at >= {start:DateTime64(3, 'UTC')} AND created_at < {end:DateTime64(3, 'UTC')})
    OR (merged_at IS NOT NULL
        AND merged_at >= {start:DateTime64(3, 'UTC')}
        AND merged_at < {end:DateTime64(3, 'UTC')}))
ORDER BY repo_id, number`

// LoadPullRequests returns the day's PR rows, deduplicated and ordered.
//
// The window predicate is Python's verbatim -- `created_at` in range OR
// `merged_at` in range -- and it is applied AFTER the dedup so the window sees
// each PR's latest values, not a stale snapshot's. This is the window whose
// narrowness produces the dropped-edge quirk documented on the package.
func (loader *ClickHouseLoader) LoadPullRequests(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, start, end time.Time,
) ([]PullRequestRow, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("reviewedges: loader unavailable")
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}
	rows, err := loader.conn.Query(ctx, pullRequestsQuery,
		clickhouse.Named("repo_ids", repoIDs),
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("start", remaining.DateTime64Argument(start, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("end", remaining.DateTime64Argument(end, remaining.DateTime64MillisecondPrecision)),
	)
	if err != nil {
		return nil, fmt.Errorf("load pull requests: %w", err)
	}
	defer rows.Close()

	var result []PullRequestRow
	for rows.Next() {
		var (
			repoID      uuid.UUID
			number      uint32
			authorEmail *string
			authorName  *string
		)
		if err := rows.Scan(&repoID, &number, &authorEmail, &authorName); err != nil {
			return nil, fmt.Errorf("scan pull request row: %w", err)
		}
		// author_email/author_name are Nullable(String) (000_raw_tables.sql:
		// 67-68). Python coerces NULL to "" with `or ""`
		// (loaders/clickhouse.py:361-362), and the empty string is what makes
		// NormalizeGitIdentity fall through to "unknown".
		result = append(result, PullRequestRow{
			RepoID:      repoID,
			Number:      number,
			AuthorEmail: derefString(authorEmail),
			AuthorName:  derefString(authorName),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// reviewsQuery dedups on git_pull_request_reviews' own ORDER BY key
// (repo_id, number, review_id) -- see pullRequestsQuery's comment for why this
// is both necessary and count-changing.
const reviewsQuery = `
SELECT
    repo_id,
    number,
    reviewer,
    submitted_at
FROM git_pull_request_reviews FINAL
WHERE repo_id IN {repo_ids:Array(UUID)}
  AND org_id = {org_id:String}
  AND submitted_at >= {start:DateTime64(3, 'UTC')}
  AND submitted_at < {end:DateTime64(3, 'UTC')}
ORDER BY repo_id, number, review_id`

// LoadReviews returns the day's review rows, deduplicated and ordered.
func (loader *ClickHouseLoader) LoadReviews(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, start, end time.Time,
) ([]ReviewRow, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("reviewedges: loader unavailable")
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}
	rows, err := loader.conn.Query(ctx, reviewsQuery,
		clickhouse.Named("repo_ids", repoIDs),
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("start", remaining.DateTime64Argument(start, remaining.DateTime64MillisecondPrecision)),
		clickhouse.Named("end", remaining.DateTime64Argument(end, remaining.DateTime64MillisecondPrecision)),
	)
	if err != nil {
		return nil, fmt.Errorf("load reviews: %w", err)
	}
	defer rows.Close()

	var result []ReviewRow
	for rows.Next() {
		var (
			repoID      uuid.UUID
			number      uint32
			reviewer    string
			submittedAt time.Time
		)
		if err := rows.Scan(&repoID, &number, &reviewer, &submittedAt); err != nil {
			return nil, fmt.Errorf("scan review row: %w", err)
		}
		// `reviewer` is a non-Nullable String in the DDL
		// (000_raw_tables.sql:90), but Python still coerces it with
		// `or "unknown"` (loaders/clickhouse.py:389) -- which fires on the
		// EMPTY string, not just on NULL. Mirrored here so an empty reviewer
		// becomes "unknown" before compute, exactly as in Python; the
		// whitespace-only case is handled downstream by
		// NormalizeGitIdentity's strip.
		if reviewer == "" {
			reviewer = "unknown"
		}
		result = append(result, ReviewRow{
			RepoID:      repoID,
			Number:      number,
			Reviewer:    reviewer,
			SubmittedAt: submittedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Writer appends ComputeReviewEdgesDaily's output to review_edges_daily.
type Writer struct {
	conn conn
}

func NewWriter(connection conn) (*Writer, error) {
	if connection == nil {
		return nil, fmt.Errorf("reviewedges: clickhouse connection is required")
	}
	return &Writer{conn: connection}, nil
}

// WriteRecords writes review_edges_daily and returns the number of rows
// written. Column list and order are write_review_edges' verbatim
// (sinks/clickhouse/work_graph.py:438-453).
//
// Fails closed on an empty orgID. org_id is NOT in this table's sort key (it
// was added by migration 024, after the table existed), so an unscoped row is
// not merely mislabelled -- it is invisible to the read-side dedup, whose
// natural key leads with org_id (clickhouse_dedup.py:115).
func (writer *Writer) WriteRecords(ctx context.Context, records []Record, orgID string) (int, error) {
	if writer == nil || writer.conn == nil {
		return 0, fmt.Errorf("reviewedges: writer unavailable")
	}
	if orgID == "" {
		return 0, fmt.Errorf("reviewedges: organization id is required to write review_edges_daily")
	}
	if len(records) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO review_edges_daily (
		repo_id, day, reviewer, author, reviews_count, computed_at, org_id
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare review_edges_daily batch: %w", err)
	}
	for _, record := range records {
		if err := batch.Append(
			record.RepoID, record.Day, record.Reviewer, record.Author,
			record.ReviewsCount, record.ComputedAt, orgID,
		); err != nil {
			return 0, fmt.Errorf("append review_edges_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send review_edges_daily batch: %w", err)
	}
	recordRowsWritten(len(records), orgID != "")
	return len(records), nil
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
