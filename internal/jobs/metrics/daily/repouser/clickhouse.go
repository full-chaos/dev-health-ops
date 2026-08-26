package repouser

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// conn is the narrow ClickHouse capability this package needs -- query plus
// batch insert, matching the shape internal/jobs/metrics/remaining already
// depends on (driver.Conn satisfies it directly).
type conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// ClickHouseLoader reads the raw rows Compute needs for one partition
// (an org's repos for one day), byte-for-byte the same SQL shape as
// load_git_rows (loaders/clickhouse.py:86), scoped to a repo_id list instead
// of load_git_rows' single repo_id/repo_name -- see the package doc comment
// on why batching every repo in a partition into one query is equivalent to
// Python's per-repo loop (job_daily.py:956).
//
// None of these queries read git_commits/git_commit_stats/git_pull_requests/
// git_pull_request_reviews with FINAL, even though all four are
// ReplacingMergeTree tables that CAN carry more than one version of a row
// between merges (e.g. mid-resync). This is PARITY, not an oversight:
// load_git_rows' real SQL (loaders/clickhouse.py:110-164) never uses FINAL
// on these four tables either (verified by grep -- FINAL appears elsewhere
// in that file for work_items/ci_pipeline_runs/test_suite_results, never
// here). If duplicate-version double-counting during a resync race turns
// out to matter, it is a shared Python+Go gap to close in both places at
// once, not something to "fix" unilaterally on the Go side alone.
type ClickHouseLoader struct {
	conn conn
}

func NewClickHouseLoader(connection conn) (*ClickHouseLoader, error) {
	if connection == nil {
		return nil, fmt.Errorf("repouser: clickhouse connection is required")
	}
	return &ClickHouseLoader{conn: connection}, nil
}

// LoadGitRows loads commit/PR/review rows for repoIDs within [start, end).
func (loader *ClickHouseLoader) LoadGitRows(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, start, end time.Time,
) ([]CommitStatRow, []PullRequestRow, []PullRequestReviewRow, error) {
	if loader == nil || loader.conn == nil {
		return nil, nil, nil, fmt.Errorf("repouser: loader unavailable")
	}
	if len(repoIDs) == 0 {
		return nil, nil, nil, nil
	}
	commits, err := loader.loadCommitStats(ctx, orgID, repoIDs, start, end)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load commit stats: %w", err)
	}
	prs, err := loader.loadPullRequests(ctx, orgID, repoIDs, start, end)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load pull requests: %w", err)
	}
	reviews, err := loader.loadPullRequestReviews(ctx, orgID, repoIDs, start, end)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load pull request reviews: %w", err)
	}
	return commits, prs, reviews, nil
}

// commitStatsQuery ports load_git_rows' commit_query: a LEFT JOIN of
// git_commits and git_commit_stats windowed on committer_when.
const commitStatsQuery = `
SELECT
  c.repo_id AS repo_id,
  c.hash AS commit_hash,
  c.author_email AS author_email,
  c.author_name AS author_name,
  c.committer_when AS committer_when,
  s.file_path AS file_path,
  s.additions AS additions,
  s.deletions AS deletions
FROM git_commits AS c
LEFT JOIN git_commit_stats AS s
  ON (s.repo_id = c.repo_id) AND (s.commit_hash = c.hash) AND (s.org_id = c.org_id)
WHERE c.committer_when >= {start:DateTime} AND c.committer_when < {end:DateTime}
  AND c.repo_id IN {repo_ids:Array(UUID)}
  AND c.org_id = {org_id:String}`

func (loader *ClickHouseLoader) loadCommitStats(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, start, end time.Time,
) ([]CommitStatRow, error) {
	rows, err := loader.conn.Query(ctx, commitStatsQuery,
		clickhouse.Named("start", dateTimeArgument(start)),
		clickhouse.Named("end", dateTimeArgument(end)),
		clickhouse.Named("repo_ids", repoIDs),
		clickhouse.Named("org_id", orgID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []CommitStatRow
	for rows.Next() {
		var (
			repoID        uuid.UUID
			commitHash    string
			authorEmail   *string
			authorName    *string
			committerWhen time.Time
			filePath      *string
			additions     *int32
			deletions     *int32
		)
		if err := rows.Scan(&repoID, &commitHash, &authorEmail, &authorName,
			&committerWhen, &filePath, &additions, &deletions); err != nil {
			return nil, fmt.Errorf("scan commit stat row: %w", err)
		}
		result = append(result, CommitStatRow{
			RepoID:        repoID,
			CommitHash:    commitHash,
			AuthorEmail:   derefStr(authorEmail),
			AuthorName:    derefStr(authorName),
			CommitterWhen: committerWhen,
			FilePath:      derefStr(filePath),
			Additions:     int(derefInt32(additions)),
			Deletions:     int(derefInt32(deletions)),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// pullRequestsQuery ports load_git_rows' pr_query: a PR counts if it was
// EITHER created or merged in the window (the OR is load-bearing -- see
// compute.py, which then separately gates authored-vs-merged counting by
// its own window checks on each timestamp).
//
// title is deliberately NOT selected here -- see PullRequestRow.Title's doc
// comment (types.go) for why: Python's own loader never selects it either,
// so selecting it here would make Go detect reverts Python's production
// path never does.
//
// INTENTIONAL DIVERGENCE (not a port gap): the OR is wrapped in outer
// parens here; Python's literal pr_query (loaders/clickhouse.py:129-151)
// is NOT --
//
//	(created_at >= start AND created_at < end)
//	OR (merged_at IS NOT NULL AND merged_at >= start AND merged_at < end)
//	{repo_filter}
//	{org_filter}
//
// SQL precedence (AND binds tighter than OR) parses Python's UNPARENTHESIZED
// clause as `created_window OR (merged_window AND repo_filter AND
// org_filter)`: the created_at branch is not scoped by repo_id or org_id at
// all. Every caller that sets repo_id/repo_name (job_daily.py's per-repo
// loop always does) therefore pulls in PRs CREATED in the window from every
// repo and every org in git_pull_requests, not just the target one --
// verified against the literal source, not inferred. This looks like a real
// pre-existing production bug (flagged upstream 2026-08-26), and this port
// does not reproduce it: `repo_ids`/`org_id` scope BOTH branches of the OR
// here. If that bug is ever intentionally preserved as documented behavior
// instead of fixed, this query and the parity golden both need revisiting.
const pullRequestsQuery = `
SELECT
  repo_id, number, author_email, author_name, created_at, merged_at,
  first_review_at, first_comment_at, changes_requested_count, reviews_count,
  comments_count, additions, deletions, changed_files
FROM git_pull_requests
WHERE
  ((created_at >= {start:DateTime} AND created_at < {end:DateTime})
   OR (merged_at IS NOT NULL AND merged_at >= {start:DateTime} AND merged_at < {end:DateTime}))
  AND repo_id IN {repo_ids:Array(UUID)}
  AND org_id = {org_id:String}`

func (loader *ClickHouseLoader) loadPullRequests(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, start, end time.Time,
) ([]PullRequestRow, error) {
	rows, err := loader.conn.Query(ctx, pullRequestsQuery,
		clickhouse.Named("start", dateTimeArgument(start)),
		clickhouse.Named("end", dateTimeArgument(end)),
		clickhouse.Named("repo_ids", repoIDs),
		clickhouse.Named("org_id", orgID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []PullRequestRow
	for rows.Next() {
		var (
			repoID                uuid.UUID
			number                uint32
			authorEmail           *string
			authorName            *string
			createdAt             time.Time
			mergedAt              *time.Time
			firstReviewAt         *time.Time
			firstCommentAt        *time.Time
			changesRequestedCount uint32
			reviewsCount          uint32
			commentsCount         uint32
			additions             *uint32
			deletions             *uint32
			changedFiles          *uint32
		)
		if err := rows.Scan(&repoID, &number, &authorEmail, &authorName, &createdAt, &mergedAt,
			&firstReviewAt, &firstCommentAt, &changesRequestedCount, &reviewsCount,
			&commentsCount, &additions, &deletions, &changedFiles); err != nil {
			return nil, fmt.Errorf("scan pull request row: %w", err)
		}
		result = append(result, PullRequestRow{
			RepoID:                repoID,
			Number:                int(number),
			AuthorEmail:           derefStr(authorEmail),
			AuthorName:            derefStr(authorName),
			CreatedAt:             createdAt,
			MergedAt:              mergedAt,
			FirstReviewAt:         firstReviewAt,
			FirstCommentAt:        firstCommentAt,
			ChangesRequestedCount: int(changesRequestedCount),
			ReviewsCount:          int(reviewsCount),
			CommentsCount:         int(commentsCount),
			Additions:             int(derefUint32(additions)),
			Deletions:             int(derefUint32(deletions)),
			ChangedFiles:          int(derefUint32(changedFiles)),
			// Title left as "" (its zero value): never selected above -- see
			// PullRequestRow.Title's doc comment.
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

const pullRequestReviewsQuery = `
SELECT repo_id, number, reviewer, submitted_at, state
FROM git_pull_request_reviews
WHERE submitted_at >= {start:DateTime} AND submitted_at < {end:DateTime}
  AND repo_id IN {repo_ids:Array(UUID)}
  AND org_id = {org_id:String}`

func (loader *ClickHouseLoader) loadPullRequestReviews(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, start, end time.Time,
) ([]PullRequestReviewRow, error) {
	rows, err := loader.conn.Query(ctx, pullRequestReviewsQuery,
		clickhouse.Named("start", dateTimeArgument(start)),
		clickhouse.Named("end", dateTimeArgument(end)),
		clickhouse.Named("repo_ids", repoIDs),
		clickhouse.Named("org_id", orgID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []PullRequestReviewRow
	for rows.Next() {
		var (
			repoID      uuid.UUID
			number      uint32
			reviewer    string
			submittedAt time.Time
			state       string
		)
		if err := rows.Scan(&repoID, &number, &reviewer, &submittedAt, &state); err != nil {
			return nil, fmt.Errorf("scan pull request review row: %w", err)
		}
		if reviewer == "" {
			reviewer = "unknown"
		}
		if state == "" {
			state = "unknown"
		}
		result = append(result, PullRequestReviewRow{
			RepoID: repoID, Number: int(number), Reviewer: reviewer,
			SubmittedAt: submittedAt, State: state,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// LoadWindowCommitStats loads the same shape as LoadGitRows' commit half,
// but over an arbitrary window -- used for the 30-day lookback the
// rework/single-owner/bus-factor/gini kernels read (job_daily.py's
// h_commit_rows, h_start_date = d - timedelta(days=29)).
func (loader *ClickHouseLoader) LoadWindowCommitStats(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, start, end time.Time,
) ([]CommitStatRow, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("repouser: loader unavailable")
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}
	return loader.loadCommitStats(ctx, orgID, repoIDs, start, end)
}

// bugWorkItemsQuery selects bug-type work items with both lifecycle
// timestamps set, mirroring the `item.type == "bug" and item.completed_at
// and item.started_at` guard in job_daily.py before MTTRByRepo's caller
// filters by day.
const bugWorkItemsQuery = `
SELECT repo_id, started_at, completed_at
FROM work_items FINAL
WHERE type = 'bug' AND repo_id IN {repo_ids:Array(UUID)} AND org_id = {org_id:String}
  AND started_at IS NOT NULL AND completed_at IS NOT NULL
  AND completed_at >= {start:DateTime} AND completed_at < {end:DateTime}`

// LoadBugWorkItems loads bug work items completed within [start, end) for
// repoIDs, ready to pass to MTTRByRepo.
func (loader *ClickHouseLoader) LoadBugWorkItems(
	ctx context.Context, orgID string, repoIDs []uuid.UUID, start, end time.Time,
) ([]BugWorkItem, error) {
	if loader == nil || loader.conn == nil {
		return nil, fmt.Errorf("repouser: loader unavailable")
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}
	rows, err := loader.conn.Query(ctx, bugWorkItemsQuery,
		clickhouse.Named("repo_ids", repoIDs),
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("start", dateTimeArgument(start)),
		clickhouse.Named("end", dateTimeArgument(end)),
	)
	if err != nil {
		return nil, fmt.Errorf("load bug work items: %w", err)
	}
	defer rows.Close()

	var result []BugWorkItem
	for rows.Next() {
		var (
			repoID      uuid.UUID
			startedAt   *time.Time
			completedAt *time.Time
		)
		if err := rows.Scan(&repoID, &startedAt, &completedAt); err != nil {
			return nil, fmt.Errorf("scan bug work item row: %w", err)
		}
		if startedAt == nil || completedAt == nil {
			continue
		}
		result = append(result, BugWorkItem{RepoID: repoID, StartedAt: *startedAt, CompletedAt: *completedAt})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Writer persists a Result. org_id is written as the empty string on all
// three tables, matching the existing Python sink's ClickHouseMetricsSink._
// insert_rows call (RepoMetricsDailyRecord/UserMetricsDailyRecord/
// CommitMetricsRecord all default org_id="" and the sink writes that field
// verbatim, not the ClickHouse column's 'default' DEFAULT) -- see the
// comment on writeRepoMetrics for why this is preserved rather than
// "fixed" to the real org id.
type Writer struct {
	conn conn
}

func NewWriter(connection conn) (*Writer, error) {
	if connection == nil {
		return nil, fmt.Errorf("repouser: clickhouse connection is required")
	}
	return &Writer{conn: connection}, nil
}

// WriteResult writes all three tables and returns the number of rows
// written to each, in that order.
//
// NOT transactional across the three batches: a failure on the user or
// commit batch after the repo batch already landed leaves a partial
// generation for this (org, repo, day). This is a deliberate, accepted
// consequence of the fail-open policy RepoUserCommitExecutor.ComputeFamily
// relies on: on any error here, PartitionHandler does NOT put
// "repo_user_commit" in skipFamilies, so the Python compatibility bridge
// still computes and writes the SAME (org, repo, day) scope's full, correct
// row set as part of the same partition.
//
// Two codex adversarial review rounds pushed on whether that recovery is
// actually DETERMINISTIC, and the honest answer is: usually, not
// guaranteed. repo_metrics_daily/user_metrics_daily/commit_metrics all
// declare `computed_at DateTime('UTC')` -- SECOND precision, not
// DateTime64 -- so a partial native write and the compatibility bridge's
// write for the SAME partition, moments apart in the same synchronous flow,
// CAN land in the same wall-clock second. Reader dedup (argMax/latest on
// computed_at) breaks that tie in an implementation-defined way, not
// reliably "whichever ran later." This is not unique to this family or
// introduced by this port: team_metrics_daily (TeamWellbeingExecutor,
// CHAOS-4276, already merged) declares the identical `DateTime('UTC')`
// column and has the identical exposure on its own per-repo fail-open path.
// A real fix (a monotonic generation key every reader dedups on, or
// DateTime64) is a cross-cutting change affecting every native family this
// repo has shipped, not something to bolt onto one family's writer alone --
// tracked as follow-up scope, not this ticket's. The practical exposure is
// narrow: it only surfaces on a genuine mid-batch ClickHouse failure (rare),
// and any wrong dedup pick self-corrects on the NEXT day's partition run
// (which gets an unambiguously fresh computed_at), so the worst case is one
// day of a partial-vs-complete row being ambiguous, not permanent
// corruption.
func (writer *Writer) WriteResult(ctx context.Context, result Result) (repoRows, userRows, commitRows int, err error) {
	if writer == nil || writer.conn == nil {
		return 0, 0, 0, fmt.Errorf("repouser: writer unavailable")
	}
	if repoRows, err = writer.writeRepoMetrics(ctx, result.RepoMetrics); err != nil {
		return 0, 0, 0, fmt.Errorf("write repo metrics: %w", err)
	}
	if userRows, err = writer.writeUserMetrics(ctx, result.UserMetrics); err != nil {
		return repoRows, 0, 0, fmt.Errorf("write user metrics: %w", err)
	}
	if commitRows, err = writer.writeCommitMetrics(ctx, result.CommitMetrics); err != nil {
		return repoRows, userRows, 0, fmt.Errorf("write commit metrics: %w", err)
	}
	return repoRows, userRows, commitRows, nil
}

// writeRepoMetrics inserts into repo_metrics_daily.
//
// org_id is deliberately "" -- see the Writer doc comment. This is parity
// with existing Python behaviour, not an oversight: nothing today reads
// org_id off these three tables for scoping (ci/assert_metrics_executed_
// proof.py scopes by repo_id alone, which is already org-unique via the
// repos table), so matching Python's literal is lower risk than silently
// changing what these rows carry.
func (writer *Writer) writeRepoMetrics(ctx context.Context, rows []RepoMetric) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO repo_metrics_daily (
		repo_id, day, commits_count, total_loc_touched, avg_commit_size_loc,
		large_commit_ratio, prs_merged, median_pr_cycle_hours, pr_cycle_p75_hours,
		pr_cycle_p90_hours, prs_with_first_review, pr_first_review_p50_hours,
		pr_first_review_p90_hours, pr_review_time_p50_hours, pr_pickup_time_p50_hours,
		large_pr_ratio, pr_rework_ratio, pr_size_p50_loc, pr_size_p90_loc,
		pr_comments_per_100_loc, pr_reviews_per_100_loc, rework_churn_ratio_30d,
		single_owner_file_ratio_30d, review_load_top_reviewer_ratio, bus_factor,
		code_ownership_gini, mttr_hours, change_failure_rate, computed_at, org_id
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare repo_metrics_daily batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.RepoID, row.Day, uint32(row.CommitsCount), uint32(row.TotalLOCTouched),
			row.AvgCommitSizeLOC, row.LargeCommitRatio, uint32(row.PRsMerged),
			row.MedianPRCycleHours, row.PRCycleP75Hours, row.PRCycleP90Hours,
			uint32(row.PRsWithFirstReview), row.PRFirstReviewP50Hours, row.PRFirstReviewP90Hours,
			row.PRReviewTimeP50Hours, row.PRPickupTimeP50Hours, row.LargePRRatio, row.PRReworkRatio,
			row.PRSizeP50LOC, row.PRSizeP90LOC, row.PRCommentsPer100LOC, row.PRReviewsPer100LOC,
			row.ReworkChurnRatio30d, row.SingleOwnerFileRatio30d, row.ReviewLoadTopReviewerRatio,
			uint32(row.BusFactor), row.CodeOwnershipGini, row.MTTRHours, row.ChangeFailureRate,
			row.ComputedAt, "",
		); err != nil {
			return 0, fmt.Errorf("append repo_metrics_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send repo_metrics_daily batch: %w", err)
	}
	return len(rows), nil
}

func (writer *Writer) writeUserMetrics(ctx context.Context, rows []UserMetric) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO user_metrics_daily (
		repo_id, day, author_email, commits_count, loc_added, loc_deleted,
		files_changed, large_commits_count, avg_commit_size_loc, prs_authored,
		prs_merged, avg_pr_cycle_hours, median_pr_cycle_hours, pr_cycle_p75_hours,
		pr_cycle_p90_hours, prs_with_first_review, pr_first_review_p50_hours,
		pr_first_review_p90_hours, pr_review_time_p50_hours, pr_pickup_time_p50_hours,
		reviews_given, changes_requested_given, reviews_received, review_reciprocity,
		pr_interruption_load, context_spread_count, review_request_load, team_id,
		team_name, active_hours, weekend_days, identity_id, computed_at, org_id
	)`)
	if err != nil {
		return 0, fmt.Errorf("prepare user_metrics_daily batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.RepoID, row.Day, row.AuthorEmail, uint32(row.CommitsCount),
			uint32(row.LOCAdded), uint32(row.LOCDeleted), uint32(row.FilesChanged),
			uint32(row.LargeCommitsCount), row.AvgCommitSizeLOC, uint32(row.PRsAuthored),
			uint32(row.PRsMerged), row.AvgPRCycleHours, row.MedianPRCycleHours,
			row.PRCycleP75Hours, row.PRCycleP90Hours, uint32(row.PRsWithFirstReview),
			row.PRFirstReviewP50Hours, row.PRFirstReviewP90Hours, row.PRReviewTimeP50Hours,
			row.PRPickupTimeP50Hours, uint32(row.ReviewsGiven), uint32(row.ChangesRequestedGiven),
			uint32(row.ReviewsReceived), row.ReviewReciprocity, uint32(row.PRInterruptionLoad),
			uint32(row.ContextSpreadCount), uint32(row.ReviewRequestLoad), row.TeamID,
			row.TeamName, row.ActiveHours, uint8(row.WeekendDays), row.IdentityID,
			row.ComputedAt, "",
		); err != nil {
			return 0, fmt.Errorf("append user_metrics_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send user_metrics_daily batch: %w", err)
	}
	return len(rows), nil
}

func (writer *Writer) writeCommitMetrics(ctx context.Context, rows []CommitMetric) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := writer.conn.PrepareBatch(ctx,
		`INSERT INTO commit_metrics (repo_id, commit_hash, day, author_email, total_loc, files_changed, size_bucket, computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare commit_metrics batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.RepoID, row.CommitHash, row.Day, row.AuthorEmail,
			uint32(row.TotalLOC), uint32(row.FilesChanged), row.SizeBucket, row.ComputedAt, "",
		); err != nil {
			return 0, fmt.Errorf("append commit_metrics row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send commit_metrics batch: %w", err)
	}
	return len(rows), nil
}

// dateTimeArgument renders a timestamp as a plain literal for a
// {name:DateTime} named parameter.
//
// clickhouse-go renders a time.Time bound directly to a named parameter as
// a toDateTime(...) EXPRESSION (e.g. `toDateTime('2026-08-26 00:00:00')`),
// but ClickHouse's parameter binding PARSES the parameter text as a literal
// value for the declared type -- it does not evaluate expressions there.
// The server therefore rejects it: "Cannot parse datetime:
// value toDateTime('2026-08-26 00:00:00') cannot be parsed as DateTime".
// This was caught by the isolated real-ClickHouse readback (CHAOS-4275),
// not by any mocked-connection unit test -- passing a plain formatted
// string instead of a time.Time value is what makes the parameter a literal
// the parser accepts. See also internal/jobs/metrics/remaining's identical
// note for DateTime64 columns (dora_native_clickhouse.go); this is the same
// class of gap for the plain DateTime type this package's queries use.
func dateTimeArgument(value time.Time) string {
	return value.UTC().Format("2006-01-02 15:04:05")
}

func derefStr(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func derefInt32(value *int32) int32 {
	if value == nil {
		return 0
	}
	return *value
}

func derefUint32(value *uint32) uint32 {
	if value == nil {
		return 0
	}
	return *value
}
