package chquery

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// workItemsDeduped mirrors Python's WORK_ITEMS_DEDUPED
// (metrics/sinks/clickhouse/idempotency.py:5). work_items IS read with FINAL,
// unlike git_commits/git_pull_requests/git_commit_stats below, which are not --
// that asymmetry is Python's and is preserved deliberately.
const workItemsDeduped = "work_items FINAL"

// normalizeTimestamp ports utils/evidence.py:27-38 _ensure_utc for the values
// this package returns.
//
// This is NOT decoration. The columns are inconsistent about timezones, and the
// inconsistency is invisible until it moves a work unit's time bounds across a
// window edge:
//
//	work_items.created_at / updated_at        DateTime64(3)          -- NO timezone
//	work_items.closed_at / completed_at       Nullable(DateTime64(3)) -- NO timezone
//	git_commits.author_when / committer_when  DateTime64(3, 'UTC')
//	git_pull_requests.created_at              DateTime64(3, 'UTC')
//	git_pull_requests.merged_at / closed_at   Nullable(DateTime64(3, 'UTC'))
//	work_item_cycle_times.computed_at         DateTime('UTC')
//
// (Read from system.columns on the live stack, 2026-09-02 — not from migrations.)
//
// Python receives the tz-naive ones as naive datetimes and _ensure_utc stamps
// them `.replace(tzinfo=timezone.utc)` — it REINTERPRETS the wall clock as UTC
// rather than converting it. Go's driver always attaches a Location, so the
// equivalent is to rebuild the same wall clock in UTC, not to call .UTC(),
// which would convert and shift the instant if the driver attached anything
// other than UTC.
//
// For the columns that DO declare 'UTC' the two are identical, so applying this
// uniformly is safe and removes a per-column decision that a future fetcher
// would have to get right again.
//
// VERIFICATION OWED: the integration test asserts these values against the same
// rows fetched through Python, so the claim is measured rather than argued. Do
// not treat this comment as the proof.
func normalizeTimestamp(value time.Time) time.Time {
	return time.Date(
		value.Year(), value.Month(), value.Day(),
		value.Hour(), value.Minute(), value.Second(), value.Nanosecond(),
		time.UTC,
	)
}

func normalizeOptionalTimestamp(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := normalizeTimestamp(*value)
	return &normalized
}

// WorkItem is one row of the work-item projection the materializer reads.
type WorkItem struct {
	WorkItemID  string
	Provider    string
	RepoID      string
	Title       string
	Description string
	Type        string
	Labels      []string
	ParentID    string
	EpicID      string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt *time.Time
}

// FetchWorkItems ports queries.py:95-127 fetch_work_items.
//
// An empty id list returns no rows WITHOUT querying, matching Python — an
// unguarded `IN ()` is both wasteful and a different query.
func (reader *Reader) FetchWorkItems(ctx context.Context, workItemIDs []string, organizationID string) ([]WorkItem, error) {
	if reader == nil || reader.conn == nil {
		return nil, ErrUnavailable
	}
	ids := dedupeStrings(workItemIDs)
	if len(ids) == 0 {
		return nil, nil
	}

	whereSQL := "WHERE work_item_id IN {work_item_ids:Array(String)}"
	arguments := []any{clickhouse.Named("work_item_ids", ids)}
	if organizationID != "" {
		whereSQL += " AND org_id = {org_id:String}"
		arguments = append(arguments, clickhouse.Named("org_id", organizationID))
	}

	query := fmt.Sprintf(`
        SELECT
            work_item_id,
            provider,
            toString(repo_id) AS repo_id,
            title,
            description,
            type,
            labels,
            parent_id,
            epic_id,
            created_at,
            updated_at,
            completed_at
        FROM %s
        %s
    `, workItemsDeduped, whereSQL)

	rows, err := reader.conn.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query work_items: %w", err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]WorkItem, 0, len(ids))
	for rows.Next() {
		var (
			item        WorkItem
			description *string // Nullable(String)
			completedAt *time.Time
		)
		if err := rows.Scan(
			&item.WorkItemID, &item.Provider, &item.RepoID, &item.Title,
			&description, &item.Type, &item.Labels, &item.ParentID, &item.EpicID,
			&item.CreatedAt, &item.UpdatedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan work_items row: %w", err)
		}
		if description != nil {
			item.Description = *description
		}
		item.CreatedAt = normalizeTimestamp(item.CreatedAt)
		item.UpdatedAt = normalizeTimestamp(item.UpdatedAt)
		item.CompletedAt = normalizeOptionalTimestamp(completedAt)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_items rows: %w", err)
	}
	return items, nil
}

// FetchParentTitles ports queries.py:129-158 fetch_parent_titles.
//
// Python drops rows with a falsy work_item_id OR a falsy title, so an empty
// title is ABSENT from the map rather than mapped to "". That distinction is
// visible downstream: the text bundle asks whether a parent title exists.
func (reader *Reader) FetchParentTitles(ctx context.Context, workItemIDs []string, organizationID string) (map[string]string, error) {
	if reader == nil || reader.conn == nil {
		return nil, ErrUnavailable
	}
	ids := dedupeStrings(workItemIDs)
	if len(ids) == 0 {
		return map[string]string{}, nil
	}

	whereSQL := "WHERE work_item_id IN {work_item_ids:Array(String)}"
	arguments := []any{clickhouse.Named("work_item_ids", ids)}
	if organizationID != "" {
		whereSQL += " AND org_id = {org_id:String}"
		arguments = append(arguments, clickhouse.Named("org_id", organizationID))
	}

	query := fmt.Sprintf("\n        SELECT work_item_id, title\n        FROM %s\n        %s\n    ", workItemsDeduped, whereSQL)

	rows, err := reader.conn.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query work_items titles: %w", err)
	}
	defer func() { _ = rows.Close() }()

	titles := make(map[string]string, len(ids))
	for rows.Next() {
		var workItemID, title string
		if err := rows.Scan(&workItemID, &title); err != nil {
			return nil, fmt.Errorf("scan work_items title row: %w", err)
		}
		if workItemID == "" || title == "" {
			continue
		}
		titles[workItemID] = title
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_items title rows: %w", err)
	}
	return titles, nil
}

// FetchWorkItemActiveHours ports queries.py:160-186 fetch_work_item_active_hours.
//
// CHAOS-4804: the org filter is CONDITIONAL but the GROUP BY is on
// work_item_id ALONE, not (org_id, work_item_id). With an org the two agree;
// with an empty org the filter disappears and the grouping collapses every
// org's rows for a work-item id into one, returning whichever tenant wrote
// last. work_item_id is provider-scoped, not tenant-scoped, so that is a real
// cross-tenant read.
//
// Python's shape is preserved here DELIBERATELY. Fixing it on the Go side
// alone would be an unflagged behaviour divergence between two planes that
// must group identically, which is the failure mode this whole port exists to
// avoid. CHAOS-4804 carries the fix for both planes at once — including that
// the same-named API-plane function (api/queries/work_units.py:76) filters
// UNCONDITIONALLY and therefore fails the OPPOSITE way on the same input.
func (reader *Reader) FetchWorkItemActiveHours(ctx context.Context, workItemIDs []string, organizationID string) (map[string]float64, error) {
	if reader == nil || reader.conn == nil {
		return nil, ErrUnavailable
	}
	ids := dedupeStrings(workItemIDs)
	if len(ids) == 0 {
		return map[string]float64{}, nil
	}

	whereSQL := "WHERE work_item_id IN {work_item_ids:Array(String)}"
	arguments := []any{clickhouse.Named("work_item_ids", ids)}
	if organizationID != "" {
		whereSQL += " AND org_id = {org_id:String}"
		arguments = append(arguments, clickhouse.Named("org_id", organizationID))
	}

	query := fmt.Sprintf(`
        SELECT
            work_item_id,
            argMax(active_time_hours, computed_at) AS active_time_hours
        FROM work_item_cycle_times
        %s
        GROUP BY work_item_id
    `, whereSQL)

	rows, err := reader.conn.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query work_item_cycle_times: %w", err)
	}
	defer func() { _ = rows.Close() }()

	hours := make(map[string]float64, len(ids))
	for rows.Next() {
		var (
			workItemID string
			active     float64
		)
		if err := rows.Scan(&workItemID, &active); err != nil {
			return nil, fmt.Errorf("scan work_item_cycle_times row: %w", err)
		}
		hours[workItemID] = active
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_item_cycle_times rows: %w", err)
	}
	return hours, nil
}

// PullRequest is one git_pull_requests row.
type PullRequest struct {
	RepoID    string
	Number    uint32
	Title     string
	Body      string
	CreatedAt time.Time
	MergedAt  *time.Time
	ClosedAt  *time.Time
	// Additions/Deletions are Nullable(UInt32); Python coerces NULL to 0.0 via
	// _float_value, so a missing churn value contributes nothing rather than
	// excluding the PR.
	Additions float64
	Deletions float64
}

// FetchPullRequests ports queries.py:188-236 fetch_pull_requests.
//
// One query PER REPO, matching Python's loop, rather than a single tuple-IN.
// The shapes are not equivalent: a combined query would need (repo_id, number)
// pairs, and a naive cross-product of repo ids and numbers would return PRs
// that belong to neither request. Kept per-repo so the semantics are obviously
// identical.
func (reader *Reader) FetchPullRequests(ctx context.Context, repoNumbers map[string][]uint32, organizationID string) ([]PullRequest, error) {
	if reader == nil || reader.conn == nil {
		return nil, ErrUnavailable
	}

	pullRequests := make([]PullRequest, 0)
	for _, repoID := range sortedRepoKeys(repoNumbers) {
		numbers := repoNumbers[repoID]
		if len(numbers) == 0 {
			continue
		}

		whereSQL := "WHERE repo_id = {repo_id:String} AND number IN {numbers:Array(UInt32)}"
		arguments := []any{
			clickhouse.Named("repo_id", repoID),
			clickhouse.Named("numbers", numbers),
		}
		if organizationID != "" {
			whereSQL += " AND org_id = {org_id:String}"
			arguments = append(arguments, clickhouse.Named("org_id", organizationID))
		}

		query := fmt.Sprintf(`
            SELECT
                toString(repo_id) AS repo_id,
                number,
                title,
                body,
                created_at,
                merged_at,
                closed_at,
                additions,
                deletions
            FROM git_pull_requests
            %s
        `, whereSQL)

		rows, err := reader.conn.Query(ctx, query, arguments...)
		if err != nil {
			return nil, fmt.Errorf("query git_pull_requests for repo %s: %w", repoID, err)
		}

		for rows.Next() {
			var (
				pullRequest PullRequest
				title       *string
				body        *string
				mergedAt    *time.Time
				closedAt    *time.Time
				additions   *uint32
				deletions   *uint32
			)
			if err := rows.Scan(
				&pullRequest.RepoID, &pullRequest.Number, &title, &body,
				&pullRequest.CreatedAt, &mergedAt, &closedAt, &additions, &deletions,
			); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan git_pull_requests row: %w", err)
			}
			if title != nil {
				pullRequest.Title = *title
			}
			if body != nil {
				pullRequest.Body = *body
			}
			if additions != nil {
				pullRequest.Additions = float64(*additions)
			}
			if deletions != nil {
				pullRequest.Deletions = float64(*deletions)
			}
			pullRequest.CreatedAt = normalizeTimestamp(pullRequest.CreatedAt)
			pullRequest.MergedAt = normalizeOptionalTimestamp(mergedAt)
			pullRequest.ClosedAt = normalizeOptionalTimestamp(closedAt)
			pullRequests = append(pullRequests, pullRequest)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("iterate git_pull_requests rows: %w", err)
		}
		_ = rows.Close()
	}
	return pullRequests, nil
}

// Commit is one git_commits row.
type Commit struct {
	RepoID        string
	Hash          string
	Message       string
	AuthorWhen    time.Time
	CommitterWhen time.Time
}

// FetchCommits ports queries.py:238-276 fetch_commits.
func (reader *Reader) FetchCommits(ctx context.Context, repoCommits map[string][]string, organizationID string) ([]Commit, error) {
	if reader == nil || reader.conn == nil {
		return nil, ErrUnavailable
	}

	commits := make([]Commit, 0)
	for _, repoID := range sortedRepoKeys(repoCommits) {
		hashes := repoCommits[repoID]
		if len(hashes) == 0 {
			continue
		}

		whereSQL := "WHERE repo_id = {repo_id:String} AND hash IN {hashes:Array(String)}"
		arguments := []any{
			clickhouse.Named("repo_id", repoID),
			clickhouse.Named("hashes", hashes),
		}
		if organizationID != "" {
			whereSQL += " AND org_id = {org_id:String}"
			arguments = append(arguments, clickhouse.Named("org_id", organizationID))
		}

		query := fmt.Sprintf(`
            SELECT
                toString(repo_id) AS repo_id,
                hash,
                message,
                author_when,
                committer_when
            FROM git_commits
            %s
        `, whereSQL)

		rows, err := reader.conn.Query(ctx, query, arguments...)
		if err != nil {
			return nil, fmt.Errorf("query git_commits for repo %s: %w", repoID, err)
		}

		for rows.Next() {
			var (
				commit  Commit
				message *string // Nullable(String)
			)
			if err := rows.Scan(
				&commit.RepoID, &commit.Hash, &message,
				&commit.AuthorWhen, &commit.CommitterWhen,
			); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan git_commits row: %w", err)
			}
			if message != nil {
				commit.Message = *message
			}
			commit.AuthorWhen = normalizeTimestamp(commit.AuthorWhen)
			commit.CommitterWhen = normalizeTimestamp(commit.CommitterWhen)
			commits = append(commits, commit)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("iterate git_commits rows: %w", err)
		}
		_ = rows.Close()
	}
	return commits, nil
}

// FetchCommitChurn ports queries.py:278-318 fetch_commit_churn.
//
// The returned map is keyed "{repo_id}@{hash}" — the canonical commit node id —
// so it joins directly against a component's commit nodes.
func (reader *Reader) FetchCommitChurn(ctx context.Context, repoCommits map[string][]string, organizationID string) (map[string]float64, error) {
	if reader == nil || reader.conn == nil {
		return nil, ErrUnavailable
	}

	churn := make(map[string]float64)
	for _, repoID := range sortedRepoKeys(repoCommits) {
		hashes := repoCommits[repoID]
		if len(hashes) == 0 {
			continue
		}

		whereSQL := "WHERE repo_id = {repo_id:String} AND commit_hash IN {hashes:Array(String)}"
		arguments := []any{
			clickhouse.Named("repo_id", repoID),
			clickhouse.Named("hashes", hashes),
		}
		if organizationID != "" {
			whereSQL += " AND org_id = {org_id:String}"
			arguments = append(arguments, clickhouse.Named("org_id", organizationID))
		}

		// additions/deletions are Int32 and non-nullable, so sum() is Int64 and
		// cannot be NULL for a present group. An ABSENT group contributes no
		// row at all, which is why the caller's lookup must default to 0
		// rather than expecting a key.
		query := fmt.Sprintf(`
            SELECT
                commit_hash,
                sum(additions) + sum(deletions) AS churn_loc
            FROM git_commit_stats
            %s
            GROUP BY commit_hash
        `, whereSQL)

		rows, err := reader.conn.Query(ctx, query, arguments...)
		if err != nil {
			return nil, fmt.Errorf("query git_commit_stats for repo %s: %w", repoID, err)
		}

		for rows.Next() {
			var (
				commitHash string
				churnLOC   int64
			)
			if err := rows.Scan(&commitHash, &churnLOC); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan git_commit_stats row: %w", err)
			}
			churn[repoID+"@"+commitHash] = float64(churnLOC)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("iterate git_commit_stats rows: %w", err)
		}
		_ = rows.Close()
	}
	return churn, nil
}

// ResolveRepoIDsForTeams ports queries.py:320-347 resolve_repo_ids_for_teams.
func (reader *Reader) ResolveRepoIDsForTeams(ctx context.Context, teamIDs []string, organizationID string) ([]string, error) {
	if reader == nil || reader.conn == nil {
		return nil, ErrUnavailable
	}
	teams := make([]string, 0, len(teamIDs))
	for _, teamID := range teamIDs {
		if teamID != "" {
			teams = append(teams, teamID)
		}
	}
	if len(teams) == 0 {
		return nil, nil
	}

	whereSQL := "WHERE team_id IN {team_ids:Array(String)}"
	arguments := []any{clickhouse.Named("team_ids", teams)}
	if organizationID != "" {
		whereSQL += " AND org_id = {org_id:String}"
		arguments = append(arguments, clickhouse.Named("org_id", organizationID))
	}

	query := fmt.Sprintf(`
        SELECT DISTINCT toString(repo_id) AS id
        FROM user_metrics_daily
        %s
    `, whereSQL)

	rows, err := reader.conn.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("query user_metrics_daily: %w", err)
	}
	defer func() { _ = rows.Close() }()

	repoIDs := make([]string, 0)
	for rows.Next() {
		var repoID string
		if err := rows.Scan(&repoID); err != nil {
			return nil, fmt.Errorf("scan user_metrics_daily row: %w", err)
		}
		if repoID == "" {
			continue
		}
		repoIDs = append(repoIDs, repoID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate user_metrics_daily rows: %w", err)
	}
	return repoIDs, nil
}
