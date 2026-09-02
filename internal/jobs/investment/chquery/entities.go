package chquery

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// workItemsDeduped mirrors Python's WORK_ITEMS_DEDUPED
// (metrics/sinks/clickhouse/idempotency.py:5). work_items IS read with FINAL,
// unlike git_commits/git_pull_requests/git_commit_stats below, which are not --
// that asymmetry is Python's and is preserved deliberately.
const workItemsDeduped = "work_items FINAL"

// normalizeTimestamp puts a scanned timestamp on UTC, matching what
// evidence._ensure_utc produces for the same row.
//
// # THIS CONVERTS. IT USED TO REINTERPRET, AND THAT WAS WRONG.
//
// PR2 shipped this as a wall-clock REBUILD -- time.Date(y, m, d, h, ...,
// time.UTC) -- deliberately modelling Python's `.replace(tzinfo=utc)`. That
// choice was made by reading _ensure_utc's naive branch and assuming the driver
// returned naive values. Measured against a real container with a non-UTC
// server timezone (Asia/Kolkata, +05:30), the assumption was backwards:
//
//	column                    python driver returns    _ensure_utc does
//	DateTime64(3)   (no tz)   AWARE, in the SERVER tz  .astimezone() CONVERTS
//	DateTime64(3,'UTC')       NAIVE                    .replace()    REINTERPRETS
//
// The column WITHOUT a declared timezone comes back timezone-AWARE, and the one
// WITH 'UTC' comes back naive. So the reinterpreting version was wrong for
// exactly the columns it looked right for:
//
//	work_items.created_at   DateTime64(3)        reinterpret -> off by 5h30m
//	git_commits.author_when DateTime64(3,'UTC')  reinterpret -> correct
//
// work_items is the naive-declared table, so PR2's version silently shifted
// every work-item timestamp by the ClickHouse server's UTC offset. Measured
// epochs for one row seeded at '2026-09-02 10:30:00' in both columns:
//
//	                        python _ensure_utc   reinterpret      .UTC()
//	DateTime64(3)           1788325200000        1788345000000    1788325200000
//	DateTime64(3,'UTC')     1788345000000        1788345000000    1788345000000
//
// .UTC() agrees with Python on BOTH, because the two drivers return the same
// INSTANT and differ only in the location attached to it. Converting preserves
// the instant; rebuilding the wall clock changes it whenever that location is
// not already UTC.
//
// This also removes a dependency on the deployment: with
// apply_server_timezone=False the Python driver attaches the CLIENT's local
// zone instead of the server's, so a reinterpreting port would have been wrong
// by a per-machine offset. Converting is correct under every setting because it
// never reads the wall clock at all.
//
// CHAOS-4441 plan section 5f. The defect was carried by PR2; the fix rides PR3.
func normalizeTimestamp(value time.Time) time.Time {
	return value.UTC()
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
		// Python returns [] here, not None (queries.py:99-101). An empty slice
		// rather than nil keeps the shapes identical for any caller that
		// serialises or reflect-compares the result.
		return []WorkItem{}, nil
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
		// Every String column, matching the Python driver, which applies its
		// hex substitution to all of them rather than to a chosen subset. The
		// ids are included deliberately: work_item_id feeds work_unit_id, so a
		// value the two planes spell differently re-addresses the row.
		item.WorkItemID = pythonparity.DecodeClickHouseStringValue(item.WorkItemID)
		item.Provider = pythonparity.DecodeClickHouseStringValue(item.Provider)
		item.RepoID = pythonparity.DecodeClickHouseStringValue(item.RepoID)
		item.Title = pythonparity.DecodeClickHouseStringValue(item.Title)
		item.Description = pythonparity.DecodeClickHouseStringValue(item.Description)
		item.Type = pythonparity.DecodeClickHouseStringValue(item.Type)
		item.ParentID = pythonparity.DecodeClickHouseStringValue(item.ParentID)
		item.EpicID = pythonparity.DecodeClickHouseStringValue(item.EpicID)
		item.Labels = decodeClickHouseStrings(item.Labels)
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
		workItemID = pythonparity.DecodeClickHouseStringValue(workItemID)
		title = pythonparity.DecodeClickHouseStringValue(title)
		// The emptiness gate runs AFTER the substitution, as it does in Python:
		// an undecodable value becomes a non-empty hex string and is therefore
		// KEPT, not dropped.
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
		hours[pythonparity.DecodeClickHouseStringValue(workItemID)] = active
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
			pullRequest.RepoID = pythonparity.DecodeClickHouseStringValue(pullRequest.RepoID)
			pullRequest.Title = pythonparity.DecodeClickHouseStringValue(pullRequest.Title)
			pullRequest.Body = pythonparity.DecodeClickHouseStringValue(pullRequest.Body)
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
			commit.RepoID = pythonparity.DecodeClickHouseStringValue(commit.RepoID)
			commit.Hash = pythonparity.DecodeClickHouseStringValue(commit.Hash)
			commit.Message = pythonparity.DecodeClickHouseStringValue(commit.Message)
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
			churn[repoID+"@"+pythonparity.DecodeClickHouseStringValue(commitHash)] = float64(churnLOC)
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
		// Python returns [] here, not None (queries.py:322-324).
		return []string{}, nil
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
		repoID = pythonparity.DecodeClickHouseStringValue(repoID)
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

// decodeClickHouseStrings applies the driver's decode policy to every element
// of an Array(String).
//
// clickhouse-connect decodes array elements one at a time, so the substitution
// is per ELEMENT: one undecodable label is hexed while its neighbours are left
// alone. Hexing the whole array, or skipping arrays entirely, both diverge.
func decodeClickHouseStrings(values []string) []string {
	for index, value := range values {
		values[index] = pythonparity.DecodeClickHouseStringValue(value)
	}
	return values
}
