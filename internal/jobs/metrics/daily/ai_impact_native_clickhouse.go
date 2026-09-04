package daily

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiimpact"
)

// The dedup rule for every reader in this file, stated once.
//
// Each source table is a ReplacingMergeTree(last_synced), so `FINAL` and
// `argMax(..., last_synced) GROUP BY <the table's exact ORDER BY key>` select
// the same row -- but argMax does it merge-independently, which is why the
// standing rule prefers it and forbids FINAL underneath an aggregation.
//
// EVERY dedup below takes ONE argMax over a tuple and unwraps it with
// tupleElement. One argMax PER COLUMN is wrong: two aggregates over the same
// GROUP BY resolve ties independently, so on a shared last_synced they can
// take values from DIFFERENT physical rows and emit a row that never existed.
// This file was written after that defect was found in
// ai_governance_native_clickhouse.go; see that file's doc comment and
// TestGovernanceDedupPicksOneWholeRowOnALastSyncedTie for the full account.
// The tuple form also preserves NULLs, which a bare argMax over a Nullable
// column silently skips.

// LoadAIImpactPullRequests reads the PR rows compute_ai_impact_metrics_daily
// consumes, over Python's window: created_at in [start, end) OR merged_at
// non-null and in [start, end) (ai_impact.py:344-350 filters again in-memory
// on event_at, so the SQL window is the same predicate pushed down).
//
// org_id is carried through the dedup and filtered AFTER it, for the reason
// spelled out in ai_governance's loader: migration 024 added org_id as a plain
// column, NOT part of any sorting key, so two rows sharing a dedup key can
// disagree on it and the FINAL-equivalent answer is the winning row's value.
func LoadAIImpactPullRequests(
	ctx context.Context, conn repositoryRows, organizationID string,
	repoIDs []uuid.UUID, start, end time.Time,
) ([]aiimpact.PullRequestRow, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !start.Before(end) {
		return nil, ErrInvalidState
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}

	rows, err := conn.Query(ctx, `
SELECT repo_id, number, created_at, merged_at, reviews_count,
       changes_requested_count, additions, deletions, changed_files
FROM (
    SELECT
        repo_id,
        number,
        tupleElement(latest, 1) AS created_at,
        tupleElement(latest, 2) AS merged_at,
        tupleElement(latest, 3) AS reviews_count,
        tupleElement(latest, 4) AS changes_requested_count,
        tupleElement(latest, 5) AS additions,
        tupleElement(latest, 6) AS deletions,
        tupleElement(latest, 7) AS changed_files,
        tupleElement(latest, 8) AS org_id
    FROM (
        SELECT
            repo_id,
            number,
            argMax(
                tuple(created_at, merged_at, reviews_count, changes_requested_count,
                      additions, deletions, changed_files, org_id),
                last_synced
            ) AS latest
        FROM git_pull_requests
        WHERE repo_id IN ?
        GROUP BY repo_id, number
    )
)
WHERE org_id = ?
  AND ((created_at >= ? AND created_at < ?)
    OR (merged_at IS NOT NULL AND merged_at >= ? AND merged_at < ?))
ORDER BY repo_id, number`,
		repositoryUUIDStrings(repoIDs), organizationID,
		start.UTC(), end.UTC(), start.UTC(), end.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load ai impact pull requests: %w", err)
	}
	defer rows.Close()

	var result []aiimpact.PullRequestRow
	for rows.Next() {
		var (
			repoID                uuid.UUID
			number                uint32
			createdAt             time.Time
			mergedAt              *time.Time
			reviewsCount          uint32
			changesRequestedCount uint32
			additions             *uint32
			deletions             *uint32
			changedFiles          *uint32
		)
		if err := rows.Scan(&repoID, &number, &createdAt, &mergedAt, &reviewsCount,
			&changesRequestedCount, &additions, &deletions, &changedFiles); err != nil {
			return nil, fmt.Errorf("scan ai impact pull request row: %w", err)
		}
		// reviews_count / changes_requested_count are non-nullable UInt32 with
		// a DEFAULT 0, so they arrive as plain values. They are handed on as
		// POINTERS because compute must distinguish "column present and
		// non-zero" from "zero", which is what Python's
		// `int(pr.get(..., derived) or derived)` truthiness turns into a
		// fallback to the review-derived count.
		reviews, changesRequested := reviewsCount, changesRequestedCount
		result = append(result, aiimpact.PullRequestRow{
			RepoID: repoID, Number: int64(number), CreatedAt: createdAt, MergedAt: mergedAt,
			ReviewsCount: &reviews, ChangesRequestedCount: &changesRequested,
			Additions: additions, Deletions: deletions, ChangedFiles: changedFiles,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact pull request rows: %w", err)
	}
	return result, nil
}

// LoadAIImpactReviews reads git_pull_request_reviews for the window, deduped
// to the table's ORDER BY key (repo_id, number, review_id).
func LoadAIImpactReviews(
	ctx context.Context, conn repositoryRows, organizationID string,
	repoIDs []uuid.UUID, start, end time.Time,
) ([]aiimpact.PullRequestReviewRow, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !start.Before(end) {
		return nil, ErrInvalidState
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}

	rows, err := conn.Query(ctx, `
SELECT repo_id, number, state, submitted_at
FROM (
    SELECT
        repo_id,
        number,
        review_id,
        tupleElement(latest, 1) AS state,
        tupleElement(latest, 2) AS submitted_at,
        tupleElement(latest, 3) AS org_id
    FROM (
        SELECT
            repo_id,
            number,
            review_id,
            argMax(tuple(state, submitted_at, org_id), last_synced) AS latest
        FROM git_pull_request_reviews
        WHERE repo_id IN ?
        GROUP BY repo_id, number, review_id
    )
)
WHERE org_id = ? AND submitted_at >= ? AND submitted_at < ?
ORDER BY repo_id, number, review_id`,
		repositoryUUIDStrings(repoIDs), organizationID, start.UTC(), end.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load ai impact reviews: %w", err)
	}
	defer rows.Close()

	var result []aiimpact.PullRequestReviewRow
	for rows.Next() {
		var (
			repoID      uuid.UUID
			number      uint32
			state       string
			submittedAt time.Time
		)
		if err := rows.Scan(&repoID, &number, &state, &submittedAt); err != nil {
			return nil, fmt.Errorf("scan ai impact review row: %w", err)
		}
		reviewState, submitted := state, submittedAt
		result = append(result, aiimpact.PullRequestReviewRow{
			RepoID: repoID, Number: int64(number),
			State: &reviewState, SubmittedAt: &submitted,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact review rows: %w", err)
	}
	return result, nil
}

// LoadAIImpactAttributions ports load_ai_pr_attributions
// (metrics/loaders/ai_impact.py:22) -- a UNION ALL of two linkage paths,
// ordered, then deduped to the FIRST row per (repo_id, number).
//
// Path 1 reaches the attribution through work_graph_issue_pr, so the work item
// supplies work_type; path 2 matches the attribution directly against the PR
// number (or a "<repo>#<number>" subject id) and hard-codes "pull_request".
//
// # The dedup is FIRST-ROW-WINS, and its order is only partly determined
//
// Python collects into `seen` and skips repeats, so the FIRST row of the
// ordered UNION wins. The ORDER BY is
// `merged_at DESC NULLS LAST, repo_id, number DESC` -- but for a PR matched by
// BOTH paths every one of those keys is IDENTICAL, so the tie is not broken by
// the query at all and ClickHouse may return either path first. Python's
// work_type for such a PR is therefore already nondeterministic.
//
// This loader adds `work_type` as a final, explicit tie-break so the Go answer
// is stable. It changes nothing for PRs matched by a single path (the common
// case, and the only case the oracle fixture exercises); for a doubly-matched
// PR it picks deterministically where Python picks arbitrarily. Recorded here
// rather than silently inherited, per the rule that a port cannot reproduce an
// order the reference does not have.
//
// team_id is projected as a literal ” by both paths and then normalised to
// None by the loader's own `raw.get("team_id") or None` -- so it is NEVER a
// real value, and compute's team_resolver branch is taken for every attributed
// PR. It is therefore not read here at all; see aiimpact.RepoPatternResolver.
func LoadAIImpactAttributions(
	ctx context.Context, conn repositoryRows, organizationID string,
	repoIDs []uuid.UUID, start, end time.Time,
) ([]aiimpact.AttributionRow, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !start.Before(end) {
		return nil, ErrInvalidState
	}
	if len(repoIDs) == 0 {
		return nil, nil
	}

	rows, err := conn.Query(ctx, `
SELECT repo_id, number, kind, work_type
FROM (
    SELECT
        pr.repo_id AS repo_id,
        pr.number AS number,
        attr.kind AS kind,
        coalesce(nullIf(wi.type, ''), 'pull_request') AS work_type,
        pr.merged_at AS merged_at
    FROM git_pull_requests AS pr
    INNER JOIN work_graph_issue_pr AS link
        ON link.repo_id = pr.repo_id AND link.pr_number = pr.number
    INNER JOIN ai_attribution_resolved AS attr
        ON attr.subject_type = 'pull_request'
        AND attr.subject_id = link.work_item_id
        AND attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
    LEFT JOIN work_items AS wi FINAL
        ON wi.repo_id = link.repo_id AND wi.work_item_id = link.work_item_id
    WHERE pr.repo_id IN ?
      AND pr.org_id = ?
      AND toString(attr.org_id) = ?
      AND ((pr.created_at >= ? AND pr.created_at < ?)
        OR (pr.merged_at IS NOT NULL AND pr.merged_at >= ? AND pr.merged_at < ?))
    UNION ALL
    SELECT
        pr.repo_id AS repo_id,
        pr.number AS number,
        attr.kind AS kind,
        'pull_request' AS work_type,
        pr.merged_at AS merged_at
    FROM git_pull_requests AS pr
    INNER JOIN ai_attribution_resolved AS attr
        ON attr.subject_type = 'pull_request'
        AND attr.repo_id = pr.repo_id
        AND (attr.subject_id = toString(pr.number)
             OR attr.subject_id = concat(toString(pr.repo_id), '#', toString(pr.number)))
        AND attr.kind IN ('ai_assisted', 'agent_created', 'ai_review')
    WHERE pr.repo_id IN ?
      AND pr.org_id = ?
      AND toString(attr.org_id) = ?
      AND ((pr.created_at >= ? AND pr.created_at < ?)
        OR (pr.merged_at IS NOT NULL AND pr.merged_at >= ? AND pr.merged_at < ?))
)
ORDER BY merged_at DESC NULLS LAST, repo_id, number DESC, work_type`,
		repositoryUUIDStrings(repoIDs), organizationID, organizationID,
		start.UTC(), end.UTC(), start.UTC(), end.UTC(),
		repositoryUUIDStrings(repoIDs), organizationID, organizationID,
		start.UTC(), end.UTC(), start.UTC(), end.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load ai impact attributions: %w", err)
	}
	defer rows.Close()

	type attributionKey struct {
		repoID uuid.UUID
		number uint32
	}
	seen := make(map[attributionKey]struct{})
	var result []aiimpact.AttributionRow
	for rows.Next() {
		var (
			repoID   uuid.UUID
			number   uint32
			kind     string
			workType string
		)
		if err := rows.Scan(&repoID, &number, &kind, &workType); err != nil {
			return nil, fmt.Errorf("scan ai impact attribution row: %w", err)
		}
		key := attributionKey{repoID: repoID, number: number}
		// FIRST row wins, mirroring Python's `seen` set.
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		attributionKind, attributionWorkType := kind, workType
		result = append(result, aiimpact.AttributionRow{
			RepoID: repoID, Number: int64(number),
			Kind: &attributionKind, WorkType: &attributionWorkType,
			// Always nil: see this function's doc comment.
			TeamID: nil,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact attribution rows: %w", err)
	}
	return result, nil
}

// LoadAIImpactPRCommitLinkage ports the pr_commit_stats build
// (job_daily.py:1755-1797): work_graph_pr_commit LEFT JOINed to
// git_commit_stats for file paths and to git_commits for committer_when.
//
// A non-nil return means the linkage was AVAILABLE, even when empty -- the
// caller must pass that distinction through, because nil means "unknown" and
// makes test_gap_rate null rather than 100% (CHAOS-2183).
//
// git_commit_stats carries no org_id column, so its join stays on
// (repo_id, commit_hash); the work_graph_pr_commit side is already org-scoped
// by the WHERE clause, exactly as in Python.
func LoadAIImpactPRCommitLinkage(
	ctx context.Context, conn repositoryRows, organizationID string,
	repoIDs []uuid.UUID, prNumbers []uint32,
) (map[aiimpact.PRKey][]aiimpact.CommitStatRow, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	// Available-but-empty, NOT unknown: Python sets `pr_commit_stats = {}`
	// when there are no in-window PRs and only leaves it None on an exception.
	linkage := make(map[aiimpact.PRKey][]aiimpact.CommitStatRow)
	if len(repoIDs) == 0 || len(prNumbers) == 0 {
		return linkage, nil
	}

	rows, err := conn.Query(ctx, `
SELECT p.repo_id, p.pr_number, p.commit_hash, p.evidence, c.committer_when, s.file_path
FROM work_graph_pr_commit AS p
LEFT JOIN git_commit_stats AS s
    ON s.repo_id = p.repo_id AND s.commit_hash = p.commit_hash
LEFT JOIN git_commits AS c
    ON c.repo_id = p.repo_id AND c.hash = p.commit_hash AND c.org_id = p.org_id
WHERE p.org_id = ? AND p.repo_id IN ? AND p.pr_number IN ?`,
		organizationID, repositoryUUIDStrings(repoIDs), prNumbers,
	)
	if err != nil {
		return nil, fmt.Errorf("load ai impact pr commit linkage: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			repoID        uuid.UUID
			prNumber      uint32
			commitHash    string
			evidence      string
			committerWhen *time.Time
			filePath      *string
		)
		if err := rows.Scan(&repoID, &prNumber, &commitHash, &evidence, &committerWhen, &filePath); err != nil {
			return nil, fmt.Errorf("scan ai impact pr commit linkage row: %w", err)
		}
		key := aiimpact.PRKey{RepoID: repoID, Number: int64(prNumber)}
		hash, linkEvidence := commitHash, evidence
		linkage[key] = append(linkage[key], aiimpact.CommitStatRow{
			FilePath: filePath, CommitHash: &hash,
			CommitterWhen: committerWhen, Evidence: &linkEvidence,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact pr commit linkage rows: %w", err)
	}
	return linkage, nil
}

// LoadAIImpactTeams reads the teams rows build_repo_pattern_resolver consumes
// (job_daily.py:1178 `get_all_teams`). Deduped to the table's ORDER BY key
// (id) with one tuple argMax over ReplacingMergeTree(updated_at).
//
// Ordered by id so the resolver's build order -- and therefore its stable
// tie-break between equal-length prefixes -- is deterministic. Python's
// get_all_teams has no such guarantee; see RepoPatternResolver's doc comment.
func LoadAIImpactTeams(
	ctx context.Context, conn repositoryRows, organizationID string,
) ([]aiimpact.Team, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	rows, err := conn.Query(ctx, `
SELECT id, name, repo_patterns
FROM (
    SELECT
        id,
        tupleElement(latest, 1) AS name,
        tupleElement(latest, 2) AS repo_patterns,
        tupleElement(latest, 3) AS is_active,
        tupleElement(latest, 4) AS org_id
    FROM (
        SELECT id, argMax(tuple(name, repo_patterns, is_active, org_id), updated_at) AS latest
        FROM teams
        GROUP BY id
    )
)
WHERE org_id = ? AND is_active = 1
ORDER BY id`, organizationID)
	if err != nil {
		return nil, fmt.Errorf("load ai impact teams: %w", err)
	}
	defer rows.Close()

	var result []aiimpact.Team
	for rows.Next() {
		var (
			id           string
			name         string
			repoPatterns []string
		)
		if err := rows.Scan(&id, &name, &repoPatterns); err != nil {
			return nil, fmt.Errorf("scan ai impact team row: %w", err)
		}
		result = append(result, aiimpact.Team{ID: id, Name: name, RepoPatterns: repoPatterns})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact team rows: %w", err)
	}
	return result, nil
}

// LoadAIImpactRepoNames reads the repo full names the team resolver matches
// against, using discover_repos' own dedup shape (job_daily.py:176-189): ONE
// argMax over a tuple, so every projected value comes from the same winning
// physical row.
func LoadAIImpactRepoNames(
	ctx context.Context, conn repositoryRows, organizationID string, repoIDs []uuid.UUID,
) (map[uuid.UUID]string, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	names := make(map[uuid.UUID]string)
	if len(repoIDs) == 0 {
		return names, nil
	}
	rows, err := conn.Query(ctx, `
SELECT id, tupleElement(latest, 1) AS repo
FROM (
    SELECT id, argMax(tuple(repo, org_id), last_synced) AS latest
    FROM repos
    WHERE id IN ?
    GROUP BY org_id, id
)
WHERE tupleElement(latest, 2) = ?`,
		repositoryUUIDStrings(repoIDs), organizationID)
	if err != nil {
		return nil, fmt.Errorf("load ai impact repo names: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id   uuid.UUID
			repo string
		)
		if err := rows.Scan(&id, &repo); err != nil {
			return nil, fmt.Errorf("scan ai impact repo name row: %w", err)
		}
		names[id] = repo
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai impact repo name rows: %w", err)
	}
	return names, nil
}

// aiImpactBatchConn is the narrow write capability WriteAIImpactMetrics needs.
type aiImpactBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// WriteAIImpactMetrics ports write_ai_impact_metrics
// (sinks/clickhouse/ai_impact.py:58) -- same table, same column order as its
// _COLUMNS list.
//
// team_id is written as `row.team_id or ""` (sinks/clickhouse/ai_impact.py:67)
// because the column is a non-nullable String with an empty-string sentinel.
// That coercion is Python's and is reproduced exactly; note it means a nil
// team and an empty-string team collapse to the SAME ORDER BY key, which is a
// pre-existing property of the schema, not of this port.
func WriteAIImpactMetrics(
	ctx context.Context, conn aiImpactBatchConn,
	records []aiimpact.Record, computedAt time.Time,
) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}
	if conn == nil {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO ai_impact_metrics_daily (
		org_id, team_id, repo_id, work_type, day, attribution_bucket,
		prs_total, prs_merged, ai_assisted_prs, agent_created_prs, human_prs, unknown_prs,
		ai_assisted_pr_ratio, agent_created_pr_count,
		cycle_time_avg_hours, baseline_cycle_time_avg_hours, ai_cycle_time_delta_hours,
		reviews_per_pr, baseline_reviews_per_pr, ai_review_amplification,
		changes_requested_per_pr, rework_prs, rework_drag_rate, followup_commits_count,
		revert_prs, revert_rate, incidents_count, incident_drag_rate,
		test_gap_prs, test_gap_rate,
		leverage_prs_component, leverage_cycle_time_component, leverage_review_component,
		leverage_rework_component, leverage_test_component, leverage_incident_component,
		computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare ai_impact_metrics_daily batch: %w", err)
	}
	computedAtUTC := computedAt.UTC()
	for _, record := range records {
		teamID := ""
		if record.TeamID != nil {
			teamID = *record.TeamID
		}
		if err := batch.Append(
			record.OrgID, teamID, record.RepoID, record.WorkType, record.Day,
			string(record.AttributionBucket),
			record.PRsTotal, record.PRsMerged, record.AIAssistedPRs, record.AgentCreatedPRs,
			record.HumanPRs, record.UnknownPRs,
			record.AIAssistedPRRatio, record.AgentCreatedPRCount,
			record.CycleTimeAvgHours, record.BaselineCycleTimeAvgHours, record.AICycleTimeDeltaHours,
			record.ReviewsPerPR, record.BaselineReviewsPerPR, record.AIReviewAmplification,
			record.ChangesRequestedPerPR, record.ReworkPRs, record.ReworkDragRate,
			record.FollowupCommitsCount, record.RevertPRs, record.RevertRate,
			record.IncidentsCount, record.IncidentDragRate,
			record.TestGapPRs, record.TestGapRate,
			record.LeveragePRsComponent, record.LeverageCycleTime, record.LeverageReview,
			record.LeverageRework, record.LeverageTest, record.LeverageIncident,
			computedAtUTC,
		); err != nil {
			return 0, fmt.Errorf("append ai_impact_metrics_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send ai_impact_metrics_daily batch: %w", err)
	}
	return len(records), nil
}
