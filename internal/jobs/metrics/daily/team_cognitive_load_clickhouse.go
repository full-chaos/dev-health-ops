package daily

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// userMetricsCognitiveLoadInput is one deduped user_metrics_daily row's
// cognitive-load-relevant columns for one (repo_id, author_email) pair,
// this day. Mirrors what build_team_cognitive_load_rows_for_day reads via
// getattr(row, "repo_id"/"author_email"/"pr_interruption_load"/
// "review_request_load", 0).
type userMetricsCognitiveLoadInput struct {
	RepoID             uuid.UUID
	AuthorEmail        string
	PRInterruptionLoad float64
	ReviewRequestLoad  float64
}

// loadUserMetricsCognitiveLoadInputsForDay reads user_metrics_daily for
// (org_id, day), argMax(tuple(...), computed_at)-deduped per (repo_id,
// author_email) -- the SAME single-tuple-argMax discipline
// _fetch_repo_complexity_for_day documents (CHAOS-4365 codex R1: a per-column
// argMax can assemble a "Frankenstein" row from different physical rows when
// two writes tie on computed_at; a single tuple argMax always picks one).
func loadUserMetricsCognitiveLoadInputsForDay(
	ctx context.Context, conn driver.Conn, organizationID string, day time.Time,
) ([]userMetricsCognitiveLoadInput, error) {
	rows, err := conn.Query(ctx, `
SELECT
	repo_id,
	author_email,
	tupleElement(latest, 1) AS pr_interruption_load,
	tupleElement(latest, 2) AS review_request_load
FROM (
	SELECT
		repo_id,
		author_email,
		argMax(tuple(pr_interruption_load, review_request_load), computed_at) AS latest
	FROM user_metrics_daily
	WHERE org_id = ? AND day = ?
	GROUP BY repo_id, author_email
)`,
		organizationID, day,
	)
	if err != nil {
		return nil, fmt.Errorf("load user_metrics_daily for cognitive load: %w", err)
	}
	defer rows.Close()

	var result []userMetricsCognitiveLoadInput
	for rows.Next() {
		var row userMetricsCognitiveLoadInput
		var prInterruption, reviewRequest uint32
		if err := rows.Scan(&row.RepoID, &row.AuthorEmail, &prInterruption, &reviewRequest); err != nil {
			return nil, fmt.Errorf("scan user_metrics_daily cognitive load row: %w", err)
		}
		row.PRInterruptionLoad = float64(prInterruption)
		row.ReviewRequestLoad = float64(reviewRequest)
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate user_metrics_daily cognitive load rows: %w", err)
	}
	return result, nil
}

// teamMetricsCognitiveLoadInput is one deduped team_metrics_daily row's
// cognitive-load-relevant columns for one (team_id, repo_id) pair, this day
// -- ONE ROW PER REPO, per migration 080's reader contract. This executor's
// OWN aggregation (buildTeamCognitiveLoadRows, mirroring
// build_team_cognitive_load_rows_for_day) does the cross-repo SUM as it maps
// each repo to its team, so the query below stops at per-repo dedup and does
// not pre-sum -- summing here AND in the aggregator would double-count.
//
// team_metrics_daily's OWN team_id column is read only to know a legacy
// (pre-migration-080) bucket exists; it is NEVER used to resolve a team
// (CHAOS-4396) -- team resolution is repo_to_team's job.
type teamMetricsCognitiveLoadInput struct {
	RepoID                 uuid.UUID
	AfterHoursCommitsCount int
	WeekendCommitsCount    int
	CommitsCount           int
}

// loadTeamMetricsCognitiveLoadInputsForDay reads team_metrics_daily for
// (org_id, day), argMax(tuple(...), computed_at)-deduped per (team_id,
// repo_id) -- migration 080's own documented reader contract. A legacy row
// (repo_id = ”, pre-migration-080) is EXCLUDED here: it can never resolve to
// a repo_id, so it can never be ownership-attributed to a team by this
// executor -- the SAME skip build_team_cognitive_load_rows_for_day's own
// per-row loop applies ("Legacy \"\" sentinel ... skipped").
func loadTeamMetricsCognitiveLoadInputsForDay(
	ctx context.Context, conn driver.Conn, organizationID string, day time.Time,
) ([]teamMetricsCognitiveLoadInput, error) {
	rows, err := conn.Query(ctx, `
SELECT
	repo_id,
	tupleElement(latest, 1) AS after_hours_commits_count,
	tupleElement(latest, 2) AS weekend_commits_count,
	tupleElement(latest, 3) AS commits_count
FROM (
	SELECT
		team_id,
		repo_id,
		argMax(tuple(after_hours_commits_count, weekend_commits_count, commits_count), computed_at) AS latest
	FROM team_metrics_daily
	WHERE org_id = ? AND day = ? AND repo_id != ''
	GROUP BY team_id, repo_id
)`,
		organizationID, day,
	)
	if err != nil {
		return nil, fmt.Errorf("load team_metrics_daily for cognitive load: %w", err)
	}
	defer rows.Close()

	var result []teamMetricsCognitiveLoadInput
	for rows.Next() {
		var row teamMetricsCognitiveLoadInput
		var repoIDText string
		var afterHours, weekend, commits uint32
		if err := rows.Scan(&repoIDText, &afterHours, &weekend, &commits); err != nil {
			return nil, fmt.Errorf("scan team_metrics_daily cognitive load row: %w", err)
		}
		repoID, err := uuid.Parse(repoIDText)
		if err != nil {
			// A malformed repo_id can never be ownership-resolved -- skip
			// rather than fail the whole finalize on one bad row.
			continue
		}
		row.RepoID = repoID
		row.AfterHoursCommitsCount = int(afterHours)
		row.WeekendCommitsCount = int(weekend)
		row.CommitsCount = int(commits)
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate team_metrics_daily cognitive load rows: %w", err)
	}
	return result, nil
}

// teamCognitiveLoadRow is one team_cognitive_load_daily row this executor
// writes -- the Go mirror of TeamCognitiveLoadDailyRecord
// (schemas.py).
type teamCognitiveLoadRow struct {
	OrganizationID        string
	TeamID                string
	Day                   time.Time
	PRInterruptionLoad    float64
	ContextSpreadCount    float64
	ReviewRequestLoad     float64
	AfterHoursCommitRatio *float64
	WeekendCommitRatio    *float64
	ContributingRepoCount int
	SampleAuthorCount     int
	ComputedAt            time.Time
}

// contextSpreadPair mirrors Python's (author_email, repo_id) set: the
// team's true context-spread count is the number of DISTINCT pairs across
// its owned repos, never a sum of each author's own per-row
// context_spread_count (which is already that author's TOTAL distinct-repo
// count for the day, identical on every one of that author's per-repo rows
// -- codex R3 P2 on the Python side, preserved here by construction: this
// executor's query never even selects context_spread_count from
// user_metrics_daily, only derives it from row identity).
type contextSpreadPair struct {
	authorEmail string
	repoID      string
}

type teamCognitiveLoadBucket struct {
	prInterruptionLoad     float64
	reviewRequestLoad      float64
	afterHoursCommitsCount int
	weekendCommitsCount    int
	commitsCount           int
	hasTeamMetricsRow      bool
	repoIDs                map[string]struct{}
	authors                map[string]struct{}
	contextSpreadPairs     map[contextSpreadPair]struct{}
}

func newTeamCognitiveLoadBucket() *teamCognitiveLoadBucket {
	return &teamCognitiveLoadBucket{
		repoIDs:            map[string]struct{}{},
		authors:            map[string]struct{}{},
		contextSpreadPairs: map[contextSpreadPair]struct{}{},
	}
}

// buildTeamCognitiveLoadRows ports build_team_cognitive_load_rows_for_day
// (team_cognitive_load.py) exactly: bucket every user_metrics/team_metrics
// input row by its repo's resolved team, sum the additive counters, and
// recompute the after-hours/weekend ratios from the SUMMED counts (never
// averaged directly across repos). Deliberately never reads either input
// row type's own team_id (CHAOS-4396).
func buildTeamCognitiveLoadRows(
	organizationID string, day time.Time,
	userRows []userMetricsCognitiveLoadInput, teamRows []teamMetricsCognitiveLoadInput,
	repoToTeam map[string]string, computedAt time.Time,
) []teamCognitiveLoadRow {
	buckets := map[string]*teamCognitiveLoadBucket{}
	bucket := func(teamID string) *teamCognitiveLoadBucket {
		existing, ok := buckets[teamID]
		if !ok {
			existing = newTeamCognitiveLoadBucket()
			buckets[teamID] = existing
		}
		return existing
	}

	for _, row := range userRows {
		repoIDStr := row.RepoID.String()
		teamID, ok := repoToTeam[repoIDStr]
		if !ok || teamID == "" {
			continue
		}
		b := bucket(teamID)
		b.prInterruptionLoad += row.PRInterruptionLoad
		b.reviewRequestLoad += row.ReviewRequestLoad
		b.repoIDs[repoIDStr] = struct{}{}
		if row.AuthorEmail != "" {
			b.authors[row.AuthorEmail] = struct{}{}
			b.contextSpreadPairs[contextSpreadPair{authorEmail: row.AuthorEmail, repoID: repoIDStr}] = struct{}{}
		}
	}

	for _, row := range teamRows {
		repoIDStr := row.RepoID.String()
		teamID, ok := repoToTeam[repoIDStr]
		if !ok || teamID == "" {
			continue
		}
		b := bucket(teamID)
		b.afterHoursCommitsCount += row.AfterHoursCommitsCount
		b.weekendCommitsCount += row.WeekendCommitsCount
		b.commitsCount += row.CommitsCount
		b.hasTeamMetricsRow = true
		b.repoIDs[repoIDStr] = struct{}{}
	}

	teamIDs := make([]string, 0, len(buckets))
	for teamID := range buckets {
		teamIDs = append(teamIDs, teamID)
	}
	sort.Strings(teamIDs)

	records := make([]teamCognitiveLoadRow, 0, len(teamIDs))
	for _, teamID := range teamIDs {
		b := buckets[teamID]
		var afterHoursRatio, weekendRatio *float64
		switch {
		case b.hasTeamMetricsRow && b.commitsCount > 0:
			ah := float64(b.afterHoursCommitsCount) / float64(b.commitsCount)
			we := float64(b.weekendCommitsCount) / float64(b.commitsCount)
			afterHoursRatio, weekendRatio = &ah, &we
		case b.hasTeamMetricsRow:
			// Measured, not missing: every owned repo's row was itself a
			// legitimate all-zero day.
			afterHoursZero, weekendZero := 0.0, 0.0
			afterHoursRatio, weekendRatio = &afterHoursZero, &weekendZero
		default:
			afterHoursRatio, weekendRatio = nil, nil
		}
		records = append(records, teamCognitiveLoadRow{
			OrganizationID:        organizationID,
			TeamID:                teamID,
			Day:                   day,
			PRInterruptionLoad:    b.prInterruptionLoad,
			ContextSpreadCount:    float64(len(b.contextSpreadPairs)),
			ReviewRequestLoad:     b.reviewRequestLoad,
			AfterHoursCommitRatio: afterHoursRatio,
			WeekendCommitRatio:    weekendRatio,
			ContributingRepoCount: len(b.repoIDs),
			SampleAuthorCount:     len(b.authors),
			ComputedAt:            computedAt,
		})
	}
	return records
}

// teamCognitiveLoadWriter persists teamCognitiveLoadRow batches to
// team_cognitive_load_daily. Append-only, matching the Python sink exactly
// (TeamCognitiveLoadMixin.write_team_cognitive_load_daily): a redrive writes
// NEW rows with a later computed_at, never an UPDATE -- every reader dedups
// via argMax(<col>, computed_at).
type teamCognitiveLoadWriter struct {
	conn driver.Conn
}

func (writer *teamCognitiveLoadWriter) write(ctx context.Context, rows []teamCognitiveLoadRow) error {
	if writer == nil || writer.conn == nil {
		return errTeamCognitiveLoadUnavailable
	}
	if len(rows) == 0 {
		return nil
	}
	batch, err := writer.conn.PrepareBatch(ctx, `INSERT INTO team_cognitive_load_daily (
		org_id, team_id, day, pr_interruption_load, context_spread_count,
		review_request_load, after_hours_commit_ratio, weekend_commit_ratio,
		contributing_repo_count, sample_author_count, computed_at
	)`)
	if err != nil {
		return fmt.Errorf("prepare team_cognitive_load_daily batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.OrganizationID, row.TeamID, row.Day, row.PRInterruptionLoad, row.ContextSpreadCount,
			row.ReviewRequestLoad, row.AfterHoursCommitRatio, row.WeekendCommitRatio,
			uint32(row.ContributingRepoCount), uint32(row.SampleAuthorCount), row.ComputedAt,
		); err != nil {
			return fmt.Errorf("append team_cognitive_load_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send team_cognitive_load_daily batch: %w", err)
	}
	return nil
}
