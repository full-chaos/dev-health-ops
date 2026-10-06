package daily

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/checkedcast"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// LoadWorkItemEngineWorkItems reads the stored work items of one repository for
// the two engine families (work_item_issue_type, work_item_investment), with
// exactly the columns workitemengine reads.
//
// # Which items
//
// activeOnDay=false returns EVERY stored item of the repository. The
// issue-type compute opens a bucket for every item it is given before any time
// check, so a key whose items are all outside the day still gets a row of
// zeros; a narrower read would drop those rows and the family would no longer
// equal the sync deriver on the same item set.
//
// activeOnDay=true returns the items that are active on the day: created before
// the day ends, and not completed before the day starts. That is the SAME
// predicate, on the same two columns, as the first statement of the investment
// compute's loop (it skips every other item), so pushing it into the query
// cannot change a row. It is deliberately NOT the `status != 'done'` predicate
// of LoadWorkItemMetricsWorkItems: the investment compute reads completed_at,
// never status.
//
// `work_items` is ReplacingMergeTree(last_synced) keyed on
// (repo_id, work_item_id): FINAL is a complete dedup.
//
// Rows are ordered by work_item_id so that a recompute visits the items in one
// order whatever ClickHouse returns.
func LoadWorkItemEngineWorkItems(
	ctx context.Context, conn repositoryRows, organizationID string, repoID uuid.UUID,
	start, end time.Time, activeOnDay bool,
) ([]workitemengine.Item, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !start.Before(end) {
		return nil, ErrInvalidState
	}
	query := `
SELECT work_item_id, provider, type, title, labels, created_at, started_at, completed_at, story_points
FROM work_items FINAL
WHERE org_id = ? AND repo_id = ?`
	arguments := []any{organizationID, repoID.String()}
	if activeOnDay {
		query += `
  AND created_at < ?
  AND (completed_at IS NULL OR completed_at >= ?)`
		arguments = append(arguments, end.UTC(), start.UTC())
	}
	query += `
ORDER BY work_item_id`
	rows, err := conn.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("load work item engine work items: %w", err)
	}
	defer rows.Close()

	// A work item of a repository-scoped provider carries its repository id;
	// the nil UUID is how the store holds "no repository", and the compute
	// turns it back into a NULL repo_id.
	repository := repoID
	var items []workitemengine.Item
	for rows.Next() {
		var (
			item        workitemengine.Item
			startedAt   *time.Time
			completedAt *time.Time
			storyPoints *float64
		)
		if err := rows.Scan(
			&item.WorkItemID, &item.Provider, &item.Type, &item.Title, &item.Labels,
			&item.CreatedAt, &startedAt, &completedAt, &storyPoints,
		); err != nil {
			return nil, fmt.Errorf("scan work item engine work item: %w", err)
		}
		item.RepoID = &repository
		item.StartedAt = startedAt
		item.CompletedAt = completedAt
		item.StoryPoints = storyPoints
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work item engine work items: %w", err)
	}
	return items, nil
}

// workItemEngineTeamResolver answers from the stored primary attribution rows
// (work_item_team_attributions, is_primary = 1), as every other work-item
// daily family does. An item with no row, or with an empty team id, has no
// team: the compute applies the unassigned rule.
func workItemEngineTeamResolver(
	items []workitemengine.Item, attributions map[string]workItemPrimaryAttribution,
) workitemengine.TeamResolver {
	return func(index int) *string {
		attribution, ok := attributions[items[index].WorkItemID]
		if !ok || attribution.TeamID == "" {
			return nil
		}
		teamID := attribution.TeamID
		return &teamID
	}
}

// workItemEngineStoredRepoKey is the string form of a partition's repository
// as the nullable repo_id column of the three tables holds it: the nil UUID is
// stored as NULL, compared here as "".
func workItemEngineStoredRepoKey(repoID uuid.UUID) string {
	if repoID == uuid.Nil {
		return ""
	}
	return repoID.String()
}

// workItemEngineStoredRepo is the repo_id value written for a partition's
// repository: NULL for the nil UUID.
func workItemEngineStoredRepo(repoID uuid.UUID) *uuid.UUID {
	if repoID == uuid.Nil {
		return nil
	}
	value := repoID
	return &value
}

// issueTypeMetricsKey is the natural key of one issue_type_metrics_daily row
// inside one (org, repository, day).
type issueTypeMetricsKey struct{ provider, teamID, issueTypeNorm string }

// LoadIssueTypeMetricsLiveKeys returns the keys of one (org, repository, day)
// whose NEWEST row holds a count that is not zero.
//
// The table is append only and its readers take the newest computed_at per
// key. So a key that a recompute no longer produces would keep its older row
// for ever. The family writes a row of zeros for each such key; this read
// finds them. A key whose newest row is already all zeros is left alone.
func LoadIssueTypeMetricsLiveKeys(
	ctx context.Context, conn repositoryRows, organizationID string, repoID uuid.UUID, day time.Time,
) ([]issueTypeMetricsKey, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	rows, err := conn.Query(ctx, `
SELECT provider, team_id, issue_type_norm
FROM issue_type_metrics_daily
WHERE org_id = ? AND day = ? AND ifNull(toString(repo_id), '') = ?
GROUP BY provider, team_id, issue_type_norm
HAVING argMax(created_count, computed_at) != 0
    OR argMax(completed_count, computed_at) != 0
    OR argMax(active_count, computed_at) != 0
ORDER BY provider, team_id, issue_type_norm`,
		organizationID, workitemmetrics.UTCDay(day), workItemEngineStoredRepoKey(repoID),
	)
	if err != nil {
		return nil, fmt.Errorf("load issue_type_metrics_daily live keys: %w", err)
	}
	defer rows.Close()
	var keys []issueTypeMetricsKey
	for rows.Next() {
		var key issueTypeMetricsKey
		if err := rows.Scan(&key.provider, &key.teamID, &key.issueTypeNorm); err != nil {
			return nil, fmt.Errorf("scan issue_type_metrics_daily live key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate issue_type_metrics_daily live keys: %w", err)
	}
	return keys, nil
}

// withIssueTypeMetricsZeroRows appends a row of zeros for every live key the
// compute did not produce.
func withIssueTypeMetricsZeroRows(
	computed []workitemengine.IssueTypeMetricsDailyRow, live []issueTypeMetricsKey, repoID uuid.UUID,
) []workitemengine.IssueTypeMetricsDailyRow {
	held := make(map[issueTypeMetricsKey]struct{}, len(computed))
	for _, row := range computed {
		held[issueTypeMetricsKey{row.Provider, row.TeamID, row.IssueTypeNorm}] = struct{}{}
	}
	result := computed
	for _, key := range live {
		if _, ok := held[key]; ok {
			continue
		}
		result = append(result, workitemengine.IssueTypeMetricsDailyRow{
			RepoID:   workItemEngineStoredRepo(repoID),
			Provider: key.provider, TeamID: key.teamID, IssueTypeNorm: key.issueTypeNorm,
		})
	}
	return result
}

// WriteIssueTypeMetricsDaily appends one day's rows with the INSERT the sync
// effect adapter uses (workitemengine.IssueTypeMetricsInsert), in its column
// order. The table is plain MergeTree: nothing is replaced, readers take the
// newest computed_at per key.
func WriteIssueTypeMetricsDaily(
	ctx context.Context, conn workItemBatchConn, organizationID string, day time.Time,
	rows []workitemengine.IssueTypeMetricsDailyRow, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, workitemengine.IssueTypeMetricsInsert)
	if err != nil {
		return 0, fmt.Errorf("prepare issue_type_metrics_daily batch: %w", err)
	}
	dayValue := workitemmetrics.UTCDay(day)
	computedAtUTC := computedAt.UTC().Truncate(time.Second)
	for _, row := range rows {
		if row.TeamID == "" || row.IssueTypeNorm == "" {
			_ = batch.Abort()
			return 0, fmt.Errorf("%w: issue_type_metrics_daily row of %s has an empty team or type",
				ErrInvalidState, row.Provider)
		}
		counters, err := workItemUInt32s("issue_type_metrics_daily",
			fmt.Sprintf("%s team %q type %q", row.Provider, row.TeamID, row.IssueTypeNorm),
			[]workItemCounter{
				{"created_count", row.CreatedCount},
				{"completed_count", row.CompletedCount},
				{"active_count", row.ActiveCount},
			})
		if err != nil {
			_ = batch.Abort()
			return 0, err
		}
		if err := batch.Append(
			row.RepoID, dayValue, row.Provider, row.TeamID, row.IssueTypeNorm,
			counters[0], counters[1], counters[2],
			row.CycleP50Hours, row.CycleP90Hours, row.LeadP50Hours,
			computedAtUTC, organizationID,
		); err != nil {
			_ = batch.Abort()
			return 0, fmt.Errorf("append issue_type_metrics_daily row: %w", err)
		}
	}
	// Send is the one call that crosses the network: on an error the insert
	// may have landed, so report the row count (the convention of every writer
	// in this package).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send issue_type_metrics_daily batch: %w", err)
	}
	return len(rows), nil
}

// WriteInvestmentClassificationsDaily appends one day's classification rows.
// A classification with no investment area or no rule id cannot be stored
// (both columns are not nullable): it fails the write, as the sync effect
// adapter refuses the same row. Nothing is invented for it.
func WriteInvestmentClassificationsDaily(
	ctx context.Context, conn workItemBatchConn, organizationID string, day time.Time,
	rows []workitemengine.InvestmentClassificationDailyRow, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	for _, row := range rows {
		if row.ArtifactID == "" || row.InvestmentArea == nil || row.RuleID == nil {
			return 0, fmt.Errorf("%w: investment_classifications_daily row of %s has no area, rule id or artifact id",
				ErrInvalidState, row.Provider)
		}
	}
	batch, err := conn.PrepareBatch(ctx, workitemengine.InvestmentClassificationsInsert)
	if err != nil {
		return 0, fmt.Errorf("prepare investment_classifications_daily batch: %w", err)
	}
	dayValue := workitemmetrics.UTCDay(day)
	computedAtUTC := computedAt.UTC().Truncate(time.Second)
	for _, row := range rows {
		if err := batch.Append(
			row.RepoID, dayValue, row.ArtifactType, row.ArtifactID, row.Provider,
			*row.InvestmentArea, row.ProjectStream, row.Confidence, *row.RuleID,
			computedAtUTC, organizationID,
		); err != nil {
			_ = batch.Abort()
			return 0, fmt.Errorf("append investment_classifications_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send investment_classifications_daily batch: %w", err)
	}
	return len(rows), nil
}

// investmentMetricsKey is the natural key of one investment_metrics_daily row
// inside one (org, repository, day).
type investmentMetricsKey struct{ teamID, investmentArea, projectStream string }

// LoadInvestmentMetricsLiveKeys returns the keys of one (org, repository, day)
// whose NEWEST row holds a count that is not zero. See
// LoadIssueTypeMetricsLiveKeys: the family writes a row of zeros for each one
// the compute no longer produces, so that a completion that moved to another
// team, area or day is not counted twice by a reader.
func LoadInvestmentMetricsLiveKeys(
	ctx context.Context, conn repositoryRows, organizationID string, repoID uuid.UUID, day time.Time,
) ([]investmentMetricsKey, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	rows, err := conn.Query(ctx, `
SELECT ifNull(team_id, '') AS team, investment_area, project_stream
FROM investment_metrics_daily
WHERE org_id = ? AND day = ? AND ifNull(toString(repo_id), '') = ?
GROUP BY team, investment_area, project_stream
HAVING argMax(delivery_units, computed_at) != 0
    OR argMax(work_items_completed, computed_at) != 0
    OR argMax(prs_merged, computed_at) != 0
    OR argMax(churn_loc, computed_at) != 0
ORDER BY team, investment_area, project_stream`,
		organizationID, workitemmetrics.UTCDay(day), workItemEngineStoredRepoKey(repoID),
	)
	if err != nil {
		return nil, fmt.Errorf("load investment_metrics_daily live keys: %w", err)
	}
	defer rows.Close()
	var keys []investmentMetricsKey
	for rows.Next() {
		var key investmentMetricsKey
		if err := rows.Scan(&key.teamID, &key.investmentArea, &key.projectStream); err != nil {
			return nil, fmt.Errorf("scan investment_metrics_daily live key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate investment_metrics_daily live keys: %w", err)
	}
	return keys, nil
}

// withInvestmentMetricsZeroRows appends a row of zeros for every live key the
// compute did not produce.
func withInvestmentMetricsZeroRows(
	computed []workitemengine.InvestmentMetricsDailyRow, live []investmentMetricsKey, repoID uuid.UUID,
) []workitemengine.InvestmentMetricsDailyRow {
	held := make(map[investmentMetricsKey]struct{}, len(computed))
	for _, row := range computed {
		area := ""
		if row.InvestmentArea != nil {
			area = *row.InvestmentArea
		}
		held[investmentMetricsKey{row.TeamID, area, row.ProjectStream}] = struct{}{}
	}
	result := computed
	for _, key := range live {
		if _, ok := held[key]; ok {
			continue
		}
		area := key.investmentArea
		result = append(result, workitemengine.InvestmentMetricsDailyRow{
			RepoID: workItemEngineStoredRepo(repoID),
			TeamID: key.teamID, InvestmentArea: &area, ProjectStream: key.projectStream,
		})
	}
	return result
}

// WriteInvestmentMetricsDaily appends one day's investment metric rows.
func WriteInvestmentMetricsDaily(
	ctx context.Context, conn workItemBatchConn, organizationID string, day time.Time,
	rows []workitemengine.InvestmentMetricsDailyRow, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	type checked struct {
		counters []uint32
		churn    uint64
	}
	prepared := make([]checked, 0, len(rows))
	for _, row := range rows {
		if row.InvestmentArea == nil {
			return 0, fmt.Errorf("%w: investment_metrics_daily row of team %q has no investment area",
				ErrInvalidState, row.TeamID)
		}
		subject := fmt.Sprintf("team %q area %q stream %q", row.TeamID, *row.InvestmentArea, row.ProjectStream)
		counters, err := workItemUInt32s("investment_metrics_daily", subject, []workItemCounter{
			{"delivery_units", row.DeliveryUnits},
			{"work_items_completed", row.WorkItemsCompleted},
			{"prs_merged", row.PRsMerged},
		})
		if err != nil {
			return 0, err
		}
		churn, err := checkedcast.Uint64(row.ChurnLOC, "investment_metrics_daily", "churn_loc")
		if err != nil {
			return 0, fmt.Errorf("%w: %w for %s", ErrInvalidState, err, subject)
		}
		prepared = append(prepared, checked{counters: counters, churn: churn})
	}
	batch, err := conn.PrepareBatch(ctx, workitemengine.InvestmentMetricsInsert)
	if err != nil {
		return 0, fmt.Errorf("prepare investment_metrics_daily batch: %w", err)
	}
	dayValue := workitemmetrics.UTCDay(day)
	computedAtUTC := computedAt.UTC().Truncate(time.Second)
	for index, row := range rows {
		if err := batch.Append(
			row.RepoID, dayValue, row.TeamID, *row.InvestmentArea, row.ProjectStream,
			prepared[index].counters[0], prepared[index].counters[1], prepared[index].counters[2],
			prepared[index].churn, row.CycleP50Hours,
			computedAtUTC, organizationID,
		); err != nil {
			_ = batch.Abort()
			return 0, fmt.Errorf("append investment_metrics_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send investment_metrics_daily batch: %w", err)
	}
	return len(rows), nil
}
