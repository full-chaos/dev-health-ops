package daily

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// workItemMetricsRow is the `work_items` subset compute_work_item_metrics_daily
// and compute_estimate_coverage_metrics_daily read. It EMBEDS
// workItemStateWorkItem rather than restating its fields so that
// work_scope_id keeps exactly ONE derivation in this package
// (workItemStateWorkItem.workScopeID, itself a byte-for-byte port of
// WorkItem.work_scope_id) -- two copies of that rule would be free to disagree
// about, say, whether a team-only Linear issue has a scope.
type workItemMetricsRow struct {
	workItemStateWorkItem
	Type      string
	Assignees []string
	// StartedAt/ClosedAt/StoryPoints are read by compute_work_item_metrics_daily
	// and compute_estimate_coverage_metrics_daily but NOT by
	// compute_work_item_state_durations_daily, which is why they are here and
	// not on the embedded workItemStateWorkItem.
	StartedAt   *time.Time
	ClosedAt    *time.Time
	StoryPoints *float64
}

// LoadWorkItemMetricsWorkItems reads one partition's (org, repo, day-window)
// work items with the SAME predicate as ClickHouseMetricsLoader.load_work_items
// (src/dev_health_ops/metrics/loaders/clickhouse.py:454) and as this package's
// existing LoadWorkItemStateWorkItems: created before the window ends AND
// either not-yet-done or completed no earlier than the window's start.
//
// It exists alongside LoadWorkItemStateWorkItems rather than replacing it
// because the two families read DIFFERENT column sets -- work_item_state never
// looks at type/assignees/started_at/closed_at/story_points. Widening the
// shared loader would make every work_item_state partition pay for five columns
// it discards; keeping the PREDICATE identical is what actually matters for
// parity, and TestWorkItemLoadersShareTheLoadWorkItemsPredicate
// (work_item_native_executor_test.go) asserts that by parsing both queries out
// of their own source rather than trusting this comment.
//
// Python additionally appends `SETTINGS max_block_size` here (CHAOS-4361, after
// a real MemoryError on a long-lived repo's open-item backlog). That setting
// bounds how many rows clickhouse-connect buffers into ONE wide string read,
// and it exists because Python selects `* EXCEPT (description)` -- every
// remaining column, including several it never reads. This query names its
// columns explicitly and reads no wide text at all, so the buffering hazard the
// setting mitigates is absent by construction rather than merely mitigated.
// Copying the setting across would be cargo-culting a fix for a query shape
// this one does not have.
//
// `work_items` is ReplacingMergeTree(last_synced) keyed on
// (repo_id, work_item_id), so FINAL is a complete, correct dedup here --
// matching Python's WORK_ITEMS_DEDUPED = "work_items FINAL"
// (sinks/clickhouse/idempotency.py:5).
func LoadWorkItemMetricsWorkItems(
	ctx context.Context, conn repositoryRows, organizationID string, repoID uuid.UUID, start, end time.Time,
) ([]workItemMetricsRow, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !start.Before(end) {
		return nil, ErrInvalidState
	}
	rows, err := conn.Query(ctx, `
SELECT work_item_id, provider, status, project_key, project_id, native_team_key, project_name,
       created_at, completed_at, type, assignees, started_at, closed_at, story_points
FROM work_items FINAL
WHERE org_id = ? AND repo_id = ?
  AND created_at < ?
  AND (status != 'done' OR completed_at >= ?)`,
		organizationID, repoID.String(), end.UTC(), start.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load work_item metrics work items: %w", err)
	}
	defer rows.Close()

	var items []workItemMetricsRow
	for rows.Next() {
		var (
			item        workItemMetricsRow
			completedAt *time.Time
			startedAt   *time.Time
			closedAt    *time.Time
			storyPoints *float64
		)
		if err := rows.Scan(
			&item.WorkItemID, &item.Provider, &item.Status, &item.ProjectKey, &item.ProjectID,
			&item.NativeTeamKey, &item.ProjectName, &item.CreatedAt, &completedAt,
			&item.Type, &item.Assignees, &startedAt, &closedAt, &storyPoints,
		); err != nil {
			return nil, fmt.Errorf("scan work_item metrics work item: %w", err)
		}
		item.CompletedAt = completedAt
		item.StartedAt = startedAt
		item.ClosedAt = closedAt
		item.StoryPoints = storyPoints
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_item metrics work items: %w", err)
	}
	return items, nil
}

// workItemBatchConn is the narrow write capability the four writers need.
type workItemBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// WriteWorkItemMetricsDaily ports write_work_item_metrics
// (sinks/clickhouse/work_graph.py:198) -- same table, same 26 columns, same
// order.
//
// `work_item_metrics_daily` is ReplacingMergeTree(computed_at) -- migration
// 055_work_item_daily_rollups_replacing_merge_tree.py (CHAOS-2645) converted it
// from plain MergeTree for exactly this reason: append-only re-writes of the
// same (key, day) left duplicate versions that flat-aggregating readers
// double-counted. A partition recompute therefore COLLAPSES to the newest
// computed_at on the sorting key rather than duplicating.
//
// This writer is unchanged either way -- it mirrors Python's INSERT -- but the
// engine is why readers say FINAL (see capacity/recommendations), and an
// earlier version of this comment claimed the opposite. It said "plain
// MergeTree ... a partition recompute DUPLICATES rows", which was the
// pre-055 truth and would have told an operator the wrong thing.
// workItemUInt32s range-checks a row's counters through the package's single
// checkUint32Range, naming the offending COLUMN in the error rather than just
// the row. Every UInt32 destination these writers touch goes through here --
// team-lead's 2026-09-01 ruling on the file-hotspots port was that a fix for
// this closes the CLASS, not one column at a time.
// workItemCounter pairs a UInt32 column with the value bound for it, so the
// two CANNOT be mis-paired. The first version took two parallel slices and
// indexed `columns[i]` while ranging `values` -- a shorter columns slice
// PANICKED instead of returning ErrInvalidState, and an equal-length but
// reordered one silently named the wrong column in the error. Both callers
// happened to be aligned, which is exactly the kind of "correct today" that
// stops being true on the next edit.
type workItemCounter struct {
	column string
	value  int
}

func workItemUInt32s(table, subject string, counters []workItemCounter) ([]uint32, error) {
	checked := make([]uint32, len(counters))
	for index, counter := range counters {
		value, err := checkUint32Range(counter.value, table, counter.column, subject)
		if err != nil {
			return nil, err
		}
		checked[index] = value
	}
	return checked, nil
}

func WriteWorkItemMetricsDaily(
	ctx context.Context, conn workItemBatchConn, organizationID string, day time.Time,
	rows []workitemmetrics.MetricsDailyRow, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_metrics_daily (
		day, provider, work_scope_id, team_id, team_name,
		items_started, items_completed, items_started_unassigned, items_completed_unassigned,
		wip_count_end_of_day, wip_unassigned_end_of_day,
		cycle_time_p50_hours, cycle_time_p90_hours, lead_time_p50_hours, lead_time_p90_hours,
		wip_age_p50_hours, wip_age_p90_hours, bug_completed_ratio, story_points_completed,
		new_bugs_count, new_items_count, defect_intro_rate, wip_congestion_ratio,
		predictability_score, computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_item_metrics_daily batch: %w", err)
	}
	dayValue := workitemmetrics.UTCDay(day)
	computedAtUTC := computedAt.UTC()
	for _, row := range rows {
		counters, err := workItemUInt32s("work_item_metrics_daily",
			fmt.Sprintf("%s %q team %q", row.Provider, row.WorkScopeID, row.TeamID),
			[]workItemCounter{
				{"items_started", row.ItemsStarted},
				{"items_completed", row.ItemsCompleted},
				{"items_started_unassigned", row.ItemsStartedUnassigned},
				{"items_completed_unassigned", row.ItemsCompletedUnassigned},
				{"wip_count_end_of_day", row.WIPCountEndOfDay},
				{"wip_unassigned_end_of_day", row.WIPUnassignedEndOfDay},
				{"new_bugs_count", row.NewBugsCount},
				{"new_items_count", row.NewItemsCount},
			})
		if err != nil {
			return 0, err
		}
		if err := batch.Append(
			dayValue, row.Provider, row.WorkScopeID, row.TeamID, row.TeamName,
			counters[0], counters[1],
			counters[2], counters[3],
			counters[4], counters[5],
			row.CycleTimeP50Hours, row.CycleTimeP90Hours, row.LeadTimeP50Hours, row.LeadTimeP90Hours,
			row.WIPAgeP50Hours, row.WIPAgeP90Hours, row.BugCompletedRatio, row.StoryPointsCompleted,
			counters[6], counters[7], row.DefectIntroRate,
			row.WIPCongestionRatio, row.PredictabilityScore, computedAtUTC, organizationID,
		); err != nil {
			return 0, fmt.Errorf("append work_item_metrics_daily row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send work_item_metrics_daily batch: %w", err)
	}
	return len(rows), nil
}

// WriteWorkItemUserMetricsDaily ports write_work_item_user_metrics
// (sinks/clickhouse/work_graph.py:259). Also converted to
// ReplacingMergeTree(computed_at) by migration 055 -- same collapse-on-recompute
// contract as above, not duplication.
func WriteWorkItemUserMetricsDaily(
	ctx context.Context, conn workItemBatchConn, organizationID string, day time.Time,
	rows []workitemmetrics.UserMetricsDailyRow, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_user_metrics_daily (
		day, provider, work_scope_id, user_identity, team_id, team_name,
		items_started, items_completed, wip_count_end_of_day,
		cycle_time_p50_hours, cycle_time_p90_hours, computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_item_user_metrics_daily batch: %w", err)
	}
	dayValue := workitemmetrics.UTCDay(day)
	computedAtUTC := computedAt.UTC()
	for _, row := range rows {
		counters, err := workItemUInt32s("work_item_user_metrics_daily",
			fmt.Sprintf("%s %q user %q", row.Provider, row.WorkScopeID, row.UserIdentity),
			[]workItemCounter{
				{"items_started", row.ItemsStarted},
				{"items_completed", row.ItemsCompleted},
				{"wip_count_end_of_day", row.WIPCountEndOfDay},
			})
		if err != nil {
			return 0, err
		}
		if err := batch.Append(
			dayValue, row.Provider, row.WorkScopeID, row.UserIdentity, row.TeamID, row.TeamName,
			counters[0], counters[1], counters[2],
			row.CycleTimeP50Hours, row.CycleTimeP90Hours, computedAtUTC, organizationID,
		); err != nil {
			return 0, fmt.Errorf("append work_item_user_metrics_daily row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send work_item_user_metrics_daily batch: %w", err)
	}
	return len(rows), nil
}

// WriteWorkItemCycleTimes ports write_work_item_cycle_times
// (sinks/clickhouse/work_graph.py:284).
//
// THE THREE FLOW COLUMNS ARE DELIBERATELY OMITTED. Migration
// 003_flow_efficiency.sql added active_time_hours/wait_time_hours/
// flow_efficiency to this table (Float64 DEFAULT 0), and Python COMPUTES all
// three -- but its sink names only the sixteen columns below, so those three
// stay at their DEFAULT 0 for every row Python writes. Writing them here would
// make the native executor's rows differ from the bridge's on exactly those
// three columns for the same input (team-lead ruling, CHAOS-4283). The values
// are still computed and carried on workitemmetrics.CycleTimeRecord; only the
// persistence step drops them, precisely as Python does.
func WriteWorkItemCycleTimes(
	ctx context.Context, conn workItemBatchConn, organizationID string,
	rows []workitemmetrics.CycleTimeRecord, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_cycle_times (
		work_item_id, provider, day, work_scope_id, team_id, team_name, assignee,
		type, status, created_at, started_at, completed_at,
		cycle_time_hours, lead_time_hours, computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_item_cycle_times batch: %w", err)
	}
	computedAtUTC := computedAt.UTC()
	for _, row := range rows {
		if err := batch.Append(
			row.WorkItemID, row.Provider, workitemmetrics.UTCDay(row.Day), row.WorkScopeID,
			&row.TeamID, &row.TeamName, row.Assignee, row.Type, row.Status,
			row.CreatedAt.UTC(), row.StartedAt, row.CompletedAt,
			row.CycleTimeHours, row.LeadTimeHours, computedAtUTC, organizationID,
		); err != nil {
			return 0, fmt.Errorf("append work_item_cycle_times row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send work_item_cycle_times batch: %w", err)
	}
	return len(rows), nil
}

// WriteEstimateCoverageMetricsDaily ports write_estimate_coverage_metrics
// (sinks/clickhouse/work_graph.py:236).
func WriteEstimateCoverageMetricsDaily(
	ctx context.Context, conn workItemBatchConn, organizationID string, day time.Time,
	rows []workitemmetrics.EstimateCoverageRow, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO estimate_coverage_metrics_daily (
		day, provider, work_scope_id, team_id, team_name,
		estimated_count, unestimated_count, backlog_size, ratio, computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare estimate_coverage_metrics_daily batch: %w", err)
	}
	dayValue := workitemmetrics.UTCDay(day)
	computedAtUTC := computedAt.UTC()
	for _, row := range rows {
		teamID, teamName := row.TeamID, row.TeamName
		counters, err := workItemUInt32s("estimate_coverage_metrics_daily",
			fmt.Sprintf("%s %q team %q", row.Provider, row.WorkScopeID, row.TeamID),
			[]workItemCounter{
				{"estimated_count", row.EstimatedCount},
				{"unestimated_count", row.UnestimatedCount},
				{"backlog_size", row.BacklogSize},
			})
		if err != nil {
			return 0, err
		}
		if err := batch.Append(
			dayValue, row.Provider, row.WorkScopeID, &teamID, &teamName,
			counters[0], counters[1], counters[2],
			row.Ratio, computedAtUTC, organizationID,
		); err != nil {
			return 0, fmt.Errorf("append estimate_coverage_metrics_daily row: %w", err)
		}
	}
	// CHAOS-5190 confirmation-pass sweep: Send is the one call here that
	// crosses the network, so a Send error is AMBIGUOUS -- ClickHouse may
	// have committed the insert server-side and only the acknowledgement
	// was lost. Report the true row count on this specific error path
	// (never on PrepareBatch/Append, which have not crossed the network
	// and genuinely wrote nothing), so the caller fails CLOSED on the
	// ambiguity instead of silently open (matches
	// work_graph_edges_native_clickhouse.go's established pattern).
	if err := batch.Send(); err != nil {
		return len(rows), fmt.Errorf("send estimate_coverage_metrics_daily batch: %w", err)
	}
	return len(rows), nil
}
