package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/querybound"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// retractReopenedCycleRows writes, for each item of the org set that is not completed now but whose newest
// work_item_cycle_times row is a completed one, a newer row of the same key and day with no completed_at, cycle
// time or lead time. The table is ReplacingMergeTree(computed_at) keyed (org_id, provider, work_item_id) with no
// day, so the daily writer, which only writes completed items, cannot replace the row of a reopened item. The day
// stays the old row's day: it is the partition key, and a reader's day filter prunes by it before FINAL.
func retractReopenedCycleRows(
	ctx context.Context, conn driver.Conn, organizationID string, items []workItemMetricsRow, completedNow []workitemmetrics.CycleTimeRecord, computedAt time.Time,
) (int, error) {
	written := make(map[string]struct{}, len(completedNow))
	for _, record := range completedNow {
		written[record.Provider+"\x00"+record.WorkItemID] = struct{}{}
	}
	open := map[string]string{}
	ids := make([]string, 0)
	for _, item := range items {
		if item.CompletedAt != nil && (item.Status == "done" || item.Status == "canceled") {
			continue
		}
		if _, completed := written[item.Provider+"\x00"+item.WorkItemID]; completed {
			continue
		}
		open[item.Provider+"\x00"+item.WorkItemID] = item.Status
		ids = append(ids, item.WorkItemID)
	}
	type retraction struct {
		workItemID, provider, workScopeID, itemType, status string
		day, createdAt                                      time.Time
		teamID, teamName, assignee                          *string
		startedAt                                           *time.Time
		version                                             time.Time
	}
	var found []retraction
	for _, chunk := range querybound.ChunkStringsByRenderedBytes(ids, querybound.MaxArrayBytes) {
		rows, err := conn.Query(ctx, `
SELECT work_item_id, provider, day, work_scope_id, team_id, team_name, assignee, type, created_at, started_at, computed_at
FROM work_item_cycle_times FINAL
WHERE org_id = ? AND work_item_id IN ? AND completed_at IS NOT NULL`, organizationID, chunk)
		if err != nil {
			return 0, fmt.Errorf("read work_item_cycle_times of open items: %w", err)
		}
		for rows.Next() {
			var row retraction
			if err := rows.Scan(&row.workItemID, &row.provider, &row.day, &row.workScopeID, &row.teamID,
				&row.teamName, &row.assignee, &row.itemType, &row.createdAt, &row.startedAt, &row.version); err != nil {
				_ = rows.Close()
				return 0, fmt.Errorf("scan work_item_cycle_times row: %w", err)
			}
			status, isOpen := open[row.provider+"\x00"+row.workItemID]
			if !isOpen {
				continue
			}
			row.status = status
			found = append(found, row)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("iterate work_item_cycle_times rows: %w", err)
		}
		if err := rows.Close(); err != nil {
			return 0, fmt.Errorf("close work_item_cycle_times rows: %w", err)
		}
	}
	if len(found) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_cycle_times (
		work_item_id, provider, day, work_scope_id, team_id, team_name, assignee,
		type, status, created_at, started_at, completed_at,
		cycle_time_hours, lead_time_hours, computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_item_cycle_times retraction batch: %w", err)
	}
	for _, row := range found {
		// computed_at has whole-second precision: the retraction must be strictly newer than the row it replaces.
		version := computedAt.UTC()
		if !version.After(row.version) {
			version = row.version.UTC().Add(time.Second)
		}
		if err := batch.Append(
			row.workItemID, row.provider, row.day, row.workScopeID, row.teamID, row.teamName, row.assignee,
			row.itemType, row.status, row.createdAt.UTC(), row.startedAt, (*time.Time)(nil),
			(*float64)(nil), (*float64)(nil), version, organizationID,
		); err != nil {
			return 0, fmt.Errorf("append work_item_cycle_times retraction row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return len(found), fmt.Errorf("send work_item_cycle_times retraction batch: %w", err)
	}
	return len(found), nil
}
