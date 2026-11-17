package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const cycleRetractChunk = 1000

// retractReopenedCycleRows writes, for each item of the org set that is not completed now but whose newest
// work_item_cycle_times row is a completed one, a newer row of the same key and day with no completed_at, cycle
// time or lead time. The table is ReplacingMergeTree(computed_at) keyed (org_id, provider, work_item_id) with no
// day, so the daily writer, which only writes completed items, cannot replace the row of a reopened item. The day
// stays the old row's day: it is the partition key, and a reader's day filter prunes by it before FINAL.
func retractReopenedCycleRows(
	ctx context.Context, conn driver.Conn, organizationID string, items []workItemMetricsRow, computedAt time.Time,
) (int, error) {
	open := map[string]string{}
	ids := make([]string, 0)
	for _, item := range items {
		if item.CompletedAt != nil {
			continue
		}
		open[item.Provider+"\x00"+item.WorkItemID] = item.Status
		ids = append(ids, item.WorkItemID)
	}
	type retraction struct {
		workItemID, provider, workScopeID, itemType, status string
		day, createdAt                                       time.Time
		teamID, teamName, assignee                           *string
		startedAt                                            *time.Time
	}
	var found []retraction
	for start := 0; start < len(ids); start += cycleRetractChunk {
		end := min(start+cycleRetractChunk, len(ids))
		rows, err := conn.Query(ctx, `
SELECT work_item_id, provider, day, work_scope_id, team_id, team_name, assignee, type, created_at, started_at
FROM work_item_cycle_times FINAL
WHERE org_id = ? AND work_item_id IN ? AND completed_at IS NOT NULL`, organizationID, ids[start:end])
		if err != nil {
			return 0, fmt.Errorf("read work_item_cycle_times of open items: %w", err)
		}
		for rows.Next() {
			var row retraction
			if err := rows.Scan(&row.workItemID, &row.provider, &row.day, &row.workScopeID, &row.teamID,
				&row.teamName, &row.assignee, &row.itemType, &row.createdAt, &row.startedAt); err != nil {
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
	computedAtUTC := computedAt.UTC()
	for _, row := range found {
		if err := batch.Append(
			row.workItemID, row.provider, row.day, row.workScopeID, row.teamID, row.teamName, row.assignee,
			row.itemType, row.status, row.createdAt.UTC(), row.startedAt, (*time.Time)(nil),
			(*float64)(nil), (*float64)(nil), computedAtUTC, organizationID,
		); err != nil {
			return 0, fmt.Errorf("append work_item_cycle_times retraction row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return len(found), fmt.Errorf("send work_item_cycle_times retraction batch: %w", err)
	}
	return len(found), nil
}
