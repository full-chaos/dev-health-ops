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
	ctx context.Context, conn driver.Conn, organizationID string, scope workItemPartitionScope,
	completedNow []workitemmetrics.CycleTimeRecord, computedAt time.Time,
) (int, error) {
	written := make(map[string]struct{}, len(completedNow))
	for _, record := range completedNow {
		written[record.Provider+"\x00"+record.WorkItemID] = struct{}{}
	}
	type retraction struct {
		workItemID, provider, workScopeID, itemType, status string
		day, createdAt, version                             time.Time
		teamID, teamName, assignee                          *string
		startedAt                                           *time.Time
	}
	var found []retraction
	// One query per chunk of repositories: the newest completed cycle row of every item that is open now. Whether it
	// is open is decided on the item's newest copy across repositories (last_synced, then repo_id): its status is not
	// terminal, or it has no completed_at. The read does not go through the day predicate of the rollup, which never
	// loads an item whose stored status is still done.
	const query = `
SELECT c.work_item_id, c.provider, c.day, c.work_scope_id, c.team_id, c.team_name, c.assignee, c.type,
       c.created_at, c.started_at, c.computed_at, w.item_status
FROM (
  SELECT * FROM work_item_cycle_times FINAL WHERE org_id = ? AND completed_at IS NOT NULL
) AS c
INNER JOIN (
  SELECT provider, work_item_id,
         argMax(status, (last_synced, repo_id)) AS item_status,
         argMax(isNull(completed_at), (last_synced, repo_id)) AS item_has_no_completion
  FROM work_items FINAL
  WHERE org_id = ? %s
  GROUP BY provider, work_item_id
  HAVING item_status NOT IN ('done', 'canceled') OR item_has_no_completion
) AS w ON c.provider = w.provider AND c.work_item_id = w.work_item_id`
	run := func(sql string, args ...any) error {
		rows, err := conn.Query(ctx, sql, args...)
		if err != nil {
			return fmt.Errorf("read work_item_cycle_times of open items: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var row retraction
			if err := rows.Scan(&row.workItemID, &row.provider, &row.day, &row.workScopeID, &row.teamID,
				&row.teamName, &row.assignee, &row.itemType, &row.createdAt, &row.startedAt, &row.version,
				&row.status); err != nil {
				return fmt.Errorf("scan work_item_cycle_times row: %w", err)
			}
			if _, completed := written[row.provider+"\x00"+row.workItemID]; completed {
				continue
			}
			found = append(found, row)
		}
		return rows.Err()
	}
	if scope.everyRepository {
		if err := run(fmt.Sprintf(query, ""), organizationID, organizationID); err != nil {
			return 0, err
		}
	} else {
		for _, chunk := range chunkRepositoryIDs(scope.repoIDs, querybound.MaxArrayBytes) {
			if err := run(fmt.Sprintf(query, "AND work_item_id IN (SELECT work_item_id FROM work_items FINAL WHERE org_id = ? AND repo_id IN ?)"), organizationID, organizationID, organizationID, chunk); err != nil {
				return 0, err
			}
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
		version := computedAt.UTC().Truncate(time.Second)
		if oldVersion := row.version.UTC().Truncate(time.Second); !version.After(oldVersion) {
			version = oldVersion.Add(time.Second)
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
