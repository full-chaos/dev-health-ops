package daily

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/querybound"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// workItemIdentity is an item of the organization, whatever repository its rows are stored under.
type workItemIdentity struct{ provider, workItemID string }

// workItemCopy is one stored copy of an item: its row of one repository.
type workItemCopy struct {
	repoID                                            uuid.UUID
	lastSynced                                        time.Time
	provider, workItemID, status                      string
	completedAt                                       *time.Time
	projectKey, projectID, nativeTeamKey, projectName string
}

func (stored workItemCopy) identity() workItemIdentity {
	return workItemIdentity{stored.provider, stored.workItemID}
}

// completed is the completion rule of the cycle rows: the status is terminal and completed_at is set.
func (stored workItemCopy) completed() bool {
	return workitemmetrics.IsTerminalStatus(stored.status) && stored.completedAt != nil
}

func (stored workItemCopy) scopeKey() workItemScopeKey {
	return workItemScopeKey{
		provider: stored.provider,
		scope:    workitemmetrics.WorkScopeID(stored.provider, stored.projectKey, stored.projectID, stored.nativeTeamKey, stored.projectName),
	}
}

// currentWorkItemCopies is the current copy of each item: workitemmetrics.OncePerProviderAndID over every copy, with
// no day predicate (the newest last_synced; on a tie, the lower repository id).
func currentWorkItemCopies(copies []workItemCopy) map[workItemIdentity]workItemCopy {
	versions := make([]workitemmetrics.ItemVersion, len(copies))
	for index, stored := range copies {
		versions[index] = workitemmetrics.ItemVersion{
			Provider: stored.provider, WorkItemID: stored.workItemID, RepoID: stored.repoID.String(), LastSynced: stored.lastSynced,
		}
	}
	indexes, _ := workitemmetrics.OncePerProviderAndID(versions)
	current := make(map[workItemIdentity]workItemCopy, len(indexes))
	for _, index := range indexes {
		current[copies[index].identity()] = copies[index]
	}
	return current
}

// workItemCopiesReadSQL reads every copy of the listed items. Its arguments are the organization and the ids.
const workItemCopiesReadSQL = `
SELECT repo_id, last_synced, provider, work_item_id, status, completed_at,
       project_key, project_id, native_team_key, project_name
FROM work_items FINAL
WHERE org_id = ? AND work_item_id IN ?`

// loadWorkItemCopies reads every copy of the given items, once per chunk of the id list (package querybound).
func loadWorkItemCopies(ctx context.Context, conn repositoryRows, organizationID string, ids []string) ([]workItemCopy, error) {
	var copies []workItemCopy
	for _, chunk := range querybound.ChunkStringsByRenderedBytes(ids, maxWorkItemScopeFilterBytes) {
		rows, err := conn.Query(ctx, workItemCopiesReadSQL, organizationID, chunk)
		if err != nil {
			return nil, fmt.Errorf("load work item copies: %w", err)
		}
		for rows.Next() {
			var stored workItemCopy
			if err := rows.Scan(&stored.repoID, &stored.lastSynced, &stored.provider, &stored.workItemID, &stored.status,
				&stored.completedAt, &stored.projectKey, &stored.projectID, &stored.nativeTeamKey, &stored.projectName); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan work item stored: %w", err)
			}
			copies = append(copies, stored)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, fmt.Errorf("iterate work item copies: %w", err)
		}
	}
	return copies, nil
}

// keepCycleTimesOfCurrentCopies keeps a completion row only when the item's current copy is completed and is the copy
// the day's compute read (winners: the org winner's repository of each item). The day predicate never loads a done
// copy with no completed_at, so the day's winner can be an older copy that still holds a completion.
func keepCycleTimesOfCurrentCopies(
	ctx context.Context, conn repositoryRows, organizationID string, winners map[workItemIdentity]uuid.UUID,
	records []workitemmetrics.CycleTimeRecord,
) ([]workitemmetrics.CycleTimeRecord, error) {
	if len(records) == 0 {
		return records, nil
	}
	seen := make(map[string]struct{}, len(records))
	ids := make([]string, 0, len(records))
	for _, record := range records {
		if _, duplicate := seen[record.WorkItemID]; !duplicate {
			seen[record.WorkItemID] = struct{}{}
			ids = append(ids, record.WorkItemID)
		}
	}
	sort.Strings(ids)
	copies, err := loadWorkItemCopies(ctx, conn, organizationID, ids)
	if err != nil {
		return nil, err
	}
	current := currentWorkItemCopies(copies)
	kept := make([]workitemmetrics.CycleTimeRecord, 0, len(records))
	for _, record := range records {
		identity := workItemIdentity{record.Provider, record.WorkItemID}
		stored, found := current[identity]
		winner, read := winners[identity]
		if found && read && stored.completed() && stored.repoID == winner {
			kept = append(kept, record)
		}
	}
	return kept, nil
}

// workItemCycleRetractionReadSQL is the newest completed cycle row of each month partition (FINAL replaces within a
// partition only) of every item that has an open copy, joined to every copy of that item. "Has an open copy" is a
// superset of "the current copy is open"; the current copy is decided in Go. Its arguments are the organization
// three times.
const workItemCycleRetractionReadSQL = `
SELECT c.work_item_id, c.provider, c.day, c.work_scope_id, c.team_id, c.team_name, c.assignee, c.type,
       c.created_at, c.started_at, c.computed_at,
       w.repo_id, w.last_synced, w.status, w.completed_at, w.project_key, w.project_id, w.native_team_key, w.project_name
FROM (
  SELECT * FROM work_item_cycle_times FINAL WHERE org_id = ? AND completed_at IS NOT NULL
) AS c
INNER JOIN (
  SELECT repo_id, last_synced, provider, work_item_id, status, completed_at,
         project_key, project_id, native_team_key, project_name
  FROM work_items FINAL
  WHERE org_id = ? AND (provider, work_item_id) IN (
    SELECT provider, work_item_id FROM work_items FINAL
    WHERE org_id = ? AND (status NOT IN ('done', 'canceled') OR completed_at IS NULL))
) AS w ON c.provider = w.provider AND c.work_item_id = w.work_item_id`

// retractReopenedCycleRows writes, for each item whose current copy is not completed but that has a completed
// work_item_cycle_times row, a newer row of the same key and day with no completed_at, cycle time or lead time. The
// table is ReplacingMergeTree(computed_at) keyed (org_id, provider, work_item_id) and partitioned by month of day, so
// the daily writer, which only writes completed items, cannot replace the row of a reopened item. The day stays the
// old row's day: it is the partition key, and a reader's day filter prunes by it before FINAL.
//
// The read runs after this invocation's own cycle insert and decides on the current copy only: a completion this
// invocation wrote from a read that the item's reopen has since overtaken is retracted here. An item is the
// partition's when the run is organization-wide, a copy of it is stored in a listed repository, or its current copy
// is in a work scope of the compute (scopes).
func retractReopenedCycleRows(
	ctx context.Context, conn driver.Conn, organizationID string, scope workItemPartitionScope,
	scopes map[workItemScopeKey]struct{}, computedAt time.Time,
) (int, error) {
	type cycleRow struct {
		workItemID, provider, workScopeID, itemType string
		day, createdAt, version                     time.Time
		teamID, teamName, assignee                  *string
		startedAt                                   *time.Time
	}
	type cycleRowKey struct {
		identity workItemIdentity
		day      int64
	}
	cycleRows := make(map[cycleRowKey]cycleRow)
	copiesSeen := make(map[workItemIdentity]map[uuid.UUID]struct{})
	var copies []workItemCopy
	rows, err := conn.Query(ctx, workItemCycleRetractionReadSQL, organizationID, organizationID, organizationID)
	if err != nil {
		return 0, fmt.Errorf("read work_item_cycle_times of open items: %w", err)
	}
	for rows.Next() {
		var row cycleRow
		var stored workItemCopy
		if err := rows.Scan(&row.workItemID, &row.provider, &row.day, &row.workScopeID, &row.teamID,
			&row.teamName, &row.assignee, &row.itemType, &row.createdAt, &row.startedAt, &row.version,
			&stored.repoID, &stored.lastSynced, &stored.status, &stored.completedAt,
			&stored.projectKey, &stored.projectID, &stored.nativeTeamKey, &stored.projectName); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan work_item_cycle_times row: %w", err)
		}
		stored.provider, stored.workItemID = row.provider, row.workItemID
		identity := stored.identity()
		cycleRows[cycleRowKey{identity, row.day.Unix()}] = row
		if copiesSeen[identity] == nil {
			copiesSeen[identity] = make(map[uuid.UUID]struct{})
		}
		if _, seen := copiesSeen[identity][stored.repoID]; !seen {
			copiesSeen[identity][stored.repoID] = struct{}{}
			copies = append(copies, stored)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, fmt.Errorf("read work_item_cycle_times of open items: %w", err)
	}
	current := currentWorkItemCopies(copies)
	listed := make(map[uuid.UUID]struct{}, len(scope.repoIDs))
	for _, repoID := range scope.repoIDs {
		listed[repoID] = struct{}{}
	}
	partitionOwns := func(identity workItemIdentity, stored workItemCopy) bool {
		if scope.everyRepository {
			return true
		}
		if _, ofScopes := scopes[stored.scopeKey()]; ofScopes {
			return true
		}
		for repoID := range copiesSeen[identity] {
			if _, isListed := listed[repoID]; isListed {
				return true
			}
		}
		return false
	}
	type retraction struct {
		row    cycleRow
		status string
	}
	var found []retraction
	for key, row := range cycleRows {
		stored, ok := current[key.identity]
		if !ok || stored.completed() || !partitionOwns(key.identity, stored) {
			continue
		}
		found = append(found, retraction{row: row, status: stored.status})
	}
	if len(found) == 0 {
		return 0, nil
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].row.workItemID != found[j].row.workItemID {
			return found[i].row.workItemID < found[j].row.workItemID
		}
		return found[i].row.day.Before(found[j].row.day)
	})
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_cycle_times (
		work_item_id, provider, day, work_scope_id, team_id, team_name, assignee,
		type, status, created_at, started_at, completed_at,
		cycle_time_hours, lead_time_hours, computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare work_item_cycle_times retraction batch: %w", err)
	}
	for _, item := range found {
		row := item.row
		// computed_at has whole-second precision: the retraction must be strictly newer than the row it replaces.
		version := computedAt.UTC().Truncate(time.Second)
		if oldVersion := row.version.UTC().Truncate(time.Second); !version.After(oldVersion) {
			version = oldVersion.Add(time.Second)
		}
		if err := batch.Append(
			row.workItemID, row.provider, row.day, row.workScopeID, row.teamID, row.teamName, row.assignee,
			row.itemType, item.status, row.createdAt.UTC(), row.startedAt, (*time.Time)(nil),
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
