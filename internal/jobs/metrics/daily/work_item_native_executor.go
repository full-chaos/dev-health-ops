package daily

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// WorkItemExecutor is the NATIVE implementation of the `work_item`
// metrics.daily family (CHAOS-4283) -- ports compute_work_item_metrics_daily
// (src/dev_health_ops/metrics/compute_work_items.py:1075) and writes the three
// tables its Python caller writes: work_item_metrics_daily,
// work_item_user_metrics_daily, work_item_cycle_times.
//
// # The arithmetic is NOT here
//
// It lives in internal/jobs/metrics/workitemmetrics, shared with
// internal/providersync's sync-time deriver, which had already ported and
// oracle-tested it. That package's doc comment explains why a second copy was
// refused. This file is the daily family's I/O: load, resolve, compute, write.
//
// # Team attribution: READ, not recompute (CHAOS-4278 ruling, upheld here)
//
// Python resolves team attribution inline through the 9-source
// resolve_team_attribution cascade (compute_work_items.py:507). This executor
// instead reads that cascade's already-materialised output,
// work_item_team_attributions.is_primary=1, exactly as WorkItemStateExecutor
// does -- same loader, same latest-snapshot fence, same measured-equivalence
// evidence (see LoadWorkItemPrimaryTeamAttributions's doc comment). Team here
// means OWNERSHIP, which is what that table's primary row records; nothing in
// this path consults membership.
//
// # Phase: post_bridge
//
// Because the attribution table is written by `work_item_attribution`, still
// Python-bridged, during the SAME partition's compatibility call, this family
// declares "phase":"post_bridge" in families.json and is registered via
// SetPostBridgeNativeFamilies -- identical reasoning to work_item_state, whose
// codex round-1 P1 established it. Running pre_bridge would read a stale (or,
// for a brand-new item, absent) snapshot.
//
// # Per-repo iteration
//
// run_daily_metrics_job is invoked once PER repo_id by the compatibility
// bridge's fan-out loop (worker_metrics.py:1729), so Python computes this
// family over one repo's rows per call. This executor mirrors that boundary
// explicitly rather than aggregating the partition's repos together -- the
// grouping key (provider, work_scope_id, team_id) does not include repo_id, so
// aggregating would silently MERGE two repos' groups into one row where Python
// emits two.
type WorkItemExecutor struct {
	conn   driver.Conn
	nowUTC func() time.Time
}

var errWorkItemUnavailable = errors.New("work_item native executor unavailable")

// NewWorkItemExecutor fails closed on a nil connection, matching every other
// native family executor's construction contract.
func NewWorkItemExecutor(conn driver.Conn) (*WorkItemExecutor, error) {
	if conn == nil {
		return nil, errWorkItemUnavailable
	}
	return &WorkItemExecutor{conn: conn, nowUTC: func() time.Time { return time.Now().UTC() }}, nil
}

// ComputeFamily runs the work_item computation for one partition.
func (executor *WorkItemExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errWorkItemUnavailable
	}
	scope, err := newWorkItemPartitionScope(run, partition, "work_item")
	if err != nil {
		return 0, err
	}

	total := 0
	for _, repoID := range scope.repoIDs {
		items, err := LoadWorkItemMetricsWorkItems(
			ctx, executor.conn, run.OrganizationID, repoID, scope.start, scope.end,
		)
		if err != nil {
			return total, err
		}
		if len(items) == 0 {
			// Python guards the whole work-item block with `if work_items:`
			// (job_daily.py:1549), so a repo with no items produces no rows for
			// any of the three tables.
			continue
		}
		transitions, err := LoadWorkItemStateTransitions(
			ctx, executor.conn, run.OrganizationID, repoID, scope.end,
		)
		if err != nil {
			return total, err
		}
		attributions, err := LoadWorkItemPrimaryTeamAttributions(
			ctx, executor.conn, run.OrganizationID, repoID,
		)
		if err != nil {
			return total, err
		}

		// One honest, real-wall-clock timestamp per repo group -- the same
		// cadence Python's per-repo_id bridge calls produce, and the convention
		// WriteTeamMetricsDailyPerRepo and WorkItemStateExecutor already set.
		computedAt := executor.nowUTC()

		sorted := sortWorkItemMetricsRows(items)
		projected := workItemMetricsItems(sorted)
		triplet := workitemmetrics.ComputeDailyTriplet(
			scope.day,
			projected,
			workItemMetricsTransitions(transitions),
			workitemmetrics.AssertAligned(len(sorted), projected, workItemMetricsResolver(sorted, attributions)),
		)

		written, err := WriteWorkItemMetricsDaily(
			ctx, executor.conn, run.OrganizationID, scope.day, triplet.MetricsDaily, computedAt,
		)
		if err != nil {
			return total, err
		}
		total += written
		written, err = WriteWorkItemUserMetricsDaily(
			ctx, executor.conn, run.OrganizationID, scope.day, triplet.UserMetricsDaily, computedAt,
		)
		if err != nil {
			return total, err
		}
		total += written
		written, err = WriteWorkItemCycleTimes(
			ctx, executor.conn, run.OrganizationID, triplet.CycleTimes, computedAt,
		)
		if err != nil {
			return total, err
		}
		total += written
	}
	return total, nil
}

// workItemPartitionScope is the (day, window, repoIDs) triple both work-item
// executors derive identically from a run/partition pair.
type workItemPartitionScope struct {
	day, start, end time.Time
	repoIDs         []uuid.UUID
}

func newWorkItemPartitionScope(run Run, partition Partition, family string) (workItemPartitionScope, error) {
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return workItemPartitionScope{}, fmt.Errorf(
			"%w: partition %s run has no organization or target day (%s)",
			ErrInvalidState, partition.ID, family)
	}
	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return workItemPartitionScope{}, fmt.Errorf(
			"%w: partition %s repo_ids (%s): %v", ErrInvalidState, partition.ID, family, err)
	}
	day := workitemmetrics.UTCDay(run.TargetDay)
	return workItemPartitionScope{
		day: day, start: day, end: day.AddDate(0, 0, 1), repoIDs: repoIDs,
	}, nil
}

// sortWorkItemMetricsRows makes the compute reproducible run to run regardless
// of ClickHouse row-return order.
//
// It cannot change the OUTPUT of the group/user aggregations (addition and
// counting commute), but it CAN change one thing: which item's team_name a
// bucket records, since the bucket keeps its FIRST contributing item's name and
// team_name is only a property of team_id in every real case. Fixing the order
// makes that tie deterministic instead of storage-dependent -- the same
// reasoning, and the same convention, as computeWellbeingPerRepo and
// computeWorkItemStateDurationsForRepo.
func sortWorkItemMetricsRows(items []workItemMetricsRow) []workItemMetricsRow {
	sorted := make([]workItemMetricsRow, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].WorkItemID < sorted[j].WorkItemID })
	return sorted
}

func workItemMetricsItems(rows []workItemMetricsRow) []workitemmetrics.Item {
	items := make([]workitemmetrics.Item, 0, len(rows))
	for index, row := range rows {
		items = append(items, workitemmetrics.Item{
			SourceIndex: index,
			WorkItemID:  row.WorkItemID,
			Provider:    row.Provider,
			Type:        row.Type,
			Status:      row.Status,
			Assignee:    workitemmetrics.FirstAssignee(row.Assignees),
			CreatedAt:   row.CreatedAt,
			StartedAt:   row.StartedAt,
			CompletedAt: row.CompletedAt,
			ClosedAt:    row.ClosedAt,
			StoryPoints: row.StoryPoints,
		})
	}
	return items
}

func workItemMetricsTransitions(rows []workItemStateTransition) []workitemmetrics.Transition {
	transitions := make([]workitemmetrics.Transition, 0, len(rows))
	for _, row := range rows {
		transitions = append(transitions, workitemmetrics.Transition{
			WorkItemID: row.WorkItemID,
			OccurredAt: row.OccurredAt,
			ToStatus:   row.ToStatus,
		})
	}
	return transitions
}

// workItemMetricsResolver answers from the attribution table, applying the same
// normalize_team_id/normalize_team_name defaults Python applies to a nil
// resolver result. A work item with no row in work_item_team_attributions (not
// yet attributed, or never synced) resolves to unassigned/Unassigned, which is
// exactly what resolve_team_attribution returns when every cascade source
// declines.
func workItemMetricsResolver(
	rows []workItemMetricsRow, attributions map[string]workItemPrimaryAttribution,
) workitemmetrics.Resolver {
	return func(index int) workitemmetrics.Attribution {
		row := rows[index]
		teamID, teamName := resolveWorkItemPrimaryTeam(attributions[row.WorkItemID])
		return workitemmetrics.Attribution{
			WorkScopeID: row.workScopeID(),
			TeamID:      teamID,
			TeamName:    teamName,
		}
	}
}

var _ NativeFamilyExecutor = (*WorkItemExecutor)(nil)
