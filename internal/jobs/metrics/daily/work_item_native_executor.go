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
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
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
// # One compute for each work scope
//
// The grouping key (provider, work_scope_id, team_id) has no repo_id, and the
// tables replace rows by that key. The family therefore computes a work scope
// once, over the items of every repository of the organization, and writes it
// once for the partition: see work_item_scope_read.go.
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

	// The version is taken before the reads: the newest version is then the
	// newest read, also when two partitions of one run share a work scope.
	computedAt := executor.nowUTC()
	read, err := loadWorkItemScopeRead(ctx, executor.conn, "work_item", run, partition, scope, true, true)
	if err != nil {
		return 0, err
	}
	if len(read.Items) == 0 {
		return 0, nil
	}

	sorted := sortWorkItemMetricsRows(read.Items)
	projected := workItemMetricsItems(sorted)
	triplet := workitemmetrics.ComputeDailyTriplet(
		scope.day,
		projected,
		workItemMetricsTransitions(read.Transitions),
		workitemmetrics.AssertAligned(len(sorted), projected, workItemMetricsResolver(sorted, read.Attributions)),
	)

	// Each Write* call reports its true row count on an ambiguous Send error,
	// so the count is added before the error check.
	total, err := WriteWorkItemMetricsDaily(
		ctx, executor.conn, run.OrganizationID, scope.day, triplet.MetricsDaily, computedAt,
	)
	if err != nil {
		return wrapWorkItemScopePartialWrite("work_item", total, partition, err)
	}
	written, err := WriteWorkItemUserMetricsDaily(
		ctx, executor.conn, run.OrganizationID, scope.day, triplet.UserMetricsDaily, computedAt,
	)
	total += written
	if err != nil {
		return wrapWorkItemScopePartialWrite("work_item", total, partition, err)
	}
	written, err = WriteWorkItemCycleTimes(
		ctx, executor.conn, run.OrganizationID, triplet.CycleTimes, computedAt,
	)
	total += written
	if err != nil {
		return wrapWorkItemScopePartialWrite("work_item", total, partition, err)
	}
	// The stale-key rule (stale_team_keys.go): a (scope, team) key of the day
	// that this compute no longer produces gets a row of zeros.
	produced := make([]staleKey, 0, len(triplet.MetricsDaily))
	for _, row := range triplet.MetricsDaily {
		produced = append(produced, staleKey{row.Provider, row.WorkScopeID, row.TeamID})
	}
	written, err = supersedeStaleTeamKeys(
		ctx, executor.conn, teamkeytables.WorkItemMetricsDaily, run.OrganizationID, scope.day,
		read.staleKeyScope(), produced, computedAt,
	)
	total += written
	if err != nil {
		return wrapWorkItemScopePartialWrite("work_item", total, partition, err)
	}
	return total, nil
}

// workItemPartitionScope is the (day, window, repoIDs) triple the work-item
// executors derive identically from a run/partition pair.
type workItemPartitionScope struct {
	day, start, end time.Time
	repoIDs         []uuid.UUID
}

// wrapWorkItemPartialWrite is the partial-write rule of the families that
// loop the partition's repositories (work_item_issue_type and
// work_item_investment): a later repository can fail after the rows of an
// earlier one landed. total == 0 returns the error unwrapped (a genuine
// refusal, nothing to distinguish); total > 0 wraps ErrPartialWrite naming
// the repo and the true row count, so daily.go's dispatcher (which only
// distinguishes ErrPartialWrite from every other error) reports
// PartialWrite/N-rows instead of Refused/0-rows when real rows already
// landed. Mirrors wrapWorkGraphEdgesPartialWrite.
func wrapWorkItemPartialWrite(family string, total int, repoID uuid.UUID, err error) (int, error) {
	if total == 0 {
		return 0, err
	}
	return total, fmt.Errorf("%w: %s failed on repo %s after %d row(s) already landed: %w",
		ErrPartialWrite, family, repoID, total, err)
}

// wrapWorkItemScopePartialWrite is wrapWorkItemPartialWrite for the families
// that write once for a partition: a failure after rows landed is
// ErrPartialWrite with the true row count, and with no row landed the error is
// returned unwrapped (a genuine refusal).
func wrapWorkItemScopePartialWrite(family string, total int, partition Partition, err error) (int, error) {
	if total == 0 {
		return 0, err
	}
	return total, fmt.Errorf("%w: %s failed on partition %s after %d row(s) already landed: %w",
		ErrPartialWrite, family, partition.ID, total, err)
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
