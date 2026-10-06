package daily

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
)

// WorkItemIssueTypeFamilyName is the families.json name of the family that
// writes issue_type_metrics_daily.
const WorkItemIssueTypeFamilyName = "work_item_issue_type"

// WorkItemIssueTypeExecutor is the daily family that computes
// issue_type_metrics_daily from STORED work items.
//
// # Why this family exists
//
// Until it existed the only writer of the table was the work-items sync unit,
// which computes a day from only the items that unit fetched. After one hourly
// unit with one changed issue the day's newest rows counted one issue. The
// owner's design is that the sync writes raw rows and the daily job computes
// every derived table from stored rows; this is that computation for this
// table.
//
// # The arithmetic is NOT here
//
// It is workitemengine.ComputeIssueTypeMetricsDaily, the same function the sync
// deriver calls. This file is the family's I/O: load, resolve, compute, write.
//
// # Team attribution: READ, not recompute
//
// As every other work-item daily family: the primary row of
// work_item_team_attributions (see LoadWorkItemPrimaryTeamAttributions).
// families.json orders this family after work_item_attribution, which writes
// that table in the same partition.
//
// # Append only, newest per key
//
// The table is plain MergeTree. Every run appends one row per key of the
// repository's stored items; readers take the newest computed_at per key. A
// key that had a live row for the day and that this run no longer produces
// gets a row of zeros, from this run's compute, so the older row does not stay
// the newest one. A repository with no stored item and no earlier row for the
// day gets nothing: a day with no data is never filled.
type WorkItemIssueTypeExecutor struct {
	conn       driver.Conn
	normalizer workitemengine.TypeNormalizer
	nowUTC     func() time.Time
}

var errWorkItemIssueTypeUnavailable = errors.New("work_item_issue_type native executor unavailable")

// NewWorkItemIssueTypeExecutor fails closed on a nil connection or a nil
// status-mapping engine: without the engine there is no type rule, and a
// family that wrote raw provider types would write different keys from the
// sync deriver.
func NewWorkItemIssueTypeExecutor(
	conn driver.Conn, normalizer workitemengine.TypeNormalizer,
) (*WorkItemIssueTypeExecutor, error) {
	if conn == nil || isNilEngine(normalizer) {
		return nil, errWorkItemIssueTypeUnavailable
	}
	return &WorkItemIssueTypeExecutor{
		conn: conn, normalizer: normalizer,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFamily computes the day's issue-type rows of every repository of the
// partition.
func (executor *WorkItemIssueTypeExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil || isNilEngine(executor.normalizer) {
		return 0, errWorkItemIssueTypeUnavailable
	}
	scope, err := newWorkItemPartitionScope(run, partition, WorkItemIssueTypeFamilyName)
	if err != nil {
		return 0, err
	}

	total := 0
	for _, repoID := range scope.repoIDs {
		items, err := LoadWorkItemEngineWorkItems(
			ctx, executor.conn, run.OrganizationID, repoID, scope.start, scope.end, false,
		)
		if err != nil {
			return wrapWorkItemPartialWrite(WorkItemIssueTypeFamilyName, total, repoID, err)
		}
		live, err := LoadIssueTypeMetricsLiveKeys(ctx, executor.conn, run.OrganizationID, repoID, scope.day)
		if err != nil {
			return wrapWorkItemPartialWrite(WorkItemIssueTypeFamilyName, total, repoID, err)
		}
		if len(items) == 0 && len(live) == 0 {
			continue
		}
		attributions := map[string]workItemPrimaryAttribution{}
		if len(items) > 0 {
			attributions, err = LoadWorkItemPrimaryTeamAttributions(
				ctx, executor.conn, run.OrganizationID, repoID,
			)
			if err != nil {
				return wrapWorkItemPartialWrite(WorkItemIssueTypeFamilyName, total, repoID, err)
			}
		}

		computedAt := executor.nowUTC()
		rows := workitemengine.ComputeIssueTypeMetricsDaily(
			items, scope.start, scope.end,
			workItemEngineTeamResolver(items, attributions), executor.normalizer,
		)
		rows = withIssueTypeMetricsZeroRows(rows, live, repoID)

		written, err := WriteIssueTypeMetricsDaily(
			ctx, executor.conn, run.OrganizationID, scope.day, rows, computedAt,
		)
		total += written
		if err != nil {
			return wrapWorkItemPartialWrite(WorkItemIssueTypeFamilyName, total, repoID, err)
		}
	}
	return total, nil
}

var _ NativeFamilyExecutor = (*WorkItemIssueTypeExecutor)(nil)
