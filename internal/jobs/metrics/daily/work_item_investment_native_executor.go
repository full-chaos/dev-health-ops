package daily

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
)

// WorkItemInvestmentFamilyName is the families.json name of the family that
// writes investment_classifications_daily and investment_metrics_daily.
const WorkItemInvestmentFamilyName = "work_item_investment"

// WorkItemInvestmentExecutor is the daily family that computes
// investment_classifications_daily and investment_metrics_daily from STORED
// work items. See WorkItemIssueTypeExecutor for why the family exists; the two
// tables are one family because one function computes both in one pass
// (workitemengine.ComputeInvestmentDaily), exactly as the sync deriver does.
//
// # Which items
//
// The items active on the day (created before the day ends, not completed
// before it starts): the compute skips every other item, so the narrower read
// cannot change a row.
//
// # Append only, newest per key
//
// Both tables are plain MergeTree. investment_metrics_daily has one row per
// (repository, team, area, stream) that had a completion on the day. When a
// completion leaves a key (the item was reopened, its team or its labels
// changed) the compute no longer produces that key, so this family writes a
// row of zeros for each key whose newest row of the day is not zero: a reader
// that takes the newest computed_at per key then sees the zero, and the
// completion is not counted under two keys. A day with no earlier row and no
// completion gets no row.
//
// investment_classifications_daily has one row per active item; an item that
// is no longer active on the day keeps its older row (there is no zero for a
// classification, and the table has no reader).
type WorkItemInvestmentExecutor struct {
	conn       driver.Conn
	classifier workitemengine.InvestmentClassifier
	nowUTC     func() time.Time
}

var errWorkItemInvestmentUnavailable = errors.New("work_item_investment native executor unavailable")

// NewWorkItemInvestmentExecutor fails closed on a nil connection or a nil
// investment-rule engine.
func NewWorkItemInvestmentExecutor(
	conn driver.Conn, classifier workitemengine.InvestmentClassifier,
) (*WorkItemInvestmentExecutor, error) {
	if conn == nil || isNilEngine(classifier) {
		return nil, errWorkItemInvestmentUnavailable
	}
	return &WorkItemInvestmentExecutor{
		conn: conn, classifier: classifier,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// isNilEngine reports a nil interface or an interface that holds a nil
// pointer. The worker passes concrete engine pointers; a typed nil would pass
// a plain `== nil` check and fail on the first item.
func isNilEngine(engine any) bool {
	if engine == nil {
		return true
	}
	value := reflect.ValueOf(engine)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

// ComputeFamily computes the day's investment rows of every repository of the
// partition.
func (executor *WorkItemInvestmentExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil || isNilEngine(executor.classifier) {
		return 0, errWorkItemInvestmentUnavailable
	}
	scope, err := newWorkItemPartitionScope(run, partition, WorkItemInvestmentFamilyName)
	if err != nil {
		return 0, err
	}

	total := 0
	for _, repoID := range scope.repoIDs {
		items, err := LoadWorkItemEngineWorkItems(
			ctx, executor.conn, run.OrganizationID, repoID, scope.start, scope.end, true,
		)
		if err != nil {
			return wrapWorkItemPartialWrite(WorkItemInvestmentFamilyName, total, repoID, err)
		}
		live, err := LoadInvestmentMetricsLiveKeys(ctx, executor.conn, run.OrganizationID, repoID, scope.day)
		if err != nil {
			return wrapWorkItemPartialWrite(WorkItemInvestmentFamilyName, total, repoID, err)
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
				return wrapWorkItemPartialWrite(WorkItemInvestmentFamilyName, total, repoID, err)
			}
		}

		computedAt := executor.nowUTC()
		classifications, metrics, err := workitemengine.ComputeInvestmentDaily(
			items, scope.start, scope.end,
			workItemEngineTeamResolver(items, attributions), executor.classifier,
		)
		if err != nil {
			return wrapWorkItemPartialWrite(WorkItemInvestmentFamilyName, total, repoID, err)
		}
		metrics = withInvestmentMetricsZeroRows(metrics, live, repoID)

		written, err := WriteInvestmentClassificationsDaily(
			ctx, executor.conn, run.OrganizationID, scope.day, classifications, computedAt,
		)
		total += written
		if err != nil {
			return wrapWorkItemPartialWrite(WorkItemInvestmentFamilyName, total, repoID, err)
		}
		written, err = WriteInvestmentMetricsDaily(
			ctx, executor.conn, run.OrganizationID, scope.day, metrics, computedAt,
		)
		total += written
		if err != nil {
			return wrapWorkItemPartialWrite(WorkItemInvestmentFamilyName, total, repoID, err)
		}
	}
	return total, nil
}

var _ NativeFamilyExecutor = (*WorkItemInvestmentExecutor)(nil)
