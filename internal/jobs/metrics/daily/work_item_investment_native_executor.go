package daily

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
)

// WorkItemInvestmentFamilyName is the families.json name of the family that writes work_item_completion_daily.
// The name is the family's old one (it wrote the rule-classified investment tables); renaming it would move the
// families contract, the migration matrix and the CI maps for no change of behaviour.
const WorkItemInvestmentFamilyName = "work_item_investment"

// WorkItemInvestmentExecutor is the daily family that computes work_item_completion_daily from STORED work items
// (CHAOS-9264): what each repository and team completed on the day, with no rule classification. See
// WorkItemIssueTypeExecutor for why the family exists.
//
// # Which items
//
// The items active on the day (created before the day ends, not completed before it starts): the compute skips
// every other item, so the narrower read cannot change a row.
//
// # Append only, newest per key
//
// The table is plain MergeTree. It has one row per (repository, team) that had a completion on the day. When a
// completion leaves a key (the item was reopened or its team changed) the compute no longer produces that key, so
// this family writes a row of zeros for each key whose newest row of the day is not zero: a reader that takes the
// newest computed_at per key then sees the zero, and the completion is not counted under two keys. A day with no
// earlier row and no completion gets no row.
type WorkItemInvestmentExecutor struct {
	conn   driver.Conn
	nowUTC func() time.Time
}

var errWorkItemInvestmentUnavailable = errors.New("work_item_investment native executor unavailable")

// NewWorkItemInvestmentExecutor fails closed on a nil connection.
func NewWorkItemInvestmentExecutor(conn driver.Conn) (*WorkItemInvestmentExecutor, error) {
	if conn == nil {
		return nil, errWorkItemInvestmentUnavailable
	}
	return &WorkItemInvestmentExecutor{conn: conn, nowUTC: func() time.Time { return time.Now().UTC() }}, nil
}

// ComputeFamily computes the day's completion rows of every repository of the partition.
func (executor *WorkItemInvestmentExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
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
		live, err := LoadWorkItemCompletionLiveKeys(ctx, executor.conn, run.OrganizationID, repoID, scope.day)
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
		rows, err := workitemengine.ComputeWorkItemCompletionDaily(
			items, scope.start, scope.end, workItemEngineTeamResolver(items, attributions),
		)
		if err != nil {
			return wrapWorkItemPartialWrite(WorkItemInvestmentFamilyName, total, repoID, err)
		}
		rows = withWorkItemCompletionZeroRows(rows, live, repoID)

		written, err := WriteWorkItemCompletionDaily(
			ctx, executor.conn, run.OrganizationID, scope.day, rows, computedAt,
		)
		total += written
		if err != nil {
			return wrapWorkItemPartialWrite(WorkItemInvestmentFamilyName, total, repoID, err)
		}
	}
	return total, nil
}

var _ NativeFamilyExecutor = (*WorkItemInvestmentExecutor)(nil)

// isNilEngine reports a nil interface or an interface that holds a nil pointer. The worker passes concrete engine
// pointers; a typed nil would pass a plain `== nil` check and fail on the first item.
func isNilEngine(engine any) bool {
	if engine == nil {
		return true
	}
	value := reflect.ValueOf(engine)
	return value.Kind() == reflect.Pointer && value.IsNil()
}
