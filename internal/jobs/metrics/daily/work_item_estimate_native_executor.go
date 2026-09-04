package daily

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// WorkItemEstimateExecutor is the NATIVE implementation of the
// `work_item_estimate` metrics.daily family (CHAOS-4283) -- ports
// compute_estimate_coverage_metrics_daily
// (src/dev_health_ops/metrics/compute_work_items.py:1425) and writes
// estimate_coverage_metrics_daily.
//
// It is a SEPARATE executor from WorkItemExecutor, not a third return value of
// it, because families.json declares work_item and work_item_estimate as two
// families with two `writes` sets. The registry is per-family and the
// fail-open policy is per-family: if this family's write fails, work_item's
// three tables must still be counted as computed, and vice versa. That is the
// same discipline file_hotspots/file_risk_hotspots follow (CHAOS-4277).
//
// The cost is that both executors load the same repo's work items -- Python
// does the same thing (both computes are called from job_daily.py:1550/1570
// over one shared load, but each re-walks the full list), and the loads are
// FINAL reads of an already-narrow table. Correctness of the family boundary
// beats saving one query.
//
// Attribution, phase, and per-repo iteration are identical to WorkItemExecutor
// -- see its doc comment.
type WorkItemEstimateExecutor struct {
	conn   driver.Conn
	nowUTC func() time.Time
}

var errWorkItemEstimateUnavailable = errors.New("work_item_estimate native executor unavailable")

// NewWorkItemEstimateExecutor fails closed on a nil connection.
func NewWorkItemEstimateExecutor(conn driver.Conn) (*WorkItemEstimateExecutor, error) {
	if conn == nil {
		return nil, errWorkItemEstimateUnavailable
	}
	return &WorkItemEstimateExecutor{conn: conn, nowUTC: func() time.Time { return time.Now().UTC() }}, nil
}

// ComputeFamily runs the work_item_estimate computation for one partition.
func (executor *WorkItemEstimateExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errWorkItemEstimateUnavailable
	}
	scope, err := newWorkItemPartitionScope(run, partition, "work_item_estimate")
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
			continue
		}
		attributions, err := LoadWorkItemPrimaryTeamAttributions(
			ctx, executor.conn, run.OrganizationID, repoID,
		)
		if err != nil {
			return total, err
		}

		computedAt := executor.nowUTC()
		sorted := sortWorkItemMetricsRows(items)
		projected := workItemMetricsItems(sorted)
		rows := workitemmetrics.ComputeEstimateCoverage(
			scope.day,
			projected,
			workitemmetrics.AssertAligned(len(sorted), len(projected), workItemMetricsResolver(sorted, attributions)),
		)

		written, err := WriteEstimateCoverageMetricsDaily(
			ctx, executor.conn, run.OrganizationID, scope.day, rows, computedAt,
		)
		if err != nil {
			return total, err
		}
		total += written
	}
	return total, nil
}

var _ NativeFamilyExecutor = (*WorkItemEstimateExecutor)(nil)
