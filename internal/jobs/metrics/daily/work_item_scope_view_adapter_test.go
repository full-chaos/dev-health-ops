package daily

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// Helpers of the scope-view tests: they name the scope set the tests read.

func scopeViewItems(read workItemScopeRead) []workItemMetricsRow { return read.ScopeItems }

func scopeViewAttribution(read workItemScopeRead, scope, workItemID string) (workItemPrimaryAttribution, bool) {
	attribution, found := read.ScopeAttributions[workitemmetrics.AttributionKey(scope, workItemID)]
	return attribution, found
}

func computeTripletViews(
	ctx context.Context, conn repositoryRows, run Run, partition Partition, scope workItemPartitionScope,
) (workItemScopeRead, workitemmetrics.Triplet, workitemmetrics.Triplet, error) {
	read, views, err := computeWorkItemTriplet(ctx, conn, run, partition, scope)
	return read, views.Org, views.Scope, err
}

func stateViews(scope workItemPartitionScope, read workItemScopeRead, computedAt time.Time) (org, scoped []workItemStateDailyRow) {
	org, _, _, scoped = computeWorkItemStateViews(scope, read, computedAt, nil)
	return org, scoped
}

func writeMetricsViews(ctx context.Context, conn workItemBatchConn, org, scope []workitemmetrics.MetricsDailyRow) (int, error) {
	return WriteWorkItemMetricsDaily(ctx, conn, "org-a", rowViewsDay, org, scope, rowViewsComputedAt)
}

func writeUserViews(ctx context.Context, conn workItemBatchConn, org, scope []workitemmetrics.UserMetricsDailyRow) (int, error) {
	return WriteWorkItemUserMetricsDaily(ctx, conn, "org-a", rowViewsDay, org, scope, rowViewsComputedAt)
}

func writeStateViews(ctx context.Context, conn workItemBatchConn, org, scope []workItemStateDailyRow) (int, error) {
	return WriteWorkItemStateDurationsDaily(ctx, conn.(workItemStateBatchConn), "org-a", rowViewsDay, org, scope, rowViewsComputedAt)
}

func metricsViewKeys(org, scope []workitemmetrics.MetricsDailyRow) []staleKey {
	return workItemViewKeys(org, scope, metricsDailyKey)
}

func userViewKeys(org, scope []workitemmetrics.UserMetricsDailyRow) []staleKey {
	return workItemViewKeys(org, scope, userMetricsDailyKey)
}

func stateViewKeys(org, scope []workItemStateDailyRow) []staleKey {
	return workItemViewKeys(org, scope, stateDurationsDailyKey)
}
