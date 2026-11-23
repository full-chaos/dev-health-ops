package providersync

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const workItemDoneWithoutCompletedAtName = "dev_health_work_item_done_without_completed_at_total"

// recordWorkItemsDoneWithoutCompletedAt counts, by provider, the stored rows that are done with no completed_at. The
// daily work item reads leave such an item out on every day, so the counter is how the first one is seen. It counts
// writes: one item stored again by each sync adds one each time.
func recordWorkItemsDoneWithoutCompletedAt(ctx context.Context, rows []workItemStoredRow) {
	byProvider := map[string]int64{}
	for _, row := range rows {
		if row.Status == "done" && row.CompletedAt == nil {
			byProvider[row.Provider]++
		}
	}
	if len(byProvider) == 0 {
		return
	}
	doneWithoutCompletedAt, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/providersync").Int64Counter(
		workItemDoneWithoutCompletedAtName,
		metric.WithDescription("Stored work item rows that are done with no completed_at, by provider. The daily work item reads count such an item on no day."))
	if err != nil {
		return
	}
	for provider, rowCount := range byProvider {
		doneWithoutCompletedAt.Add(ctx, rowCount, metric.WithAttributes(attribute.String("provider", provider)))
	}
}
