package providersync

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// linearOwnershipSnapshotIncompleteName is the Linear twin of
// jira_team_catalog_ownership_snapshot_incomplete_total: one count per sync
// whose ownership snapshot was judged incomplete, so no open row was closed,
// labelled by the cause.
const linearOwnershipSnapshotIncompleteName = "linear_reference_catalog_ownership_snapshot_incomplete_total"

// recordLinearOwnershipSnapshotIncomplete counts one incomplete snapshot for
// one cause. The counter is resolved at the call, from the current meter
// provider, so a provider installed after start-up still receives it.
func recordLinearOwnershipSnapshotIncomplete(ctx context.Context, reason string) {
	counter, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/providersync").Int64Counter(
		linearOwnershipSnapshotIncompleteName,
		metric.WithDescription("Linear catalog syncs whose ownership snapshot was judged incomplete, so no open row was closed."))
	if err != nil {
		return
	}
	counter.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}
