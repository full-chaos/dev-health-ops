package providersync

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const jiraOwnershipSnapshotIncompleteName = "jira_team_catalog_ownership_snapshot_incomplete_total"

var jiraOwnershipSnapshotIncomplete = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/providersync").Int64Counter(
		jiraOwnershipSnapshotIncompleteName,
		metric.WithDescription("Jira team catalog syncs whose ownership snapshot was judged incomplete, so no open row was closed."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(jiraOwnershipSnapshotIncompleteName)
	}
	return counter
}()

// recordJiraOwnershipSnapshotIncomplete counts one incomplete snapshot, once
// per cause that made it so (a snapshot can have several).
func recordJiraOwnershipSnapshotIncomplete(ctx context.Context, searchComplete, legacyComplete, liveEmpty bool) {
	for reason, hit := range map[string]bool{
		"project_search": !searchComplete, "legacy_links": !legacyComplete, "no_live_ownership": liveEmpty,
	} {
		if hit {
			jiraOwnershipSnapshotIncomplete.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
		}
	}
}
