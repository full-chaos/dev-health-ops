package providersync

import (
	"context"
	"log/slog"

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

// The reasons of the Jira snapshot terms. They are also the label values of
// jira_team_catalog_ownership_snapshot_incomplete_total.
const (
	jiraSnapshotProjectSearch = "project_search"
	jiraSnapshotLegacyLinks   = "legacy_links"
)

// judgeJiraOwnershipSnapshot reports the plan of one Jira catalog write: an
// abandoned close (a read that did not reach its end, or no live ownership
// row while open rows exist) is counted per reason and logged, and closed
// nothing. It returns whether the close was abandoned.
func judgeJiraOwnershipSnapshot(ctx context.Context, orgID string, plan SnapshotPlan, keptRows int) bool {
	if !ReportSnapshotPlan(ctx, "jira", orgID, plan) {
		return false
	}
	searchComplete, legacyComplete, liveEmpty := true, true, false
	for _, reason := range plan.SnapshotReasons() {
		switch reason {
		case jiraSnapshotProjectSearch:
			searchComplete = false
		case jiraSnapshotLegacyLinks:
			legacyComplete = false
		case SnapshotEmptyAnswer:
			liveEmpty = true
		}
	}
	recordJiraOwnershipSnapshotIncomplete(ctx, searchComplete, legacyComplete, liveEmpty)
	slog.Default().WarnContext(ctx, "jira_team_catalog_ownership_snapshot_incomplete",
		"org_id", orgID, "project_search_complete", searchComplete,
		"legacy_links_complete", legacyComplete, "no_live_ownership", liveEmpty, "open_rows_kept", keptRows)
	return true
}
