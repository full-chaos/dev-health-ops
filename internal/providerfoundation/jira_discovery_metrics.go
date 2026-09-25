package providerfoundation

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// jiraProjectDiscoveryName is the Python api's JIRA_PROJECT_DISCOVERY_TOTAL
// (metrics/prometheus.py). It is ONE instrument for every outcome and every
// caller: the source discovery service (internal/scheduler/sync) counts the
// discovery outcomes and the API's enable route counts
// rejected_at_enable_repo_limit, and a second instrument under the same name
// would collide on the api's /metrics.
const jiraProjectDiscoveryName = "jira_project_discovery_total"

// Jira project discovery outcomes, the values of the counter's outcome label.
const (
	JiraDiscoveryDiscovered       = "discovered"
	JiraDiscoveryCreated          = "created"
	JiraDiscoveryExisting         = "existing"
	JiraDiscoveryZero             = "discovered_zero"
	JiraDiscoverySkippedNoPlanner = "skipped_no_planner_parent"
	JiraDiscoverySuperseded       = "superseded_by_scope_change"
	JiraDiscoveryCapped           = "capped_by_repo_limit"
	JiraDiscoveryRecoveredFromCap = "recovered_from_repo_limit_cap"
	JiraDiscoveryRejectedAtEnable = "rejected_at_enable_repo_limit"
)

var jiraProjectDiscovery = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/providerfoundation").Int64Counter(
		jiraProjectDiscoveryName,
		metric.WithDescription("Jira per-project integration_sources discovery outcomes, and enables rejected at the org's repo limit."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(jiraProjectDiscoveryName)
	}
	return counter
}()

// RecordJiraProjectDiscovery adds n to jira_project_discovery_total{outcome}.
// A zero n still creates the series, as prometheus_client's inc(0) does; a
// caller that Python guards with a condition guards it the same way.
func RecordJiraProjectDiscovery(ctx context.Context, outcome string, n int) {
	if n < 0 {
		return
	}
	jiraProjectDiscovery.Add(ctx, int64(n), metric.WithAttributes(attribute.String("outcome", outcome)))
}
