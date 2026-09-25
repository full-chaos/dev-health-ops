package integrationsadmin

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// jiraProjectDiscoveryName is the Python api's JIRA_PROJECT_DISCOVERY_TOTAL
// (metrics/prometheus.py). IntegrationSourceService.set_enabled counts one
// outcome="rejected_at_enable_repo_limit" when an operator's enable of a
// Jira source would exceed the org's max_repos.
const jiraProjectDiscoveryName = "jira_project_discovery_total"

var jiraProjectDiscovery = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/api/integrationsadmin").Int64Counter(
		jiraProjectDiscoveryName,
		metric.WithDescription("Jira per-project integration_sources discovery outcomes, and enables rejected at the org's repo limit."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(jiraProjectDiscoveryName)
	}
	return counter
}()

// recordEnableRejectedAtRepoLimit counts one enable refused at max_repos.
func recordEnableRejectedAtRepoLimit(ctx context.Context) {
	jiraProjectDiscovery.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "rejected_at_enable_repo_limit")))
}
