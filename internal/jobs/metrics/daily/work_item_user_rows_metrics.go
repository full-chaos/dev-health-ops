package daily

import (
	"context"
	"sort"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// WorkItemUserRowsSpanningTeamsCounter counts the persons whose items of one day
// belong to more than one team, for each provider: the one row work_item_user_metrics_daily
// keeps for such a person is labelled by one of the teams (the team with the most of the
// person's items, the lowest team id on a tie).
const WorkItemUserRowsSpanningTeamsCounter = "dev_health_work_item_user_rows_spanning_teams_total"

// recordUserRowsSpanningTeams adds the persons of one computed partition that span
// teams, once for each provider.
func recordUserRowsSpanningTeams(ctx context.Context, byProvider map[string]int) {
	if len(byProvider) == 0 {
		return
	}
	counter, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily").Int64Counter(
		WorkItemUserRowsSpanningTeamsCounter, metric.WithUnit("{person}"),
		metric.WithDescription("persons whose work items of one day belong to more than one team; their one user-metrics row is labelled by the team with the most items"))
	if err != nil {
		return
	}
	providers := make([]string, 0, len(byProvider))
	for provider := range byProvider {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	for _, provider := range providers {
		counter.Add(context.WithoutCancel(ctx), int64(byProvider[provider]),
			metric.WithAttributes(attribute.String("provider", provider)))
	}
}
