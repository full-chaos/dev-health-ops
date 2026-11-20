package remaining

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RecommendationsDiscoveryCounter counts the team discovery runs of the
// recommendations job that left a team set out, by outcome.
const RecommendationsDiscoveryCounter = "dev_health_recommendations_discovery_total"

// recommendationsDiscoveryProjectOwnersSkippedBound: the project ownership read
// hit its row bound, so discovery kept the work-item and repository owner teams
// and skipped the project owner teams.
const recommendationsDiscoveryProjectOwnersSkippedBound = "project_owners_skipped_bound"

func recordRecommendationsDiscoveryOutcome(ctx context.Context, outcome string) {
	counter, err := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining").Int64Counter(
		RecommendationsDiscoveryCounter, metric.WithUnit("{run}"),
		metric.WithDescription("recommendations team discovery runs that left a team set out, by outcome"))
	if err != nil {
		return
	}
	counter.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// warnf logs through the executor's logger when one is set, else slog.
func (executor *RecommendationsExecutor) warnf(msg string, args ...any) {
	if executor.logger != nil {
		executor.logger.Warn(msg, args...)
		return
	}
	slog.Warn(msg, args...)
}
