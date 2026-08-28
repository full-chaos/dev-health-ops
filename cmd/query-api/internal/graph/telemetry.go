package graph

// New logic gets telemetry in the same PR (root AGENTS.md standing
// order): this is query-api's first live resolver, so without this file
// an operator would have zero signal that the featureFlags route is
// receiving traffic at all, let alone whether it is degrading (missing
// ClickHouse table) or erroring -- the same "invisible fallback" risk
// principal/telemetry.go's doc comment calls out for the envelope
// verifier, one layer up the request path.

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	featureFlagsCallCounter    = mustCounter("devhealth_query_api_feature_flags_calls_total", "featureFlags resolver invocations")
	featureFlagsOutcomeCounter = mustCounter("devhealth_query_api_feature_flags_outcome_total", "featureFlags resolver outcomes, by result")
	tracer                     = otel.Tracer("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph")
)

func mustCounter(name, description string) metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph")
	counter, err := meter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		// Same otel guarantee principal/telemetry.go relies on:
		// Int64Counter never returns a nil counter even on error, so a
		// broken meter provider must not panic the resolver it instruments.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

// recordFeatureFlagsCall starts a span for one featureFlags resolver
// invocation and counts it -- called unconditionally at resolver entry,
// before the ClickHouse query, so "did we get any traffic" is answerable
// even if the query itself never returns.
func recordFeatureFlagsCall(ctx context.Context) {
	featureFlagsCallCounter.Add(ctx, 1)
	_, span := tracer.Start(ctx, "query-api.featureFlags")
	span.End()
}

// recordFeatureFlagsOutcome increments the outcome counter. outcome is
// one of "ok" (a real, non-degraded result), "degraded" (the
// FEATURE_FLAG_NOT_MATERIALIZED path), or "error".
func recordFeatureFlagsOutcome(outcome string) {
	featureFlagsOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}
