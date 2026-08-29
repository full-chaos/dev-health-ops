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
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
)

var (
	featureFlagsCallCounter    = mustCounter("devhealth_query_api_feature_flags_calls_total", "featureFlags resolver invocations")
	featureFlagsOutcomeCounter = mustCounter("devhealth_query_api_feature_flags_outcome_total", "featureFlags resolver outcomes, by result")
	reviewEdgesCallCounter     = mustCounter("devhealth_query_api_review_edges_calls_total", "reviewEdges resolver invocations")
	reviewEdgesOutcomeCounter  = mustCounter("devhealth_query_api_review_edges_outcome_total", "reviewEdges resolver outcomes, by result")
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

// startFeatureFlagsSpan starts a span for one featureFlags resolver
// invocation and counts it -- called at resolver entry, before the
// ClickHouse query, so "did we get any traffic" is answerable even if
// the query itself never returns. The returned context carries the span;
// callers MUST use it for the resolver work they want measured and MUST
// call the returned finish func exactly once (typically via defer) once
// that work completes, passing the outcome that closes it out.
//
// Starting and ending the span back-to-back here, before doing any real
// work, produced a zero-duration span with no resolver latency or
// failure information for every request (codex review, 2026-08-28) --
// telemetry that looks like coverage but measures nothing, the same
// "invisible fallback" class of defect this file's own package doc
// warns about.
func startFeatureFlagsSpan(ctx context.Context) (context.Context, func(outcome string)) {
	featureFlagsCallCounter.Add(ctx, 1)
	spanCtx, span := tracer.Start(ctx, "query-api.featureFlags")
	return spanCtx, func(outcome string) {
		span.SetAttributes(attribute.String("outcome", outcome))
		if outcome == "error" {
			span.SetStatus(codes.Error, "featureFlags resolver error")
		}
		span.End()
		recordFeatureFlagsOutcome(outcome)
	}
}

// recordFeatureFlagsOutcome increments the outcome counter. outcome is
// one of "ok" (a real, non-degraded result), "degraded" (the
// FEATURE_FLAG_NOT_MATERIALIZED path), or "error".
func recordFeatureFlagsOutcome(outcome string) {
	featureFlagsOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// startReviewEdgesSpan is startFeatureFlagsSpan's counterpart for the
// reviewEdges resolver (CHAOS-4368 Wave 2) -- same "count and start the
// span before the ClickHouse query, finish only once real resolver work
// completes" contract, so a hung or never-returning query still shows up
// as traffic received.
func startReviewEdgesSpan(ctx context.Context) (context.Context, func(outcome string)) {
	reviewEdgesCallCounter.Add(ctx, 1)
	spanCtx, span := tracer.Start(ctx, "query-api.reviewEdges")
	return spanCtx, func(outcome string) {
		span.SetAttributes(attribute.String("outcome", outcome))
		if outcome == "error" {
			span.SetStatus(codes.Error, "reviewEdges resolver error")
		}
		span.End()
		recordReviewEdgesOutcome(outcome)
	}
}

// recordReviewEdgesOutcome increments the outcome counter. outcome is
// one of "ok" or "error" -- reviewEdges has no degraded-result path
// (unlike featureFlags; see reviewedges package's doc comment), so
// "degraded" is not part of this operation's outcome vocabulary.
func recordReviewEdgesOutcome(outcome string) {
	reviewEdgesOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}
