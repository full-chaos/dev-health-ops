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
	featureFlagsCallCounter            = mustCounter("devhealth_query_api_feature_flags_calls_total", "featureFlags resolver invocations")
	featureFlagsOutcomeCounter         = mustCounter("devhealth_query_api_feature_flags_outcome_total", "featureFlags resolver outcomes, by result")
	reviewEdgesCallCounter             = mustCounter("devhealth_query_api_review_edges_calls_total", "reviewEdges resolver invocations")
	reviewEdgesOutcomeCounter          = mustCounter("devhealth_query_api_review_edges_outcome_total", "reviewEdges resolver outcomes, by result")
	cognitiveLoadCallCounter           = mustCounter("devhealth_query_api_cognitive_load_calls_total", "cognitiveLoad resolver invocations")
	cognitiveLoadOutcomeCounter        = mustCounter("devhealth_query_api_cognitive_load_outcome_total", "cognitiveLoad resolver outcomes, by result")
	complexityTimeseriesCallCounter    = mustCounter("devhealth_query_api_complexity_timeseries_calls_total", "complexityTimeseries resolver invocations")
	complexityTimeseriesOutcomeCounter = mustCounter("devhealth_query_api_complexity_timeseries_outcome_total", "complexityTimeseries resolver outcomes, by result")
	hotspotsCallCounter                = mustCounter("devhealth_query_api_hotspots_calls_total", "hotspots resolver invocations")
	hotspotsOutcomeCounter             = mustCounter("devhealth_query_api_hotspots_outcome_total", "hotspots resolver outcomes, by result")
	operatingReviewCallCounter         = mustCounter("devhealth_query_api_operating_review_calls_total", "operatingReview resolver invocations")
	operatingReviewOutcomeCounter      = mustCounter("devhealth_query_api_operating_review_outcome_total", "operatingReview resolver outcomes, by result")
	workGraphEdgesCallCounter          = mustCounter("devhealth_query_api_work_graph_edges_calls_total", "workGraphEdges resolver invocations")
	workGraphEdgesOutcomeCounter       = mustCounter("devhealth_query_api_work_graph_edges_outcome_total", "workGraphEdges resolver outcomes, by result")
	workGraphFlowCallCounter           = mustCounter("devhealth_query_api_work_graph_flow_calls_total", "workGraphFlow resolver invocations")
	workGraphFlowOutcomeCounter        = mustCounter("devhealth_query_api_work_graph_flow_outcome_total", "workGraphFlow resolver outcomes, by result")
	workGraphArtifactsCallCounter      = mustCounter("devhealth_query_api_work_graph_artifacts_calls_total", "workGraphArtifacts resolver invocations")
	workGraphArtifactsOutcomeCounter   = mustCounter("devhealth_query_api_work_graph_artifacts_outcome_total", "workGraphArtifacts resolver outcomes, by result")
	analyticsCallCounter               = mustCounter("devhealth_query_api_analytics_calls_total", "analytics resolver invocations")
	analyticsOutcomeCounter            = mustCounter("devhealth_query_api_analytics_outcome_total", "analytics resolver outcomes, by result")

	tracer = otel.Tracer("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph")
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

// startCognitiveLoadSpan is startFeatureFlagsSpan's counterpart for the
// cognitiveLoad resolver (CHAOS-4369 Wave 3) -- same "count and start the
// span before the ClickHouse query/queries, finish only once real
// resolver work completes" contract.
func startCognitiveLoadSpan(ctx context.Context) (context.Context, func(outcome string)) {
	cognitiveLoadCallCounter.Add(ctx, 1)
	spanCtx, span := tracer.Start(ctx, "query-api.cognitiveLoad")
	return spanCtx, func(outcome string) {
		span.SetAttributes(attribute.String("outcome", outcome))
		if outcome == "error" {
			span.SetStatus(codes.Error, "cognitiveLoad resolver error")
		}
		span.End()
		recordCognitiveLoadOutcome(outcome)
	}
}

// recordCognitiveLoadOutcome increments the outcome counter. outcome is
// one of "ok" or "error" -- like reviewEdges (unlike featureFlags),
// cognitiveLoad has no degraded-result path, so "degraded" is not part of
// this operation's outcome vocabulary.
func recordCognitiveLoadOutcome(outcome string) {
	cognitiveLoadOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// startComplexityTimeseriesSpan is startFeatureFlagsSpan's counterpart for
// the complexityTimeseries resolver (CHAOS-4369 Wave 3) -- same "count and
// start the span before the ClickHouse query, finish only once real
// resolver work completes" contract.
func startComplexityTimeseriesSpan(ctx context.Context) (context.Context, func(outcome string)) {
	complexityTimeseriesCallCounter.Add(ctx, 1)
	spanCtx, span := tracer.Start(ctx, "query-api.complexityTimeseries")
	return spanCtx, func(outcome string) {
		span.SetAttributes(attribute.String("outcome", outcome))
		if outcome == "error" {
			span.SetStatus(codes.Error, "complexityTimeseries resolver error")
		}
		span.End()
		recordComplexityTimeseriesOutcome(outcome)
	}
}

// recordComplexityTimeseriesOutcome increments the outcome counter.
// outcome is one of "ok" or "error" -- complexityTimeseries has no
// degraded-result path (unlike featureFlags; see complexitytimeseries
// package's doc comment), so "degraded" is not part of this operation's
// outcome vocabulary.
func recordComplexityTimeseriesOutcome(outcome string) {
	complexityTimeseriesOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// startHotspotsSpan is startFeatureFlagsSpan's counterpart for the
// hotspots resolver (CHAOS-4369 Wave 3) -- same "count and start the
// span before the ClickHouse query, finish only once real resolver work
// completes" contract.
func startHotspotsSpan(ctx context.Context) (context.Context, func(outcome string)) {
	hotspotsCallCounter.Add(ctx, 1)
	spanCtx, span := tracer.Start(ctx, "query-api.hotspots")
	return spanCtx, func(outcome string) {
		span.SetAttributes(attribute.String("outcome", outcome))
		if outcome == "error" {
			span.SetStatus(codes.Error, "hotspots resolver error")
		}
		span.End()
		recordHotspotsOutcome(outcome)
	}
}

// recordHotspotsOutcome increments the outcome counter. outcome is one
// of "ok" or "error" -- hotspots has no degraded-result path (unlike
// featureFlags; see hotspots package's doc comment), so "degraded" is
// not part of this operation's outcome vocabulary.
func recordHotspotsOutcome(outcome string) {
	hotspotsOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// startOperatingReviewSpan is startFeatureFlagsSpan's counterpart for the
// operatingReview resolver (CHAOS-4352 Wave 4 Lane B, CHAOS-4505) -- same
// "count and start the span before the ClickHouse queries, finish only
// once real resolver work completes" contract. This span/counter pair
// measures the WHOLE resolver call (all 20 queries, both periods); the
// operatingreview package's own
// devhealth_query_api_operating_review_fetch_swallowed_total counter
// (declared in that package, not here -- see its doc comment for why)
// measures the finer, per-table swallow granularity this span cannot see.
func startOperatingReviewSpan(ctx context.Context) (context.Context, func(outcome string)) {
	operatingReviewCallCounter.Add(ctx, 1)
	spanCtx, span := tracer.Start(ctx, "query-api.operatingReview")
	return spanCtx, func(outcome string) {
		span.SetAttributes(attribute.String("outcome", outcome))
		if outcome == "error" {
			span.SetStatus(codes.Error, "operatingReview resolver error")
		}
		span.End()
		recordOperatingReviewOutcome(outcome)
	}
}

// recordOperatingReviewOutcome increments the outcome counter. outcome is
// "ok" or "error" at THIS span's granularity -- a per-table swallow inside
// operatingreview.Resolve does not surface as "error" here (Resolve
// itself does not fail when a table's fetch is swallowed; see
// operatingreview's package doc comment), only a genuine top-level
// failure (e.g. a nil client) does.
func recordOperatingReviewOutcome(outcome string) {
	operatingReviewOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// startWorkGraphEdgesSpan is startFeatureFlagsSpan's counterpart for the
// workGraphEdges resolver (CHAOS-4352 Wave 4 Lane A / CHAOS-4504). Unlike
// reviewEdges/cognitiveLoad/complexityTimeseries/hotspots, workGraphEdges
// DOES have a degraded-result path (MEMBERSHIP_NOT_MATERIALIZED, same as
// featureFlags) -- callers check result.DegradedReason and call
// finish("degraded") rather than finish("ok"), same as startFeatureFlagsSpan's
// call site.
func startWorkGraphEdgesSpan(ctx context.Context) (context.Context, func(outcome string)) {
	workGraphEdgesCallCounter.Add(ctx, 1)
	spanCtx, span := tracer.Start(ctx, "query-api.workGraphEdges")
	return spanCtx, func(outcome string) {
		span.SetAttributes(attribute.String("outcome", outcome))
		if outcome == "error" {
			span.SetStatus(codes.Error, "workGraphEdges resolver error")
		}
		span.End()
		recordWorkGraphEdgesOutcome(outcome)
	}
}

// recordWorkGraphEdgesOutcome increments the outcome counter. outcome is
// one of "ok" (a real, non-degraded result), "degraded"
// (MEMBERSHIP_NOT_MATERIALIZED), or "error".
func recordWorkGraphEdgesOutcome(outcome string) {
	workGraphEdgesOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// startWorkGraphFlowSpan is startWorkGraphEdgesSpan's counterpart for the
// workGraphFlow resolver -- same degraded-result outcome vocabulary.
func startWorkGraphFlowSpan(ctx context.Context) (context.Context, func(outcome string)) {
	workGraphFlowCallCounter.Add(ctx, 1)
	spanCtx, span := tracer.Start(ctx, "query-api.workGraphFlow")
	return spanCtx, func(outcome string) {
		span.SetAttributes(attribute.String("outcome", outcome))
		if outcome == "error" {
			span.SetStatus(codes.Error, "workGraphFlow resolver error")
		}
		span.End()
		recordWorkGraphFlowOutcome(outcome)
	}
}

// recordWorkGraphFlowOutcome increments the outcome counter -- one of
// "ok", "degraded", or "error".
func recordWorkGraphFlowOutcome(outcome string) {
	workGraphFlowOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// startWorkGraphArtifactsSpan is startWorkGraphEdgesSpan's counterpart for
// the workGraphArtifacts resolver -- same degraded-result outcome
// vocabulary.
func startWorkGraphArtifactsSpan(ctx context.Context) (context.Context, func(outcome string)) {
	workGraphArtifactsCallCounter.Add(ctx, 1)
	spanCtx, span := tracer.Start(ctx, "query-api.workGraphArtifacts")
	return spanCtx, func(outcome string) {
		span.SetAttributes(attribute.String("outcome", outcome))
		if outcome == "error" {
			span.SetStatus(codes.Error, "workGraphArtifacts resolver error")
		}
		span.End()
		recordWorkGraphArtifactsOutcome(outcome)
	}
}

// recordWorkGraphArtifactsOutcome increments the outcome counter -- one of
// "ok", "degraded", or "error".
func recordWorkGraphArtifactsOutcome(outcome string) {
	workGraphArtifactsOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// startAnalyticsSpan is startFeatureFlagsSpan's counterpart for the
// analytics operation (CHAOS-4506). Only "ok"/"error" are recorded HERE
// -- this is the resolver-level verdict (did Resolve return an error at
// all), which is a coarser signal than the sankey/flowMatrix swallow
// events internal/analytics/telemetry.go records at their own finer
// granularity (a batch that swallows a sankey execution failure but
// otherwise succeeds still reports "ok" here, with the swallow itself
// visible via the analytics-package-level counter instead -- the two are
// deliberately not merged into one vocabulary, see that file's doc
// comment for why).
func startAnalyticsSpan(ctx context.Context) (context.Context, func(outcome string)) {
	analyticsCallCounter.Add(ctx, 1)
	spanCtx, span := tracer.Start(ctx, "query-api.analytics")
	return spanCtx, func(outcome string) {
		span.SetAttributes(attribute.String("outcome", outcome))
		if outcome == "error" {
			span.SetStatus(codes.Error, "analytics resolver error")
		}
		span.End()
		recordAnalyticsOutcome(outcome)
	}
}

// recordAnalyticsOutcome increments the outcome counter. outcome is one
// of "ok" or "error" -- see startAnalyticsSpan's doc comment for why
// "degraded" is not part of this vocabulary.
func recordAnalyticsOutcome(outcome string) {
	analyticsOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}
