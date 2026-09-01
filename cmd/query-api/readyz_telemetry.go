package main

// New logic gets telemetry in the same PR (root AGENTS.md standing
// order): CHAOS-4512 is itself a readiness check that FAILED SILENTLY --
// /readyz answered 200 unconditionally while /query's dependencies were
// unreachable, with no signal anywhere that this was happening. A fixed
// readyzHandler that only LOGS its verdict has the same blind spot one
// layer up: an operator watching a dashboard, not a log stream, still
// cannot tell "no traffic yet" apart from "every /readyz check is
// failing", nor tell a verified-healthy 200 apart from the
// nothing-to-check unconfigured-mode 200 (see readyzHandler's doc
// comment in main.go). This counter closes that gap; it does not
// replace the log.Printf call in main.go, which carries the actual
// error text this counter cannot.

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var readyzOutcomeCounter = mustReadyzOutcomeCounter()

func mustReadyzOutcomeCounter() metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api")
	counter, err := meter.Int64Counter(
		"devhealth_query_api_readyz_total",
		metric.WithDescription("query-api /readyz checks, by outcome"),
	)
	if err != nil {
		// Same otel guarantee internal/graph/telemetry.go and
		// internal/principal/telemetry.go rely on: Int64Counter never
		// returns a nil counter even on error, so a broken meter
		// provider must not panic the handler it instruments.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_query_api_readyz_total")
	}
	return counter
}

// recordReadyzOutcome increments the /readyz outcome counter. outcome is
// one of "healthy" (dependencies checked and reachable), "unhealthy"
// (a dependency check failed or timed out -- readyzHandler's response
// was 503), or "not_configured" (/query is not mounted in this
// deployment; readyzHandler's response was 200, but there was nothing
// to check -- see that handler's doc comment for why that is not the
// same claim as "healthy").
func recordReadyzOutcome(outcome string) {
	readyzOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}
