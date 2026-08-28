package principal

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// New logic gets telemetry in the same PR (root AGENTS.md standing order).
// A silently-failing envelope verifier is exactly the "invisible fallback"
// the platform's timeout/fallback discipline warns about: an operator
// needs to be able to tell "no requests yet" apart from "every request is
// being rejected". This counter is the query-api-side equivalent of the
// Python edge's devhealth_go_api_envelope_issued_total.
var verifyOutcomeCounter = mustVerifyOutcomeCounter()

func mustVerifyOutcomeCounter() metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal")
	counter, err := meter.Int64Counter(
		"devhealth_query_api_envelope_verify_total",
		metric.WithDescription("Effective-principal envelope verifications by query-api, by outcome"),
	)
	if err != nil {
		// otel's own contract: a broken counter must not break the verifier
		// it instruments. Falls through to the global no-op meter's
		// behavior instead of panicking -- Int64Counter never returns a nil
		// counter, even on error (a stated go.opentelemetry.io/otel/metric
		// guarantee), so Add below is always safe to call.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_query_api_envelope_verify_total")
	}
	return counter
}

// recordVerifyOutcome increments the verify-outcome counter. outcome is
// one of "verified", "rejected", or "unsupported_schema_version" -- see
// Verify's doc comment.
func recordVerifyOutcome(outcome string) {
	verifyOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}
