package internalidentity

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// New logic gets telemetry in the same PR. An operator must tell "no
// internal callers yet" from "every internal call is refused": this counter
// carries the outcome ("accepted", or the refusal reason) and the carrier
// the request used.
var outcomeCounter = mustOutcomeCounter()

func mustOutcomeCounter() metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity")
	counter, err := meter.Int64Counter(
		"devhealth_query_api_internal_identity_total",
		metric.WithDescription("Internal-path authentications by query-api, by carrier and outcome"),
	)
	if err != nil {
		// otel's guarantee (a nil counter is never returned) makes Add safe.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_query_api_internal_identity_total")
	}
	return counter
}

// RecordOutcome counts one internal-path authentication. carrier is
// "headers", "envelope" or "none"; outcome is "accepted" or a refusal reason.
func RecordOutcome(carrier, outcome string) {
	outcomeCounter.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("carrier", carrier),
		attribute.String("outcome", outcome),
	))
}
