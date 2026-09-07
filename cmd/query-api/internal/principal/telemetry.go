package principal

import (
	"context"
	"log/slog"
	"strings"

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

// envelopeRejectedCounter is CHAOS-5443's deliverable: a per-REASON
// breakdown of envelope rejections, registered the same way (same
// otel.Meter call, same package) as verifyOutcomeCounter above and every
// other query-api instrument -- so it is exported wherever this
// process's OTel MeterProvider already ships verifyOutcomeCounter,
// with no separate registration step. recordVerifyOutcome's "rejected"
// bucket already told an operator THAT requests are failing; this tells
// them WHY, without reading a log line, which is what makes an
// unknown_kid spike (a rotation gone wrong) distinguishable at a glance
// from an expired spike (clock skew, or nothing -- tokens just aged out).
var envelopeRejectedCounter = mustEnvelopeRejectedCounter()

func mustEnvelopeRejectedCounter() metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal")
	counter, err := meter.Int64Counter(
		"dev_health_query_api_envelope_rejected_total",
		metric.WithDescription("Effective-principal envelope rejections by query-api, by reason"),
	)
	if err != nil {
		// Same otel guarantee mustVerifyOutcomeCounter relies on above.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("dev_health_query_api_envelope_rejected_total")
	}
	return counter
}

// recordEnvelopeRejected increments envelopeRejectedCounter. reason is
// one of classifyRejectionReason's seven values.
func recordEnvelopeRejected(reason string) {
	envelopeRejectedCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// logEnvelopeRejection is CHAOS-5443's other deliverable: a WARN log at
// the rejection site naming WHY, in place of the previous total silence
// (a bare "unauthorized" 401 with nothing in the logs at all). kid, iss,
// and aud are logged because they are exactly what a kid-mismatch or
// audience/issuer misconfiguration needs to diagnose in minutes instead
// of the 40 this ticket's live incident took -- tokenString and any key
// material are never passed to this function at all, so there is nothing
// here that could leak them even by a future editing mistake.
func logEnvelopeRejection(ctx context.Context, reason, kid string, claims *Claims, meta requestMeta) {
	slog.WarnContext(ctx, "query_api.envelope.rejected",
		"reason", reason,
		"kid", kid,
		"iss", claims.Issuer,
		"aud", strings.Join(claims.Audience, ","),
		"remote_addr", meta.remoteAddr,
		"request_id", meta.requestID,
	)
}
