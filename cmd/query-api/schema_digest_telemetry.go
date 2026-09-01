package main

// New logic gets telemetry in the same PR (root AGENTS.md standing
// order). CHAOS-4696 PR2's whole point is that GO_API_SCHEMA_DIGEST was
// an opaque, unverified string whose wrong value made every
// PostgresSwitch lookup fail closed SILENTLY, indistinguishable from
// "not canaried yet" -- a fixed verifySchemaDigest that only logs and
// crashes has the same blind spot one layer up as readyz_telemetry.go's
// own doc comment describes for CHAOS-4512: an operator watching a
// dashboard, not a log/crash-loop stream, needs a signal too. This
// counter is that signal; it does not replace the log.Printf call in
// verifySchemaDigest, which carries the actual configured/computed
// values this counter cannot.

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var schemaDigestOutcomeCounter = mustSchemaDigestOutcomeCounter()

func mustSchemaDigestOutcomeCounter() metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api")
	counter, err := meter.Int64Counter(
		"devhealth_query_api_schema_digest_verify_total",
		metric.WithDescription("query-api GO_API_SCHEMA_DIGEST startup verifications, by outcome"),
	)
	if err != nil {
		// Same otel guarantee readyz_telemetry.go/internal/principal's
		// telemetry.go rely on: Int64Counter never returns a nil counter
		// even on error, so a broken meter provider must not panic
		// startup.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_query_api_schema_digest_verify_total")
	}
	return counter
}

// recordSchemaDigestMismatch increments the schema-digest verification
// counter with outcome="mismatch". There is no corresponding
// recordSchemaDigestMatch call: a match is the only path that reaches
// mounting /query at all, so its count is already implicit in
// process-start metrics/logs, and a per-request or per-process "match"
// increment would only restate "the process is running" in a second
// place. What this counter exists to make visible is the state that was
// PREVIOUSLY INVISIBLE: a mismatch, which crashes the process before
// this binary otherwise emits anything distinguishing "misconfigured"
// from "not started yet".
//
// Same caveat as readyz_telemetry.go's/internal/principal's counters:
// this binary does not currently call otel.SetMeterProvider anywhere
// (internal/platform/tracing.Init wires a TRACER provider only, and
// main.go does not call it at all), so today this Add is a no-op against
// the global default meter provider until a real one is wired up. The
// log.Printf call in verifySchemaDigest is what actually reaches an
// operator today; this counter is the same structural placeholder this
// package's other telemetry already establishes, ready the moment a real
// meter provider is configured, not a claim that it is exported now.
func recordSchemaDigestMismatch() {
	schemaDigestOutcomeCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("outcome", "mismatch")))
}
