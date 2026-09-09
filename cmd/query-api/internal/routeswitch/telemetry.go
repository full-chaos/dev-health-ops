package routeswitch

// New logic gets telemetry in the same PR (root AGENTS.md standing
// order). CHAOS-5415: PostgresSwitch.Enabled silently fell back to
// Python whenever (schema_digest, document_digest, operation) missed a
// go_api_routing_state row -- no counter, no log line, indistinguishable
// from "not canaried yet" to an operator. Live-proven: on the bigboy
// compose stack every one of the 12 existing rows was keyed on a
// schema_digest one SDL move stale (CHAOS-4703), so every delegated
// operation had been silently serving Python since -- undetected,
// because this signal did not exist.

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var digestMissCounter = mustRouteswitchCounter(
	"devhealth_query_api_routeswitch_digest_miss_total",
	"PostgresSwitch.Enabled lookups against go_api_routing_state that found no row for the (schema_digest, document_digest, operation) key -- delegation is silently serving Python for that operation, by operation",
)

func mustRouteswitchCounter(name, description string) metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/routeswitch")
	counter, err := meter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		// Same otel guarantee analytics/telemetry.go relies on: the
		// instrument constructor never returns a nil instrument even on
		// error, so a broken meter provider must not panic a request
		// path over an observability concern.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

// recordDigestMiss is a package var, not a plain func -- same reasoning
// as analytics.recordDegradation: "the metric+log fired" is the only
// observable that distinguishes an instrumented digest-miss fallback
// from one that silently reverts to Python, so a test must be able to
// substitute a spy here.
var recordDigestMiss = defaultRecordDigestMiss

func defaultRecordDigestMiss(ctx context.Context, operation, schemaDigest, documentDigest string) {
	digestMissCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", operation)))
	slog.WarnContext(ctx, "routeswitch: no go_api_routing_state row for operation; falling back to Python",
		"operation", operation, "schema_digest", schemaDigest, "document_digest", documentDigest)
}
