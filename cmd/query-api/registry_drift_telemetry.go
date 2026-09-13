package main

// The observability half of registry_route.go's drift check.
//
// classifyRoutingDrift already turns a per-digest row census into a
// human-readable log line, and that line already says ROUTING ROWS STALE
// loudly. But a log line only reaches someone who is tailing logs at the
// moment it is written -- the September incident this file exists for sat
// unnoticed for six days precisely because nobody was. Two more durable
// surfaces close that:
//
//   - a gauge pair a dashboard/alert can watch continuously, not just at
//     the moment of a restart;
//   - an ERROR-level structured log record (not the plain stdlib `log`
//     line above it, which carries no level at all) for the one outcome
//     that is an actual incident, so a log-level-based alert rule fires
//     on it and does NOT fire on the "nothing enabled yet" default
//     posture or on a live, healthy digest.
//
// Both are driven from the exact same counts map logRoutingStateDrift
// already computed -- never a second query -- so they cannot see a
// different answer than the log line beside them.
import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// routingRowsForDigestGauge reports how many go_api_routing_state rows are
// keyed to the schema digest THIS process just computed -- zero is exactly
// the condition that made every Go operation unreachable while looking
// healthy in psql. Reported on every startup drift check, healthy or not,
// so a dashboard has a continuous series to chart a deploy marker against
// rather than only ever seeing points during an incident.
var routingRowsForDigestGauge = mustRegistryDriftInt64Gauge(
	"devhealth_query_api_routing_rows_for_digest",
	"go_api_routing_state rows keyed to the schema digest this query-api process computed at startup",
)

// routingRowsTotalGauge is routingRowsForDigestGauge's disambiguator: an
// empty table (0 at digest, 0 total) is the documented safe default, not
// an incident, and reads identically to a fully-stale table (0 at digest,
// N total) on the digest gauge alone. Alerting on
// "routing_rows_for_digest == 0 AND routing_rows_total > 0" is what
// distinguishes them without parsing the log line.
var routingRowsTotalGauge = mustRegistryDriftInt64Gauge(
	"devhealth_query_api_routing_rows_total",
	"go_api_routing_state rows across every schema digest, at query-api startup",
)

func mustRegistryDriftInt64Gauge(name, description string) metric.Int64Gauge {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api")
	gauge, err := meter.Int64Gauge(name, metric.WithDescription(description))
	if err != nil {
		// Same otel guarantee readyz_telemetry.go relies on: the
		// instrument constructor never returns a nil instrument even on
		// error, so a broken meter provider must not stop the process
		// from starting over an observability concern.
		gauge, _ = otel.GetMeterProvider().Meter("noop").Int64Gauge(name)
	}
	return gauge
}

// recordRoutingRowsForDigest is a package var, not a plain func -- same
// reasoning as recordStaleInvestmentMembershipScope and recordDegradation
// elsewhere in this codebase: "the metric and the log record fired" is the
// only observable that distinguishes a correctly-instrumented drift check
// from one that silently emits nothing, so a test must be able to
// substitute a spy here.
var recordRoutingRowsForDigest = defaultRecordRoutingRowsForDigest

// defaultRecordRoutingRowsForDigest records both gauges and, for the one
// outcome that is an actual incident (rows exist, none at the live
// digest), an ERROR-level structured log record distinct from the plain
// stdlib log.Print line beside it in logRoutingStateDrift. The healthy and
// empty-table outcomes log at INFO: present in the record stream so an
// operator can confirm the check itself is alive, but at a level no
// error-level alert rule will fire on.
func defaultRecordRoutingRowsForDigest(ctx context.Context, counts map[string]int64, schemaDigest string) {
	var total int64
	for _, count := range counts {
		total += count
	}
	live := counts[schemaDigest]

	attrs := metric.WithAttributes(attribute.String("schema_digest", schemaDigest))
	routingRowsForDigestGauge.Record(ctx, live, attrs)
	routingRowsTotalGauge.Record(ctx, total, attrs)

	if total > 0 && live == 0 {
		slog.Default().LogAttrs(ctx, slog.LevelError,
			"query-api: routing rows stale -- every go_api_routing_state row is keyed to a schema digest this binary does not compute, so no Go operation is reachable and every request silently falls back to Python",
			slog.String("schema_digest", schemaDigest),
			slog.Int64("routing_rows_for_digest", live),
			slog.Int64("routing_rows_total", total),
		)
		return
	}
	slog.Default().LogAttrs(ctx, slog.LevelInfo,
		"query-api: routing rows drift check",
		slog.String("schema_digest", schemaDigest),
		slog.Int64("routing_rows_for_digest", live),
		slog.Int64("routing_rows_total", total),
	)
}
