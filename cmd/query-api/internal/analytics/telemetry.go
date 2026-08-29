package analytics

// New logic gets telemetry in the same PR (root AGENTS.md standing
// order). This file exists for one specific defect class rather than
// general instrumentation: resolveSankey and resolveFlowMatrix both
// SWALLOW execute errors and degrade to an empty result, mirroring
// analytics.py:654-656 and :959-961. That data behaviour is correct
// parity -- but Python pairs each swallow with a logger.error, and the
// Go port originally dropped the error entirely.
//
// Without this, a degraded result is byte-identical to a legitimately
// empty one: a ClickHouse failure renders an empty chord and emits no
// signal anywhere. That is the exact "invisible fallback" shape
// internal/graph/telemetry.go's doc comment calls out, and it fails
// toward plausible -- an operator sees "no data", not "broken".
//
// The counter restores the signal Python had, and the span event
// carries the error text that logger.error carried.

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var degradedCounter = mustAnalyticsCounter(
	"devhealth_query_api_analytics_degraded_total",
	"analytics phases that swallowed an execute error and returned an empty result, by phase",
)

func mustAnalyticsCounter(name, description string) metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics")
	counter, err := meter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		// Same otel guarantee internal/graph/telemetry.go relies on:
		// Int64Counter never returns a nil counter even on error, so a
		// broken meter provider must not panic the resolver.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

// recordDegradation is a package var, not a plain func, so a test can
// observe that a swallow actually reported itself. Asserting on the
// swallowed-to-empty RESULT cannot distinguish a degraded phase from a
// genuinely empty one -- that indistinguishability is the whole defect
// this file addresses -- so the report is the only observable, and it
// has to be injectable to be assertable.
var recordDegradation = defaultRecordDegradation

func defaultRecordDegradation(ctx context.Context, phase string, err error) {
	degradedCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("phase", phase)))
	trace.SpanFromContext(ctx).AddEvent("analytics.degraded", trace.WithAttributes(
		attribute.String("phase", phase),
		attribute.String("error", err.Error()),
		attribute.String("error.cause", rootCause(err).Error()),
	))
}

// rootCause walks to the deepest error in the %w chain.
//
// Recording only err.Error() makes this telemetry near-useless against
// the REAL client: dev-health-go/clickhouse wraps every driver failure
// as *operationError, whose Error() is the fixed string
// "ClickHouse " + operation + " failed" and deliberately omits the
// cause (client.go:212-213) -- the driver's actual message, table name
// and error code are reachable ONLY through Unwrap(). Our own
// fmt.Errorf("...: %w") wrappers add call-site context on top but
// recover none of it, so every distinct ClickHouse failure would land
// in telemetry as the same "...: ClickHouse query failed" string and
// an operator could not tell a missing table from a timeout from a
// syntax error.
//
// err.Error() is still recorded alongside, because the wrapper chain is
// what identifies WHICH query degraded; the cause is what says why.
func rootCause(err error) error {
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return err
		}
		err = next
	}
}
