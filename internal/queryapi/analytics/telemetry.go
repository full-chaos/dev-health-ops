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

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
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
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/internal/queryapi/analytics")
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
	// No error text on the span (CHAOS-7936): the class and the Go type, plus the ClickHouse exception's code and name when the
	// chain holds one: that pair tells a missing table (code 60) from a timeout (159) without the driver's message, which can
	// quote a statement, a table and a value.
	attrs := append([]attribute.KeyValue{attribute.String("phase", phase)}, logging.ErrorSpanAttributes(err)...)
	attrs = append(attrs, clickHouseExceptionAttributes(err)...)
	trace.SpanFromContext(ctx).AddEvent("analytics.degraded", trace.WithAttributes(attrs...))
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
// NOTE (the span-text change): neither err.Error() nor the root cause's text is put on a span any more: the degraded event and
// the coverage event carry error.class, error.type and the ClickHouse exception's code and name (logging.ErrorAs finds it
// through the bounded walk). rootCause is still used by the resolver's own log line (resolve.go), which is a named follow-up.
func rootCause(err error) error {
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return err
		}
		err = next
	}
}

// clickHouseExceptionAttributes is the code and name of the ClickHouse exception the error chain holds, none when it holds none.
func clickHouseExceptionAttributes(err error) []attribute.KeyValue {
	var exception *clickhousedriver.Exception
	if logging.ErrorAs(err, &exception) && exception != nil {
		return []attribute.KeyValue{
			attribute.Int64("clickhouse_code", int64(exception.Code)),
			attribute.String("clickhouse_exception", exception.Name),
		}
	}
	return nil
}
