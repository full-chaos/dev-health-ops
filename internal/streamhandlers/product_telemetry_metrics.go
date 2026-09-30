package streamhandlers

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

// productTelemetryNonFiniteCounter counts the NaN and infinite payload numbers a stream entry
// carried and the consumer stored as null (CHAOS-6299), so a client or producer that emits
// them is visible even though the entry is no longer refused.
var productTelemetryNonFiniteCounter = mustNonFiniteCounter()

func mustNonFiniteCounter() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/streamhandlers").Int64Counter(
		"dev_health_stream_product_telemetry_nonfinite_total",
		metric.WithDescription("Non-finite (NaN or infinite) product-telemetry numbers stored as null"),
	)
	if err != nil {
		// otel's no-op fallback: Add is always safe to call.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("dev_health_stream_product_telemetry_nonfinite_total")
	}
	return counter
}

// recordNonFiniteNulled counts n values and logs once per entry at debug, naming the stream
// entry but never any payload.
func recordNonFiniteNulled(ctx context.Context, message streamrunner.Message, n int) {
	if n == 0 {
		return
	}
	productTelemetryNonFiniteCounter.Add(ctx, int64(n))
	slog.DebugContext(ctx, "product telemetry: non-finite numbers stored as null",
		"stream", message.Stream, "entry_id", message.ID, "count", n)
}
