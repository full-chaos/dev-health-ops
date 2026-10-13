package session

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/full-chaos/dev-health-ops/internal/api/session"

func newCounter(name, description string) metric.Int64Counter {
	counter, err := otel.Meter(meterName).Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

var (
	activityReadFailures = newCounter("session_org_activity_read_failures_total",
		"Organization activity table reads that failed, by table and error class.")
	activityFallbacks = newCounter("session_org_activity_fallback_total",
		"Logins whose organization was chosen by earliest join because an activity read failed.")
)

// recordActivityReadFailed counts one failed table read. Labels are the fixed
// table names and the error class, never an organization id or error text.
func recordActivityReadFailed(ctx context.Context, table, class string) {
	activityReadFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String("table", table), attribute.String("error_class", class)))
}

func recordActivityFallback(ctx context.Context) { activityFallbacks.Add(ctx, 1) }
