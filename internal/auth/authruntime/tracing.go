package authruntime

import (
	"context"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
)

// traceServiceName is the service.name of auth-service spans when
// OTEL_SERVICE_NAME is unset.
const traceServiceName = "auth-service"

// configureTraced is configure with the process tracer provider installed
// first (internal/platform/tracing reads the OTEL_* variables; OTEL_ENABLED
// false leaves it off). Its component leads the list, so it starts first and
// stops last: the final span flush runs after the API listener has drained.
func configureTraced(ctx context.Context, cfg authconfig.Config, logger *slog.Logger) ([]lifecycle.Component, error) {
	traces := tracing.InitWithServiceName(logger, traceServiceName)
	components, err := configure(ctx, cfg, logger)
	if err != nil {
		_ = traces.Shutdown(context.WithoutCancel(ctx))
		return nil, err
	}
	return append([]lifecycle.Component{traces}, components...), nil
}

// traceInitFailures and traceSpanCounts are the tracing counters the operator
// /metrics carries (dev_health_otel_init_failures_total, dev_health_otel_spans_total).
func traceInitFailures() health.MetricsSource { return tracing.InitFailuresSource() }

func traceSpanCounts() health.MetricsSource { return tracing.SpansSource() }
