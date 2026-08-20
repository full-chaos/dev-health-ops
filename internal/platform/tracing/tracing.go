// Package tracing installs the process-wide OpenTelemetry tracer provider for
// Go worker binaries. It mirrors src/dev_health_ops/tracing.py's env contract
// exactly (same variable names, defaults, and off-switch semantics) so a
// sync run that crosses the Python/Go boundary is traced consistently on
// both sides (CHAOS-3993).
//
// Sampling is head-based and decided once, on the Python side, when a sync
// run starts: the traceparent propagated into worker_job_outbox carries that
// decision's sampled flag, and every Go span parented from it inherits it
// rather than making its own call. At the default OTEL_SAMPLE_RATE=0.1, that
// means roughly 9 of every 10 sync runs produce no trace at all in Tempo --
// raise OTEL_SAMPLE_RATE (both sides must agree, since only Python's head
// decision is live today) to see more of them.
package tracing

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// defaultServiceName matches tracing.py's OTEL_SERVICE_NAME default exactly.
// Deployments differentiate the 9 Go binaries (and the Python services) by
// setting OTEL_SERVICE_NAME explicitly, the same way they already do for
// Python; this package does not invent a per-binary default that would
// diverge from that contract.
const (
	defaultServiceName = "dev-health-ops"
	defaultEnvironment = "production"
	defaultEndpoint    = "localhost:4317"
	defaultSampleRate  = 0.1
)

// Component wraps the installed TracerProvider as a lifecycle.Component so
// buffered spans flush only once every other component has stopped. A zero
// Component (tracing disabled or unavailable) shuts down as a no-op.
type Component struct {
	provider *sdktrace.TracerProvider
}

func (Component) Name() string { return "otel-tracing" }

// Start is a no-op: the provider is already installed by Init before any
// component starts, and River client construction depends on
// otel.GetTracerProvider() returning it from process start.
func (Component) Start(context.Context) error { return nil }

func (component Component) Shutdown(ctx context.Context) error {
	if component.provider == nil {
		return nil
	}
	return component.provider.Shutdown(ctx)
}

// Init installs the global TracerProvider and W3C trace-context propagator.
// It never returns an error: exactly like tracing.py's broad except clause,
// any failure to build the exporter or provider is logged as a warning and
// tracing is left disabled, so a bad OTEL_* value can never crash a worker
// process. Register the returned Component first in the lifecycle component
// list so it starts first and shuts down last, after every other component
// has stopped producing spans.
func Init(logger *slog.Logger) Component {
	if logger == nil {
		logger = slog.Default()
	}
	if !enabledFromEnv() {
		logger.Debug("OpenTelemetry tracing disabled via OTEL_ENABLED=false")
		return Component{}
	}

	serviceName := stringEnv("OTEL_SERVICE_NAME", defaultServiceName)
	environment := stringEnv("OTEL_ENVIRONMENT", defaultEnvironment)
	endpoint := stringEnv("OTEL_EXPORTER_OTLP_ENDPOINT", defaultEndpoint)
	sampleRate, err := sampleRateFromEnv()
	if err != nil {
		logger.Warn("OpenTelemetry initialisation failed", "error", err)
		return Component{}
	}

	provider, err := newProvider(serviceName, environment, endpoint, sampleRate)
	if err != nil {
		logger.Warn("OpenTelemetry initialisation failed", "error", err)
		return Component{}
	}

	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	logger.Info("OpenTelemetry tracing initialised",
		"otlp_endpoint", endpoint,
		"service_name", serviceName,
		"sample_rate", sampleRate,
	)
	return Component{provider: provider}
}

func newProvider(serviceName, environment, endpoint string, sampleRate float64) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(
		context.Background(),
		otlptracegrpc.WithEndpoint(endpoint),
		// The default endpoint is a bare host:port pointing at a local
		// otel-collector sidecar, the same target tracing.py's OTLPSpanExporter
		// reaches with no TLS material configured anywhere in this deployment.
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(context.Background(), resource.WithAttributes(
		attribute.String("service.name", serviceName),
		attribute.String("deployment.environment", environment),
	))
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler(sampleRate)),
		sdktrace.WithBatcher(exporter),
	)
	return provider, nil
}

// sampler is deliberately head-based like tracing.py's TraceIdRatioBased: it
// is not wrapped in a ParentBased decision, so both sides of the Python/Go
// boundary make the same kind of sampling call rather than one side
// unconditionally deferring to the other's decision.
func sampler(rate float64) sdktrace.Sampler {
	switch {
	case rate >= 1.0:
		return sdktrace.AlwaysSample()
	case rate <= 0.0:
		return sdktrace.NeverSample()
	default:
		return sdktrace.TraceIDRatioBased(rate)
	}
}

func enabledFromEnv() bool {
	value := strings.ToLower(stringEnv("OTEL_ENABLED", "true"))
	switch value {
	case "false", "0", "no":
		return false
	default:
		return true
	}
}

func stringEnv(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

func sampleRateFromEnv() (float64, error) {
	value, ok := os.LookupEnv("OTEL_SAMPLE_RATE")
	if !ok {
		return defaultSampleRate, nil
	}
	return strconv.ParseFloat(value, 64)
}
