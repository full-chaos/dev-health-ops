// Package tracing installs the process-wide OpenTelemetry tracer provider for
// Go worker binaries. It mirrors src/dev_health_ops/tracing.py's env contract
// exactly (same variable names, defaults, and off-switch semantics) so a
// sync run that crosses the Python/Go boundary is traced consistently on
// both sides (CHAOS-3993).
//
// Sampling is head-based and decided once, on the Python side, when a sync
// run starts: the traceparent propagated into worker_job_outbox carries that
// decision's sampled flag, and every Go span parented from it (via
// sdktrace.ParentBased) inherits it rather than making an independent call --
// a Go span only makes its own ratio decision when it has no parent at all.
// At the default OTEL_SAMPLE_RATE=0.1, that means roughly 9 of every 10 sync
// runs produce no trace at all in Tempo -- raise OTEL_SAMPLE_RATE to see more
// of them (both sides should agree on a value: it only changes root-span
// decisions today, since only Python's head decision is live).
package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"math"
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
	exporter, err := otlptracegrpc.New(context.Background(), dialOptions(endpoint)...)
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
		sdktrace.WithSampler(sdktrace.ParentBased(sampler(sampleRate))),
		sdktrace.WithBatcher(exporter),
	)
	return provider, nil
}

// dialOptions picks the right otlptracegrpc option for the shape of endpoint.
// WithEndpoint requires a bare host:port ("no scheme or path"); WithEndpointURL
// requires a full URL. This package's own default ("localhost:4317") is bare,
// but every deployed value (deploy/kubernetes/configmap.yaml,
// deploy/helm/dev-health/values.yaml, deploy/docker-compose/compose.production.yml)
// sets a URL-shaped value ("http://otel-collector...:4317") -- the same value
// tracing.py's OTLPSpanExporter already receives and handles transparently.
// Passing a URL-shaped value to WithEndpoint would make gRPC try to dial a
// host literally containing "http://", so the two forms cannot share one
// option: detect which one endpoint is instead of assuming the bare form.
func dialOptions(endpoint string) []otlptracegrpc.Option {
	scheme, _, hasScheme := strings.Cut(endpoint, "://")
	if !hasScheme {
		return []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(endpoint),
			otlptracegrpc.WithInsecure(),
		}
	}
	options := []otlptracegrpc.Option{otlptracegrpc.WithEndpointURL(endpoint)}
	if !strings.EqualFold(scheme, "https") {
		options = append(options, otlptracegrpc.WithInsecure())
	}
	return options
}

// sampler is the root sampler tracing.py's raw TraceIdRatioBased mirrors: the
// decision for a span with NO parent (the common case until CHAOS-3996 wires
// trace-context propagation further). ParentBased wraps it in newProvider so
// a span that DOES have a parent (extracted from envelope.TraceParent)
// inherits that parent's sampled flag instead of making an independent
// ratio decision that could disagree with it when the two languages'
// OTEL_SAMPLE_RATE values differ.
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
	rate, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, err
	}
	// ParseFloat happily accepts "NaN", and NaN fails every comparison in
	// sampler() (both the >=1.0 and <=0.0 branches), so it would silently
	// fall through to TraceIDRatioBased(NaN) -- an always-undefined sampling
	// decision rather than a caught configuration error.
	if math.IsNaN(rate) {
		return 0, fmt.Errorf("OTEL_SAMPLE_RATE must be a finite number, got %q", value)
	}
	return rate, nil
}
