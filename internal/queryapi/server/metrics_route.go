package server

// registry_drift_telemetry.go, readyz_telemetry.go, and
// internal/routeswitch/telemetry.go each call otel.Meter(...) and Record/Add
// against the instruments that returns -- but nothing in this binary ever
// installed a real OTel MeterProvider. otel.Meter without one falls back to
// the package's global no-op provider, so every one of those Record/Add
// calls has always been discarded, and this process has never exposed a
// Prometheus /metrics route to read them back from anyway. This file wires
// both halves: a real MeterProvider so the instruments actually record, and
// an HTTP handler that renders what they collected as Prometheus exposition
// text.

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// newPrometheusMeterProvider builds an OTel SDK MeterProvider backed by a
// Prometheus reader on a dedicated *prometheus.Registry -- never
// prometheus.DefaultRegisterer, whose process-wide default registry panics
// on a second registration of the same collector. main() only ever
// constructs one of these, but a test binary that also wants a manual
// reader over the same instruments (to assert a recorded value directly)
// passes it in via extraReaders so both readers observe the one provider
// every package-level otel.Meter(...) instrument in this binary delegates
// to -- that delegation is a process-wide sync.Once (go.opentelemetry.io/
// otel/internal/global), so a second, independent MeterProvider built
// later would never see those already-created instruments at all.
func newPrometheusMeterProvider(extraReaders ...sdkmetric.Reader) (*sdkmetric.MeterProvider, *prometheus.Registry, error) {
	registry := prometheus.NewRegistry()
	exporter, err := otelprometheus.New(otelprometheus.WithRegisterer(registry))
	if err != nil {
		return nil, nil, err
	}

	res, err := meterResource()
	if err != nil {
		return nil, nil, err
	}
	opts := make([]sdkmetric.Option, 0, len(extraReaders)+2)
	opts = append(opts, sdkmetric.WithResource(res), sdkmetric.WithReader(exporter))
	for _, reader := range extraReaders {
		opts = append(opts, sdkmetric.WithReader(reader))
	}
	return sdkmetric.NewMeterProvider(opts...), registry, nil
}

// meterResource is the metrics resource: service.name is otelServiceName,
// the name the traces and the startup Info log carry, unless
// OTEL_SERVICE_NAME or OTEL_RESOURCE_ATTRIBUTES sets it (WithFromEnv runs
// after WithAttributes, so the environment wins, as it does for traces).
// Without it the SDK default is "unknown_service:<executable>", so the
// metrics identity would follow the binary's file name, which is dho.
func meterResource() (*resource.Resource, error) {
	return resource.New(context.Background(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(semconv.ServiceName(otelServiceName)),
		resource.WithFromEnv(),
	)
}

// metricsHandler renders registry's collected series as Prometheus
// exposition text, the same content-type and format promhttp.Handler()
// would serve against the process default -- pointed at a specific
// registry instead so a second construction in-process (every test that
// calls newPrometheusMeterProvider) never collides with it.
func metricsHandler(registry *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}
