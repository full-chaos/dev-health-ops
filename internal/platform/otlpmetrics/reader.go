package otlpmetrics

import (
	"context"
	"log/slog"
	"strings"

	prometheusbridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Pipeline is the OTLP side of a MeterProvider.
type Pipeline struct {
	// Reader goes into sdkmetric.WithReader of the process MeterProvider.
	Reader sdkmetric.Reader
	// Resource goes into sdkmetric.WithResource.
	Resource *resource.Resource
	// Stats is the push's own counters (register it as a metrics source).
	Stats *Stats
}

// NewPipeline builds the periodic OTLP/gRPC reader. It never dials: the gRPC
// client connects lazily, so an unreachable collector fails exports (logged
// and counted), not startup. skip names the registry sources the bridge must
// not read (the OTel instruments' own fragment).
func NewPipeline(ctx context.Context, options Options, source FragmentSource, skip map[string]bool, logger *slog.Logger) (*Pipeline, error) {
	if logger == nil {
		logger = slog.Default()
	}
	exporter, err := otlpmetricgrpc.New(ctx, dialOptions(options.Endpoint)...)
	if err != nil {
		return nil, err
	}
	stats := &Stats{}
	gatherer := &Gatherer{Source: source, Skip: skip, Logger: logger, Stats: stats}
	reader := sdkmetric.NewPeriodicReader(
		&countingExporter{Exporter: exporter, stats: stats, logger: logger, endpoint: options.Endpoint},
		sdkmetric.WithInterval(options.Interval),
		sdkmetric.WithProducer(prometheusbridge.NewMetricProducer(prometheusbridge.WithGatherer(gatherer))),
	)
	attrs := []attribute.KeyValue{
		attribute.String("service.name", options.ServiceName),
		attribute.String("deployment.environment", options.Environment),
	}
	if options.InstanceID != "" {
		attrs = append(attrs, attribute.String("service.instance.id", options.InstanceID))
	}
	return &Pipeline{Reader: reader, Resource: resource.NewSchemaless(attrs...), Stats: stats}, nil
}

// countingExporter makes a failed export loud: a log line with the endpoint
// and the error, and a counter on /metrics.
type countingExporter struct {
	sdkmetric.Exporter
	stats    *Stats
	logger   *slog.Logger
	endpoint string
}

func (e *countingExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	err := e.Exporter.Export(ctx, rm)
	if err != nil {
		e.stats.exportFailed()
		e.logger.Warn("otlp metrics export failed", "endpoint", e.endpoint, "error", err)
		return err
	}
	e.stats.exported()
	return nil
}

// dialOptions picks the otlpmetricgrpc option for the shape of endpoint, the
// same rule tracing applies to the trace exporter: a bare host:port uses
// WithEndpoint, a URL uses WithEndpointURL, and everything but https is
// plaintext.
func dialOptions(endpoint string) []otlpmetricgrpc.Option {
	scheme, _, hasScheme := strings.Cut(endpoint, "://")
	if !hasScheme {
		return []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(endpoint), otlpmetricgrpc.WithInsecure()}
	}
	options := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpointURL(endpoint)}
	if !strings.EqualFold(scheme, "https") {
		options = append(options, otlpmetricgrpc.WithInsecure())
	}
	return options
}
