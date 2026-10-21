package otlpmetrics

import (
	"context"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func exportOnce(ctx context.Context, endpoint string) error {
	exporter, err := otlpmetricgrpc.New(ctx, dialOptions(endpoint)...)
	if err != nil {
		panic(err)
	}
	defer func() { _ = exporter.Shutdown(ctx) }()
	return exporter.Export(ctx, &metricdata.ResourceMetrics{})
}

func TestMetricExporterNeverDialsTheEnvironmentProxy(t *testing.T) {
	for _, endpoint := range []string{"http://upstream.invalid:4317", "upstream.invalid:4317"} {
		t.Run(endpoint, func(t *testing.T) {
			proxycapture.Check(t, runCaptureChild, func(ctx context.Context) { _ = exportOnce(ctx, endpoint) }).AssertNothingReached(t)
		})
	}
}

type headerCollector struct {
	collectormetrics.UnimplementedMetricsServiceServer
	seen chan metadata.MD
}

func (c headerCollector) Export(ctx context.Context, _ *collectormetrics.ExportMetricsServiceRequest) (*collectormetrics.ExportMetricsServiceResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	select {
	case c.seen <- md:
	default:
	}
	return &collectormetrics.ExportMetricsServiceResponse{}, nil
}

func TestMetricExporterStillSendsTheEnvironmentHeaders(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-probe-header=probe-value")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	collector := headerCollector{seen: make(chan metadata.MD, 1)}
	collectormetrics.RegisterMetricsServiceServer(server, collector)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exportOnce(ctx, "http://"+listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	select {
	case md := <-collector.seen:
		if got := md.Get("x-probe-header"); len(got) != 1 || got[0] != "probe-value" {
			t.Fatalf("x-probe-header = %v, want [probe-value]", got)
		}
	default:
		t.Fatal("the collector saw no export")
	}
}

// runCaptureChild re-runs this test binary for proxycapture.Check.
func runCaptureChild(args, env []string) ([]byte, error) {
	command := exec.Command(os.Args[0], args...)
	command.Env = env
	return command.CombinedOutput()
}
