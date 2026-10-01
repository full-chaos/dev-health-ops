package apimetrics

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/otlpmetrics"
)

type collector struct {
	colmetricpb.UnimplementedMetricsServiceServer
	mu      sync.Mutex
	metrics []*metricpb.Metric
}

func (c *collector) Export(_ context.Context, req *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, resource := range req.GetResourceMetrics() {
		for _, scope := range resource.GetScopeMetrics() {
			c.metrics = append(c.metrics, scope.GetMetrics()...)
		}
	}
	return &colmetricpb.ExportMetricsServiceResponse{}, nil
}

func startCollector(t *testing.T) (*collector, string) {
	t.Helper()
	c := &collector{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	colmetricpb.RegisterMetricsServiceServer(server, c)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return c, lis.Addr().String()
}

type fragment string

func (f fragment) WritePrometheus(w io.Writer) error {
	_, err := io.WriteString(w, string(f))
	return err
}

func pushOptions(addr string) otlpmetrics.Options {
	return otlpmetrics.Options{Enabled: true, Endpoint: addr, Interval: time.Hour, ServiceName: "dev-health-go-test", Environment: "test", InstanceID: "pod-1"}
}

func countNamed(metrics []*metricpb.Metric, name string) (metricsNamed int, points int) {
	for _, m := range metrics {
		if m.GetName() != name {
			continue
		}
		metricsNamed++
		switch data := m.GetData().(type) {
		case *metricpb.Metric_Sum:
			points += len(data.Sum.GetDataPoints())
		case *metricpb.Metric_Gauge:
			points += len(data.Gauge.GetDataPoints())
		}
	}
	return
}

// TestAnInstrumentIsPushedOnceNotTwice: the process's OTel instruments reach
// OTLP natively through the provider's OTLP reader, and their Prometheus text
// (the SourceName fragment) is excluded from the bridge. Without the exclusion
// the same instrument is sent twice, once natively and once re-read from its
// own /metrics text.
func TestAnInstrumentIsPushedOnceNotTwice(t *testing.T) {
	c, addr := startCollector(t)
	registry := health.NewRegistry(time.Second)
	provider, source, err := buildProvider(PushOptions{Options: pushOptions(addr), Registry: registry, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("buildProvider: %v", err)
	}
	if err := registry.RegisterMetrics(SourceName, source); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterMetrics("hand_written", fragment("# TYPE hand_written_total counter\nhand_written_total 5\n")); err != nil {
		t.Fatal(err)
	}
	counter, err := provider.Meter("probe").Int64Counter("probe_instrument_total")
	if err != nil {
		t.Fatal(err)
	}
	counter.Add(context.Background(), 3)

	// The /metrics text still carries both (the pull endpoint is unchanged).
	var text bytes.Buffer
	if _, err := registry.WriteMetricsPartial(&text); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(text.Bytes(), []byte("probe_instrument_total 3")) || !bytes.Contains(text.Bytes(), []byte("hand_written_total 5")) {
		t.Fatalf("the pull endpoint lost a family: %s", text.String())
	}

	// One export only: Shutdown would export a second time.
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if metrics, points := countNamed(c.metrics, "probe_instrument_total"); metrics != 1 || points != 1 {
		t.Errorf("the instrument arrived as %d metrics with %d points, want exactly 1 and 1 (pushed twice)", metrics, points)
	}
	if metrics, points := countNamed(c.metrics, "hand_written_total"); metrics != 1 || points != 1 {
		t.Errorf("the hand-written family arrived as %d metrics with %d points, want 1 and 1", metrics, points)
	}
}

func TestTheKillSwitchLeavesThePullEndpointAndPushesNothing(t *testing.T) {
	c, addr := startCollector(t)
	registry := health.NewRegistry(time.Second)
	options := pushOptions(addr)
	options.Enabled = false
	provider, source, err := buildProvider(PushOptions{Options: options, Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	counter, _ := provider.Meter("probe").Int64Counter("killed_total")
	counter.Add(context.Background(), 1)
	_ = provider.ForceFlush(context.Background())
	if source.pushStats != nil {
		t.Error("a push pipeline exists with the push disabled")
	}
	var text bytes.Buffer
	if err := source.WritePrometheus(&text); err != nil || !bytes.Contains(text.Bytes(), []byte("killed_total 1")) {
		t.Errorf("the pull endpoint lost the instrument (err %v): %s", err, text.String())
	}
	_ = provider.Shutdown(context.Background())
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.metrics) != 0 {
		t.Errorf("the collector received %d metrics with the push disabled", len(c.metrics))
	}
}

func TestRegisterWithPushRegistersThePushCountersOnTheRegistry(t *testing.T) {
	_, addr := startCollector(t)
	registry := health.NewRegistry(time.Second)
	// InstallWith is once per process; this test uses buildProvider's output
	// registration path directly through the same helper Register uses.
	_, source, err := buildProvider(PushOptions{Options: pushOptions(addr), Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	if source.pushStats == nil {
		t.Fatal("no push stats with the push enabled")
	}
	if err := registerSource(registry, source); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterMetrics(otlpmetrics.SourceName, source.pushStats); err != nil {
		t.Fatal(err)
	}
	var text bytes.Buffer
	if _, err := registry.WriteMetricsPartial(&text); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(text.Bytes(), []byte("dev_health_otlp_metrics_export_failures_total 0")) {
		t.Errorf("the push counters are not on /metrics: %s", text.String())
	}
}
