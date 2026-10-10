package tracing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

type slowSpanCollector struct {
	coltracepb.UnimplementedTraceServiceServer
	delay time.Duration
	mu    sync.Mutex
	spans int
}

func (c *slowSpanCollector) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	time.Sleep(c.delay)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, resource := range req.GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			c.spans += len(scope.GetSpans())
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (c *slowSpanCollector) received() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.spans
}

// startCollector runs a real OTLP gRPC collector and points the tracing env at it.
func startCollector(t *testing.T, rate string, delay time.Duration) *slowSpanCollector {
	t.Helper()
	collector := &slowSpanCollector{delay: delay}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(server, collector)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SAMPLE_RATE", rate)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", listener.Addr().String())
	return collector
}

// isolatedCounters gives the test its own counters: providers built by earlier
// tests of this package still export in the background and would move the
// process-wide ones.
func isolatedCounters(t *testing.T) {
	t.Helper()
	previous := spanOutcomes
	spanOutcomes = &spanCounters{}
	t.Cleanup(func() { spanOutcomes = previous })
}

// snapshot is the counters at one moment.
type snapshot struct{ sampled, notSampled, ended, exported, exportFailed, unaccounted uint64 }

func take() snapshot {
	c := spanOutcomes
	return snapshot{c.sampled.Load(), c.notSampled.Load(), c.ended.Load(), c.exported.Load(), c.exportFailed.Load(), c.unaccounted()}
}

func runSpans(t *testing.T, count int) {
	t.Helper()
	tracer := otel.Tracer("span-counters-test")
	for i := 0; i < count; i++ {
		_, span := tracer.Start(context.Background(), "request")
		span.End()
	}
}

func stopTracing(t *testing.T, component Component) {
	t.Helper()
	if err := component.Shutdown(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	otel.SetTracerProvider(noop.NewTracerProvider())
}

// With every span sampled and a collector that answers, each span is counted as
// sampled, ended and exported, and nothing is unaccounted for.
func TestSpanCountersAccountForEverySpanAtRateOne(t *testing.T) {
	isolatedCounters(t)
	collector := startCollector(t, "1", 0)
	component := InitWithServiceName(slog.New(slog.NewTextHandler(io.Discard, nil)), "counters-test")
	before := take()
	runSpans(t, 50)
	stopTracing(t, component)
	after := take()
	if got := after.sampled - before.sampled; got != 50 {
		t.Errorf("sampled = %d, want 50", got)
	}
	if got := after.ended - before.ended; got != 50 {
		t.Errorf("ended = %d, want 50", got)
	}
	if got := after.exported - before.exported; got != 50 {
		t.Errorf("exported = %d, want 50", got)
	}
	if got := after.unaccounted; got != 0 {
		t.Errorf("unaccounted = %d, want 0", got)
	}
	if got := collector.received(); got != 50 {
		t.Errorf("collector received %d spans, want 50 (the counter must agree with the collector)", got)
	}
}

// At rate 0 nothing is recorded: every start is a not_sampled decision and no
// span ends as a recorded one. This is the series that makes "the sampler took
// them" readable.
func TestSpanCountersCountNotSampledAtRateZero(t *testing.T) {
	isolatedCounters(t)
	startCollector(t, "0", 0)
	component := InitWithServiceName(slog.New(slog.NewTextHandler(io.Discard, nil)), "counters-test")
	before := take()
	runSpans(t, 50)
	stopTracing(t, component)
	after := take()
	if got := after.notSampled - before.notSampled; got != 50 {
		t.Errorf("not_sampled = %d, want 50", got)
	}
	if after.sampled != before.sampled || after.ended != before.ended || after.exported != before.exported {
		t.Errorf("a span was recorded at rate 0: before %+v after %+v", before, after)
	}
}

// A burst past the exporter queue against a slow collector loses spans without
// any error: the unaccounted gauge, read after the queue has settled, is the
// number lost, and exported + unaccounted = ended.
func TestSpanCountersShowSpansADroppedQueueLost(t *testing.T) {
	isolatedCounters(t)
	collector := startCollector(t, "1", 300*time.Millisecond)
	component := InitWithServiceName(slog.New(slog.NewTextHandler(io.Discard, nil)), "counters-test")
	before := take()
	const burst = 6000
	runSpans(t, burst)
	time.Sleep(8 * time.Second) // the queue drains: what is left unaccounted was dropped
	settled := take()
	stopTracing(t, component)
	if got := settled.ended - before.ended; got != burst {
		t.Fatalf("ended = %d, want %d", got, burst)
	}
	lost := settled.unaccounted
	if lost == 0 {
		t.Fatalf("a %d-span burst against a slow collector lost nothing: unaccounted = 0 (exported %d)", burst, settled.exported-before.exported)
	}
	if got := (settled.exported - before.exported) + lost; got != burst {
		t.Errorf("exported %d + unaccounted %d = %d, want ended %d", settled.exported-before.exported, lost, got, burst)
	}
	if got := uint64(collector.received()); got < settled.exported-before.exported {
		t.Errorf("collector received %d, fewer than the %d counted exported", got, settled.exported-before.exported)
	}
}

type erroringExporter struct{}

func (erroringExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return errors.New("collector refused")
}
func (erroringExporter) Shutdown(context.Context) error { return nil }

// A failed export call counts every span it carried as export_failed, never as exported.
func TestSpanCountersCountAFailedExport(t *testing.T) {
	counters := &spanCounters{}
	exporter := countingExporter{SpanExporter: erroringExporter{}, counters: counters}
	spans := make([]sdktrace.ReadOnlySpan, 3)
	if err := exporter.ExportSpans(context.Background(), spans); err == nil {
		t.Fatal("the failure was swallowed")
	}
	if counters.exportFailed.Load() != 3 || counters.exported.Load() != 0 {
		t.Errorf("failed %d exported %d, want 3 and 0", counters.exportFailed.Load(), counters.exported.Load())
	}
}

// Every series is written on a process that has not started a span.
func TestSpanCountersAreWrittenAtZero(t *testing.T) {
	var out bytes.Buffer
	if err := (&spanCounters{}).WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`dev_health_otel_spans_total{outcome="sampled"} 0`,
		`dev_health_otel_spans_total{outcome="not_sampled"} 0`,
		`dev_health_otel_spans_total{outcome="ended"} 0`,
		`dev_health_otel_spans_total{outcome="exported"} 0`,
		`dev_health_otel_spans_total{outcome="export_failed"} 0`,
		"dev_health_otel_spans_unaccounted 0",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in:\n%s", want, out.String())
		}
	}
}

// The unaccounted gauge is ended less exported less failed, and never below zero.
func TestSpanUnaccountedIsEndedLessExportedLessFailed(t *testing.T) {
	counters := &spanCounters{}
	counters.ended.Store(5)
	counters.exported.Store(2)
	counters.exportFailed.Store(1)
	if got := counters.unaccounted(); got != 2 {
		t.Errorf("unaccounted = %d, want 2 (5 ended, 2 exported, 1 failed)", got)
	}
	counters.exported.Store(9) // read while spans move: exported can pass ended
	if got := counters.unaccounted(); got != 0 {
		t.Errorf("unaccounted = %d, want 0 (never below zero)", got)
	}
}
