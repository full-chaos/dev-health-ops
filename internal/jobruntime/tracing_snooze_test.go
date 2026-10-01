package jobruntime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
)

// snoozeSpanCollector is a real OTLP/gRPC TraceService receiver: the spans the
// test asserts on crossed the wire the way production spans reach the host
// collector.
type snoozeSpanCollector struct {
	coltracepb.UnimplementedTraceServiceServer
	mu    sync.Mutex
	spans []*tracepb.Span
}

func (c *snoozeSpanCollector) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, resource := range req.GetResourceSpans() {
		for _, scope := range resource.GetScopeSpans() {
			c.spans = append(c.spans, scope.GetSpans()...)
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// TestJobSpanStatusByHandlerOutcome drives adapter.Work (the production call
// chain) through the real tracing component and a real OTLP receiver, and pins
// the status of the dev_health.job.execute span per handler outcome
// (CHAOS-7685): a planned snooze is not an error; every real failure is.
// Not parallel: it swaps the process-global tracer provider.
func TestJobSpanStatusByHandlerOutcome(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name        string
		err         error
		wantStatus  tracepb.Status_StatusCode
		wantSnoozed bool
		wantError   bool
	}{
		{"success", nil, tracepb.Status_STATUS_CODE_OK, false, false},
		{"RetryableAfter snooze", RetryableAfter(boom, 5*time.Second), tracepb.Status_STATUS_CODE_UNSET, true, false},
		{"BudgetContention snooze", BudgetContention(boom, 5*time.Second), tracepb.Status_STATUS_CODE_UNSET, true, false},
		{"RateLimited snooze", RateLimited(boom, 5*time.Second), tracepb.Status_STATUS_CODE_UNSET, true, false},
		{"plain Retryable failure", Retryable(boom), tracepb.Status_STATUS_CODE_ERROR, false, true},
		{"Permanent failure", Permanent(boom), tracepb.Status_STATUS_CODE_ERROR, false, true},
		{"unmarked failure", boom, tracepb.Status_STATUS_CODE_ERROR, false, true},
	}

	collector := &snoozeSpanCollector{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(server, collector)
	go func() { _ = server.Serve(lis) }()
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", lis.Addr().String())
	t.Setenv("OTEL_SAMPLE_RATE", "1")
	component := tracing.InitWithServiceName(slog.New(slog.NewTextHandler(io.Discard, nil)), "dev-health-ops")
	t.Cleanup(func() {
		_ = component.Shutdown(context.Background())
		server.Stop()
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})

	for index, test := range cases {
		var logs bytes.Buffer
		handlerErr := test.err
		adapter := newRetentionAdapter(t, HandlerFunc[RetentionCleanupArgs](
			func(context.Context, *Execution[RetentionCleanupArgs]) error { return handlerErr },
		), &recordingObserver{}, &recordingClaim{state: ClaimProceed}, &recordingLease{}, &logs)
		job := retentionJob(t, 1)
		job.ID = int64(1000 + index)
		_ = adapter.Work(context.Background(), job)
	}
	if err := component.Shutdown(context.Background()); err != nil {
		t.Fatalf("flush spans: %v", err)
	}

	collector.mu.Lock()
	spans := append([]*tracepb.Span(nil), collector.spans...)
	collector.mu.Unlock()
	byID := map[int64]*tracepb.Span{}
	for _, span := range spans {
		if span.GetName() != "dev_health.job.execute" {
			continue
		}
		for _, kv := range span.GetAttributes() {
			if kv.GetKey() == "dev_health.job.id" {
				byID[kv.GetValue().GetIntValue()] = span
			}
		}
	}
	for index, test := range cases {
		span := byID[int64(1000+index)]
		if span == nil {
			t.Fatalf("%s: no dev_health.job.execute span reached the collector", test.name)
		}
		if got := span.GetStatus().GetCode(); got != test.wantStatus {
			t.Errorf("%s: status = %v, want %v", test.name, got, test.wantStatus)
		}
		snoozed := false
		for _, kv := range span.GetAttributes() {
			if kv.GetKey() == "dev_health.job.snoozed" {
				snoozed = kv.GetValue().GetBoolValue()
			}
		}
		if snoozed != test.wantSnoozed {
			t.Errorf("%s: dev_health.job.snoozed = %v, want %v", test.name, snoozed, test.wantSnoozed)
		}
		hasException := false
		for _, event := range span.GetEvents() {
			if event.GetName() == "exception" {
				hasException = true
			}
		}
		if hasException != test.wantError {
			t.Errorf("%s: exception event = %v, want %v", test.name, hasException, test.wantError)
		}
	}
}
