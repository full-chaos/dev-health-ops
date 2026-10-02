package streamrunner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func installStreamSpanRecorder(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	return exporter
}

func spanStrAttr(span tracetest.SpanStub, key string) string {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value.Emit()
		}
	}
	return ""
}

// CHAOS-7879: a non-empty batch is one span, each handled event one child, with
// a fixed outcome word; an idle cycle emits nothing; a failed handler is an
// Error span; no stream key, message id or payload reaches a span.
func TestRunnerEmitsBatchAndHandleSpansWithOutcomes(t *testing.T) {
	exporter := installStreamSpanRecorder(t)
	transport := &fakeTransport{new: []Message{
		{Stream: "test:stream", ID: "1-0"},
		{Stream: "test:stream", ID: "2-0"},
		{Stream: "test:stream", ID: "3-0"},
	}}
	calls := 0
	runner, err := New(transport, handlerFunc(func(context.Context, Message) error {
		calls++
		switch calls {
		case 2:
			return &PermanentError{Reason: "schema_invalid"}
		case 3:
			return errors.New("clickhouse unavailable")
		}
		return nil
	}), testConfig(), health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	_ = runner.window(context.Background())

	outcomes := map[string]int{}
	var batches int
	for _, span := range exporter.GetSpans() {
		switch span.Name {
		case "dev_health.stream.batch":
			batches++
			if got := spanStrAttr(span, "dev_health.stream.messages"); got != "3" {
				t.Errorf("batch messages = %q, want 3", got)
			}
			if span.Status.Code != codes.Error {
				t.Errorf("a batch with a failed event must be an Error span")
			}
		case "dev_health.stream.handle":
			outcomes[spanStrAttr(span, "dev_health.stream.outcome")]++
			if spanStrAttr(span, "dev_health.stream.runner") != "stream_test" {
				t.Errorf("handle span runner attr = %q", spanStrAttr(span, "dev_health.stream.runner"))
			}
		}
		for _, kv := range span.Attributes {
			if v := kv.Value.Emit(); v == "1-0" || v == "2-0" || v == "3-0" || v == "test:stream" {
				t.Errorf("span %s leaks stream/message identity in %s", span.Name, kv.Key)
			}
		}
	}
	if batches != 1 || outcomes["acked"] != 1 || outcomes["quarantined"] != 1 || outcomes["transient_error"] != 1 {
		t.Fatalf("batches=%d outcomes=%v, want 1 batch and one each of acked/quarantined/transient_error", batches, outcomes)
	}

	exporter.Reset()
	if err := runner.window(context.Background()); err != nil {
		t.Fatalf("idle window: %v", err)
	}
	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("an idle cycle emitted %d spans, want 0", len(spans))
	}
}
