package streamrunner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
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

type failingReadTransport struct{ *fakeTransport }

func (failingReadTransport) ReadNew(context.Context, []string, string, string, int, time.Duration) ([]Message, error) {
	return nil, errors.New("redis down {payload-in-error}")
}

type failingQuarantineTransport struct{ *fakeTransport }

func (failingQuarantineTransport) Quarantine(context.Context, Message, string) error {
	return errors.New("dlq write failed")
}

func spansByName(exporter *tracetest.InMemoryExporter, name string) []tracetest.SpanStub {
	var out []tracetest.SpanStub
	for _, span := range exporter.GetSpans() {
		if span.Name == name {
			out = append(out, span)
		}
	}
	return out
}

func assertNoIdentity(t *testing.T, exporter *tracetest.InMemoryExporter) {
	t.Helper()
	for _, span := range exporter.GetSpans() {
		if strings.Contains(fmt.Sprint(span), "payload-in-error") || strings.Contains(fmt.Sprint(span), "clickhouse unavailable") {
			t.Errorf("span %s carries error text", span.Name)
		}
		for _, kv := range span.Attributes {
			if v := kv.Value.Emit(); v == "1-0" || v == "2-0" || v == "3-0" || v == "test:stream" {
				t.Errorf("span %s leaks stream/message identity in %s", span.Name, kv.Key)
			}
		}
	}
}

// CHAOS-7879: a non-empty batch is one span, each handled event one child of
// it, with a fixed outcome word and an Error status only for a failure; an idle
// cycle emits nothing; no stream key, message id, payload or error text reaches
// a span.
func TestRunnerEmitsBatchAndHandleSpansWithOutcomes(t *testing.T) {
	exporter := installStreamSpanRecorder(t)
	transport := &fakeTransport{new: []Message{
		{Stream: "test:stream", ID: "1-0"},
		{Stream: "test:stream", ID: "2-0"},
		{Stream: "test:stream", ID: "3-0"},
	}}
	calls := 0
	var handlerSpanIDs []oteltrace.SpanID
	runner, err := New(transport, handlerFunc(func(ctx context.Context, _ Message) error {
		handlerSpanIDs = append(handlerSpanIDs, oteltrace.SpanContextFromContext(ctx).SpanID())
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

	batches := spansByName(exporter, "dev_health.stream.batch")
	if len(batches) != 1 {
		t.Fatalf("got %d batch spans, want 1", len(batches))
	}
	batch := batches[0]
	if got := spanStrAttr(batch, "dev_health.stream.messages"); got != "3" {
		t.Errorf("batch messages = %q, want 3", got)
	}
	if got := spanStrAttr(batch, "dev_health.stream.lanes"); got != "1" {
		t.Errorf("batch lanes = %q, want 1", got)
	}
	if got := spanStrAttr(batch, "dev_health.stream.failed"); got != "1" {
		t.Errorf("batch failed = %q, want 1 (only the transient failure is a batch failure)", got)
	}
	if spanStrAttr(batch, "dev_health.stream.runner") != "stream_test" {
		t.Errorf("batch runner attr = %q", spanStrAttr(batch, "dev_health.stream.runner"))
	}
	if batch.Status.Code != codes.Error {
		t.Errorf("a batch with a failed event must be an Error span")
	}
	if got := spanStrAttr(batch, "dev_health.work.stage"); got != "handle" {
		t.Errorf("failed batch stage = %q, want handle", got)
	}
	// The handler runs INSIDE its handle span: the ctx it receives carries it, so
	// the sink's own spans nest under the event.
	handleIDs := map[oteltrace.SpanID]bool{}
	for _, span := range spansByName(exporter, "dev_health.stream.handle") {
		handleIDs[span.SpanContext.SpanID()] = true
	}
	for _, id := range handlerSpanIDs {
		if !handleIDs[id] {
			t.Errorf("handler ctx span %s is not a handle span", id)
		}
	}
	if len(handlerSpanIDs) != 3 {
		t.Fatalf("handler ran %d times, want 3", len(handlerSpanIDs))
	}
	wantError := map[string]bool{"acked": false, "quarantined": false, "transient_error": true}
	seen := map[string]int{}
	for _, span := range spansByName(exporter, "dev_health.stream.handle") {
		outcome := spanStrAttr(span, "dev_health.stream.outcome")
		seen[outcome]++
		if span.Parent.SpanID() != batch.SpanContext.SpanID() {
			t.Errorf("handle span %s is not a child of the batch span", outcome)
		}
		if isError := span.Status.Code == codes.Error; isError != wantError[outcome] {
			t.Errorf("handle %s status Error = %v, want %v", outcome, isError, wantError[outcome])
		}
		if spanStrAttr(span, "dev_health.stream.runner") != "stream_test" {
			t.Errorf("handle span runner attr = %q", spanStrAttr(span, "dev_health.stream.runner"))
		}
	}
	if seen["acked"] != 1 || seen["quarantined"] != 1 || seen["transient_error"] != 1 || len(seen) != 3 {
		t.Fatalf("outcomes = %v, want one each of acked/quarantined/transient_error", seen)
	}
	assertNoIdentity(t, exporter)

	exporter.Reset()
	if err := runner.window(context.Background()); err != nil {
		t.Fatalf("idle window: %v", err)
	}
	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatalf("an idle cycle emitted %d spans, want 0", len(spans))
	}
}

func TestRunnerHandleSpanOutcomeAckError(t *testing.T) {
	exporter := installStreamSpanRecorder(t)
	transport := &fakeTransport{new: []Message{{Stream: "test:stream", ID: "1-0"}}, ackErr: errors.New("ack failed")}
	runner, err := New(transport, handlerFunc(func(context.Context, Message) error { return nil }), testConfig(), health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	_ = runner.window(context.Background())
	handles := spansByName(exporter, "dev_health.stream.handle")
	if len(handles) != 1 || spanStrAttr(handles[0], "dev_health.stream.outcome") != "ack_error" || handles[0].Status.Code != codes.Error {
		t.Fatalf("handle spans = %v, want one ack_error Error span", handles)
	}
	assertNoIdentity(t, exporter)
}

func TestRunnerHandleSpanOutcomeQuarantineError(t *testing.T) {
	exporter := installStreamSpanRecorder(t)
	base := &fakeTransport{new: []Message{{Stream: "test:stream", ID: "1-0"}}}
	runner, err := New(failingQuarantineTransport{base}, handlerFunc(func(context.Context, Message) error {
		return &PermanentError{Reason: "schema_invalid"}
	}), testConfig(), health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	_ = runner.window(context.Background())
	handles := spansByName(exporter, "dev_health.stream.handle")
	if len(handles) != 1 || spanStrAttr(handles[0], "dev_health.stream.outcome") != "quarantine_error" || handles[0].Status.Code != codes.Error {
		t.Fatalf("handle spans = %v, want one quarantine_error Error span", handles)
	}
	assertNoIdentity(t, exporter)
}

// A failed read is work that went wrong, not an idle cycle: it gets its own
// Error span carrying the runner name and the lane count, never the error text.
func TestRunnerFailedReadEmitsAnErrorSpan(t *testing.T) {
	exporter := installStreamSpanRecorder(t)
	runner, err := New(failingReadTransport{&fakeTransport{}}, handlerFunc(func(context.Context, Message) error { return nil }), testConfig(), health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.window(context.Background()); err == nil {
		t.Fatal("a failed read returned nil")
	}
	reads := spansByName(exporter, "dev_health.stream.read_failed")
	if len(reads) != 1 || reads[0].Status.Code != codes.Error {
		t.Fatalf("read_failed spans = %v, want one Error span", reads)
	}
	if got := spanStrAttr(reads[0], "dev_health.work.stage"); got != "read" {
		t.Errorf("read_failed stage = %q, want read", got)
	}
	if spanStrAttr(reads[0], "dev_health.stream.runner") != "stream_test" || spanStrAttr(reads[0], "dev_health.stream.lanes") != "1" {
		t.Errorf("read_failed attrs = %v", reads[0].Attributes)
	}
	if len(spansByName(exporter, "dev_health.stream.batch")) != 0 {
		t.Errorf("a failed read produced a batch span")
	}
	assertNoIdentity(t, exporter)
}

// A clean batch is not an Error span and carries no stage.
func TestRunnerCleanBatchHasNoErrorOrStage(t *testing.T) {
	exporter := installStreamSpanRecorder(t)
	transport := &fakeTransport{new: []Message{{Stream: "test:stream", ID: "1-0"}}}
	runner, err := New(transport, handlerFunc(func(context.Context, Message) error { return nil }), testConfig(), health.NewRegistry(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.window(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, span := range exporter.GetSpans() {
		if span.Status.Code == codes.Error || spanStrAttr(span, "dev_health.work.stage") != "" {
			t.Errorf("span %s of a clean batch has Error status or a stage", span.Name)
		}
	}
	if got := spanStrAttr(spansByName(exporter, "dev_health.stream.batch")[0], "dev_health.stream.failed"); got != "0" {
		t.Errorf("clean batch failed = %q, want 0", got)
	}
}
