package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	testTraceHex = "0102030405060708090a0b0c0d0e0f10"
	testSpanHex  = "1112131415161718"
)

func testSpanContext(t *testing.T) oteltrace.SpanContext {
	t.Helper()
	traceID, err := oteltrace.TraceIDFromHex(testTraceHex)
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := oteltrace.SpanIDFromHex(testSpanHex)
	if err != nil {
		t.Fatal(err)
	}
	return oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: oteltrace.FlagsSampled})
}

func TestJSONLoggerPutsSpanIDsOnLine(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	logger := NewJSON(&out, slog.LevelInfo)
	ctx := oteltrace.ContextWithSpanContext(context.Background(), testSpanContext(t))
	logger.InfoContext(ctx, "hello", "k", "v")
	var line map[string]any
	if err := json.Unmarshal(out.Bytes(), &line); err != nil {
		t.Fatalf("line: %v: %s", err, out.String())
	}
	if line["trace_id"] != testTraceHex || line["span_id"] != testSpanHex {
		t.Fatalf("ids missing or wrong: %s", out.String())
	}
}

func TestJSONLoggerNoSpanNoIDKeys(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	logger := NewJSON(&out, slog.LevelInfo)
	logger.InfoContext(context.Background(), "hello")
	if strings.Contains(out.String(), "trace_id") || strings.Contains(out.String(), "span_id") {
		t.Fatalf("keys without a span: %s", out.String())
	}
}

func TestJSONLoggerInvalidSpanContextNoIDKeys(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	logger := NewJSON(&out, slog.LevelInfo)
	ctx := oteltrace.ContextWithSpanContext(context.Background(), oteltrace.NewSpanContext(oteltrace.SpanContextConfig{}))
	logger.InfoContext(ctx, "hello")
	if strings.Contains(out.String(), "trace_id") || strings.Contains(out.String(), "span_id") {
		t.Fatalf("keys for an invalid span ctx: %s", out.String())
	}
}

func TestJSONLoggerHandAddedTraceIDStaysSingle(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	logger := NewJSON(&out, slog.LevelInfo)
	ctx := oteltrace.ContextWithSpanContext(context.Background(), testSpanContext(t))
	logger.InfoContext(ctx, "hello", "trace_id", testTraceHex)
	if n := strings.Count(out.String(), `"trace_id"`); n != 1 {
		t.Fatalf("trace_id keys = %d: %s", n, out.String())
	}
	if n := strings.Count(out.String(), `"span_id"`); n != 1 {
		t.Fatalf("span_id keys = %d: %s", n, out.String())
	}
}

func TestJSONLoggerWithAttrsKeepsSpanIDs(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	logger := NewJSON(&out, slog.LevelInfo).With("caller", "cli")
	ctx := oteltrace.ContextWithSpanContext(context.Background(), testSpanContext(t))
	logger.InfoContext(ctx, "hello")
	if !strings.Contains(out.String(), `"trace_id":"`+testTraceHex+`"`) || !strings.Contains(out.String(), `"span_id":"`+testSpanHex+`"`) {
		t.Fatalf("ids missing after With: %s", out.String())
	}
}
