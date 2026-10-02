package tracing

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func installWorkSpanRecorder(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	return exporter
}

type payloadError struct{}

func (payloadError) Error() string { return "row {customer: secret-payload}" }

// EndWorkSpan classifies the outcome of one unit of work. Each case is one
// way a unit ends; the error TEXT must never reach the span (CHAOS-7879).
func TestEndWorkSpanClassifiesOutcomes(t *testing.T) {
	cases := []struct {
		name          string
		err           error
		wantError     bool
		wantCancelled bool
		wantType      string
	}{
		{name: "nil", err: nil},
		{name: "plain", err: errors.New("boom"), wantError: true, wantType: "*errors.errorString"},
		{name: "wrapped payload error", err: fmt.Errorf("write: %w", payloadError{}), wantError: true, wantType: "tracing.payloadError"},
		{name: "bare cancel", err: context.Canceled, wantCancelled: true},
		{name: "wrapped cancel", err: fmt.Errorf("window: %w", context.Canceled), wantCancelled: true},
		{name: "deadline is a failure", err: context.DeadlineExceeded, wantError: true, wantType: "context.deadlineExceededError"},
		{name: "wrapped deadline is a failure", err: fmt.Errorf("window timed out: %w", context.DeadlineExceeded), wantError: true, wantType: "context.deadlineExceededError"},
		{name: "joined failures", err: errors.Join(errors.New("a"), errors.New("b")), wantError: true, wantType: "*errors.joinError"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exporter := installWorkSpanRecorder(t)
			_, span := StartWorkSpan(context.Background(), "test-scope", "unit")
			EndWorkSpan(span, tc.err)
			spans := exporter.GetSpans()
			if len(spans) != 1 {
				t.Fatalf("got %d spans, want 1", len(spans))
			}
			got := spans[0]
			if isError := got.Status.Code == codes.Error; isError != tc.wantError {
				t.Errorf("status Error = %v, want %v", isError, tc.wantError)
			}
			if got.Status.Description != "" {
				t.Errorf("status description %q must be empty (error text)", got.Status.Description)
			}
			var cancelled bool
			for _, kv := range got.Attributes {
				if string(kv.Key) == "dev_health.work.cancelled" {
					cancelled = kv.Value.AsBool()
				}
			}
			if cancelled != tc.wantCancelled {
				t.Errorf("dev_health.work.cancelled = %v, want %v", cancelled, tc.wantCancelled)
			}
			if tc.wantError {
				if len(got.Events) != 1 || got.Events[0].Name != "exception" {
					t.Fatalf("events = %v, want one exception event", got.Events)
				}
				var typ string
				for _, kv := range got.Events[0].Attributes {
					if string(kv.Key) == "exception.type" {
						typ = kv.Value.AsString()
					}
				}
				if typ != tc.wantType {
					t.Errorf("exception.type = %q, want %q", typ, tc.wantType)
				}
			} else if len(got.Events) != 0 {
				t.Errorf("a non-failure carries events: %v", got.Events)
			}
			if strings.Contains(fmt.Sprint(got), "secret-payload") {
				t.Errorf("error text reached the span: %v", got)
			}
		})
	}
}

func TestEndWorkSpanNilSpanIsSafe(t *testing.T) {
	EndWorkSpan(nil, errors.New("x"))
}
