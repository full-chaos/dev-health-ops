package tracing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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

type netTimeoutError struct{}

func (netTimeoutError) Error() string { return "dial tcp secret-payload.example:443: i/o timeout" }
func (netTimeoutError) Timeout() bool { return true }

type netPlainError struct{}

func (netPlainError) Error() string { return "dial tcp secret-payload.example:443: refused" }
func (netPlainError) Timeout() bool { return false }

func jsonSyntaxError() error {
	var v struct{ A int }
	return json.Unmarshal([]byte(`{"A": secret-payload`), &v)
}

type sqlStateError struct{}

func (sqlStateError) Error() string {
	return "ERROR: duplicate key value (secret-payload) (SQLSTATE 23505)"
}
func (sqlStateError) SQLState() string { return "23505" }

func jsonDecodeError() error {
	var v struct{ A int }
	return json.Unmarshal([]byte(`{"A": "secret-payload"}`), &v)
}

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
		{name: "plain", err: errors.New("boom"), wantError: true, wantType: "other"},
		{name: "wrapped payload error", err: fmt.Errorf("write: %w", payloadError{}), wantError: true, wantType: "other"},
		{name: "bare cancel", err: context.Canceled, wantCancelled: true},
		{name: "cancel with marker text", err: fmt.Errorf("secret-payload customer window: %w", context.Canceled), wantCancelled: true},
		{name: "net error that is not a timeout", err: fmt.Errorf("dial: %w", netPlainError{}), wantError: true, wantType: "other"},
		{name: "json syntax", err: fmt.Errorf("handle: %w", jsonSyntaxError()), wantError: true, wantType: "decode"},
		{name: "wrapped cancel", err: fmt.Errorf("window: %w", context.Canceled), wantCancelled: true},
		{name: "deadline is a failure", err: context.DeadlineExceeded, wantError: true, wantType: "timeout"},
		{name: "wrapped deadline is a failure", err: fmt.Errorf("window timed out: %w", context.DeadlineExceeded), wantError: true, wantType: "timeout"},
		{name: "joined failures", err: errors.Join(errors.New("a"), errors.New("b")), wantError: true, wantType: "other"},
		{name: "net timeout", err: fmt.Errorf("fetch: %w", netTimeoutError{}), wantError: true, wantType: "timeout"},
		{name: "json decode", err: fmt.Errorf("handle: %w", jsonDecodeError()), wantError: true, wantType: "decode"},
		{name: "sql state", err: fmt.Errorf("write: %w", sqlStateError{}), wantError: true, wantType: "store"},
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
			wantDescription := ""
			if tc.wantError {
				wantDescription = tc.wantType
			}
			if got.Status.Description != wantDescription {
				t.Errorf("status description %q, want the class %q (never error text)", got.Status.Description, wantDescription)
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
					if string(kv.Key) == "error.type" {
						typ = kv.Value.AsString()
					}
				}
				if typ != tc.wantType {
					t.Errorf("error.type = %q, want %q", typ, tc.wantType)
				}
				if !slices.Contains(ErrorClasses, typ) {
					t.Errorf("error.type %q is not in the closed list %v", typ, ErrorClasses)
				}
			} else if len(got.Events) != 0 {
				t.Errorf("a non-failure carries events: %v", got.Events)
			}
			// The marker in the error text must be in NO attribute, event
			// attribute, event name or status description.
			var all []string
			all = append(all, got.Name, got.Status.Description)
			for _, kv := range got.Attributes {
				all = append(all, string(kv.Key), kv.Value.Emit())
			}
			for _, event := range got.Events {
				all = append(all, event.Name)
				for _, kv := range event.Attributes {
					all = append(all, string(kv.Key), kv.Value.Emit())
				}
			}
			for _, text := range all {
				if strings.Contains(text, "secret-payload") || strings.Contains(text, "customer") {
					t.Errorf("error text reached the span: %q", text)
				}
			}
		})
	}
}

func TestEndWorkSpanNilSpanIsSafe(t *testing.T) {
	EndWorkSpan(nil, errors.New("x"))
}

// The class words are what a dashboard filters on: pin the literals.
func TestErrorClassWordsArePinned(t *testing.T) {
	if got := strings.Join(ErrorClasses, ","); got != "timeout,decode,store,other" {
		t.Errorf("ErrorClasses = %q", got)
	}
}
