package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// An error of a coordinator job can carry a URL, an id, a response body: its text must not reach the job's span
// (CHAOS-7896). The planted error holds secret-shaped markers in its text and in the text of an error it wraps; none of
// them may appear in the status description, an attribute or an event of the finished span.

const (
	plantedMarker = "the planted detail of ticket 7896"
	plantedUser   = "hunter2"
	plantedHost   = "internal-host.example.test"
)

type plantedFailure struct{ message string }

func (failure *plantedFailure) Error() string { return failure.message }

func plantedError() error {
	inner := &plantedFailure{message: "response body: {\"token\":\"" + plantedMarker + "\"}"}
	return fmt.Errorf("GET https://x:%s@%s/orgs/42/repos?access_token=%s: %w", plantedUser, plantedHost, plantedMarker, inner)
}

func TestFinishCoordinatorSpanRecordsNoErrorText(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	_, span := provider.Tracer("span-error-text-test").Start(context.Background(), "coordinator")
	finishCoordinatorSpan(span, plantedError())

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported %d spans, want 1", len(spans))
	}
	got := spans[0]
	seen := []string{got.Status.Description}
	collect := func(attributes []attribute.KeyValue) {
		for _, kv := range attributes {
			seen = append(seen, string(kv.Key), kv.Value.Emit())
		}
	}
	collect(got.Attributes)
	for _, event := range got.Events {
		seen = append(seen, event.Name)
		collect(event.Attributes)
	}
	for _, text := range seen {
		for _, marker := range []string{plantedMarker, plantedUser, plantedHost, "access_token", "orgs/42"} {
			if strings.Contains(text, marker) {
				t.Fatalf("the finished span carries error text (%q holds %q)", text, marker)
			}
		}
	}
	if got.Status.Code != codes.Error || got.Status.Description != "coordinator job failed" {
		t.Fatalf("status = %v %q, want Error and the fixed description", got.Status.Code, got.Status.Description)
	}
	var typed []string
	for _, event := range got.Events {
		for _, kv := range event.Attributes {
			if kv.Key == "exception.type" {
				typed = append(typed, kv.Value.AsString())
			}
		}
	}
	if len(typed) != 1 || typed[0] != "*syncdispatchruntime.plantedFailure" {
		t.Fatalf("exception.type events = %q, want exactly the Go type name of the innermost error", typed)
	}
}

func TestFinishCoordinatorSpanOnSuccessHasNoError(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	_, span := provider.Tracer("t").Start(context.Background(), "coordinator")
	finishCoordinatorSpan(span, nil)
	got := exporter.GetSpans()[0]
	if got.Status.Code != codes.Ok || len(got.Events) != 0 {
		t.Fatalf("a successful span: status %v, %d events", got.Status.Code, len(got.Events))
	}
}

func TestErrorTypeNameIsTheInnermostGoTypeNeverAMessage(t *testing.T) {
	for name, err := range map[string]error{
		"a plain error":   errors.New(plantedMarker),
		"a wrapped error": fmt.Errorf("outer %s: %w", plantedMarker, &plantedFailure{message: plantedMarker}),
		"a joined error":  errors.Join(errors.New(plantedMarker), errors.New("x")),
		"a deep chain":    fmt.Errorf("a: %w", fmt.Errorf("b: %w", &plantedFailure{message: plantedMarker})),
	} {
		t.Run(name, func(t *testing.T) {
			if got := errorTypeName(err); strings.Contains(got, plantedMarker) || got == "" {
				t.Fatalf("errorTypeName = %q", got)
			}
		})
	}
	if got := errorTypeName(fmt.Errorf("w: %w", &plantedFailure{message: "m"})); got != "*syncdispatchruntime.plantedFailure" {
		t.Fatalf("errorTypeName of a wrapped pointer error = %q", got)
	}
}
