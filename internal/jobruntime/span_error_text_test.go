package jobruntime

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

// A handler error can carry a URL, an id, a response body: its text must not reach a span (CHAOS-7896). The planted
// error below holds secret-shaped markers in its text and in the text of an error it wraps; none of them may appear in
// the status description, an attribute or an event of the finished span.

const (
	plantedMarker = "planted" + "-marker-7896"
	plantedUser   = "hunter2"
	plantedHost   = "internal-host.example.test"
)

// plantedFailure is the innermost error: its Go type name is the only thing about it that may reach the span.
type plantedFailure struct{ message string }

func (failure *plantedFailure) Error() string { return failure.message }

func plantedError() error {
	inner := &plantedFailure{message: "response body: {\"token\":\"" + plantedMarker + "\"}"}
	return fmt.Errorf("GET https://x:%s@%s/orgs/42/repos?access_token=%s: %w", plantedUser, plantedHost, plantedMarker, inner)
}

func TestFinishJobSpanRecordsNoErrorText(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	_, span := provider.Tracer("span-error-text-test").Start(context.Background(), "job")
	finishJobSpan(span, decision{result: ResultRetry, category: CategoryRetryable}, plantedError())

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported %d spans, want 1", len(spans))
	}
	got := spans[0]
	var seen []string
	seen = append(seen, got.Status.Description)
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
	if got.Status.Code != codes.Error || got.Status.Description != string(CategoryRetryable) {
		t.Fatalf("status = %v %q, want Error and the fixed category %q", got.Status.Code, got.Status.Description, CategoryRetryable)
	}
	var typed []string
	for _, event := range got.Events {
		for _, kv := range event.Attributes {
			if kv.Key == "exception.type" {
				typed = append(typed, kv.Value.AsString())
			}
		}
	}
	if len(typed) != 1 || typed[0] != "*jobruntime.plantedFailure" {
		t.Fatalf("exception.type events = %q, want exactly the Go type name of the innermost error", typed)
	}
}

func TestFinishJobSpanStatusDescriptionIsAFixedClassForEveryCategory(t *testing.T) {
	for _, category := range []ErrorCategory{CategoryValidation, CategoryPanic, CategoryTimeout, CategoryCancelled, CategoryRetryable, CategoryPermanent, CategoryTerminalDomain, CategoryTenant, CategoryIdempotency, CategoryNone, ""} {
		t.Run(string(category), func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			_, span := provider.Tracer("t").Start(context.Background(), "job")
			finishJobSpan(span, decision{result: ResultRetry, category: category}, errors.New(plantedMarker))
			description := exporter.GetSpans()[0].Status.Description
			want := string(category)
			if category == "" || category == CategoryNone {
				want = "error"
			}
			if description != want || strings.Contains(description, plantedMarker) {
				t.Fatalf("status description = %q, want %q", description, want)
			}
		})
	}
}

func TestErrorTypeNameIsTheInnermostGoTypeNeverAMessage(t *testing.T) {
	for name, err := range map[string]error{
		"a plain error":       errors.New(plantedMarker),
		"a wrapped error":     fmt.Errorf("outer %s: %w", plantedMarker, &plantedFailure{message: plantedMarker}),
		"a joined error":      errors.Join(errors.New(plantedMarker), errors.New("x")),
		"a deeply wrapped":    fmt.Errorf("a: %w", fmt.Errorf("b: %w", &plantedFailure{message: plantedMarker})),
		"a typed value error": plantedValueError(plantedMarker),
	} {
		t.Run(name, func(t *testing.T) {
			if got := errorTypeName(err); strings.Contains(got, plantedMarker) || got == "" {
				t.Fatalf("errorTypeName = %q", got)
			}
		})
	}
	if got := errorTypeName(fmt.Errorf("w: %w", &plantedFailure{message: "m"})); got != "*jobruntime.plantedFailure" {
		t.Fatalf("errorTypeName of a wrapped pointer error = %q", got)
	}
}

type plantedValueError string

func (e plantedValueError) Error() string { return string(e) }
