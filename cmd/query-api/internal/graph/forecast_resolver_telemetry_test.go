package graph

import (
	"context"
	"testing"

	"github.com/vektah/gqlparser/v2/gqlerror"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// CHAOS-5349 r1 P2's regression guard.
//
// The three capacity/forecast resolvers used to run their authorization guard
// BEFORE starting a span, so an org-scoping rejection produced no span and no
// counter. The review's probe reported `capacityForecast spans=0`,
// `capacityForecasts spans=0`, `throughputForecast spans=0`. In the metrics that
// makes "this operation is being called and rejected" indistinguishable from
// "this operation is receiving no traffic at all" -- which is the first question
// an operator asks, and the one the telemetry was supposed to answer.
//
// The assertion is on a REAL recorded span, not on a stubbed span helper. A stub
// would sit exactly where the defect was and re-introduce the layer-masking this
// package's own telemetry_test.go documents: the seam that makes the behaviour
// testable is the seam that hides it.
//
// Note what is NOT asserted: that the resolver reaches ClickHouse. It must not
// -- the client is nil here, so if the guard ever stopped short-circuiting, these
// tests would panic rather than pass quietly.

// recordSpans installs an in-memory tracer provider for the duration of one
// test and returns the recorder. The global provider is restored afterwards, so
// tests in this package cannot leak a provider into each other.
func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	// tracer is captured at package init from the global provider, so it has to
	// be re-taken after the swap or every span lands in the old one.
	previousTracer := tracer
	tracer = provider.Tracer("test")

	t.Cleanup(func() {
		tracer = previousTracer
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	return recorder
}

func requireDeniedSpan(t *testing.T, recorder *tracetest.SpanRecorder, wantName string) {
	t.Helper()
	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("%s: recorded %d spans, want exactly 1 -- an authorization rejection "+
			"that produces no span is invisible in the metrics, which is CHAOS-5349 r1 P2",
			wantName, len(ended))
	}
	span := ended[0]
	if span.Name() != wantName {
		t.Errorf("span name: got %q, want %q", span.Name(), wantName)
	}

	var outcome string
	for _, attribute := range span.Attributes() {
		if string(attribute.Key) == "outcome" {
			outcome = attribute.Value.AsString()
		}
	}
	if outcome != "denied" {
		t.Errorf("%s: outcome attribute is %q, want \"denied\" -- an authorization "+
			"rejection counted as \"ok\" or \"error\" is worse than uncounted, because it "+
			"looks like a working operation or a broken one rather than a refused caller",
			wantName, outcome)
	}
}

func requireAuthorizationError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("got nil error, want an authorization rejection")
	}
	gqlErr, ok := err.(*gqlerror.Error)
	if !ok {
		t.Fatalf("error is %T, want *gqlerror.Error so the client sees a coded rejection", err)
	}
	if code, _ := gqlErr.Extensions["code"].(string); code != "AUTHORIZATION_ERROR" {
		t.Errorf("extensions.code: got %v, want AUTHORIZATION_ERROR", gqlErr.Extensions["code"])
	}
}

func TestCapacityForecastRecordsAuthorizationDenial(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	// No authctx claims on the context at all: the envelope-less case.
	result, err := resolver.CapacityForecast(context.Background(), "org-requested", nil)
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpan(t, recorder, "query-api.capacityForecast")
}

func TestCapacityForecastsRecordsAuthorizationDenial(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.CapacityForecasts(context.Background(), "org-requested", nil)
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpan(t, recorder, "query-api.capacityForecasts")
}

func TestThroughputForecastRecordsAuthorizationDenial(t *testing.T) {
	recorder := recordSpans(t)
	resolver := &queryResolver{&Resolver{}}

	result, err := resolver.ThroughputForecast(
		context.Background(), "org-requested", model.ThroughputForecastInput{HistoryWeeks: 12})
	if result != nil {
		t.Errorf("got a result for an unauthorized call: %+v", result)
	}
	requireAuthorizationError(t, err)
	requireDeniedSpan(t, recorder, "query-api.throughputForecast")
}
