package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// The test pipeline other packages read their spans through is the production scrub: a protected key leaves as the
// value hidden, a clean key leaves as it is.
func TestTheTestPipelineExportsThroughTheProductionScrub(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider, err := ProductionPipelineForTest(exporter)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	_, span := provider.Tracer("test").Start(context.Background(), "test.span", trace.WithAttributes(
		attribute.String("auth_token", sentinelToken()), attribute.Int64("dev_health.test.count", 3)))
	span.End()
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported spans = %d, want 1", len(spans))
	}
	got := map[string]string{}
	for _, kv := range spans[0].Attributes {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	if got["auth_token"] != hidden || got["dev_health.test.count"] != "3" || len(got) != 2 {
		t.Fatalf("exported attributes = %v, want the protected value hidden and the clean count as it is", got)
	}
}
