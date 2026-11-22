package tracing

import sdktrace "go.opentelemetry.io/otel/sdk/trace"

// ProductionPipelineForTest builds a tracer provider the way Init builds one (providerWithExporter: the ParentBased
// sampler at rate 1, the span counters, the batcher and the export scrub in front of exporter), so a test in another
// package reads its spans as a collector would store them: a key the scrub would hide shows as redacted.key there.
// It exists because scrubExporter is unexported. No production code calls it; call ForceFlush before reading exporter.
func ProductionPipelineForTest(exporter sdktrace.SpanExporter) (*sdktrace.TracerProvider, error) {
	return providerWithExporter(exporter, "dev-health-test", "test", 1.0)
}
