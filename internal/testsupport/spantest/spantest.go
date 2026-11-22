// Package spantest reads the spans a test makes twice: raw, as the code set them, and exported, after the production
// pipeline (tracing.ProductionPipelineForTest: sampler, batcher and the export scrub) as a collector would store them.
// Install sets the global tracer provider, so a test that uses it must not run in parallel.
package spantest

import (
	"context"
	"slices"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/full-chaos/dev-health-ops/internal/platform/tracing"
)

// Placeholder and Hidden are what the export scrub writes for a key or a value it removes.
const (
	Placeholder = "redacted.key"
	Hidden      = "[REDACTED]"
)

const parentName = "spantest.parent"

// Pipeline is the installed provider with its raw recorder and its exporter.
type Pipeline struct {
	t        *testing.T
	provider *sdktrace.TracerProvider
	raw      *tracetest.SpanRecorder
	exported *tracetest.InMemoryExporter
	// Parent is a context whose span is recording.
	Parent context.Context
}

// Pair is one span as the code ended it and as it left the process.
type Pair struct {
	Raw      sdktrace.ReadOnlySpan
	Exported tracetest.SpanStub
}

// Install builds the production pipeline, records the raw spans beside it and makes it the global tracer provider until
// the test ends.
func Install(t *testing.T) *Pipeline {
	t.Helper()
	exported := tracetest.NewInMemoryExporter()
	provider, err := tracing.ProductionPipelineForTest(exported)
	if err != nil {
		t.Fatal(err)
	}
	raw := tracetest.NewSpanRecorder()
	provider.RegisterSpanProcessor(raw)
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	parent, span := provider.Tracer("spantest").Start(context.Background(), parentName)
	t.Cleanup(func() { span.End() })
	return &Pipeline{t: t, provider: provider, raw: raw, exported: exported, Parent: parent}
}

// UnsampledParent is a context whose span is valid and not sampled (a remote parent that chose not to sample).
func UnsampledParent() context.Context {
	return trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		Remote:  true,
	}))
}

// Ended is every ended span except the pipeline's own parent.
func (p *Pipeline) Ended() []sdktrace.ReadOnlySpan {
	var spans []sdktrace.ReadOnlySpan
	for _, span := range p.raw.Ended() {
		if span.Name() != parentName {
			spans = append(spans, span)
		}
	}
	return spans
}

// Pairs is every ended span named name, by start time, with its exported form.
func (p *Pipeline) Pairs(name string) []Pair {
	p.t.Helper()
	if err := p.provider.ForceFlush(context.Background()); err != nil {
		p.t.Fatal(err)
	}
	exported := map[trace.SpanID]tracetest.SpanStub{}
	for _, stub := range p.exported.GetSpans() {
		exported[stub.SpanContext.SpanID()] = stub
	}
	var pairs []Pair
	for _, span := range p.raw.Ended() {
		if span.Name() != name {
			continue
		}
		stub, ok := exported[span.SpanContext().SpanID()]
		if !ok {
			p.t.Fatalf("span %q was recorded but never exported", name)
		}
		pairs = append(pairs, Pair{Raw: span, Exported: stub})
	}
	sort.SliceStable(pairs, func(i, j int) bool { return pairs[i].Raw.StartTime().Before(pairs[j].Raw.StartTime()) })
	return pairs
}

// Keys is the sorted key set of attrs.
func Keys(attrs []attribute.KeyValue) []string {
	keys := make([]string, 0, len(attrs))
	for _, kv := range attrs {
		keys = append(keys, string(kv.Key))
	}
	sort.Strings(keys)
	return keys
}

// Value is the value of key in attrs.
func Value(attrs []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// Check asserts one pair: the raw key set and the exported key set both EQUAL want; the export kept every value as the
// code set it (no placeholder key, no hidden value); and no forbidden text appears in the name, an attribute, an event
// or the status description, raw or exported.
func Check(t *testing.T, pair Pair, want []string, forbidden ...string) {
	t.Helper()
	want = slices.Sorted(slices.Values(want))
	name := pair.Raw.Name()
	if got := Keys(pair.Raw.Attributes()); !slices.Equal(got, want) {
		t.Errorf("%s raw keys = %v, want exactly %v", name, got, want)
	}
	if got := Keys(pair.Exported.Attributes); !slices.Equal(got, want) {
		t.Errorf("%s exported keys = %v, want exactly %v", name, got, want)
	}
	for _, kv := range pair.Exported.Attributes {
		raw, _ := Value(pair.Raw.Attributes(), string(kv.Key))
		if string(kv.Key) == Placeholder || kv.Value.Emit() == Hidden || kv.Value.Type() != raw.Type() || kv.Value.Emit() != raw.Emit() {
			t.Errorf("%s exported %s=%q, raw %q: the export changed a measurement", name, kv.Key, kv.Value.Emit(), raw.Emit())
		}
	}
	if pair.Exported.Name != name {
		t.Errorf("exported name %q, raw %q", pair.Exported.Name, name)
	}
	rawText, exportedText := RawText(pair.Raw), ExportedText(pair.Exported)
	for _, word := range forbidden {
		if word == "" {
			continue
		}
		if strings.Contains(rawText, word) {
			t.Errorf("%s raw span carries %q:\n%s", name, word, rawText)
		}
		if strings.Contains(exportedText, word) {
			t.Errorf("%s exported span carries %q:\n%s", name, word, exportedText)
		}
	}
}

// RawText is every text of a raw span a collector could store.
func RawText(span sdktrace.ReadOnlySpan) string {
	events := make([]sdktrace.Event, len(span.Events()))
	copy(events, span.Events())
	return text(span.Name(), span.Status().Description, span.Attributes(), events)
}

// ExportedText is every text of an exported span a collector would store.
func ExportedText(stub tracetest.SpanStub) string {
	return text(stub.Name, stub.Status.Description, stub.Attributes, stub.Events)
}

func text(name, status string, attrs []attribute.KeyValue, events []sdktrace.Event) string {
	var b strings.Builder
	b.WriteString(name + "\n" + status)
	write := func(kvs []attribute.KeyValue) {
		for _, kv := range kvs {
			b.WriteString("\n" + string(kv.Key) + "=" + kv.Value.Emit())
		}
	}
	write(attrs)
	for _, event := range events {
		b.WriteString("\n" + event.Name)
		write(event.Attributes)
	}
	return b.String()
}
