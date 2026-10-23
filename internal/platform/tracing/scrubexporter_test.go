package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// sentinelCore is the body of a credential-shaped value built at run time, so no secret-shaped literal sits in the source.
var sentinelCore = strings.Repeat("A1b2C3d4", 5)

func sentinelToken() string { return "gh" + "p_" + sentinelCore }

// exportedText flattens every part of the exported spans a collector would store.
func exportedText(spans tracetest.SpanStubs) string {
	var b strings.Builder
	add := func(values ...string) { b.WriteString(strings.Join(values, "\x00") + "\x00") }
	attrs := func(kvs []attribute.KeyValue) {
		for _, kv := range kvs {
			add(string(kv.Key), kv.Value.Emit())
		}
	}
	for _, s := range spans {
		add(s.Name, s.Status.Description, s.InstrumentationScope.Name, s.InstrumentationScope.Version)
		attrs(s.Attributes)
		attrs(s.Resource.Attributes())
		attrs(s.InstrumentationScope.Attributes.ToSlice())
		for _, e := range s.Events {
			add(e.Name)
			attrs(e.Attributes)
		}
		for _, l := range s.Links {
			attrs(l.Attributes)
		}
	}
	return b.String()
}

func newScrubbedProvider(t *testing.T, service string) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider, err := providerWithExporter(exporter, service, "test", 1.0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return provider, exporter
}

func TestASentinelInAnyPartOfASpanNeverReachesTheExporter(t *testing.T) {
	token := sentinelToken()
	provider, exporter := newScrubbedProvider(t, "svc-"+token)
	ctx := context.Background()
	tracer := provider.Tracer("scope "+token, trace.WithInstrumentationAttributes(attribute.String("scope_attr", token)))
	_, linked := tracer.Start(ctx, "linked")
	linked.End()

	_, span := tracer.Start(ctx, "operation "+token,
		trace.WithAttributes(attribute.String("at_start", token)),
		trace.WithLinks(trace.Link{SpanContext: linked.SpanContext(), Attributes: []attribute.KeyValue{attribute.String("link_attr", token)}}))
	span.SetAttributes(attribute.String("set_later", token), attribute.StringSlice("slice", []string{"fine", token}))
	span.SetAttributes(attribute.String("url.full", "postgres://user:"+sentinelCore+"@db.internal/app"))
	span.AddEvent("event "+token, trace.WithAttributes(attribute.String("event_attr", token)))
	span.RecordError(errors.New("failed with " + token))
	span.SetStatus(codes.Error, "status "+token)
	span.End()
	if err := provider.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("exported %d spans, want 2", len(spans))
	}
	text := exportedText(spans)
	if strings.Contains(text, sentinelCore) {
		t.Fatalf("the sentinel reached the exporter:\n%s", strings.ReplaceAll(text, "\x00", " | "))
	}
	for _, want := range []string{"operation", "at_start", "set_later", "fine", "event_attr", "exception", "exception.type", "link_attr", "scope_attr"} {
		if !strings.Contains(text, want) {
			t.Errorf("the scrub dropped %q: attribute names and non-secret text must survive", want)
		}
	}
}

func TestProtectedKeysHideTheirValueWhateverTheType(t *testing.T) {
	provider, exporter := newScrubbedProvider(t, "svc")
	_, span := provider.Tracer("t").Start(context.Background(), "op")
	span.SetAttributes(attribute.String("password", "hunter2-not-a-secret-shape"), attribute.Int("api_key", 12345),
		attribute.StringSlice("client_secret", []string{"one", "two"}), attribute.String("outcome", "ok"))
	span.End()
	_ = provider.ForceFlush(context.Background())
	got := map[string]string{}
	for _, kv := range exporter.GetSpans()[0].Attributes {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	for _, key := range []string{"password", "api_key", "client_secret"} {
		if got[key] != hidden {
			t.Errorf("%s = %q, want %q", key, got[key], hidden)
		}
	}
	if got["outcome"] != "ok" {
		t.Errorf("a safe key lost its value: %q", got["outcome"])
	}
}

func TestExceptionTextIsReplacedAndTheTypeKept(t *testing.T) {
	provider, exporter := newScrubbedProvider(t, "svc")
	_, span := provider.Tracer("t").Start(context.Background(), "op")
	span.RecordError(errors.New("connection refused for tenant acme"))
	span.End()
	_ = provider.ForceFlush(context.Background())
	var seen bool
	for _, e := range exporter.GetSpans()[0].Events {
		if e.Name != "exception" {
			continue
		}
		seen = true
		got := map[string]string{}
		for _, kv := range e.Attributes {
			got[string(kv.Key)] = kv.Value.Emit()
		}
		if got["exception.message"] != hidden || got["exception.stacktrace"] != "" && got["exception.stacktrace"] != hidden {
			t.Errorf("exception text survived: %v", got)
		}
		if got["exception.type"] == "" || got["exception.type"] == hidden {
			t.Errorf("exception.type was lost: %v", got)
		}
	}
	if !seen {
		t.Fatal("no exception event exported")
	}
}

func TestNeverSafeKeysAreNotSafe(t *testing.T) {
	for _, key := range neverSafe {
		if safeKeys[key] {
			t.Errorf("%s is in safeKeys", key)
		}
	}
	for _, key := range []string{"url.full", "http.url", "db.statement", "url.query"} {
		out, changed := scrubValue(attribute.String(key, "https://u:"+sentinelCore+"@host/p?token="+sentinelCore), false)
		if !changed || strings.Contains(out.Value.AsString(), sentinelCore) {
			t.Errorf("%s: credential survived: %q", key, out.Value.AsString())
		}
	}
	for key := range safeKeys {
		if strings.Contains(key, "*") || strings.HasSuffix(key, ".") {
			t.Errorf("safe key %q looks like a prefix", key)
		}
	}
}

func TestASpanThatNeedsNoScrubbingIsExportedAsItIs(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	_, span := provider.Tracer("t").Start(context.Background(), "plain", trace.WithAttributes(attribute.String("outcome", "ok"), attribute.Int("n", 3)))
	span.AddEvent("tick", trace.WithAttributes(attribute.String("reason", "done")))
	span.End()
	snapshot := exporter.GetSpans().Snapshots()
	if len(snapshot) != 1 {
		t.Fatalf("%d spans", len(snapshot))
	}
	if _, wrapped := scrubSpan(snapshot[0]).(scrubbedSpan); wrapped {
		t.Fatal("an unchanged span was wrapped")
	}
	if allocs := testing.AllocsPerRun(100, func() { _ = scrubSpan(snapshot[0]) }); allocs != 0 {
		t.Errorf("scrubbing an unchanged span allocates %v times", allocs)
	}
}

func TestDistinctSecretsGiveOneOutputValue(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		out, _ := scrubValue(attribute.String("api_key", fmt.Sprintf("value-%d", i)), false)
		seen[out.Value.AsString()] = true
	}
	if len(seen) != 1 {
		t.Fatalf("1000 distinct protected values gave %d distinct outputs", len(seen))
	}
}

func TestTheSpanScrubIsTheLogRedactor(t *testing.T) {
	token := sentinelToken()
	for _, c := range []struct{ key, value string }{
		{"note", "see " + token + " for access"},
		{"detail", "Authorization: Bearer " + sentinelCore},
		{"dsn", "postgres://user:" + sentinelCore + "@db/app"},
		{"password", "x"},
		{"plain", "nothing to hide"},
	} {
		var buf bytes.Buffer
		logging.NewJSON(&buf, -8).Info("m", c.key, c.value)
		var line map[string]any
		if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
			t.Fatal(err)
		}
		out, _ := scrubValue(attribute.String(c.key, c.value), false)
		if line[c.key] != out.Value.AsString() {
			t.Errorf("%s: log %q, span %q", c.key, line[c.key], out.Value.AsString())
		}
	}
}

// The scrub is only a guarantee if it is the only way a span leaves the process.
func TestTheProviderHasOneExportPathAndItIsTheScrub(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "tracing.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var batchers int
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch selector.Sel.Name {
		case "WithSyncer", "WithBatcher", "WithSpanProcessor":
			var text bytes.Buffer
			ast.Fprint(&text, nil, call.Args[0], nil)
			switch selector.Sel.Name {
			case "WithBatcher":
				batchers++
				if !strings.Contains(text.String(), "scrubExporter") {
					t.Errorf("the batcher's exporter is not wrapped by scrubExporter")
				}
			case "WithSyncer":
				t.Errorf("a syncer would export without the scrub")
			case "WithSpanProcessor":
				if !strings.Contains(text.String(), "endCounter") {
					t.Errorf("a span processor other than the outcome counter is registered")
				}
			}
		}
		return true
	})
	if batchers != 1 {
		t.Fatalf("%d batchers, want exactly 1", batchers)
	}
}

func BenchmarkScrubSpan(b *testing.B) {
	token := sentinelToken()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	for _, withSecret := range []bool{false, true} {
		exporter.Reset()
		_, span := provider.Tracer("bench").Start(context.Background(), "operation")
		for i := 0; i < 10; i++ {
			value := fmt.Sprintf("value-%d", i)
			if withSecret && i == 0 {
				value = "see " + token
			}
			span.SetAttributes(attribute.String(fmt.Sprintf("key_%d", i), value))
		}
		span.AddEvent("one", trace.WithAttributes(attribute.String("reason", "ok")))
		span.AddEvent("two", trace.WithAttributes(attribute.Int("n", 2)))
		span.End()
		snapshot := exporter.GetSpans().Snapshots()[0]
		name := "clean"
		if withSecret {
			name = "one_secret"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = scrubSpan(snapshot)
			}
		})
	}
}
