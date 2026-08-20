package tracing

import (
	"context"
	"log/slog"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(noopWriter{}, nil))
}

type noopWriter struct{}

func (noopWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestInitDisabledViaEnvReturnsNoopComponent(t *testing.T) {
	for _, value := range []string{"false", "0", "no", "False", "NO"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("OTEL_ENABLED", value)
			component := Init(discardLogger())
			if component.provider != nil {
				t.Fatalf("expected disabled component for OTEL_ENABLED=%s, got a live provider", value)
			}
			if err := component.Shutdown(t.Context()); err != nil {
				t.Fatalf("no-op component shutdown must not error: %v", err)
			}
		})
	}
}

func TestInitMalformedSampleRateNeverCrashesAndDisablesTracing(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SAMPLE_RATE", "not-a-float")

	component := Init(discardLogger())
	if component.provider != nil {
		t.Fatal("a malformed OTEL_SAMPLE_RATE must disable tracing, not panic or produce a live provider")
	}
}

func TestInitNaNSampleRateDisablesTracingInsteadOfProducingAnUndefinedSampler(t *testing.T) {
	// strconv.ParseFloat happily accepts "NaN", and NaN fails every
	// comparison in sampler() (both the >=1.0 and <=0.0 branches), so
	// without an explicit check this would silently reach
	// TraceIDRatioBased(NaN) instead of being caught as bad configuration.
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SAMPLE_RATE", "NaN")

	component := Init(discardLogger())
	if component.provider != nil {
		t.Fatal("OTEL_SAMPLE_RATE=NaN must disable tracing, not produce a live provider")
	}
}

func TestSampleRateFromEnvRejectsNaN(t *testing.T) {
	t.Setenv("OTEL_SAMPLE_RATE", "NaN")
	if _, err := sampleRateFromEnv(); err == nil {
		t.Fatal("expected an error for OTEL_SAMPLE_RATE=NaN")
	}
}

func TestInitDefaultEnabledConstructsAProvider(t *testing.T) {
	// No OTEL_* vars set: OTEL_ENABLED defaults to true, and the OTLP gRPC
	// exporter dials lazily, so construction must succeed even though nothing
	// is listening on the default endpoint.
	component := Init(discardLogger())
	if component.provider == nil {
		t.Fatal("expected a live provider when tracing is enabled by default")
	}
	if err := component.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown of a live provider with no collector listening must still succeed: %v", err)
	}
}

func TestInitAcceptsTheURLShapedEndpointEveryDeploymentActuallySets(t *testing.T) {
	// deploy/kubernetes/configmap.yaml, deploy/helm/dev-health/values.yaml,
	// and deploy/docker-compose/compose.production.yml all set
	// OTEL_EXPORTER_OTLP_ENDPOINT to a URL, not this package's own bare
	// "host:port" default -- otlptracegrpc.WithEndpoint requires "no scheme
	// or path", so passing a URL there (rather than to WithEndpointURL)
	// would make gRPC try to dial a target literally containing "http://".
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability.svc.cluster.local:4317")

	component := Init(discardLogger())
	if component.provider == nil {
		t.Fatal("expected a live provider for a URL-shaped OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	if err := component.Shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown must still succeed: %v", err)
	}
}

func TestDialOptionsChoosesByEndpointShape(t *testing.T) {
	for _, endpoint := range []string{"localhost:4317", "otel-collector:4317"} {
		t.Run("bare/"+endpoint, func(t *testing.T) {
			if got := len(dialOptions(endpoint)); got != 2 {
				t.Fatalf("bare host:port must produce [WithEndpoint, WithInsecure], got %d options", got)
			}
		})
	}
	t.Run("http scheme is insecure", func(t *testing.T) {
		if got := len(dialOptions("http://otel-collector:4317")); got != 2 {
			t.Fatalf("http:// must produce [WithEndpointURL, WithInsecure], got %d options", got)
		}
	})
	t.Run("https scheme is not forced insecure", func(t *testing.T) {
		if got := len(dialOptions("https://otel-collector:4317")); got != 1 {
			t.Fatalf("https:// must produce only [WithEndpointURL], got %d options", got)
		}
	})
}

func TestParentBasedSamplerHonorsThePropagatedSampledFlag(t *testing.T) {
	// newProvider wraps sampler() in sdktrace.ParentBased so a span parented
	// from envelope.TraceParent (extracted in internal/jobruntime.startJobSpan)
	// inherits the trace's ROOT sampling decision instead of making an
	// independent ratio call that could disagree with it if Python and Go's
	// OTEL_SAMPLE_RATE values differ.
	traceID, err := oteltrace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := oteltrace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("sampled parent samples even at rate 0", func(t *testing.T) {
		parent := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: traceID, SpanID: spanID, TraceFlags: oteltrace.FlagsSampled, Remote: true,
		})
		ctx := oteltrace.ContextWithRemoteSpanContext(context.Background(), parent)
		result := sdktrace.ParentBased(sampler(0)).ShouldSample(sdktrace.SamplingParameters{
			ParentContext: ctx, TraceID: traceID,
		})
		if result.Decision != sdktrace.RecordAndSample {
			t.Fatalf("expected RecordAndSample for a sampled parent even at rate 0, got %v", result.Decision)
		}
	})

	t.Run("unsampled parent does not sample even at rate 1", func(t *testing.T) {
		parent := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: traceID, SpanID: spanID, TraceFlags: 0, Remote: true,
		})
		ctx := oteltrace.ContextWithRemoteSpanContext(context.Background(), parent)
		result := sdktrace.ParentBased(sampler(1)).ShouldSample(sdktrace.SamplingParameters{
			ParentContext: ctx, TraceID: traceID,
		})
		if result.Decision != sdktrace.Drop {
			t.Fatalf("expected Drop for an unsampled parent even at rate 1, got %v", result.Decision)
		}
	})

	t.Run("no parent falls back to the root ratio decision", func(t *testing.T) {
		result := sdktrace.ParentBased(sampler(0)).ShouldSample(sdktrace.SamplingParameters{
			ParentContext: context.Background(), TraceID: traceID,
		})
		if result.Decision != sdktrace.Drop {
			t.Fatalf("expected the root sampler's Drop decision for a root span at rate 0, got %v", result.Decision)
		}
	})
}

func TestSamplerBoundaries(t *testing.T) {
	if _, ok := sampler(1.5).(interface{ Description() string }); !ok {
		t.Fatal("sampler must always implement trace.Sampler")
	}
	if sampler(0).Description() != "AlwaysOffSampler" {
		t.Fatalf("rate<=0 must be AlwaysOff, got %s", sampler(0).Description())
	}
	if sampler(1).Description() != "AlwaysOnSampler" {
		t.Fatalf("rate>=1 must be AlwaysOn, got %s", sampler(1).Description())
	}
}

func TestEnabledFromEnvDefaultsTrue(t *testing.T) {
	if !enabledFromEnv() {
		t.Fatal("OTEL_ENABLED must default to true, mirroring tracing.py")
	}
}
