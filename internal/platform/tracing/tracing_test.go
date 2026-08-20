package tracing

import (
	"log/slog"
	"testing"
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
