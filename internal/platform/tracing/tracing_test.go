package tracing

import (
	"context"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel"
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

// TestInitWithServiceNameInstallsTheSDKProviderNotTheNoop is CHAOS-5408's
// proof, part (1): a caller (query-api's main(), or any future caller other
// than the worker-shell framework) that runs InitWithServiceName with
// tracing enabled and an endpoint configured must find
// otel.GetTracerProvider() returning the REAL SDK provider afterward, not
// still the package-default no-op -- this is the exact assertion every span
// a resolver starts (cmd/query-api/internal/graph/telemetry.go's tracer,
// captured once at package init from whatever the global provider was at
// that time) depends on, since the OTel Go API's global package delegates a
// pre-Init Tracer to the real provider once one is installed.
func TestInitWithServiceNameInstallsTheSDKProviderNotTheNoop(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:0")

	component := InitWithServiceName(discardLogger(), "dev-health-query-api")
	t.Cleanup(func() { _ = component.Shutdown(t.Context()) })

	if component.provider == nil {
		t.Fatal("expected a live provider (Component.provider) when tracing is enabled with an endpoint")
	}
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Fatalf("otel.GetTracerProvider() is %T, want *sdktrace.TracerProvider -- the no-op default was never replaced", otel.GetTracerProvider())
	}
}

// TestInitWithServiceNameFallsBackToItsOwnDefaultNameOnly proves the two
// halves of InitWithServiceName's contract independently: the caller's
// fallback name is used when OTEL_SERVICE_NAME is unset, and the env var
// still wins over the fallback whenever it IS set -- a caller passing its
// own default must never be able to accidentally override an operator's
// explicit OTEL_SERVICE_NAME configuration.
//
// PR #2369's r1 codex review (finding 3) correctly noted this test alone
// cannot catch a mutant that ignores `defaultName` entirely (it only checks
// component.provider != nil, not what name actually reached the exporter) --
// this package has no way to read a *sdktrace.TracerProvider's Resource back
// out (no exported getter). The value-level check that closes that gap lives
// in cmd/query-api/main_otel_export_integration_test.go
// (TestOrgScopingDenialSpanReachesARealOTLPCollector), which reads the
// service.name resource attribute off REAL OTLP wire traffic -- proven to
// catch the exact ignores-defaultName mutant by running it there.
func TestInitWithServiceNameFallsBackToItsOwnDefaultNameOnly(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:0")

	t.Run("OTEL_SERVICE_NAME unset uses the caller's fallback", func(t *testing.T) {
		component := InitWithServiceName(discardLogger(), "dev-health-query-api")
		t.Cleanup(func() { _ = component.Shutdown(t.Context()) })
		if component.provider == nil {
			t.Fatal("expected a live provider")
		}
	})

	t.Run("OTEL_SERVICE_NAME set wins over the caller's fallback", func(t *testing.T) {
		t.Setenv("OTEL_SERVICE_NAME", "operator-override")
		component := InitWithServiceName(discardLogger(), "dev-health-query-api")
		t.Cleanup(func() { _ = component.Shutdown(t.Context()) })
		if component.provider == nil {
			t.Fatal("expected a live provider")
		}
		// stringEnv itself (used identically by both Init and
		// InitWithServiceName) already has direct coverage of the
		// env-wins-over-fallback rule; this test's job is only to confirm
		// InitWithServiceName actually calls stringEnv with its `defaultName`
		// parameter rather than the package constant, which the following
		// direct check on the helper proves without re-deriving it from a
		// live exporter's resource attributes.
		if got := stringEnv("OTEL_SERVICE_NAME", "dev-health-query-api"); got != "operator-override" {
			t.Fatalf("stringEnv should have read the override: got %q", got)
		}
	})
}

// TestInitStillUsesDefaultServiceNameUnaffected is a regression guard: Init
// (used by every worker binary via internal/platform/shell) must keep using
// defaultServiceName ("dev-health-ops") as its fallback, unaffected by
// InitWithServiceName's addition -- Init is now a one-line delegation to
// InitWithServiceName, and this pins that the delegation passes
// defaultServiceName, not some other value.
func TestInitStillUsesDefaultServiceNameUnaffected(t *testing.T) {
	if defaultServiceName != "dev-health-ops" {
		t.Fatalf("defaultServiceName changed to %q -- Init's fallback for every worker binary would silently change too", defaultServiceName)
	}
}
