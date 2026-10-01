package tracing

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// closedPort returns a host:port nothing listens on.
func closedPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

// TestAnUnreachableCollectorCostsTheStopAtMostTheFlushBound is the executed
// repro of the container-smoke red (exit 137): a sampled span is buffered, the
// collector port is closed, and the component's final flush used to retry until
// the whole shutdown budget (the caller's context, 10 s in the binaries) was
// spent, longer than the five seconds a stopping container is given. The stop
// must return nil inside the bound, and say loudly why spans were dropped.
func TestAnUnreachableCollectorCostsTheStopAtMostTheFlushBound(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SAMPLE_RATE", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", closedPort(t))
	var logs bytes.Buffer
	component := InitWithServiceName(slog.New(slog.NewTextHandler(&logs, nil)), "flush-test")
	t.Cleanup(func() { otel.SetTracerProvider(sdktrace.NewTracerProvider()) })

	_, span := otel.Tracer("flush-test").Start(context.Background(), "buffered")
	span.End()

	// The caller's budget is far larger than the bound: only the bound may
	// decide how long the stop takes.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	started := time.Now()
	err := component.Shutdown(ctx)
	elapsed := time.Since(started)

	if err != nil {
		t.Errorf("a down collector failed the stop: %v", err)
	}
	if limit := shutdownFlushTimeout + time.Second; elapsed > limit {
		t.Errorf("the stop took %v with an unreachable collector, want <= %v (bound %v)", elapsed, limit, shutdownFlushTimeout)
	}
	if !strings.Contains(logs.String(), "collector") || !strings.Contains(logs.String(), "dropped") {
		t.Errorf("no loud line saying the collector was down and spans were dropped:\n%s", logs.String())
	}
}

type failingExporter struct{ shutdownErr error }

func (failingExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }

func (e failingExporter) Shutdown(context.Context) error { return e.shutdownErr }

// TestARealFlushErrorOtherThanTheDeadlineIsStillReported: only the bound being
// hit means "collector down"; any other error from the final flush is still the
// stop's error.
func TestARealFlushErrorOtherThanTheDeadlineIsStillReported(t *testing.T) {
	boom := errors.New("exporter exploded")
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(failingExporter{shutdownErr: boom}))
	component := Component{provider: provider, logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	err := component.Shutdown(context.Background())
	if !errors.Is(err, boom) {
		t.Errorf("Shutdown error = %v, want the exporter's %v", err, boom)
	}
}

func TestAZeroComponentStopsAsANoOp(t *testing.T) {
	if err := (Component{}).Shutdown(context.Background()); err != nil {
		t.Errorf("zero component Shutdown = %v", err)
	}
}

// TestACallerContextThatExpiredIsNotSwallowed: only the component's own bound
// means "collector down". When the caller's own (shorter) budget runs out the
// stop did not get the time it was promised, so the error is returned and the
// "collector not reachable" line is not logged.
func TestACallerContextThatExpiredIsNotSwallowed(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SAMPLE_RATE", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", closedPort(t))
	var logs bytes.Buffer
	component := InitWithServiceName(slog.New(slog.NewTextHandler(&logs, nil)), "flush-test")
	t.Cleanup(func() { otel.SetTracerProvider(sdktrace.NewTracerProvider()) })

	_, span := otel.Tracer("flush-test").Start(context.Background(), "buffered")
	span.End()

	t.Run("deadline shorter than the bound", func(t *testing.T) {
		logs.Reset()
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if err := component.Shutdown(ctx); err == nil {
			t.Fatal("a caller deadline that expired was swallowed as a down collector")
		}
		if strings.Contains(logs.String(), "collector is not reachable") {
			t.Errorf("an expired caller deadline was reported as an unreachable collector:\n%s", logs.String())
		}
	})
	t.Run("already cancelled", func(t *testing.T) {
		logs.Reset()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		// The SDK may return nil or the context's error for a cancelled caller;
		// either way it must not be reported as an unreachable collector.
		_ = component.Shutdown(ctx)
		if strings.Contains(logs.String(), "collector is not reachable") {
			t.Errorf("a cancelled caller was reported as an unreachable collector:\n%s", logs.String())
		}
	})
}
