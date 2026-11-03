package tracing

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
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

// hungExporter is a collector that never answers: every export and its shutdown wait for their context to end, or for the test to
// release them (the SDK abandons the export it started when the flush bound ends). It is in memory, no socket is opened.
type hungExporter struct{ release chan struct{} }

func (e hungExporter) ExportSpans(ctx context.Context, _ []sdktrace.ReadOnlySpan) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.release:
		return nil
	}
}

func (e hungExporter) Shutdown(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.release:
		return nil
	}
}

// TestAnUnreachableCollectorCostsTheStopAtMostTheFlushBound is the repro of the container-smoke red (exit 137): a sampled span is
// buffered, the collector never answers, and the component's final flush used to wait until the whole shutdown budget (the caller's
// context, 10 s in the binaries) was spent, longer than the five seconds a stopping container is given. The stop must return nil
// inside the bound, and say loudly why spans were dropped. It runs in a bubble on a virtual clock against an in-memory exporter, so
// the stop that outlives the bound by any amount fails.
func TestAnUnreachableCollectorCostsTheStopAtMostTheFlushBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		exporter := hungExporter{release: make(chan struct{})}
		// The export the SDK abandoned at the bound ends here, so the bubble can end.
		defer close(exporter.release)
		provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithBatcher(exporter))
		component := Component{provider: provider, logger: slog.New(slog.NewTextHandler(&logs, nil)), endpoint: "collector.test:4317"}

		_, span := provider.Tracer("flush-test").Start(context.Background(), "buffered")
		span.End()

		// The caller's budget is far larger than the bound: only the bound may decide how long the stop takes.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		started := time.Now()
		err := component.Shutdown(ctx)
		returnedAt := time.Now()

		if err != nil {
			t.Errorf("a down collector failed the stop: %v", err)
		}
		if returnedAt.After(started.Add(shutdownFlushTimeout)) {
			t.Errorf("the stop returned at +%v of virtual time with an unreachable collector, want it back at the flush bound %v", returnedAt.Sub(started), shutdownFlushTimeout)
		}
		if ctx.Err() != nil {
			t.Errorf("the stop spent the caller's whole budget with an unreachable collector, want it back at the flush bound %v", shutdownFlushTimeout)
		}
		if !strings.Contains(logs.String(), "collector") || !strings.Contains(logs.String(), "dropped") {
			t.Errorf("no loud line saying the collector was down and spans were dropped:\n%s", logs.String())
		}
	})
}

type failingExporter struct {
	shutdownErr error
	// boundLive, when set, records whether the flush context was still live when the exporter returned its error.
	boundLive *atomic.Bool
}

func (failingExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }

func (e failingExporter) Shutdown(ctx context.Context) error {
	if e.boundLive != nil {
		e.boundLive.Store(ctx.Err() == nil)
	}
	return e.shutdownErr
}

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
}

// TestAnExporterDeadlineErrorBeforeTheBoundIsAFailureNotACollectorOutage: an
// exporter that itself returns context.DeadlineExceeded from its shutdown while
// the bound has not elapsed (and the caller is live) is a real flush failure.
func TestAnExporterDeadlineErrorBeforeTheBoundIsAFailureNotACollectorOutage(t *testing.T) {
	var logs bytes.Buffer
	var boundLive atomic.Bool
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(failingExporter{shutdownErr: context.DeadlineExceeded, boundLive: &boundLive}))
	component := Component{provider: provider, logger: slog.New(slog.NewTextHandler(&logs, nil)), endpoint: "x"}
	err := component.Shutdown(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown = %v, want the exporter's own deadline error returned", err)
	}
	if !boundLive.Load() {
		t.Fatalf("the exporter's error came after the bound ended; the test did not exercise an early deadline error")
	}
	if strings.Contains(logs.String(), "collector is not reachable") {
		t.Errorf("an exporter's own deadline error was reported as an unreachable collector:\n%s", logs.String())
	}
}

// TestACancelledCallerOnAFreshComponentIsNotReportedAsAnOutage covers the
// cancelled-caller case on a component nothing has shut down yet (a second
// Shutdown of an SDK provider is a no-op, so it cannot share the component of
// the deadline cell).
func TestACancelledCallerOnAFreshComponentIsNotReportedAsAnOutage(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SAMPLE_RATE", "1")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", closedPort(t))
	var logs bytes.Buffer
	component := InitWithServiceName(slog.New(slog.NewTextHandler(&logs, nil)), "flush-test")
	t.Cleanup(func() { otel.SetTracerProvider(sdktrace.NewTracerProvider()) })
	_, span := otel.Tracer("flush-test").Start(context.Background(), "buffered")
	span.End()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = component.Shutdown(ctx) // the SDK may return nil or the context's error
	if strings.Contains(logs.String(), "collector is not reachable") {
		t.Errorf("a cancelled caller was reported as an unreachable collector:\n%s", logs.String())
	}
}

// flushBoundContract is the tracing final-flush bound the shell stop budget is built on (shell stop_bound_test.go). A
// literal on purpose: raising the bound alone turns this red.
const flushBoundContract = 2 * time.Second

func TestFlushBoundContract(t *testing.T) {
	if ShutdownFlushTimeout != flushBoundContract {
		t.Fatalf("ShutdownFlushTimeout = %v, want the contract %v: change both on purpose, with the stop budget", ShutdownFlushTimeout, flushBoundContract)
	}
}
