package selfprobe

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeTx lets a test decide whether Rollback succeeds without a live
// database.
type fakeTx struct {
	rollbackErr error
}

func (t fakeTx) Rollback(context.Context) error { return t.rollbackErr }

// fakeOpener is a scriptable TxOpener: each call to Begin pops the next
// scripted outcome, so a test can prove the monitor's behaviour across a
// sequence of samples (e.g. healthy, then wedged, then recovered) without a
// live database. It also records whether it was called at all, so tests can
// assert the synchronous first-sample behaviour Start promises.
type fakeOpener struct {
	mu      sync.Mutex
	calls   int
	beginFn func(ctx context.Context, call int) (Tx, error)
}

func (o *fakeOpener) Begin(ctx context.Context) (Tx, error) {
	o.mu.Lock()
	call := o.calls
	o.calls++
	o.mu.Unlock()
	if o.beginFn == nil {
		return fakeTx{}, nil
	}
	return o.beginFn(ctx, call)
}

func (o *fakeOpener) callCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestNeverSucceededIsPreSeededUnready is the pre-seeded-fail-closed
// requirement from CHAOS-4029: a monitor that has never completed a sample
// must report NOT ready, distinguishing "never proven" from "healthy by
// default" (matching the worker_daily_metrics_lease_total pre-seeding
// contract the ticket cites, CHAOS-3991).
func TestNeverSucceededIsPreSeededUnready(t *testing.T) {
	t.Parallel()
	monitor := New("test", &fakeOpener{}, testLogger())
	if err := monitor.Ready(context.Background()); err == nil {
		t.Fatal("expected Ready to fail before any sample has run")
	}
}

// TestFirstSuccessClearsReadiness is the RED-ON-BASELINE proof for the
// happy path: after one successful sample, Ready reports healthy.
func TestFirstSuccessClearsReadiness(t *testing.T) {
	t.Parallel()
	opener := &fakeOpener{}
	monitor := New("test", opener, testLogger())
	monitor.sample(context.Background())
	if err := monitor.Ready(context.Background()); err != nil {
		t.Fatalf("expected Ready to pass after a successful sample, got %v", err)
	}
	if opener.callCount() != 1 {
		t.Fatalf("expected exactly one Begin call, got %d", opener.callCount())
	}
}

// TestWedgedTransactionFailsReadinessAfterStaleness is the core red-on-
// baseline scenario CHAOS-4029 exists to catch: a process whose transaction
// path is wedged (every Begin fails, mirroring a lost domain pool / revoked
// grant / pgbouncer recreation) must go NOT ready once its last success ages
// past the staleness window, even though nothing about the process crashed
// or restarted.
func TestWedgedTransactionFailsReadinessAfterStaleness(t *testing.T) {
	t.Parallel()
	opener := &fakeOpener{
		beginFn: func(context.Context, int) (Tx, error) {
			return nil, errors.New("dependency_unavailable")
		},
	}
	monitor := New("test", opener, testLogger())
	clock := time.Unix(1_700_000_000, 0)
	monitor.now = func() time.Time { return clock }

	monitor.sample(context.Background())
	if err := monitor.Ready(context.Background()); err == nil {
		t.Fatal("expected Ready to fail: this monitor has never succeeded")
	}

	// Prove staleness detection separately from never-succeeded: seed one
	// real success, then let every subsequent probe fail and the clock
	// advance past the staleness window.
	opener2 := &fakeOpener{}
	monitor2 := New("test", opener2, testLogger())
	monitor2.now = func() time.Time { return clock }
	monitor2.sample(context.Background())
	if err := monitor2.Ready(context.Background()); err != nil {
		t.Fatalf("expected Ready to pass immediately after a success, got %v", err)
	}

	// The dependency wedges: every subsequent Begin fails.
	opener2.beginFn = func(context.Context, int) (Tx, error) {
		return nil, errors.New("dependency_unavailable")
	}
	clock = clock.Add(monitor2.stale + time.Second)
	monitor2.sample(context.Background())
	staleErr := monitor2.Ready(context.Background())
	if staleErr == nil {
		t.Fatal("expected Ready to fail once the last success is older than the staleness window")
	}
	if !strings.Contains(staleErr.Error(), "stale") {
		t.Fatalf("expected a staleness-shaped error, got %v", staleErr)
	}
}

// TestRecoveryClearsStaleness proves the self-heal half of the ticket's
// acceptance criterion: once a probe succeeds again, readiness clears on its
// own -- no restart required.
func TestRecoveryClearsStaleness(t *testing.T) {
	t.Parallel()
	opener := &fakeOpener{}
	monitor := New("test", opener, testLogger())
	clock := time.Unix(1_700_000_000, 0)
	monitor.now = func() time.Time { return clock }

	monitor.sample(context.Background())
	opener.beginFn = func(context.Context, int) (Tx, error) {
		return nil, errors.New("dependency_unavailable")
	}
	clock = clock.Add(monitor.stale + time.Second)
	monitor.sample(context.Background())
	if monitor.Ready(context.Background()) == nil {
		t.Fatal("expected staleness to fail readiness before recovery")
	}

	opener.beginFn = nil // dependency recovers
	monitor.sample(context.Background())
	if err := monitor.Ready(context.Background()); err != nil {
		t.Fatalf("expected readiness to self-heal after a fresh success, got %v", err)
	}
}

// TestRollbackFailureIsReported proves a transaction that BEGAN but could not
// roll back is treated as a failed sample, not a silent success -- Begin
// succeeding is necessary but not sufficient proof of a healthy round trip.
func TestRollbackFailureIsReported(t *testing.T) {
	t.Parallel()
	opener := &fakeOpener{
		beginFn: func(context.Context, int) (Tx, error) {
			return fakeTx{rollbackErr: errors.New("connection reset")}, nil
		},
	}
	monitor := New("test", opener, testLogger())
	monitor.sample(context.Background())
	if monitor.Ready(context.Background()) == nil {
		t.Fatal("expected a rollback failure to keep the monitor unready")
	}
	snap := monitor.snapshot()
	if snap.failures[ReasonRollbackFailed] != 1 {
		t.Fatalf("expected one rollback_failed sample, got %+v", snap.failures)
	}
}

// TestTimeoutIsClassifiedSeparately proves a context deadline is attributed
// to ReasonTimeout rather than the generic ReasonBeginFailed, so an operator
// reading dev_health_execution_liveness_probe_failures_total can tell a slow
// database apart from a refused connection.
func TestTimeoutIsClassifiedSeparately(t *testing.T) {
	t.Parallel()
	opener := &fakeOpener{
		beginFn: func(ctx context.Context, _ int) (Tx, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	monitor := New("test", opener, testLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	monitor.sample(ctx)
	snap := monitor.snapshot()
	if snap.failures[ReasonTimeout] != 1 {
		t.Fatalf("expected one timeout sample, got %+v", snap.failures)
	}
}

// TestNewRejectsIncompleteConstruction mirrors newQueueHealthMonitor's
// nil-safety contract: a caller that has no opener, no logger, or no name
// gets nil back rather than a monitor that would panic or probe nothing.
func TestNewRejectsIncompleteConstruction(t *testing.T) {
	t.Parallel()
	if New("", &fakeOpener{}, testLogger()) != nil {
		t.Fatal("expected nil for an empty name")
	}
	if New("test", nil, testLogger()) != nil {
		t.Fatal("expected nil for a nil opener")
	}
	if New("test", &fakeOpener{}, nil) != nil {
		t.Fatal("expected nil for a nil logger")
	}
}

// TestNewPoolPreservesNil proves NewPool(nil) yields a nil TxOpener rather
// than a non-nil interface wrapping a nil pool -- the classic Go "typed nil
// in an interface is not == nil" trap, which would make New's nil-check
// silently pass a broken monitor through.
func TestNewPoolPreservesNil(t *testing.T) {
	t.Parallel()
	if opener := NewPool(nil); opener != nil {
		t.Fatalf("expected NewPool(nil) to yield a nil TxOpener, got %#v", opener)
	}
}

// TestStartSamplesSynchronouslyBeforeReturning is the ordering guarantee the
// package doc promises: by the time Start returns, Ready already has a real
// answer -- not a "never_proven" placeholder racing the background loop.
func TestStartSamplesSynchronouslyBeforeReturning(t *testing.T) {
	t.Parallel()
	opener := &fakeOpener{}
	monitor := New("test", opener, testLogger())
	if err := monitor.Start(context.Background()); err != nil {
		t.Fatalf("Start returned an error: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = monitor.Shutdown(shutdownCtx)
	}()
	if err := monitor.Ready(context.Background()); err != nil {
		t.Fatalf("expected Ready to pass immediately after Start, got %v", err)
	}
	if opener.callCount() != 1 {
		t.Fatalf("expected exactly one synchronous Begin call from Start, got %d", opener.callCount())
	}
}

// TestShutdownIsIdempotentAndSafeBeforeStart mirrors
// queueHealthMonitor.Shutdown's contract test: the lifecycle runtime only
// stops components it started, but a component that panics or hangs when
// that assumption is violated turns an unrelated startup failure into a
// stuck shutdown.
func TestShutdownIsIdempotentAndSafeBeforeStart(t *testing.T) {
	t.Parallel()
	monitor := New("test", &fakeOpener{}, testLogger())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := monitor.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown before Start should be a no-op, got %v", err)
	}
	if err := monitor.Start(context.Background()); err != nil {
		t.Fatalf("Start returned an error: %v", err)
	}
	if err := monitor.Shutdown(ctx); err != nil {
		t.Fatalf("first Shutdown failed: %v", err)
	}
	if err := monitor.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown should also be a no-op, got %v", err)
	}
}

// panickingOpener reproduces a real-world hazard this repo's own test
// fixtures hit: a zero-value *pgxpool.Pool{} (used pervasively elsewhere as
// a "non-nil but not really connected" placeholder) panics inside
// puddle.Pool.acquire when Begin is actually called, rather than returning a
// clean error. Both Monitor.sample (the ticking path) and Once (the
// synchronous path) must survive that, not crash their caller -- doubly so
// for Monitor.sample, which also runs unattended on a background goroutine.
type panickingOpener struct{}

func (panickingOpener) Begin(context.Context) (Tx, error) {
	panic("simulated pool panic (e.g. a zero-value *pgxpool.Pool{})")
}

func TestSamplePanicRecoversAsAFailedSample(t *testing.T) {
	t.Parallel()
	monitor := New("test", panickingOpener{}, testLogger())
	monitor.sample(context.Background()) // must not panic
	if monitor.Ready(context.Background()) == nil {
		t.Fatal("expected a panicking probe to be reported as unready, not silently healthy")
	}
	snap := monitor.snapshot()
	if snap.failures[ReasonPanicked] != 1 {
		t.Fatalf("expected one panicked sample, got %+v", snap.failures)
	}
}

func TestOnceRecoversFromAPanickingOpener(t *testing.T) {
	t.Parallel()
	err := Once(context.Background(), panickingOpener{}) // must not panic
	if err == nil {
		t.Fatal("expected a panicking opener to fail Once, not succeed")
	}
}

// TestOnceIsTheSynchronousCounterpartToTheMonitor proves the one-shot,
// per-poll probe (idempotency_backend's CheckFunc) succeeds and fails on
// exactly the same conditions the ticking Monitor does, and that it never
// leaks the underlying driver error text.
func TestOnceIsTheSynchronousCounterpartToTheMonitor(t *testing.T) {
	t.Parallel()
	if err := Once(context.Background(), nil); err == nil {
		t.Fatal("expected a nil opener to fail closed")
	}
	healthy := &fakeOpener{}
	if err := Once(context.Background(), healthy); err != nil {
		t.Fatalf("expected a healthy opener to succeed, got %v", err)
	}
	beginFails := &fakeOpener{beginFn: func(context.Context, int) (Tx, error) {
		return nil, errors.New("password authentication failed for user \"secret\"")
	}}
	err := Once(context.Background(), beginFails)
	if err == nil {
		t.Fatal("expected a failing Begin to fail Once")
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "authentication") {
		t.Fatalf("Once leaked underlying driver error text: %v", err)
	}
	rollbackFails := &fakeOpener{beginFn: func(context.Context, int) (Tx, error) {
		return fakeTx{rollbackErr: errors.New("connection reset by peer")}, nil
	}}
	if err := Once(context.Background(), rollbackFails); err == nil {
		t.Fatal("expected a failing Rollback to fail Once")
	}
}

// TestWritePrometheusReportsBoundedReasonsOnly proves the telemetry surface
// never grows an unbounded label set and always reports the declared reason
// series even at zero (the same "alert on absence" discipline
// dev_health_runtime_check_failed uses).
func TestWritePrometheusReportsBoundedReasonsOnly(t *testing.T) {
	t.Parallel()
	opener := &fakeOpener{
		beginFn: func(context.Context, int) (Tx, error) {
			return nil, errors.New("boom")
		},
	}
	monitor := New("worker_execution_liveness", opener, testLogger())
	monitor.sample(context.Background())

	var output strings.Builder
	if err := monitor.WritePrometheus(&output); err != nil {
		t.Fatalf("WritePrometheus returned an error: %v", err)
	}
	rendered := output.String()
	for _, want := range []string{
		`dev_health_execution_liveness_seconds_since_success{probe="worker_execution_liveness"} -1`,
		`dev_health_execution_liveness_probe_failures_total{probe="worker_execution_liveness",reason="begin_failed"} 1`,
		`dev_health_execution_liveness_probe_failures_total{probe="worker_execution_liveness",reason="rollback_failed"} 0`,
		`dev_health_execution_liveness_probe_failures_total{probe="worker_execution_liveness",reason="timeout"} 0`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("expected metrics output to contain %q, got:\n%s", want, rendered)
		}
	}
	// The underlying driver error text must never reach the exposition
	// surface -- see health.Registry's identical discipline for CheckFunc.
	if strings.Contains(rendered, "boom") {
		t.Fatalf("metrics output leaked the underlying error text:\n%s", rendered)
	}
}
