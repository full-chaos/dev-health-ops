package shell

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// stuckSource is a push stop that ignores its context for hold, as the SDK periodic reader does while it
// waits for its export loop.
type stuckSource struct {
	hold    time.Duration
	release chan struct{}
}

func (s stuckSource) Shutdown(context.Context) error {
	select {
	case <-time.After(s.hold):
	case <-s.release:
	}
	return nil
}

// TestMetricsFlushNeverOutlivesItsBudgetEvenWhenTheSourceIgnoresTheContext holds the contract documented on
// metricsFlush: a flush that cannot finish is logged and bounded, never a shutdown failure. A source that
// blocks past the context used to hold Shutdown until the source returned, past the lifecycle attempt
// budget, which the runtime reports as a failed shutdown (CHAOS-9934).
func TestMetricsFlushNeverOutlivesItsBudgetEvenWhenTheSourceIgnoresTheContext(t *testing.T) {
	var logs bytes.Buffer
	source := stuckSource{hold: 3 * time.Second, release: make(chan struct{})}
	defer close(source.release)
	flush := metricsFlush{source: source, logger: slog.New(slog.NewTextHandler(&logs, nil))}

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := flush.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Shutdown error = %v, want nil: an unreachable collector is not a shutdown failure", err)
	}
	// The flush budget is half of the 400ms attempt; the return must come before the attempt's own
	// deadline so the runtime never sees DeadlineExceeded.
	if elapsed := time.Since(started); elapsed >= 380*time.Millisecond {
		t.Fatalf("Shutdown took %s, want it back inside its half-budget well before the 400ms attempt deadline", elapsed)
	}
	if !strings.Contains(logs.String(), "final flush abandoned") {
		t.Fatalf("the abandoned flush was not logged: %q", logs.String())
	}
}

// deadlineProbe records whether the context its Shutdown received carries a deadline, and which.
type deadlineProbe struct{ got chan time.Time }

func (p deadlineProbe) Shutdown(ctx context.Context) error {
	deadline, _ := ctx.Deadline()
	p.got <- deadline
	return nil
}

// flushBoundCeiling is the most the flush deadline may sit past the start of the flush: half of the stop budget, the
// other half being the tracing flush (stop_bound_test.go).
const flushBoundCeiling = stopBoundBudget / 2

// TestMetricsFlushHandsTheSourceAContextBoundedByTheFlushTimeout pins what the shell tests that stop the whole process
// cannot see: the push stop receives the flush context (a deadline at all), and that deadline is the flush bound, not
// the shutdown budget. No wall time is measured: the deadline the source saw is compared with the instant before the call.
func TestMetricsFlushHandsTheSourceAContextBoundedByTheFlushTimeout(t *testing.T) {
	probe := deadlineProbe{got: make(chan time.Time, 1)}
	flush := metricsFlush{source: probe, logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	before := time.Now()
	if err := flush.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown error = %v, want nil", err)
	}
	var deadline time.Time
	select {
	case deadline = <-probe.got:
	case <-time.After(30 * time.Second):
		t.Fatal("the source never saw the flush")
	}
	if deadline.IsZero() {
		t.Fatal("the source got a context with no deadline: the flush bound never reaches the push stop")
	}
	if span := deadline.Sub(before); span > flushBoundCeiling {
		t.Errorf("the source's deadline is %v after the call, want <= %v (metricsFlushTimeout %v)", span, flushBoundCeiling, metricsFlushTimeout)
	}
}
