package shell

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
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

// flushBoundContract is the flush bound the stop budget is built on: the 2 s tracing bound and this 1 s fit the 4 s stop
// budget (stop_bound_test.go). It is a literal on purpose: a change of metricsFlushTimeout alone turns this file red.
const flushBoundContract = time.Second

func TestFlushBoundContract(t *testing.T) {
	if metricsFlushTimeout != flushBoundContract {
		t.Fatalf("metricsFlushTimeout = %v, want the contract %v: change both on purpose, with the stop budget", metricsFlushTimeout, flushBoundContract)
	}
}

// TestMetricsFlushHandsTheSourceAContextBoundedByTheFlushTimeout pins what the shell tests that stop the whole process
// cannot see: the push stop receives the flush context, and its deadline is exactly the flush bound after the call, not
// the shutdown budget. It runs in a synctest bubble: the clock is fake, so nothing is measured and nothing can race.
func TestMetricsFlushHandsTheSourceAContextBoundedByTheFlushTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		probe := deadlineProbe{got: make(chan time.Time, 1)}
		flush := metricsFlush{source: probe, logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		start := time.Now()
		if err := flush.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown error = %v, want nil", err)
		}
		deadline := <-probe.got
		if deadline.IsZero() {
			t.Fatal("the source got a context with no deadline: the flush bound never reaches the push stop")
		}
		if !deadline.Equal(start.Add(flushBoundContract)) {
			t.Errorf("the source's deadline is %v after the call, want %v", deadline.Sub(start), flushBoundContract)
		}
	})
}

// TestMetricsFlushTakesHalfOfWhatIsLeftWhenThatIsLessThanTheFlushBound pins the half-budget rule: the components that
// stop after the flush still need their share.
func TestMetricsFlushTakesHalfOfWhatIsLeftWhenThatIsLessThanTheFlushBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		probe := deadlineProbe{got: make(chan time.Time, 1)}
		flush := metricsFlush{source: probe, logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		start := time.Now()
		if err := flush.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown error = %v, want nil", err)
		}
		deadline := <-probe.got
		if !deadline.Equal(start.Add(750 * time.Millisecond)) {
			t.Errorf("the source's deadline is %v after the call, want 750ms (half of the 1.5 s left)", deadline.Sub(start))
		}
	})
}
