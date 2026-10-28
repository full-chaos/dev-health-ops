package shell

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// stuckSource is a push stop that ignores its context until released, as the SDK periodic reader does while it waits for its
// export loop. It records the deadline of the context it was handed, and whether that context had already ended.
type stuckSource struct {
	release  chan struct{}
	entered  chan struct{}
	deadline atomic.Pointer[time.Time]
	ended    atomic.Bool
}

func (s *stuckSource) Shutdown(ctx context.Context) error {
	if deadline, ok := ctx.Deadline(); ok {
		s.deadline.Store(&deadline)
	}
	s.ended.Store(ctx.Err() != nil)
	close(s.entered)
	<-s.release
	return nil
}

// TestMetricsFlushNeverOutlivesItsBudgetEvenWhenTheSourceIgnoresTheContext holds the contract documented on
// metricsFlush: a flush that cannot finish is logged and bounded, never a shutdown failure. A source that
// blocks past the context used to hold Shutdown until the source returned, past the lifecycle attempt
// budget, which the runtime reports as a failed shutdown (CHAOS-9934).
func TestMetricsFlushNeverOutlivesItsBudgetEvenWhenTheSourceIgnoresTheContext(t *testing.T) {
	var logs bytes.Buffer
	source := &stuckSource{release: make(chan struct{}), entered: make(chan struct{})}
	defer close(source.release)
	flush := metricsFlush{source: source, logger: slog.New(slog.NewTextHandler(&logs, nil))}

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	callerDeadline, _ := ctx.Deadline()
	handedAt := time.Now()
	result := make(chan error, 1)
	go func() { result <- flush.Shutdown(ctx) }()
	// The source returns only when released, after this test: Shutdown coming back at all means it stopped waiting for the source.
	// The wait below is a guard for the failure path only; a passing run never reaches it.
	var err error
	select {
	case err = <-result:
	case <-time.After(30 * time.Second):
		t.Fatal("Shutdown is still waiting for a source that ignores its context: the flush is not bounded")
	}
	if err != nil {
		t.Fatalf("Shutdown error = %v, want nil: an unreachable collector is not a shutdown failure", err)
	}
	// The flush budget is half of the attempt. The deadline the source was handed must fall in the first three quarters of the
	// caller's budget, so the return comes before the attempt's own deadline and the runtime never sees DeadlineExceeded. The check
	// reads deadlines, not elapsed time.
	select {
	case <-source.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the source was never handed a context")
	}
	flushDeadline := source.deadline.Load()
	if flushDeadline == nil {
		t.Fatal("the source was handed a context with no deadline")
	}
	if source.ended.Load() {
		t.Fatal("the source was handed a context that had already ended")
	}
	if flushDeadline.After(handedAt.Add(callerDeadline.Sub(handedAt) * 3 / 4)) {
		t.Fatalf("the flush deadline %v is past three quarters of the caller's budget (caller deadline %v): the flush spends almost the whole attempt", *flushDeadline, callerDeadline)
	}
	if !strings.Contains(logs.String(), "final flush abandoned") {
		t.Fatalf("the abandoned flush was not logged: %q", logs.String())
	}
}
