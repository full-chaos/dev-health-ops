package shell

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// stuckSource is a push stop that ignores its context until released, as the SDK periodic reader does while it waits for its
// export loop. It records when it was entered, read from its own clock the moment it starts, and the deadline of the context it was
// handed.
type stuckSource struct {
	release  chan struct{}
	entered  chan struct{}
	entry    atomic.Pointer[time.Time]
	deadline atomic.Pointer[time.Time]
}

func (s *stuckSource) Shutdown(ctx context.Context) error {
	entry := time.Now()
	s.entry.Store(&entry)
	if deadline, ok := ctx.Deadline(); ok {
		s.deadline.Store(&deadline)
	}
	close(s.entered)
	<-s.release
	return nil
}

// TestMetricsFlushNeverOutlivesItsBudgetEvenWhenTheSourceIgnoresTheContext holds the contract documented on
// metricsFlush: a flush that cannot finish is logged and bounded, never a shutdown failure. A source that
// blocks past the context used to hold Shutdown until the source returned, past the lifecycle attempt
// budget, which the runtime reports as a failed shutdown (CHAOS-9934).
func TestMetricsFlushNeverOutlivesItsBudgetEvenWhenTheSourceIgnoresTheContext(t *testing.T) {
	// The bubble runs on a virtual clock, so the caller's 400 ms budget does not shrink when the host stalls the test. The flush path does
	// no real I/O here: the source is an in-memory fake that blocks on a channel, and the logger writes to a buffer.
	synctest.Test(t, func(t *testing.T) {
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
		// The flush budget is half of the attempt. Measured from the moment the source was entered, the deadline it was handed must fall
		// in the first three quarters of what was left of the caller's budget, so the return comes before the attempt's own deadline
		// and the runtime never sees DeadlineExceeded. The check reads deadlines and the source's own entry time, not elapsed time.
		select {
		case <-source.entered:
		case <-time.After(30 * time.Second):
			t.Fatal("the source was never handed a context")
		}
		flushDeadline := source.deadline.Load()
		if flushDeadline == nil {
			t.Fatal("the source was handed a context with no deadline")
		}
		entry := *source.entry.Load()
		if !flushDeadline.After(handedAt) {
			t.Fatalf("the flush deadline %v is not after the call started (%v): the source was handed no budget", *flushDeadline, handedAt)
		}
		if flushDeadline.After(entry.Add(callerDeadline.Sub(entry) * 3 / 4)) {
			t.Fatalf("the flush deadline %v is past three quarters of the caller's budget left at entry %v (caller deadline %v): the flush spends almost the whole attempt", *flushDeadline, entry, callerDeadline)
		}
		if !strings.Contains(logs.String(), "final flush abandoned") {
			t.Fatalf("the abandoned flush was not logged: %q", logs.String())
		}
	})
}
