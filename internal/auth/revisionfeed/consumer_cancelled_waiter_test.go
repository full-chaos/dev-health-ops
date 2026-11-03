package revisionfeed

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// A waiter whose ctx ended must not take the slot it was waiting for, even when the slot is released at the same time: the
// wake and the cancellation are both ready before the waiter selects, so the cell runs the race many times.
func TestACancelledWaiterNeverTakesTheReleasedSlot(t *testing.T) {
	for i := range 64 {
		g := newGatedInvalidator(false)
		c, err := NewConsumer(Config{Invalidator: g})
		if err != nil {
			t.Fatal(err)
		}
		proceed := make(chan struct{})
		waiting := make(chan struct{}, 1)
		c.onWait = func(string) {
			waiting <- struct{}{}
			<-proceed
		}
		first := make(chan handled, 1)
		go func() {
			d, err := c.Handle(t.Context(), ev("user", "u1", 1))
			first <- handled{d, err}
		}()
		receive(t, "the first Invalidate", g.entered)
		ctx, cancel := context.WithCancel(t.Context())
		second := make(chan handled, 1)
		go func() {
			d, err := c.Handle(ctx, ev("user", "u1", 2))
			second <- handled{d, err}
		}()
		receive(t, "the second Handle to wait", waiting)
		close(g.release)
		if got := receive(t, "the first Handle", first); got.d != contracts.Apply || got.err != nil {
			t.Fatalf("run %d: first = %v, %v", i, got.d, got.err)
		}
		cancel()
		close(proceed)
		got := receive(t, "the cancelled Handle", second)
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("run %d: the cancelled waiter returned %v, %v: it took the released slot", i, got.d, got.err)
		}
		if n := g.count(); n != 1 {
			t.Fatalf("run %d: %d invalidations, want 1: the cancelled waiter invalidated", i, n)
		}
	}
}
