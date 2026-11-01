package revisionfeed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// gatedInvalidator holds its first Invalidate until release is closed; failFirst makes that first call fail.
type gatedInvalidator struct {
	mu        sync.Mutex
	calls     []call
	entered   chan call
	release   chan struct{}
	failFirst bool
}

func newGatedInvalidator(failFirst bool) *gatedInvalidator {
	return &gatedInvalidator{entered: make(chan call, 8), release: make(chan struct{}), failFirst: failFirst}
}

func (g *gatedInvalidator) Invalidate(_ context.Context, typ, id string, revision int64) error {
	c := call{typ: typ, id: id, revision: revision}
	g.mu.Lock()
	first := len(g.calls) == 0
	g.calls = append(g.calls, c)
	g.mu.Unlock()
	g.entered <- c
	if first {
		<-g.release
		if g.failFirst {
			return errInvalidate
		}
	}
	return nil
}

func (g *gatedInvalidator) InvalidateAll(context.Context) error { return nil }

func (g *gatedInvalidator) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}

type handled struct {
	d   contracts.Disposition
	err error
}

type sameKeyRig struct {
	c       *Consumer
	g       *gatedInvalidator
	reader  *metric.ManualReader
	waiting chan string
}

func newSameKeyRig(t *testing.T, failFirst bool) *sameKeyRig {
	t.Helper()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	g := newGatedInvalidator(failFirst)
	c, err := NewConsumer(Config{Invalidator: g, Meter: provider.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	r := &sameKeyRig{c: c, g: g, reader: reader, waiting: make(chan string, 8)}
	c.onWait = func(key string) { r.waiting <- key }
	return r
}

func (r *sameKeyRig) handle(ctx context.Context, e contracts.ProvisioningEvent) <-chan handled {
	out := make(chan handled, 1)
	go func() {
		d, err := r.c.Handle(ctx, e)
		out <- handled{d, err}
	}()
	return out
}

func receive[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// startFirstAndSecond starts a Handle of u1 that the invalidator holds, then a second Handle of u1, and returns once the
// second waits for the first. It fails if the second reaches Invalidate while the first is in flight.
func (r *sameKeyRig) startFirstAndSecond(t *testing.T, second context.Context) (first, next <-chan handled) {
	t.Helper()
	first = r.handle(t.Context(), ev("user", "u1", 1))
	receive(t, "the first Invalidate", r.g.entered)
	next = r.handle(second, ev("user", "u1", 1))
	select {
	case key := <-r.waiting:
		if key != contracts.AggregateKey("user", "u1") {
			t.Fatalf("the second Handle waited on %q", key)
		}
	case c := <-r.g.entered:
		t.Fatalf("a second Invalidate %+v ran while the first of the same aggregate was in flight", c)
	case <-time.After(10 * time.Second):
		t.Fatal("the second Handle neither waited nor invalidated")
	}
	return first, next
}

func TestASecondHandleOfTheSameAggregateWaitsAndIsIgnored(t *testing.T) {
	r := newSameKeyRig(t, false)
	first, next := r.startFirstAndSecond(t, t.Context())
	close(r.g.release)
	if got := receive(t, "the first Handle", first); got.d != contracts.Apply || got.err != nil {
		t.Fatalf("first = %v, %v, want Apply", got.d, got.err)
	}
	if got := receive(t, "the second Handle", next); got.d != contracts.Ignore || got.err != nil {
		t.Fatalf("second = %v, %v, want Ignore: the first advanced the cursor", got.d, got.err)
	}
	if n := r.g.count(); n != 1 {
		t.Fatalf("%d invalidations, want 1", n)
	}
	for value, want := range map[string]int64{"apply": 1, "ignore": 1} {
		if got := counterValue(t, r.reader, "devhealth_auth_revision_feed_events_total", "disposition", value); got != want {
			t.Errorf("events_total{disposition=%s} = %d, want %d", value, got, want)
		}
	}
}

func TestASecondHandleAfterAFailedInvalidationAppliesAgain(t *testing.T) {
	r := newSameKeyRig(t, true)
	first, next := r.startFirstAndSecond(t, t.Context())
	close(r.g.release)
	if got := receive(t, "the first Handle", first); got.d != contracts.Apply || !errors.Is(got.err, errInvalidate) {
		t.Fatalf("first = %v, %v, want Apply and the invalidator's error", got.d, got.err)
	}
	if got := receive(t, "the second Handle", next); got.d != contracts.Apply || got.err != nil {
		t.Fatalf("second = %v, %v, want Apply: the failed first left the cursor", got.d, got.err)
	}
	if n := r.g.count(); n != 2 {
		t.Fatalf("%d invalidations, want the failed one and the second", n)
	}
	if n, ok := r.c.Watermarks().Latest("user", "u1"); !ok || n != 1 {
		t.Fatalf("mark = %d, %v, want 1", n, ok)
	}
	if got, err := r.c.Handle(t.Context(), ev("user", "u1", 1)); got != contracts.Ignore || err != nil {
		t.Fatalf("a third delivery = %v, %v, want Ignore", got, err)
	}
}

func TestAnotherAggregateDoesNotWaitForAHeldInvalidation(t *testing.T) {
	r := newSameKeyRig(t, false)
	first := r.handle(t.Context(), ev("user", "u1", 1))
	receive(t, "the first Invalidate", r.g.entered)
	other := r.handle(t.Context(), ev("user", "u2", 1))
	if c := receive(t, "the u2 Invalidate", r.g.entered); c != (call{typ: "user", id: "u2", revision: 1}) {
		t.Fatalf("second Invalidate = %+v, want user:u2 at 1", c)
	}
	if got := receive(t, "the u2 Handle", other); got.d != contracts.Apply || got.err != nil {
		t.Fatalf("u2 = %v, %v, want Apply while u1 is held", got.d, got.err)
	}
	close(r.g.release)
	if got := receive(t, "the first Handle", first); got.d != contracts.Apply || got.err != nil {
		t.Fatalf("first = %v, %v, want Apply", got.d, got.err)
	}
}

func TestAHandleWaitingForItsAggregateEndsWithItsContext(t *testing.T) {
	r := newSameKeyRig(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	first, next := r.startFirstAndSecond(t, ctx)
	cancel()
	if got := receive(t, "the cancelled Handle", next); !errors.Is(got.err, context.Canceled) {
		t.Fatalf("cancelled second = %v, %v, want context.Canceled", got.d, got.err)
	}
	close(r.g.release)
	if got := receive(t, "the first Handle", first); got.d != contracts.Apply || got.err != nil {
		t.Fatalf("first = %v, %v, want Apply", got.d, got.err)
	}
	if n := r.g.count(); n != 1 {
		t.Fatalf("%d invalidations, want 1: the cancelled Handle must not invalidate", n)
	}
}
