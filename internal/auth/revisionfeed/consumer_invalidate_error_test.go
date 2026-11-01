package revisionfeed

import (
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// acp condition on 10983 PR A r2 (CHAOS-10842): an Invalidator that fails must not be silent, and Handle invalidates BEFORE
// it advances, so a failed invalidation leaves the cursor and the mark where they were and the same event applies again.

func TestAnInvalidationFailureIsReturnedAndLeavesTheCursorAndTheMark(t *testing.T) {
	c, rec := newTestConsumer(t)
	ctx := t.Context()
	rec.failNext(1)
	got, err := c.Handle(ctx, ev("principal", "p1", 1))
	if !errors.Is(err, errInvalidate) || got != contracts.Apply {
		t.Fatalf("Handle = %v, %v, want Apply and the invalidator's error", got, err)
	}
	if n, ok := c.Watermarks().Latest("principal", "p1"); ok || n != 0 {
		t.Fatalf("the mark moved to %d after a failed invalidation", n)
	}
	got, err = c.Handle(ctx, ev("principal", "p1", 1))
	if err != nil || got != contracts.Apply {
		t.Fatalf("the retry = %v, %v, want Apply and no error: the cursor must not have advanced", got, err)
	}
	if n, ok := c.Watermarks().Latest("principal", "p1"); !ok || n != 1 {
		t.Fatalf("mark after the retry = %d, %v, want 1", n, ok)
	}
	if calls := rec.snapshot(); len(calls) != 2 {
		t.Fatalf("calls = %v, want the failed call and the retry", calls)
	}
	if got, _ := c.Handle(ctx, ev("principal", "p1", 1)); got != contracts.Ignore {
		t.Fatalf("a duplicate after the success = %v, want Ignore", got)
	}
}

func TestTheInvalidationRunsBeforeTheCursorAndTheMarkAdvance(t *testing.T) {
	c, rec := newTestConsumer(t)
	var markAtInvalidate int64 = -1
	rec.onInvalidate = func() { markAtInvalidate, _ = c.Watermarks().Latest("principal", "p1") }
	if _, err := c.Handle(t.Context(), ev("principal", "p1", 1)); err != nil {
		t.Fatal(err)
	}
	if markAtInvalidate != 0 {
		t.Fatalf("the mark read %d inside Invalidate: the mark advanced before the invalidation", markAtInvalidate)
	}
	// the cursor too: a concurrent duplicate sees Apply until the invalidation succeeded
	rec.failNext(1)
	rec.onInvalidate = nil
	if _, err := c.Handle(t.Context(), ev("principal", "p2", 1)); err == nil {
		t.Fatal("a failed invalidation returned no error")
	}
	if got, err := c.Handle(t.Context(), ev("principal", "p2", 1)); got != contracts.Apply || err != nil {
		t.Fatalf("after a failure the same revision = %v, %v, want Apply", got, err)
	}
}

func TestAFailedGapInvalidationDoesNotResyncSoTheGapRepeats(t *testing.T) {
	c, rec := newTestConsumer(t)
	ctx := t.Context()
	rec.failNext(1)
	if got, err := c.Handle(ctx, ev("organization", "o1", 3)); got != contracts.Gap || !errors.Is(err, errInvalidate) {
		t.Fatalf("Handle = %v, %v, want Gap and the error", got, err)
	}
	if _, ok := c.Watermarks().Latest("organization", "o1"); ok {
		t.Fatal("the mark moved on a failed gap invalidation")
	}
	if got, err := c.Handle(ctx, ev("organization", "o1", 3)); got != contracts.Gap || err != nil {
		t.Fatalf("the retry = %v, %v, want Gap and success", got, err)
	}
	if got, _ := c.Handle(ctx, ev("organization", "o1", 3)); got != contracts.Ignore {
		t.Fatalf("after the successful gap invalidation a repeat = %v, want Ignore", got)
	}
}

func TestResubscribedAndCatchUpReturnTheInvalidationError(t *testing.T) {
	c, rec := newTestConsumer(t)
	ctx := t.Context()
	rec.failNext(1)
	if err := c.Resubscribed(ctx); !errors.Is(err, errInvalidate) {
		t.Fatalf("Resubscribed = %v, want the invalidator's error", err)
	}
	if err := c.Resubscribed(ctx); err != nil {
		t.Fatalf("Resubscribed retry = %v", err)
	}
	rec.failNext(1)
	err := c.CatchUp(ctx, map[string]int64{"user:a": 2, "user:b": 5})
	if !errors.Is(err, errInvalidate) {
		t.Fatalf("CatchUp = %v, want the invalidator's error", err)
	}
	// one of the two floors moved, the failed one did not: the retry moves it and touches nothing else
	if err := c.CatchUp(ctx, map[string]int64{"user:a": 2, "user:b": 5}); err != nil {
		t.Fatalf("CatchUp retry = %v", err)
	}
	for id, want := range map[string]int64{"a": 2, "b": 5} {
		if n, _ := c.Watermarks().Latest("user", id); n != want {
			t.Fatalf("mark of user:%s = %d, want %d", id, n, want)
		}
	}
}

func countCalls(rig *subscriberRig, all bool) int {
	rig.rec.mu.Lock()
	defer rig.rec.mu.Unlock()
	n := 0
	for _, call := range rig.rec.calls {
		if call.all == all {
			n++
		}
	}
	return n
}

func TestTheSubscriberRetriesALiveEventWhoseInvalidationFailed(t *testing.T) {
	rig := startSubscriber(t)
	s := rig.client.next(t)
	s.hooks.OnSubscribed()
	waitFor(t, "the first catch-up", func() bool { return rig.client.hgetalls() >= 1 })
	rig.rec.failNext(1)
	rig.client.setHGetAll(map[string]string{"user:u1": "1"}, nil)
	s.hooks.OnMessage("user:u1:1")
	waitFor(t, "the mark after the retry", func() bool { return latest(rig, "user", "u1") == 1 })
	if n := countCalls(rig, false); n < 2 {
		t.Fatalf("%d invalidations of user:u1, want the failed one and a retry", n)
	}
}

func TestTheSubscriberRetriesAFailedResubscribeInvalidateAll(t *testing.T) {
	rig := startSubscriber(t)
	s := rig.client.next(t)
	rig.rec.failNext(1)
	s.hooks.OnSubscribed()
	waitFor(t, "a second InvalidateAll", func() bool { return countCalls(rig, true) >= 2 })
	waitFor(t, "the subscription gauge", func() bool { return gaugeValue(t, rig.reader) == 1 })
}

func TestAFailedInvalidationIsCountedAsInvalidateFailedAndNotAsApplied(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	rec := &recorder{}
	c, err := NewConsumer(Config{Invalidator: rec, Meter: provider.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	rec.failNext(2)
	c.Handle(ctx, ev("principal", "p1", 1))
	c.Handle(ctx, ev("organization", "o1", 4))
	for _, tc := range []struct {
		name, key, value string
		want             int64
	}{
		{"devhealth_auth_revision_feed_events_total", "disposition", "invalidate_failed", 2},
		{"devhealth_auth_revision_feed_events_total", "disposition", "apply", 0},
		{"devhealth_auth_revision_feed_events_total", "disposition", "gap", 0},
		{"devhealth_auth_revision_feed_resync_total", "cause", "gap", 0},
	} {
		if got := counterValue(t, reader, tc.name, tc.key, tc.value); got != tc.want {
			t.Errorf("after two failures %s{%s=%s} = %d, want %d", tc.name, tc.key, tc.value, got, tc.want)
		}
	}
	c.Handle(ctx, ev("principal", "p1", 1))
	if got := counterValue(t, reader, "devhealth_auth_revision_feed_events_total", "disposition", "apply"); got != 1 {
		t.Errorf("apply after the retry = %d, want 1", got)
	}
}

// A retry request raised on a subscription that already ended must not start a catch-up on the next one before that one
// is confirmed: the read would come before the subscribe, and the gauge would read 1 with no subscription.
func TestARetryRequestFromAnEndedSubscriptionWaitsForTheNextConfirmedSubscribe(t *testing.T) {
	rig := startSubscriber(t)
	first := rig.client.next(t)
	first.hooks.OnSubscribed()
	waitFor(t, "gauge 1", func() bool { return gaugeValue(t, rig.reader) == 1 })
	first.done <- errors.New("connection reset")
	second := rig.client.next(t)
	reads := rig.client.hgetalls()
	rig.rec.failNext(1)
	first.hooks.OnMessage("user:u1:1") // a late delivery on the ended subscription; its invalidation fails
	time.Sleep(50 * time.Millisecond)
	if n := rig.client.hgetalls(); n != reads {
		t.Fatalf("%d HGETALL before the second subscribe was confirmed", n-reads)
	}
	if got := gaugeValue(t, rig.reader); got != 0 {
		t.Fatalf("gauge %d before the second subscribe was confirmed, want 0", got)
	}
	second.hooks.OnSubscribed()
	waitFor(t, "gauge 1 after the second subscribe", func() bool { return gaugeValue(t, rig.reader) == 1 })
}
