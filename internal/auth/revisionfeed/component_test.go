package revisionfeed

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Codex r3 P1: Shutdown cleared c.done before the goroutine of Start ran its deferred close, so an immediate
// Start -> Shutdown closed a nil channel (a deterministic panic with one P). Looped under GOMAXPROCS(1).
func TestStartThenShutdownAtOnceDoesNotPanicUnderOneP(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	var closed atomic.Int64
	for i := 0; i < 200; i++ {
		rig := startedlessSubscriber(t)
		component := NewComponent(rig, func() { closed.Add(1) })
		if err := component.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := component.Shutdown(ctx); err != nil {
			cancel()
			t.Fatalf("shutdown %d: %v", i, err)
		}
		cancel()
	}
	if closed.Load() != 200 {
		t.Fatalf("the client was closed %d times, want 200", closed.Load())
	}
}

func TestShutdownTwiceAndShutdownWithoutStartAreSafe(t *testing.T) {
	component := NewComponent(startedlessSubscriber(t), nil)
	ctx := context.Background()
	if err := component.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := component.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := component.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := component.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownWaitsForTheSubscriberToStop(t *testing.T) {
	component := NewComponent(startedlessSubscriber(t), nil)
	if err := component.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := component.Start(context.Background()); err == nil {
		t.Fatal("a second Start was accepted")
	}
	if err := component.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func startedlessSubscriber(t *testing.T) *Subscriber {
	t.Helper()
	consumer, err := NewConsumer(Config{Invalidator: &recorder{}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := NewSubscriber(SubscriberConfig{Client: &fakeClient{}, Consumer: consumer, Backoff: func(int) time.Duration { return time.Millisecond }})
	if err != nil {
		t.Fatal(err)
	}
	return sub
}

// A Shutdown whose context ends before the subscriber stopped reports it, keeps the component stoppable, and a later
// Shutdown waits for the stop and closes the client exactly once.
func TestAShutdownThatTimesOutCanBeRetriedAndClosesTheClientOnce(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	client := &fakeClient{subscribe: func(ctx context.Context, _ string, _ SubscribeHooks) error {
		once.Do(func() { close(entered) })
		<-release
		return ctx.Err()
	}}
	consumer, err := NewConsumer(Config{Invalidator: &recorder{}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := NewSubscriber(SubscriberConfig{Client: client, Consumer: consumer, Backoff: func(int) time.Duration { return time.Millisecond }})
	if err != nil {
		t.Fatal(err)
	}
	var closed atomic.Int64
	component := NewComponent(sub, func() { closed.Add(1) })
	if err := component.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-entered
	short, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := component.Shutdown(short); err == nil {
		t.Fatal("a Shutdown that ran out of time reported success")
	}
	if closed.Load() != 0 {
		t.Fatal("the client was closed before the subscriber stopped")
	}
	close(release)
	if err := component.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatalf("the client was closed %d times, want 1", closed.Load())
	}
}
