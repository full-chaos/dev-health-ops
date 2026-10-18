package audit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type scriptedStepper struct {
	mu    sync.Mutex
	calls int
	err   func(call int) error
}

func (s *scriptedStepper) Step(ctx context.Context, _ time.Time) error {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	if s.err == nil {
		return nil
	}
	return s.err(n)
}

func testLoop(t *testing.T, s stepper) (*OutboxLoop, chan time.Time, chan struct{}) {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultLoopConfig(schema, health.NewRegistry(time.Second))
	loop, err := newOutboxLoop(s, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	ticks := make(chan time.Time)
	loop.tick = func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} }
	stepped := make(chan struct{}, 16)
	loop.stepDone = func() { stepped <- struct{}{} }
	return loop, ticks, stepped
}

func awaitStep(t *testing.T, stepped chan struct{}) {
	t.Helper()
	select {
	case <-stepped:
	case <-time.After(5 * time.Second):
		t.Fatal("no step completed")
	}
}

func TestLoopReadinessOpensOnFirstSuccessAndDegradesAfterThreeFailures(t *testing.T) {
	s := &scriptedStepper{err: func(call int) error {
		if call >= 2 && call <= 4 { // calls 2,3,4 fail
			return errors.New("boom")
		}
		return nil
	}}
	loop, ticks, stepped := testLoop(t, s)
	if err := loop.readiness(context.Background()); err == nil {
		t.Fatal("ready before any step")
	}
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = loop.Shutdown(context.Background()) })

	awaitStep(t, stepped) // call 1: success
	if err := loop.readiness(context.Background()); err != nil {
		t.Fatalf("not ready after a successful step: %v", err)
	}
	for i := 2; i <= 4; i++ {
		ticks <- time.Now()
		awaitStep(t, stepped)
		ready := loop.readiness(context.Background()) == nil
		if want := i < 4; ready != want { // closed on the third consecutive failure
			t.Fatalf("after failure %d: ready=%v want %v", i-1, ready, want)
		}
	}
	ticks <- time.Now()
	awaitStep(t, stepped) // call 5: success clears the streak
	if err := loop.readiness(context.Background()); err != nil {
		t.Fatalf("one success did not reopen readiness: %v", err)
	}
}

func TestLoopShutdownStopsTheGoroutineAndRefusesASecondStart(t *testing.T) {
	loop, _, stepped := testLoop(t, &scriptedStepper{})
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	awaitStep(t, stepped)
	if err := loop.Start(context.Background()); err == nil {
		t.Error("a second Start was accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := loop.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-loop.done:
	default:
		t.Fatal("goroutine still running after Shutdown")
	}
}

func TestLoopConfigRejectsUnusableValues(t *testing.T) {
	schema, _ := authschema.NewValidatedIdentifier("auth")
	good := DefaultLoopConfig(schema, health.NewRegistry(time.Second))
	for name, mutate := range map[string]func(*LoopConfig){
		"no registry": func(c *LoopConfig) { c.Registry = nil },
		"zero poll":   func(c *LoopConfig) { c.PollInterval = 0 },
		"big limit":   func(c *LoopConfig) { c.PublishLimit = maxStepLimit + 1 },
		"no retain":   func(c *LoopConfig) { c.PublishedRetention = 0 },
		"reap limit":  func(c *LoopConfig) { c.ReapLimit = maxReapLimit + 1 },
	} {
		c := good
		c.Registry = health.NewRegistry(time.Second)
		mutate(&c)
		if _, err := newOutboxLoop(&scriptedStepper{}, c, true); !errors.Is(err, ErrInvalidLoopConfig) {
			t.Errorf("%s: err = %v, want ErrInvalidLoopConfig", name, err)
		}
	}
}

func TestPublisherBackoffDoublesAndCaps(t *testing.T) {
	p := &Publisher{config: PublisherConfig{BaseBackoff: time.Second, MaxBackoff: 10 * time.Second}}
	want := []time.Duration{1, 2, 4, 8, 10, 10}
	for i, w := range want {
		if got := p.backoff(i + 1); got != w*time.Second {
			t.Errorf("backoff(%d) = %v, want %v", i+1, got, w*time.Second)
		}
	}
}

type reasonErr struct{ reason string }

func (e reasonErr) Error() string          { return "consumer said: secret payload " + e.reason }
func (e reasonErr) DeliveryReason() string { return e.reason }

func TestFailureReasonStoresNoErrorText(t *testing.T) {
	if got := failureReason(errors.New("payload leak: eyJhbGciOi")); got != "delivery_failed" {
		t.Errorf("plain error stored %q", got)
	}
	if got := failureReason(reasonErr{"consumer_unavailable"}); got != "consumer_unavailable" {
		t.Errorf("reason = %q", got)
	}
	if got := failureReason(reasonErr{"Has Spaces & Secrets"}); got != "delivery_failed" {
		t.Errorf("an unsafe reason was stored: %q", got)
	}
}
