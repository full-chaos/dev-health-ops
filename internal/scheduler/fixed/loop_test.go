package fixed

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type fixedTestTicker struct {
	ticks chan time.Time
	stop  sync.Once
	done  chan struct{}
}

func (ticker *fixedTestTicker) Chan() <-chan time.Time { return ticker.ticks }

func (ticker *fixedTestTicker) Stop() {
	ticker.stop.Do(func() { close(ticker.done) })
}

type fixedTestClock struct {
	mu     sync.Mutex
	now    time.Time
	ticker *fixedTestTicker
}

func (clock *fixedTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fixedTestClock) advance(delta time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(delta)
}

func (clock *fixedTestClock) NewTicker(time.Duration) loopTicker {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.ticker = &fixedTestTicker{
		ticks: make(chan time.Time, 8),
		done:  make(chan struct{}),
	}
	return clock.ticker
}

func (clock *fixedTestClock) tick(at time.Time) {
	clock.mu.Lock()
	ticker := clock.ticker
	clock.mu.Unlock()
	ticker.ticks <- at
}

// scriptedStepper fails until its failures counter is exhausted, modelling a
// window that is broken and then repaired without restarting the process.
type scriptedStepper struct {
	mu        sync.Mutex
	failures  int
	calls     atomic.Int64
	schedules []Schedule
}

func (stepper *scriptedStepper) Step(_ context.Context, observedAt time.Time) (WindowResult, error) {
	stepper.calls.Add(1)
	stepper.mu.Lock()
	defer stepper.mu.Unlock()
	result := WindowResult{
		ObservedAt: observedAt,
		Schedules: []ScheduleResult{{
			ScheduleID: stepper.schedules[0].ID,
		}},
	}
	if stepper.failures > 0 {
		stepper.failures--
		result.Schedules[0].Err = errors.New("window failed")
	}
	return result, nil
}

func (stepper *scriptedStepper) Schedules() []Schedule { return stepper.schedules }

func (stepper *scriptedStepper) repair() {
	stepper.mu.Lock()
	defer stepper.mu.Unlock()
	stepper.failures = 0
}

func newFixedTestLoop(t *testing.T, stepper Stepper) (*Loop, *fixedTestClock) {
	t.Helper()
	clock := &fixedTestClock{now: mustTime(t, "2026-07-24T00:00:00Z")}
	loop, err := newLoop(stepper, LoopConfig{
		PollInterval: minLoopPollInterval,
		StepTimeout:  time.Second,
		MaxBackoff:   2 * minLoopPollInterval,
		Registry:     health.NewRegistry(time.Second),
	}, clock)
	if err != nil {
		t.Fatalf("newLoop() = %v", err)
	}
	return loop, clock
}

// A window that fails on the very first attempt must not make the loop
// permanently dead. Startup previously treated the initial window as a gate on
// starting at all, so an operator-correctable failure — a bad retention
// override on a due occurrence, say — could only be recovered by restarting the
// process, even though the steady-state path is explicitly built to retry.
func TestInitialWindowFailureStillStartsTheLoopAndSelfHeals(t *testing.T) {
	schedule := heartbeatSchedule(t)
	stepper := &scriptedStepper{failures: 1, schedules: []Schedule{schedule}}
	loop, clock := newFixedTestLoop(t, stepper)

	if err := loop.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v; a failed first window must not prevent starting", err)
	}
	defer func() { _ = loop.Shutdown(context.Background()) }()

	if stepper.calls.Load() != 1 {
		t.Fatalf("initial window ran %d times", stepper.calls.Load())
	}
	if err := loop.Readiness(context.Background()); err == nil {
		t.Fatal("readiness opened despite a failed first window")
	}

	// The failure is repaired in-process, exactly as correcting an environment
	// variable would be. A later tick must recover with no restart.
	stepper.repair()
	clock.advance(4 * minLoopPollInterval)
	clock.tick(clock.Now())

	deadline := time.After(2 * time.Second)
	for {
		if err := loop.Readiness(context.Background()); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("loop never recovered after the failure cleared (%d windows ran)",
				stepper.calls.Load())
		case <-time.After(2 * time.Millisecond):
			clock.tick(clock.Now())
		}
	}
	if stepper.calls.Load() < 2 {
		t.Fatalf("recovery observed after only %d windows", stepper.calls.Load())
	}
}

// A healthy first window opens readiness immediately, unchanged.
func TestHealthyInitialWindowOpensReadiness(t *testing.T) {
	schedule := heartbeatSchedule(t)
	stepper := &scriptedStepper{schedules: []Schedule{schedule}}
	loop, _ := newFixedTestLoop(t, stepper)

	if err := loop.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v", err)
	}
	defer func() { _ = loop.Shutdown(context.Background()) }()
	if err := loop.Readiness(context.Background()); err != nil {
		t.Fatalf("readiness stayed closed after a healthy first window: %v", err)
	}
}

// Shutdown must close readiness regardless of how the loop was faring.
func TestShutdownClosesReadiness(t *testing.T) {
	schedule := heartbeatSchedule(t)
	stepper := &scriptedStepper{schedules: []Schedule{schedule}}
	loop, _ := newFixedTestLoop(t, stepper)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
	if err := loop.Readiness(context.Background()); err == nil {
		t.Fatal("readiness stayed open after shutdown")
	}
}
