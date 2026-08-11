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

// scriptedStepper keeps failing until it is explicitly repaired, modelling a
// window that is broken by configuration and then fixed in place. The failure
// is persistent on purpose: a self-decrementing counter would heal on its own,
// which would let a recovery test pass without the repair step doing anything.
type scriptedStepper struct {
	mu        sync.Mutex
	failing   bool
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
	if stepper.failing {
		result.Schedules[0].Err = errors.New("window failed")
	}
	return result, nil
}

func (stepper *scriptedStepper) Schedules() []Schedule { return stepper.schedules }

func (stepper *scriptedStepper) repair() {
	stepper.mu.Lock()
	defer stepper.mu.Unlock()
	stepper.failing = false
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
	stepper := &scriptedStepper{failing: true, schedules: []Schedule{schedule}}
	loop, clock := newFixedTestLoop(t, stepper)

	if err := loop.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v; a failed first window must not prevent starting", err)
	}
	defer func() { _ = loop.Shutdown(context.Background()) }()

	waitFor(t, "the first window to run", func() bool { return stepper.calls.Load() >= 1 })
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
	waitFor(t, "readiness to open", func() bool {
		return loop.Readiness(context.Background()) == nil
	})
}

func TestShutdownClosesReadiness(t *testing.T) {
	schedule := heartbeatSchedule(t)
	stepper := &blockingStepper{
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
		schedules: []Schedule{schedule},
	}
	loop, _ := newFixedTestLoop(t, stepper)
	if err := loop.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stepper.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first window never began")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- loop.Shutdown(context.Background()) }()
	waitFor(t, "shutdown to mark the loop stopping", func() bool {
		loop.mu.Lock()
		defer loop.mu.Unlock()
		return loop.stopping
	})
	close(stepper.release)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown() = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown() did not wait for the in-flight window")
	}
	if err := loop.Readiness(context.Background()); err == nil {
		t.Fatal("readiness stayed open after shutdown")
	}
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// blockingStepper parks inside its window until released.
type blockingStepper struct {
	entered   chan struct{}
	release   chan struct{}
	once      sync.Once
	schedules []Schedule
}

func (stepper *blockingStepper) Step(context.Context, time.Time) (WindowResult, error) {
	stepper.once.Do(func() { close(stepper.entered) })
	<-stepper.release
	return WindowResult{Schedules: []ScheduleResult{{ScheduleID: stepper.schedules[0].ID}}}, nil
}

func (stepper *blockingStepper) Schedules() []Schedule { return stepper.schedules }

// Start must not block for the duration of the first window. A window may run
// for up to the configured step timeout, and holding the caller that long
// delays every component sequenced after the scheduler for a result nobody
// waits on.
//
// The release channel is closed only after Start has returned, so if Start
// waited for the window this deadlocks — the timeout below turns that into a
// clean failure rather than a hang.
func TestStartDoesNotBlockOnTheDurationOfTheFirstWindow(t *testing.T) {
	schedule := heartbeatSchedule(t)
	stepper := &blockingStepper{
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
		schedules: []Schedule{schedule},
	}
	loop, _ := newFixedTestLoop(t, stepper)

	started := make(chan error, 1)
	go func() { started <- loop.Start(context.Background()) }()

	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("Start() = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start blocked on the first window instead of returning promptly")
	}

	// The window is genuinely in flight: it began, and it has not been allowed
	// to finish, yet Start already returned.
	select {
	case <-stepper.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first window never began")
	}

	close(stepper.release)
	waitFor(t, "readiness to open once the window completes", func() bool {
		return loop.Readiness(context.Background()) == nil
	})
	if err := loop.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
}
