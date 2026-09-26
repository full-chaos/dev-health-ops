package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// gatedRun is a posture run that blocks until released, so a test can hold the
// "expensive query" open and prove what a probe does meanwhile.
type gatedRun struct {
	calls   atomic.Int32
	release chan struct{}
	result  atomic.Pointer[error]
}

func newGatedRun() *gatedRun { return &gatedRun{release: make(chan struct{})} }

func (g *gatedRun) run(context.Context) error {
	g.calls.Add(1)
	<-g.release
	if err := g.result.Load(); err != nil {
		return *err
	}
	return nil
}

func (g *gatedRun) finishWith(err error) {
	g.result.Store(&err)
	close(g.release)
}

const noWaitBudget = 100 * time.Millisecond

func mustReturnQuickly(t *testing.T, what string, check func() error) error {
	t.Helper()
	started := time.Now()
	err := check()
	if elapsed := time.Since(started); elapsed > noWaitBudget {
		t.Fatalf("%s took %v (> %v): CheckNoWait waited on the query", what, elapsed, noWaitBudget)
	}
	return err
}

// Never proven: fail closed at once, one background run however many probes
// come, and a pass once that run answers.
func TestCheckNoWaitFailsClosedUntilTheFirstRunAnswers(t *testing.T) {
	t.Parallel()
	gate := newGatedRun()
	check, _ := newTestCachedPostureCheck(gate.run, PostureCheckOptions{})
	for probe := 1; probe <= 5; probe++ {
		err := mustReturnQuickly(t, fmt.Sprintf("probe %d", probe), check.CheckNoWait)
		if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "not yet proven") {
			t.Fatalf("probe %d: %v, want ErrUnavailable naming 'not yet proven'", probe, err)
		}
	}
	waitFor(t, "the one background run to start", func() bool { return gate.calls.Load() >= 1 })
	time.Sleep(50 * time.Millisecond) // a second run, if any, would have started by now
	if calls := gate.calls.Load(); calls != 1 {
		t.Fatalf("%d runs for 5 probes, want exactly 1 (single-flight)", calls)
	}
	gate.finishWith(nil)
	waitFor(t, "the first run to be recorded", func() bool { return check.CheckNoWait() == nil })
}

// Warm starts the run before any probe, once.
func TestWarmStartsTheFirstRunWithoutAProbe(t *testing.T) {
	t.Parallel()
	gate := newGatedRun()
	check, _ := newTestCachedPostureCheck(gate.run, PostureCheckOptions{})
	check.Warm()
	check.Warm()
	waitFor(t, "the warm run to start", func() bool { return gate.calls.Load() == 1 })
	gate.finishWith(nil)
	waitFor(t, "the warm run to answer", func() bool { return check.CheckNoWait() == nil })
	time.Sleep(50 * time.Millisecond)
	if calls := gate.calls.Load(); calls != 1 {
		t.Fatalf("%d runs, want 1", calls)
	}
}

// A pass older than TTL but younger than MaxStale is still a pass, returned at
// once while the refresh is held open.
func TestCheckNoWaitServesAStalePassWhileTheRefreshIsSlow(t *testing.T) {
	t.Parallel()
	first := newGatedRun()
	first.finishWith(nil)
	var gate atomic.Pointer[gatedRun]
	gate.Store(first)
	check, clock := newTestCachedPostureCheck(func(ctx context.Context) error { return gate.Load().run(ctx) }, PostureCheckOptions{})
	check.Warm()
	waitFor(t, "the first pass", func() bool { return check.CheckNoWait() == nil })

	refresh := newGatedRun()
	gate.Store(refresh)
	clock.Advance(defaultPostureTTL + time.Second)
	if err := mustReturnQuickly(t, "stale-pass probe", check.CheckNoWait); err != nil {
		t.Fatalf("a pass inside MaxStale must be served: %v", err)
	}
	waitFor(t, "the refresh to start", func() bool { return refresh.calls.Load() == 1 })
	for i := 0; i < 3; i++ {
		if err := mustReturnQuickly(t, "probe during refresh", check.CheckNoWait); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if calls := refresh.calls.Load(); calls != 1 {
		t.Fatalf("%d refresh runs, want 1", calls)
	}
	refresh.finishWith(nil)
}

// Past MaxStale with no answer since: fail closed at once, naming the age. The
// absence of an answer is never read as a pass.
func TestCheckNoWaitFailsClosedPastMaxStale(t *testing.T) {
	t.Parallel()
	first := newGatedRun()
	first.finishWith(nil)
	var gate atomic.Pointer[gatedRun]
	gate.Store(first)
	check, clock := newTestCachedPostureCheck(func(ctx context.Context) error { return gate.Load().run(ctx) }, PostureCheckOptions{})
	check.Warm()
	waitFor(t, "the first pass", func() bool { return check.CheckNoWait() == nil })

	slow := newGatedRun()
	gate.Store(slow)
	clock.Advance(defaultPostureMaxStale + time.Minute)
	err := mustReturnQuickly(t, "past-MaxStale probe", check.CheckNoWait)
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "last proven") {
		t.Fatalf("past MaxStale: %v, want ErrUnavailable naming the last proof's age", err)
	}
	slow.finishWith(nil)
	waitFor(t, "the run to heal it", func() bool { return check.CheckNoWait() == nil })
}

// A refusal is served with its age without a probe waiting, is re-run only
// after RefusalTTL, and heals when the re-run passes.
func TestCheckNoWaitServesARefusalAndHealsOnTheRerun(t *testing.T) {
	t.Parallel()
	refusal := fmt.Errorf("%w: saved_reports: missing [INSERT]", ErrPostureRefused)
	first := newGatedRun()
	first.finishWith(refusal)
	var gate atomic.Pointer[gatedRun]
	gate.Store(first)
	check, clock := newTestCachedPostureCheck(func(ctx context.Context) error { return gate.Load().run(ctx) }, PostureCheckOptions{})
	check.Warm()
	waitFor(t, "the refusal", func() bool { return errors.Is(check.CheckNoWait(), ErrPostureRefused) })
	callsAfterFirst := first.calls.Load()

	// Inside RefusalTTL: served, no new run.
	for i := 0; i < 3; i++ {
		err := mustReturnQuickly(t, "refusal probe", check.CheckNoWait)
		if !errors.Is(err, ErrPostureRefused) || !strings.Contains(err.Error(), "saved_reports") || !strings.Contains(err.Error(), "ago") {
			t.Fatalf("refusal probe: %v, want the refusal naming the privilege and its age", err)
		}
	}
	if first.calls.Load() != callsAfterFirst {
		t.Fatal("a refusal younger than RefusalTTL started another run")
	}

	// Past RefusalTTL: the refusal is STILL served while the re-run is held open.
	rerun := newGatedRun()
	gate.Store(rerun)
	clock.Advance(defaultPostureRefusalTTL + time.Second)
	if err := mustReturnQuickly(t, "post-TTL refusal probe", check.CheckNoWait); !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("the refusal must be served while its re-run is in flight: %v", err)
	}
	waitFor(t, "the re-run to start", func() bool { return rerun.calls.Load() == 1 })
	rerun.finishWith(nil)
	waitFor(t, "the fix to be picked up", func() bool { return check.CheckNoWait() == nil })
}

// A fresh pass is served from memory: probes inside TTL start no run at all.
func TestCheckNoWaitFreshPassStartsNoRun(t *testing.T) {
	t.Parallel()
	gate := newGatedRun()
	gate.finishWith(nil)
	check, _ := newTestCachedPostureCheck(gate.run, PostureCheckOptions{})
	check.Warm()
	waitFor(t, "the first pass", func() bool { return check.CheckNoWait() == nil })
	waitFor(t, "the first run to be counted", func() bool { return gate.calls.Load() >= 1 })
	before := gate.calls.Load()
	for i := 0; i < 20; i++ {
		if err := check.CheckNoWait(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if after := gate.calls.Load(); after != before {
		t.Fatalf("fresh-pass probes started %d runs, want none", after-before)
	}
}
