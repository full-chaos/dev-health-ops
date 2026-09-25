package busyprobe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
)

type fakeTx struct{}

func (fakeTx) Rollback(context.Context) error { return nil }

type fakeInner struct{ err error }

func (f fakeInner) Begin(context.Context) (selfprobe.Tx, error) {
	if f.err != nil {
		return nil, f.err
	}
	return fakeTx{}, nil
}

// The tolerance is exactly busy-not-broken: each row plants one way the probe
// could be wrongly tolerated or wrongly refused.
func TestOpenerToleratesExactlyBusyNotBroken(t *testing.T) {
	deadline := &selfprobe.AcquireError{Err: fmt.Errorf("acquire: %w", context.DeadlineExceeded)}
	beginStalled := fmt.Errorf("begin: %w", context.DeadlineExceeded) // a deadline AFTER a connection was acquired
	refused := errors.New("connection refused")
	progressing := func(context.Context) error { return nil }
	wedged := func(context.Context) error { return errors.New("no progress") }
	for _, tc := range []struct {
		name      string
		beginErr  error
		saturated bool
		progress  func(context.Context) error
		wantPass  bool
		wantBusy  bool
	}{
		{"healthy begin", nil, false, progressing, true, false},
		{"acquire deadline, saturated, progressing = busy", deadline, true, progressing, true, true},
		{"acquire deadline, NOT saturated = broken", deadline, false, progressing, false, false},
		{"acquire deadline, saturated, progress red = broken", deadline, true, wedged, false, false},
		{"connection error while saturated = broken", refused, true, progressing, false, false},
		{"deadline on the BEGIN itself (connection acquired) = broken", beginStalled, true, progressing, false, false},
		{"acquire error that is not a deadline = broken", &selfprobe.AcquireError{Err: refused}, true, progressing, false, false},
		{"no progress guard = broken", deadline, true, nil, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			counter := NewCounter("probe_busy_total", []string{"check"}, nil)
			opener := Opener{
				Inner: fakeInner{tc.beginErr}, Check: "check",
				Saturated: func() bool { return tc.saturated }, Progress: tc.progress, Counter: counter,
			}
			tx, err := opener.Begin(context.Background())
			if (err == nil) != tc.wantPass {
				t.Fatalf("Begin = %v, want pass=%v", err, tc.wantPass)
			}
			if err == nil {
				if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
					t.Fatal(rollbackErr)
				}
			}
			var metrics bytes.Buffer
			_ = counter.WritePrometheus(&metrics)
			want := `probe_busy_total{check="check"} 0`
			if tc.wantBusy {
				want = `probe_busy_total{check="check"} 1`
			}
			if !strings.Contains(metrics.String(), want) {
				t.Fatalf("metrics %q lack %q", metrics.String(), want)
			}
		})
	}
}

// A caller whose own deadline has passed gets no busy pass: it already gave up.
func TestOpenerRefusesABusyPassAfterTheCallersDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opener := Opener{
		Inner:     fakeInner{&selfprobe.AcquireError{Err: context.DeadlineExceeded}},
		Saturated: func() bool { return true }, Progress: func(context.Context) error { return nil },
		Counter: NewCounter("m", nil, nil),
	}
	if _, err := opener.Begin(ctx); err == nil {
		t.Fatal("a busy pass was returned to a caller whose context had ended")
	}
}

func TestAcquireContextLeavesTimeInsideTheCallersDeadline(t *testing.T) {
	for _, tc := range []struct {
		name      string
		budget    time.Duration
		saturated bool
		wantMax   time.Duration
		wantMin   time.Duration
	}{
		{"production budget, unsaturated: reserve the guard time", 10 * time.Second, false, 8 * time.Second, 7 * time.Second},
		{"production budget, saturated: wait one second", 10 * time.Second, true, AcquireWait, AcquireWait - 200*time.Millisecond},
		{"short budget: reserve capped at half", 2 * time.Second, false, time.Second, 800 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, cancel := context.WithTimeout(context.Background(), tc.budget)
			defer cancel()
			ctx, cancelAcquire := acquireContext(parent, tc.saturated)
			defer cancelAcquire()
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("no deadline")
			}
			if got := time.Until(deadline); got > tc.wantMax || got < tc.wantMin {
				t.Fatalf("acquire window = %s, want in [%s, %s]", got, tc.wantMin, tc.wantMax)
			}
		})
	}
	// No caller deadline: an unsaturated acquire is unbounded (as before), a
	// saturated one waits AcquireWait.
	ctx, cancel := acquireContext(context.Background(), false)
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("an unsaturated acquire with no caller deadline gained one")
	}
	ctx2, cancel2 := acquireContext(context.Background(), true)
	defer cancel2()
	if _, ok := ctx2.Deadline(); !ok {
		t.Fatal("a saturated acquire with no caller deadline stayed unbounded")
	}
}

func TestCounterRateLimitsTheLogButCountsEveryProbe(t *testing.T) {
	var logs bytes.Buffer
	counter := NewCounter("probe_busy_total", []string{"check"}, slog.New(slog.NewJSONHandler(&logs, nil)))
	clock := time.Unix(1_700_000_000, 0)
	counter.SetClock(func() time.Time { return clock })
	for i := 0; i < 5; i++ {
		counter.Record(context.Background(), "check")
	}
	if got := strings.Count(logs.String(), "passed as busy"); got != 1 {
		t.Fatalf("%d log lines within the interval, want 1", got)
	}
	clock = clock.Add(LogInterval + time.Second)
	counter.Record(context.Background(), "check")
	if got := strings.Count(logs.String(), "passed as busy"); got != 2 {
		t.Fatalf("%d log lines after the interval, want 2", got)
	}
	var metrics bytes.Buffer
	_ = counter.WritePrometheus(&metrics)
	if !strings.Contains(metrics.String(), `probe_busy_total{check="check"} 6`) {
		t.Fatalf("metrics = %q", metrics.String())
	}
}

func TestPoolProgressIsGreenOnlyWhileAcquiresKeepCompleting(t *testing.T) {
	var count int64
	clock := time.Unix(1_700_000_000, 0)
	progress := newPoolProgress(func() int64 { return count }, time.Minute, func() time.Time { return clock })
	if err := progress.Ready(context.Background()); err != nil {
		t.Fatalf("a new process gets a full window: %v", err)
	}
	clock = clock.Add(59 * time.Second)
	if err := progress.Ready(context.Background()); err != nil {
		t.Fatalf("inside the window with no acquires = %v, want green", err)
	}
	clock = clock.Add(2 * time.Second) // 61 s with no completed acquire (window is 60 s, strictly inside)
	if err := progress.Ready(context.Background()); err == nil {
		t.Fatal("a pool with no completed acquire for longer than the window is green")
	}
	count++ // an acquire completes: work is moving again
	if err := progress.Ready(context.Background()); err != nil {
		t.Fatalf("after an acquire completed = %v, want green", err)
	}
	clock = clock.Add(30 * time.Second)
	if err := progress.Ready(context.Background()); err != nil {
		t.Fatalf("30 s after the last acquire = %v, want green", err)
	}
	if err := NewPoolProgress(nil, time.Minute, nil).Ready(context.Background()); err == nil {
		t.Fatal("a nil pool is green")
	}
}

func TestSaturatedIsFalseForANilOrIdlePool(t *testing.T) {
	if Saturated(nil) {
		t.Fatal("a nil pool is saturated")
	}
}

// CHAOS-6800 r2 P1 (executed by the reviewer): a work acquire made BEFORE the
// guard's first consult must not read as fresh progress at that consult, and the
// guard must go red within window + one probe interval of it.
func TestPoolProgressDatesAMovementToTheProbeThatSeesIt(t *testing.T) {
	const interval, window = 20 * time.Second, 40 * time.Second
	start := time.Unix(1_700_000_000, 0)
	clock := start
	var count int64
	progress := newPoolProgress(func() int64 { return count }, window, func() time.Time { return clock })
	count = 1 // a work acquire, after construction, before any consult
	for _, step := range []struct {
		at      time.Duration
		wantErr bool
	}{{interval, false}, {2 * interval, false}, {3 * interval, true}, {4 * interval, true}} {
		clock = start.Add(step.at)
		if err := progress.Ready(context.Background()); (err != nil) != step.wantErr {
			t.Fatalf("consult at +%s = %v, want error=%v (acquire at +0, first seen at +%s)", step.at, err, step.wantErr, interval)
		}
	}
}

// Acquires that happened before the guard existed are the baseline, not movement.
func TestPoolProgressBaselineIsTheCountAtConstruction(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	clock := start
	count := int64(500) // long-running process: a large cumulative count
	progress := newPoolProgress(func() int64 { return count }, 60*time.Second, func() time.Time { return clock })
	clock = start.Add(10 * time.Second)
	if err := progress.Ready(context.Background()); err != nil {
		t.Fatalf("inside the window after construction: %v", err)
	}
	clock = start.Add(65 * time.Second)
	if err := progress.Ready(context.Background()); err == nil {
		t.Fatal("a pre-existing cumulative count reset the progress clock at the first consult")
	}
}
