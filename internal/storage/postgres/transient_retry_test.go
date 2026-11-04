package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDialRetryJitterStaysWithinTwentyPercent(t *testing.T) {
	for range 200 {
		got := DialRetryJitter(10 * time.Second)
		if got < 8*time.Second || got > 12*time.Second {
			t.Fatalf("jitter = %v, want within 8s..12s", got)
		}
	}
	if DialRetryJitter(0) != 0 {
		t.Fatal("zero wait must stay zero")
	}
}

func TestDialRetryStopsOnCancelAndFinalErrors(t *testing.T) {
	waits := 0
	retry := DialRetry{Budget: time.Minute, Sleep: func(context.Context, time.Duration) { waits++ }}
	final := errors.New("final")
	calls := 0
	if err := retry.Do(context.Background(), func(context.Context) error { calls++; return final }); !errors.Is(err, final) || calls != 1 || waits != 0 {
		t.Fatalf("final error: err=%v calls=%d waits=%d", err, calls, waits)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls = 0
	if err := retry.Do(ctx, func(context.Context) error { calls++; return syscall.ECONNREFUSED }); !errors.Is(err, syscall.ECONNREFUSED) || calls != 1 || waits != 0 {
		t.Fatalf("canceled: err=%v calls=%d waits=%d", err, calls, waits)
	}
}

func TestTransientWindowResetsOnSuccessAndEndsOnBudgetOrFinalError(t *testing.T) {
	var window TransientWindow
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	refused := syscall.ECONNREFUSED
	if window.Exceeded(base, refused, time.Minute) || window.Exceeded(base.Add(30*time.Second), refused, time.Minute) {
		t.Fatal("transient failures inside the budget must not end the loop")
	}
	window.Exceeded(base.Add(40*time.Second), nil, time.Minute)
	if window.Exceeded(base.Add(90*time.Second), refused, time.Minute) {
		t.Fatal("a success must restart the window")
	}
	if !window.Exceeded(base.Add(151*time.Second), refused, time.Minute) {
		t.Fatal("failures past the budget must end the loop")
	}
	if !(&TransientWindow{}).Exceeded(base, errors.New("final"), time.Minute) {
		t.Fatal("a final error must end the loop at once")
	}
}

func TestMaskUnreachableHidesTheTextAndKeepsTheCause(t *testing.T) {
	cause := fmt.Errorf("failed to connect to 127.0.0.1:5432: %w", syscall.ECONNREFUSED)
	masked := MaskUnreachable(cause)
	if strings.Contains(masked.Error(), "127.0.0.1") || !strings.Contains(masked.Error(), "cause=unreachable") {
		t.Fatalf("masked text = %q", masked.Error())
	}
	if !errors.Is(masked, syscall.ECONNREFUSED) || !IsTransientDialError(masked) {
		t.Fatal("the cause must stay visible to errors.Is and the classifier")
	}
	if MaskUnreachable(masked) != masked {
		t.Fatal("masking twice must not wrap again")
	}
	final := errors.New("final 127.0.0.1")
	if MaskUnreachable(final) != final || MaskUnreachable(nil) != nil {
		t.Fatal("a final error and nil must pass through unchanged")
	}
}

type dialRetryClock struct {
	at    time.Time
	waits []time.Duration
}

func (c *dialRetryClock) now() time.Time { return c.at }
func (c *dialRetryClock) sleep(_ context.Context, wait time.Duration) {
	c.at = c.at.Add(wait)
	c.waits = append(c.waits, wait)
}

func TestDialRetryDoesNotAttemptAtOrAfterBudget(t *testing.T) {
	clock := &dialRetryClock{at: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	retry := DialRetry{Budget: 20 * time.Second, Now: clock.now, Sleep: clock.sleep}
	var starts []time.Duration
	begin := clock.at
	err := retry.Do(context.Background(), func(context.Context) error {
		starts = append(starts, clock.at.Sub(begin))
		return syscall.ECONNREFUSED
	})
	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("err = %v, want the dial cause", err)
	}
	for _, at := range starts {
		if at >= 20*time.Second {
			t.Fatalf("attempt started at %v, at or after the 20s budget (starts=%v)", at, starts)
		}
	}
	if len(starts) != 4 {
		t.Fatalf("attempts = %d (%v), want 4 at 0s 2s 6s 14s", len(starts), starts)
	}
}

func TestDialRetryRetryableOverrideAndGiveUpHook(t *testing.T) {
	soft := errors.New("soft")
	final := errors.New("final")
	type giveUp struct {
		attempt   int
		exhausted bool
		called    int
	}
	run := func(budget time.Duration, failures ...error) (error, giveUp, int) {
		clock := &dialRetryClock{at: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
		var got giveUp
		calls := 0
		err := DialRetry{
			Budget: budget, Now: clock.now, Sleep: clock.sleep,
			Retryable: func(err error) bool { return errors.Is(err, soft) },
			OnGiveUp: func(attempt int, _ time.Duration, exhausted bool) {
				got.attempt, got.exhausted = attempt, exhausted
				got.called++
			},
		}.Do(context.Background(), func(context.Context) error {
			defer func() { calls++ }()
			if calls < len(failures) {
				return failures[calls]
			}
			return nil
		})
		return err, got, calls
	}
	if err, got, calls := run(time.Minute, soft, soft, final); !errors.Is(err, final) || calls != 3 || got != (giveUp{3, false, 1}) {
		t.Fatalf("final after soft retries: err=%v calls=%d giveUp=%+v", err, calls, got)
	}
	if err, got, calls := run(5*time.Second, soft, soft, soft, soft, soft); !errors.Is(err, soft) || got.called != 1 || !got.exhausted || got.attempt != calls {
		t.Fatalf("budget spent: err=%v calls=%d giveUp=%+v", err, calls, got)
	}
	if err, got, _ := run(time.Minute, soft, soft); err != nil || got.called != 0 {
		t.Fatalf("success: err=%v giveUp=%+v", err, got)
	}
}

func TestDialRetryGiveUpReportsExhaustedWhenTheBudgetIsAlreadySpent(t *testing.T) {
	exhausted, calls := false, 0
	err := DialRetry{
		Budget:   0,
		OnGiveUp: func(_ int, _ time.Duration, spent bool) { exhausted, calls = spent, calls+1 },
	}.Do(context.Background(), func(context.Context) error { return syscall.ECONNREFUSED })
	if !errors.Is(err, syscall.ECONNREFUSED) || !exhausted || calls != 1 {
		t.Fatalf("err=%v exhausted=%v hookCalls=%d, want the cause, true, 1", err, exhausted, calls)
	}
}
