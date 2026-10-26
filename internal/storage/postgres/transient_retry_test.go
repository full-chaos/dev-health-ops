package postgres

import (
	"context"
	"errors"
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
