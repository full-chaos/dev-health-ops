package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
)

func waitAnswered(t *testing.T, check *LazyProbeCheck) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !check.Answered() {
		if time.Now().After(deadline) {
			t.Fatal("the warmed check never answered")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// CHAOS-6934: once a check has an answer, a probe is answered from it and never waits for the query,
// the pool behind it, or a slow database.
func TestLazyProbeCheckNeverMakesAProbeWaitOnceItHasAnAnswer(t *testing.T) {
	block := make(chan struct{})
	first := true
	var check LazyProbeCheck
	run := func(ctx context.Context) error {
		if first {
			first = false
			return nil // the first execution answers at once
		}
		select {
		case <-block: // every refresh after that is stuck behind a held work pool
		case <-ctx.Done():
		}
		return nil
	}
	options := PostureCheckOptions{TTL: 10 * time.Millisecond, MaxStale: time.Hour}
	check.Warm("test", run, options)
	waitAnswered(t, &check)
	time.Sleep(50 * time.Millisecond) // older than TTL: a refresh starts and blocks

	for i := 0; i < 5; i++ {
		begun := time.Now()
		if err := check.CheckWith(context.Background(), "test", run, options); err != nil {
			t.Fatalf("probe %d = %v, want the cached pass", i, err)
		}
		if elapsed := time.Since(begun); elapsed > 200*time.Millisecond {
			t.Fatalf("probe %d took %s while the refresh was blocked, want an immediate answer", i, elapsed)
		}
	}
	close(block)
}

// The one wait: a probe that arrives before the very first execution has completed waits for that
// single execution, bounded by its own context, and reports a timeout, not a refusal.
func TestLazyProbeCheckTheColdProbeIsBoundedByItsOwnContext(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	var check LazyProbeCheck
	run := func(ctx context.Context) error {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	begun := time.Now()
	err := check.Check(ctx, "test", run)
	if elapsed := time.Since(begun); elapsed > time.Second {
		t.Fatalf("the cold probe took %s, want it bounded by its 150 ms context", elapsed)
	}
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a cold probe that ran out of budget = %v, want ErrUnavailable classified as a timeout", err)
	}
}

func TestLazyProbeCheckServesAPassWhileAnUnansweredRefreshRunsAndBoundsIt(t *testing.T) {
	var check LazyProbeCheck
	answers := make(chan error, 8)
	run := func(ctx context.Context) error {
		select {
		case err := <-answers:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	options := PostureCheckOptions{TTL: 20 * time.Millisecond, MaxStale: 250 * time.Millisecond, RunTimeout: time.Minute}

	answers <- nil
	check.Warm("test", run, options)
	waitAnswered(t, &check)
	// Past TTL a refresh starts and blocks (no answer): the pass is still served inside MaxStale.
	time.Sleep(60 * time.Millisecond)
	if err := check.CheckWith(context.Background(), "test", run, options); err != nil {
		t.Fatalf("a pass inside MaxStale with an unanswered refresh = %v, want a pass", err)
	}
	// Past MaxStale with no answer it REFUSES, at once, naming why (a dead database turns readiness red).
	time.Sleep(300 * time.Millisecond)
	begun := time.Now()
	err := check.CheckWith(context.Background(), "test", run, options)
	if err == nil || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a pass older than MaxStale with no answer = %v, want ErrUnavailable (absence of an answer is never a pass)", err)
	}
	if time.Since(begun) > 200*time.Millisecond {
		t.Fatal("the fail-closed answer waited")
	}
}

func TestLazyProbeCheckSurfacesADefinitiveRefusalAndTheRealCauseOfAnUnansweredQuery(t *testing.T) {
	var refusing LazyProbeCheck
	refusal := errors.New("grants drifted")
	runRefusal := func(context.Context) error { return errors.Join(ErrPostureRefused, refusal) }
	refusing.Warm("test", runRefusal, PostureCheckOptions{})
	waitAnswered(t, &refusing)
	if err := refusing.Check(context.Background(), "test", runRefusal); !errors.Is(err, ErrPostureRefused) {
		t.Fatalf("a definitive refusal = %v, want ErrPostureRefused", err)
	}

	var broken LazyProbeCheck
	cause := errors.New("connection refused")
	runBroken := func(context.Context) error { return cause }
	broken.Warm("test", runBroken, PostureCheckOptions{})
	waitAnswered(t, &broken)
	err := broken.Check(context.Background(), "test", runBroken)
	if !errors.Is(err, cause) {
		t.Fatalf("the real cause of an unanswered query = %v, want it named", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a genuine failure %v was classified as a timeout", err)
	}
}

// A process proves its roles at startup: Warm starts the background query without any probe.
func TestLazyProbeCheckWarmRunsTheQueryBeforeTheFirstProbe(t *testing.T) {
	ran := make(chan struct{}, 1)
	var check LazyProbeCheck
	check.Warm("test", func(context.Context) error {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil
	}, PostureCheckOptions{})
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("Warm did not start the background query")
	}
	waitAnswered(t, &check)
	if err := check.Check(context.Background(), "test", func(context.Context) error { return errors.New("must not be the kept run") }); err != nil {
		t.Fatalf("the first probe after the warm answer = %v, want a pass", err)
	}
}

// The refresh is bounded by the probe deadline it is configured with.
func TestLazyProbeCheckBoundsTheBackgroundQueryByItsRunTimeout(t *testing.T) {
	var check LazyProbeCheck
	seen := make(chan time.Duration, 1)
	check.Warm("test", func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			seen <- -1
			return nil
		}
		seen <- time.Until(deadline)
		return nil
	}, PostureCheckOptions{RunTimeout: 1500 * time.Millisecond})
	select {
	case remaining := <-seen:
		if remaining <= 0 || remaining > 1500*time.Millisecond {
			t.Fatalf("background query deadline = %s away, want at most the 1.5 s RunTimeout", remaining)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the background query never ran")
	}
}
