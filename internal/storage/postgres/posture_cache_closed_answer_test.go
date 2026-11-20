package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// CHAOS-8654 r1 P2: past RefusalTTL a running check re-asks; a closed one cannot, so Check keeps
// serving the stored refusal, as CheckNoWait does.
func TestClosedCachedPostureCheckKeepsServingItsRefusalPastRefusalTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runs atomic.Int32
		refusal := errors.Join(ErrUnavailable, ErrPostureRefused)
		check := NewCachedRunCheck("api_role", func(context.Context) error {
			runs.Add(1)
			return refusal
		}, PostureCheckOptions{RefusalTTL: time.Second})
		check.Warm()
		synctest.Wait()
		check.Close()
		time.Sleep(2 * time.Second)

		if err := check.Check(context.Background()); !errors.Is(err, ErrPostureRefused) {
			t.Fatalf("Check() after Close past RefusalTTL = %v, want the stored refusal", err)
		}
		if err := check.CheckNoWait(); !errors.Is(err, ErrPostureRefused) {
			t.Fatalf("CheckNoWait() after Close past RefusalTTL = %v, want the stored refusal", err)
		}
		synctest.Wait()
		if got := runs.Load(); got != 1 {
			t.Fatalf("runs = %d, want 1 (no execution after Close)", got)
		}
	})
}

// detachedPostureContext outlives its parent's deadline (a rollback after the statement timed out)
// and, under a CachedPostureCheck, still ends when the check is closed.
func TestDetachedPostureContextOutlivesTheDeadlineButNotTheOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		owner, closeOwner := context.WithCancel(context.Background())
		parent, cancelParent := context.WithTimeout(withPostureOwner(owner), time.Second)
		defer cancelParent()
		time.Sleep(2 * time.Second)
		if parent.Err() == nil {
			t.Fatal("the parent deadline did not pass")
		}
		detached, cancel := detachedPostureContext(parent, time.Minute)
		defer cancel()
		if err := detached.Err(); err != nil {
			t.Fatalf("detached context ended with its parent's deadline: %v", err)
		}
		closeOwner()
		synctest.Wait()
		if !errors.Is(detached.Err(), context.Canceled) {
			t.Fatalf("detached context after the owner closed = %v, want context.Canceled", detached.Err())
		}
	})
}

func TestDetachedPostureContextWithoutAnOwnerKeepsItsOwnTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		detached, cancel := detachedPostureContext(parent, time.Minute)
		defer cancel()
		cancelParent()
		synctest.Wait()
		if err := detached.Err(); err != nil {
			t.Fatalf("detached context without an owner ended with its parent: %v", err)
		}
		time.Sleep(2 * time.Minute)
		if !errors.Is(detached.Err(), context.DeadlineExceeded) {
			t.Fatalf("detached context past its timeout = %v, want context.DeadlineExceeded", detached.Err())
		}
	})
}

// Each execution runs under its check's lifetime, so a step it detaches from its own deadline (the
// rollback, the diagnostics after a refusal) still ends when the check is closed.
func TestCachedPostureCheckDetachedStepsEndWhenTheCheckCloses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stepEnded := make(chan error, 1)
		check := NewCachedRunCheck("api_role", func(ctx context.Context) error {
			step, cancel := detachedPostureContext(ctx, time.Hour)
			defer cancel()
			<-step.Done()
			stepEnded <- step.Err()
			return step.Err()
		}, PostureCheckOptions{RunTimeout: time.Second})
		check.Warm()
		time.Sleep(2 * time.Second) // past RunTimeout: only the check's lifetime can end the step now
		synctest.Wait()
		select {
		case err := <-stepEnded:
			t.Fatalf("the detached step ended with the execution's deadline: %v", err)
		default:
		}
		check.Close()
		if err := <-stepEnded; !errors.Is(err, context.Canceled) {
			t.Fatalf("detached step after Close = %v, want context.Canceled", err)
		}
	})
}
