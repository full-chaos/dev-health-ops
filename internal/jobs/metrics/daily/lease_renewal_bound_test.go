package daily

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// CHAOS-4290, #2241 r3 Finding 1.
//
// runWithLeaseRenewal cancels the work context only on the RESULT of renew().
// The call used the caller's context, which carries no deadline of its own, so
// a renewal stuck in a network black hole never returned and the work was never
// cancelled -- while the lease it was supposed to be renewing expired.
//
// In production that window is five minutes: a 10-minute lease against a
// 15-minute adapter timeout. Another worker reclaims the run, computes the same
// family, and appends a second generation the dedup read silently prefers.
//
// The shape here is the reviewer's: a tiny lease, a renewal that blocks until
// the parent context ends, and a cooperative worker. The assertion is that the
// work is cancelled WELL BEFORE the parent deadline -- if the bound is absent,
// nothing cancels it until the parent expires.
func TestLeaseRenewalIsBoundedByTheLease(t *testing.T) {
	const lease = 150 * time.Millisecond
	parent, cancelParent := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelParent()

	var renewCalls atomic.Int64
	// A renewal that never returns on its own. Without a per-call deadline it
	// blocks until the PARENT is done, which is the defect.
	renew := func(ctx context.Context) error {
		renewCalls.Add(1)
		<-ctx.Done()
		return ctx.Err()
	}

	workCancelled := make(chan time.Duration, 1)
	started := time.Now()
	work := func(ctx context.Context) error {
		<-ctx.Done()
		workCancelled <- time.Since(started)
		return ctx.Err()
	}

	err := runWithLeaseRenewal(parent, lease, renew, work)
	if err == nil {
		t.Fatal("runWithLeaseRenewal returned nil after a renewal that never succeeded")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want it to wrap context.DeadlineExceeded -- a renewal that "+
			"timed out is a LEASE LOSS, and the caller must be able to tell that from "+
			"an ordinary failure", err)
	}

	select {
	case elapsed := <-workCancelled:
		// One tick (lease/3) to fire, plus one bounded renewal (lease/3) to time
		// out. Anything near the parent's 3s means nothing bounded the renewal.
		if elapsed > lease {
			t.Fatalf("work ran %v with a %v lease -- the renewal was not bounded, so the "+
				"executor kept writing past a lease another worker can reclaim", elapsed, lease)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("work was never cancelled -- an unbounded renewal holds the job open " +
			"while its lease expires, which is the two-writer hazard via timing")
	}

	if renewCalls.Load() == 0 {
		t.Fatal("renew was never called, so this test proved nothing about renewal bounds")
	}
}

// The control: a renewal that SUCCEEDS promptly must not be cancelled by the new
// bound. A deadline tight enough to kill healthy renewals would convert a
// liveness fix into an availability bug, and that failure would look identical
// to the one above from the outside.
func TestABoundedRenewalDoesNotCancelHealthyWork(t *testing.T) {
	const lease = 150 * time.Millisecond
	var renewCalls atomic.Int64
	renew := func(context.Context) error { renewCalls.Add(1); return nil }

	done := make(chan struct{})
	work := func(ctx context.Context) error {
		// Long enough to require several successful renewals.
		select {
		case <-time.After(lease * 3):
			close(done)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := runWithLeaseRenewal(context.Background(), lease, renew, work); err != nil {
		t.Fatalf("runWithLeaseRenewal = %v, want nil -- healthy renewals must not be "+
			"cancelled by the bound", err)
	}
	select {
	case <-done:
	default:
		t.Fatal("work did not run to completion")
	}
	if renewCalls.Load() < 2 {
		t.Fatalf("renew ran %d time(s); fewer than two means the work finished before "+
			"the bound was ever exercised, so this control proves nothing",
			renewCalls.Load())
	}
}
