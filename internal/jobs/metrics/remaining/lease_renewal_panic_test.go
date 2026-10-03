package remaining

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// CHAOS-8161 (r2 P3 of the CHAOS-8024 PR): a panicking work function must stop the lease-renewal goroutine. A leaked
// renewer keeps extending the lease of a claim the panic path is about to release, so the partition looks held for as
// long as the process lives. The test counts renew calls after the panic has been recovered: with the stop channel
// closed by defer the count is frozen; without it the goroutine keeps ticking and the count grows.
func TestRunWithLeaseRenewalStopsRenewingWhenTheWorkPanics(t *testing.T) {
	const lease = 30 * time.Millisecond
	var renewals atomic.Int64
	renewed := make(chan struct{}, 1)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the work panic was swallowed; want it re-raised to the caller")
			}
		}()
		_ = runWithLeaseRenewal(
			context.Background(),
			lease,
			func(context.Context) error {
				renewals.Add(1)
				select {
				case renewed <- struct{}{}:
				default:
				}
				return nil
			},
			func(context.Context) error {
				// Panic only after the renewer is provably running, so a leak cannot hide behind a goroutine that
				// never started.
				select {
				case <-renewed:
				case <-time.After(5 * time.Second):
					t.Error("the renewer never ran")
				}
				panic("work panic")
			},
		)
	}()
	time.Sleep(2 * lease) // let a renewal that raced the stop signal land before sampling
	atFrozen := renewals.Load()
	if atFrozen == 0 {
		t.Fatal("no renewal happened before the panic: the test did not exercise the renewer")
	}
	time.Sleep(6 * lease) // six lease periods = at least ~12 further ticks if the goroutine leaked
	if after := renewals.Load(); after != atFrozen {
		t.Fatalf("renewals grew from %d to %d after the work panicked: the renewal goroutine leaked", atFrozen, after)
	}
}
