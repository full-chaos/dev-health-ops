package jobruntime

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// lostClaim is a proceeding claim whose lease renewal can be retired mid-run,
// the shape postgresClaim takes when consecutive renewal attempts fail for
// longer than one lease.
type lostClaim struct {
	recordingClaim
	lost chan struct{}
}

func (claim *lostClaim) Lost() <-chan struct{} { return claim.lost }

// TestAdapterCancelsHandlerWhenIdempotencyClaimIsLost asserts the outcome the
// handler experiences, not the mechanism that produces it: once the claim's
// lease is gone, the work must stop. A handler still running on an expired
// lease is exactly the window in which Begin's running-with-expired-lease
// branch hands the same job to a duplicate worker.
//
// The registry timeout for this kind is 300s, so the only cancellation this
// test can observe within its bound is lease loss.
func TestAdapterCancelsHandlerWhenIdempotencyClaimIsLost(t *testing.T) {
	t.Parallel()
	claim := &lostClaim{recordingClaim: recordingClaim{state: ClaimProceed}, lost: make(chan struct{})}
	entered := make(chan struct{})
	cancelled := make(chan bool, 1)
	adapter := newRetentionAdapter(t, HandlerFunc[RetentionCleanupArgs](
		func(ctx context.Context, _ *Execution[RetentionCleanupArgs]) error {
			close(entered)
			select {
			case <-ctx.Done():
				cancelled <- true
				return ctx.Err()
			case <-time.After(2 * time.Second):
				cancelled <- false
				return nil
			}
		}), &recordingObserver{}, claim, &recordingLease{}, &bytes.Buffer{})

	job := retentionJob(t, 1)
	worked := make(chan error, 1)
	go func() { worked <- adapter.Work(context.Background(), job) }()

	<-entered
	close(claim.lost)
	if !<-cancelled {
		t.Fatal("handler kept running after its idempotency claim lost the lease: handler context was never canceled")
	}
	<-worked
}
