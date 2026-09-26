package selfprobe

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// CHAOS-6955. domain_transaction is health.Registry's `Once` probe. The registry (and the worker's
// preclaim-readiness retry, which retries ONLY when every failed check timed out) classify a
// check as retryable by errors.Is(err, context.DeadlineExceeded). Once returned a fixed
// "begin probe transaction: unavailable" and dropped the cause, so a BEGIN that ran into its
// deadline while the one-connection readiness pool was held by another check read as a hard
// failure: at rev 191 go-sync exited on attempt 1 (elapsed 10.0 s, reason dependency_check_failed)
// instead of retrying through the roll storm.

func TestOnceKeepsTheDeadlineClassButNeverTheDriverTextOrCancellation(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		cause error
		want  error
	}{
		"deadline":         {fmt.Errorf("acquire connection: %w", context.DeadlineExceeded), context.DeadlineExceeded},
		"deadline wrapped": {fmt.Errorf("a: %w", fmt.Errorf("b password=secret: %w", context.DeadlineExceeded)), context.DeadlineExceeded},
	} {
		tc := tc
		t.Run(name+"/begin", func(t *testing.T) {
			t.Parallel()
			err := Once(context.Background(), &fakeOpener{beginFn: func(context.Context, int) (Tx, error) { return nil, tc.cause }})
			if !errors.Is(err, tc.want) {
				t.Fatalf("Once(begin fails with %v) = %v, want it to satisfy errors.Is(%v): the registry classifies a timeout by it", tc.cause, err, tc.want)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "acquire") {
				t.Fatalf("Once leaked the underlying error text: %v", err)
			}
		})
		t.Run(name+"/rollback", func(t *testing.T) {
			t.Parallel()
			err := Once(context.Background(), &fakeOpener{beginFn: func(context.Context, int) (Tx, error) {
				return fakeTx{rollbackErr: tc.cause}, nil
			}})
			if !errors.Is(err, tc.want) {
				t.Fatalf("Once(rollback fails with %v) = %v, want errors.Is(%v)", tc.cause, err, tc.want)
			}
		})
	}
}

// r1 P1 (fixed): a check that returns because it was CANCELED -- not because it ran into its own
// deadline -- has completed for some other reason, not merely run out of time on a busy pool.
// health.Registry classifies context.Canceled as retryable too (the same bucket as a deadline), so
// preserving it here would let a completed cancellation retry for the whole preclaim budget instead
// of failing fast like any other genuine problem. Once must never expose it.
func TestOnceNeverClassifiesCancellationAsARetryableTimeout(t *testing.T) {
	t.Parallel()
	for name, cause := range map[string]error{
		"bare":    context.Canceled,
		"wrapped": fmt.Errorf("acquire connection: %w", context.Canceled),
		"nested":  fmt.Errorf("a: %w", fmt.Errorf("b: %w", context.Canceled)),
	} {
		cause := cause
		t.Run(name+"/begin", func(t *testing.T) {
			t.Parallel()
			err := Once(context.Background(), &fakeOpener{beginFn: func(context.Context, int) (Tx, error) { return nil, cause }})
			if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Once(begin canceled) = %v, want a hard failure that is neither canceled nor a deadline", err)
			}
		})
		t.Run(name+"/rollback", func(t *testing.T) {
			t.Parallel()
			err := Once(context.Background(), &fakeOpener{beginFn: func(context.Context, int) (Tx, error) {
				return fakeTx{rollbackErr: cause}, nil
			}})
			if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Once(rollback canceled) = %v, want a hard failure that is neither canceled nor a deadline", err)
			}
		})
	}
}

// A failure that is NOT the caller's deadline stays a hard failure: a wrong password or a refused
// connection must not read as "slow".
func TestOnceDoesNotClassifyAnOrdinaryFailureAsATimeout(t *testing.T) {
	t.Parallel()
	err := Once(context.Background(), &fakeOpener{beginFn: func(context.Context, int) (Tx, error) {
		return nil, errors.New("password authentication failed")
	}})
	if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("Once(ordinary failure) = %v, want a failure that is neither a deadline nor a cancellation", err)
	}
}
