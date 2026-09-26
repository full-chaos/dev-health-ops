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
// check by errors.Is(err, context.DeadlineExceeded / Canceled). Once returned a fixed
// "begin probe transaction: unavailable" and dropped the cause, so a BEGIN that ran into its
// deadline while the one-connection readiness pool was held by another check read as a hard
// failure: at rev 191 go-sync exited on attempt 1 (elapsed 10.0 s, reason dependency_check_failed)
// instead of retrying through the roll storm.

func TestOnceKeepsTheDeadlineAndCancelClassButNeverTheDriverText(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		cause error
		want  error
	}{
		"deadline":         {fmt.Errorf("acquire connection: %w", context.DeadlineExceeded), context.DeadlineExceeded},
		"canceled":         {fmt.Errorf("acquire connection: %w", context.Canceled), context.Canceled},
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

// A failure that is NOT the caller's deadline or cancellation stays a hard failure: a wrong
// password or a refused connection must not read as "slow".
func TestOnceDoesNotClassifyAnOrdinaryFailureAsATimeout(t *testing.T) {
	t.Parallel()
	err := Once(context.Background(), &fakeOpener{beginFn: func(context.Context, int) (Tx, error) {
		return nil, errors.New("password authentication failed")
	}})
	if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("Once(ordinary failure) = %v, want a failure that is neither a deadline nor a cancellation", err)
	}
}
