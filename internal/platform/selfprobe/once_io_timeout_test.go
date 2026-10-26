package selfprobe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/dialclass"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type ioTimeoutFake struct{}

func (ioTimeoutFake) Error() string   { return "dial tcp 10.0.0.9:5432: i/o timeout secret-host" }
func (ioTimeoutFake) Timeout() bool   { return true }
func (ioTimeoutFake) Temporary() bool { return false }

func ioTimeoutCases() map[string]error {
	return map[string]error{
		"os_deadline":    os.ErrDeadlineExceeded,
		"wrapped":        fmt.Errorf("read secret-host: %w", os.ErrDeadlineExceeded),
		"net_op_error":   &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded},
		"net_error_fake": ioTimeoutFake{},
	}
}

type stubOpener struct {
	beginErr, rollbackErr error
}

func (o stubOpener) Begin(context.Context) (Tx, error) {
	if o.beginErr != nil {
		return nil, o.beginErr
	}
	return stubTx{o.rollbackErr}, nil
}

type stubTx struct{ err error }

func (t stubTx) Rollback(context.Context) error { return t.err }

func causeOf(t *testing.T, err error) (cause string, timedOut, transient bool) {
	t.Helper()
	registry := health.NewRegistry(time.Second)
	if regErr := registry.RegisterRequired("probe", func(context.Context) error { return err }); regErr != nil {
		t.Fatal(regErr)
	}
	status := registry.CheckRequired(context.Background()).Checks[0]
	return status.Cause, status.TimedOut, status.Transient
}

// CHAOS-11617: an OS socket deadline or net timeout reads cause=timeout (the form a context deadline has), keeps
// the dial class transient for the startup retry loops, and still drops the driver text.
func TestOnceClassifiesAnIOTimeoutAsATransientTimeout(t *testing.T) {
	t.Parallel()
	for name, failure := range ioTimeoutCases() {
		for stage, opener := range map[string]TxOpener{
			"begin":    stubOpener{beginErr: failure},
			"rollback": stubOpener{rollbackErr: failure},
		} {
			err := Once(context.Background(), opener)
			if err == nil {
				t.Fatalf("%s/%s: Once = nil", name, stage)
			}
			if !dialclass.IsTransient(err) || errors.Is(err, dialclass.ErrUnreachable) {
				t.Errorf("%s/%s: Once = %v, want transient and not ErrUnreachable", name, stage, err)
			}
			if cause, timedOut, transient := causeOf(t, err); cause != "timeout" || !timedOut || transient {
				t.Errorf("%s/%s: registry cause=%q timed_out=%v transient=%v, want timeout true false", name, stage, cause, timedOut, transient)
			}
			for _, leaked := range []string{"secret-host", "10.0.0.9"} {
				if strings.Contains(err.Error(), leaked) {
					t.Errorf("%s/%s: Once = %q leaks %q", name, stage, err.Error(), leaked)
				}
			}
		}
	}
}

func TestOnceKeepsEveryOtherClassWhenAnIOTimeoutMovesToTimeout(t *testing.T) {
	t.Parallel()
	ctxErr := Once(context.Background(), stubOpener{beginErr: fmt.Errorf("begin: %w", context.DeadlineExceeded)})
	if !errors.Is(ctxErr, context.DeadlineExceeded) {
		t.Errorf("ctx deadline: Once = %v, want errors.Is(context.DeadlineExceeded)", ctxErr)
	}
	if cause, timedOut, _ := causeOf(t, ctxErr); cause != "timeout" || !timedOut {
		t.Errorf("ctx deadline: cause=%q timed_out=%v, want timeout true", cause, timedOut)
	}
	refused := Once(context.Background(), stubOpener{beginErr: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}})
	if !errors.Is(refused, dialclass.ErrUnreachable) || !dialclass.IsTransient(refused) || errors.Is(refused, context.DeadlineExceeded) {
		t.Errorf("refused: Once = %v, want transient wrapping ErrUnreachable", refused)
	}
	plain := Once(context.Background(), stubOpener{beginErr: errors.New("boom secret-host")})
	if dialclass.IsTransient(plain) || errors.Is(plain, dialclass.ErrUnreachable) || errors.Is(plain, context.DeadlineExceeded) {
		t.Errorf("plain: Once = %v, want a hard failure", plain)
	}
	if cause, timedOut, transient := causeOf(t, plain); cause != "error" || timedOut || transient {
		t.Errorf("plain: cause=%q timed_out=%v transient=%v, want error false false", cause, timedOut, transient)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Once(canceled, stubOpener{beginErr: context.Canceled}); dialclass.IsTransient(err) || errors.Is(err, context.Canceled) {
		t.Errorf("canceled: Once = %v, want a hard failure without the cancellation", err)
	}
}

// Monitor decides begin_failed vs timeout from ctx.Err() only: an I/O timeout on a live context stays begin_failed
// (the monitor never reads dialclass), before and after.
func TestMonitorAttemptKeepsAnIOTimeoutOnALiveContextAsBeginFailed(t *testing.T) {
	t.Parallel()
	for name, failure := range ioTimeoutCases() {
		m := &Monitor{opener: stubOpener{beginErr: failure}}
		if got := m.attempt(context.Background()); got != ReasonBeginFailed {
			t.Errorf("%s: begin attempt = %q, want %q", name, got, ReasonBeginFailed)
		}
		m = &Monitor{opener: stubOpener{rollbackErr: failure}}
		if got := m.attempt(context.Background()); got != ReasonRollbackFailed {
			t.Errorf("%s: rollback attempt = %q, want %q", name, got, ReasonRollbackFailed)
		}
	}
}
