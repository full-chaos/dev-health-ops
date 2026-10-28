package riverstore

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

func telemetryCause(t *testing.T, err error) (cause string, timedOut, transient bool) {
	t.Helper()
	registry := health.NewRegistry(time.Second)
	if regErr := registry.RegisterRequired("execution_liveness", func(context.Context) error { return err }); regErr != nil {
		t.Fatal(regErr)
	}
	status := registry.CheckRequired(context.Background()).Checks[0]
	return status.Cause, status.TimedOut, status.Transient
}

// CHAOS-11617: the telemetry sanitizer reports an OS or net timeout as a timeout (transient for the retry loops,
// cause=timeout in the registry), never as unreachable, and never with the driver text.
func TestQueueTelemetryReadFailureClassifiesIOTimeoutsAsTimeout(t *testing.T) {
	t.Parallel()
	for name, failure := range map[string]error{
		"os_deadline":    os.ErrDeadlineExceeded,
		"wrapped":        fmt.Errorf("query secret-host: %w", os.ErrDeadlineExceeded),
		"net_op_error":   &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded},
		"net_error_fake": ioTimeoutFake{},
	} {
		got := queueTelemetryReadFailure(failure)
		if !errors.Is(got, ErrQueueTelemetryUnavailable) {
			t.Errorf("%s: %v, want ErrQueueTelemetryUnavailable", name, got)
		}
		if !dialclass.IsTransient(got) || errors.Is(got, dialclass.ErrUnreachable) {
			t.Errorf("%s: %v, want transient and not ErrUnreachable", name, got)
		}
		if cause, timedOut, transient := telemetryCause(t, got); cause != "timeout" || !timedOut || transient {
			t.Errorf("%s: registry cause=%q timed_out=%v transient=%v, want timeout true false", name, cause, timedOut, transient)
		}
		for _, leaked := range []string{"secret-host", "10.0.0.9"} {
			if strings.Contains(got.Error(), leaked) {
				t.Errorf("%s: leaks %q: %v", name, leaked, got)
			}
		}
	}
}

func TestQueueTelemetryReadFailureKeepsEveryOtherClass(t *testing.T) {
	t.Parallel()
	ctxErr := queueTelemetryReadFailure(fmt.Errorf("acquire: %w", context.DeadlineExceeded))
	if !errors.Is(ctxErr, context.DeadlineExceeded) {
		t.Errorf("ctx deadline: %v, want errors.Is(context.DeadlineExceeded)", ctxErr)
	}
	if cause, timedOut, _ := telemetryCause(t, ctxErr); cause != "timeout" || !timedOut {
		t.Errorf("ctx deadline: cause=%q timed_out=%v", cause, timedOut)
	}
	if err := queueTelemetryReadFailure(fmt.Errorf("acquire: %w", context.Canceled)); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled: %v, want errors.Is(context.Canceled)", err)
	}
	refused := queueTelemetryReadFailure(&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED})
	if !errors.Is(refused, dialclass.ErrUnreachable) || !dialclass.IsTransient(refused) {
		t.Errorf("refused: %v, want transient ErrUnreachable", refused)
	}
	plain := queueTelemetryReadFailure(errors.New("decode row args secret"))
	if plain != ErrQueueTelemetryUnavailable || dialclass.IsTransient(plain) {
		t.Errorf("plain: %v, want the bare sentinel", plain)
	}
}
