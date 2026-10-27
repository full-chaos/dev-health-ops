package workerservice

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/selfprobe"
)

type ioTimeoutFake struct{}

func (ioTimeoutFake) Error() string   { return "dial tcp 10.0.0.9:5432: i/o timeout secret-host" }
func (ioTimeoutFake) Timeout() bool   { return true }
func (ioTimeoutFake) Temporary() bool { return false }

type beginFails struct{ err error }

func (o beginFails) Begin(context.Context) (selfprobe.Tx, error) { return nil, o.err }

func ioTimeoutErrors() map[string]error {
	return map[string]error{
		"os_deadline":    os.ErrDeadlineExceeded,
		"wrapped":        fmt.Errorf("read: %w", os.ErrDeadlineExceeded),
		"net_op_error":   &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded},
		"net_error_fake": ioTimeoutFake{},
	}
}

// CHAOS-11617: for every check that goes through dependencyCheckFailed, an I/O timeout reads cause=timeout (the
// form of a context deadline), and the preclaim retry decision stays "retry": the readiness retries only when
// every failed check timed out or was transient, and an I/O timeout is both before and after.
func TestDependencyCheckFailedClassifiesIOTimeoutsPerCheck(t *testing.T) {
	t.Parallel()
	type outcome struct {
		cause     string
		timedOut  bool
		transient bool
		retryable bool
	}
	for _, checkName := range []string{"queue_postgres", "domain_postgres", "river_schema", "domain_transaction", "execution_liveness"} {
		cases := map[string]struct {
			failure error
			want    outcome
		}{
			"ctx_deadline": {context.DeadlineExceeded, outcome{"timeout", true, false, true}},
			"refused":      {&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, outcome{"unreachable", false, true, true}},
			"plain":        {errors.New("posture mismatch"), outcome{"error", false, false, false}},
		}
		for name, failure := range ioTimeoutErrors() {
			cases["io_"+name] = struct {
				failure error
				want    outcome
			}{failure, outcome{"timeout", true, false, true}}
			// the production sanitizer in front of the wrapper (domain_transaction path)
			cases["once_"+name] = struct {
				failure error
				want    outcome
			}{selfprobe.Once(context.Background(), beginFails{failure}), outcome{"timeout", true, false, true}}
		}
		for caseName, tc := range cases {
			registry := health.NewRegistry(time.Second)
			failure := tc.failure
			if err := registry.RegisterRequired(checkName, func(context.Context) error { return dependencyCheckFailed(failure) }); err != nil {
				t.Fatal(err)
			}
			readiness := registry.CheckRequired(context.Background())
			status := readiness.Checks[0]
			got := outcome{status.Cause, status.TimedOut, status.Transient, preclaimReadinessRetryable(readiness)}
			if got != tc.want {
				t.Errorf("%s/%s = %+v, want %+v", checkName, caseName, got, tc.want)
			}
			if want := checkName + "=" + tc.want.cause; failedCauses(readiness) != want {
				t.Errorf("%s/%s failedCauses = %q, want %q", checkName, caseName, failedCauses(readiness), want)
			}
		}
	}
}

func TestPreclaimReadinessStillRetriesAnIOTimeoutUntilTheCheckAnswers(t *testing.T) {
	t.Parallel()
	for name, failure := range ioTimeoutErrors() {
		registry := health.NewRegistry(2 * time.Second)
		var attempts atomic.Int32
		if err := registry.RegisterRequired("queue_postgres", func(context.Context) error {
			if attempts.Add(1) == 3 {
				return nil
			}
			return dependencyCheckFailed(failure)
		}); err != nil {
			t.Fatal(err)
		}
		clock := &preclaimFakeClock{}
		component := preclaimReadinessComponent{registry: registry, budget: time.Minute, now: clock.now, sleep: clock.sleep}
		if err := component.Start(context.Background()); err != nil {
			t.Errorf("%s: Start() = %v, want nil once the check answers", name, err)
		}
		if got := attempts.Load(); got != 3 {
			t.Errorf("%s: check ran %d times, want 3 (two retries)", name, got)
		}
	}
}
