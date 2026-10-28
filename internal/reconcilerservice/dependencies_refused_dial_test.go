package reconcilerservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// CHAOS-11644: a raw refused dial on a reconciler readiness check reads cause=unreachable, as it
// does on every other service (dialclass.IsTransient decides), and nothing else changes cause.

func refusedDialForms() map[string]error {
	return map[string]error{
		"raw_errno":    syscall.ECONNREFUSED,
		"net_op_error": &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		"wrapped":      fmt.Errorf("probe: %w", &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}),
	}
}

func readinessRefusalFor(checkName string, err error) *fakeReconcilerDatabase {
	switch checkName {
	case "domain_postgres":
		return &fakeReconcilerDatabase{domainErr: err}
	case "domain_transaction":
		return &fakeReconcilerDatabase{domainTransactionErr: err}
	case "queue_postgres":
		return &fakeReconcilerDatabase{queueErr: err}
	case "coordinator_postgres":
		return &fakeReconcilerDatabase{coordinatorErr: err}
	case "river_schema":
		return &fakeReconcilerDatabase{schemaErr: err}
	case "posture_manifest_lockstep":
		return &fakeReconcilerDatabase{postureLockstepErr: err}
	}
	panic("unknown check " + checkName)
}

var reconcilerReadinessChecks = []string{
	"domain_postgres", "domain_transaction", "queue_postgres", "coordinator_postgres", "river_schema", "posture_manifest_lockstep",
}

func requireRefusalCause(t *testing.T, checkName string, failure error, wantCause string, wantTimedOut bool) {
	t.Helper()
	cause, timedOut := reconcilerReadinessCauseHarness(t, readinessRefusalFor(checkName, failure), checkName)
	if cause != wantCause || timedOut != wantTimedOut {
		t.Fatalf("%s refused with %v, got cause=%q timed_out=%v, want cause=%q timed_out=%v",
			checkName, failure, cause, timedOut, wantCause, wantTimedOut)
	}
}

// --- red: the change ---

func TestReconcilerReadinessRefusalReadsARawRefusedDialAsUnreachableAcrossChecks(t *testing.T) {
	failure := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	for _, checkName := range reconcilerReadinessChecks {
		t.Run(checkName, func(t *testing.T) {
			requireRefusalCause(t, checkName, failure, health.CauseUnreachable, false)
		})
	}
}

func TestReconcilerReadinessRefusalReadsEveryRefusedDialFormAsUnreachable(t *testing.T) {
	for name, failure := range refusedDialForms() {
		t.Run(name, func(t *testing.T) {
			requireRefusalCause(t, "queue_postgres", failure, health.CauseUnreachable, false)
		})
	}
}

func TestReconcilerReadinessRefusalReadsAPoolerOutageAsUnreachable(t *testing.T) {
	for _, code := range []string{"57P03", "53300"} {
		t.Run(code, func(t *testing.T) {
			requireRefusalCause(t, "queue_postgres", &pgconn.PgError{Code: code}, health.CauseUnreachable, false)
		})
	}
}

func TestReconcilerReadinessRefusalReadsAnEOFLikeTheWorkers(t *testing.T) {
	for name, failure := range map[string]error{"eof": io.EOF, "unexpected_eof": io.ErrUnexpectedEOF} {
		t.Run(name, func(t *testing.T) {
			requireRefusalCause(t, "domain_postgres", failure, health.CauseUnreachable, false)
		})
	}
}

func TestDependencyCheckFailedMarksATransientDialErrorTransient(t *testing.T) {
	for name, failure := range refusedDialForms() {
		t.Run(name, func(t *testing.T) {
			err := dependencyCheckFailed(failure)
			if !health.IsTransient(err) {
				t.Errorf("dependencyCheckFailed(%v) is not transient", failure)
			}
			if !errors.Is(err, errReconcilerDependencyUnavailable) {
				t.Errorf("dependencyCheckFailed(%v) lost the dependency sentinel", failure)
			}
			if !errors.Is(err, syscall.ECONNREFUSED) {
				t.Errorf("dependencyCheckFailed(%v) lost its cause", failure)
			}
		})
	}
}

// --- green before and after: nothing else changes cause ---

func TestReconcilerReadinessRefusalKeepsCanceledAsCanceledWhenItCarriesARefusedDial(t *testing.T) {
	for name, failure := range map[string]error{
		"canceled":            context.Canceled,
		"join_canceled_first": errors.Join(context.Canceled, syscall.ECONNREFUSED),
		"join_canceled_last":  errors.Join(syscall.ECONNREFUSED, context.Canceled),
		"net_canceled":        &net.OpError{Op: "dial", Err: errors.Join(syscall.ECONNREFUSED, context.Canceled)},
	} {
		t.Run(name, func(t *testing.T) {
			requireRefusalCause(t, "domain_postgres", failure, "canceled", true)
		})
	}
}

func TestReconcilerReadinessRefusalKeepsTimeoutAsTimeoutWhenItCarriesARefusedDial(t *testing.T) {
	for name, failure := range map[string]error{
		"deadline":            context.DeadlineExceeded,
		"os_deadline":         os.ErrDeadlineExceeded,
		"net_timeout":         ioTimeoutFake{},
		"join_deadline_first": errors.Join(context.DeadlineExceeded, syscall.ECONNREFUSED),
		"join_deadline_last":  errors.Join(syscall.ECONNREFUSED, context.DeadlineExceeded),
	} {
		t.Run(name, func(t *testing.T) {
			requireRefusalCause(t, "domain_postgres", failure, "timeout", true)
		})
	}
}

func TestReconcilerReadinessRefusalKeepsAuthAndPermissionAsError(t *testing.T) {
	for name, failure := range map[string]error{
		"28P01":     &pgconn.PgError{Code: "28P01"},
		"28000":     &pgconn.PgError{Code: "28000"},
		"42501":     &pgconn.PgError{Code: "42501"},
		"plain":     errors.New("role posture refused"),
		"wrapped_a": fmt.Errorf("check: %w", &pgconn.PgError{Code: "28P01"}),
	} {
		t.Run(name, func(t *testing.T) {
			requireRefusalCause(t, "queue_postgres", failure, "error", false)
			if health.IsTransient(dependencyCheckFailed(failure)) {
				t.Errorf("dependencyCheckFailed(%v) is transient", failure)
			}
		})
	}
}

func TestDependencyCheckFailedKeepsTheErrorTextOfARefusedDial(t *testing.T) {
	failure := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	want := errReconcilerDependencyUnavailable.Error() + ": " + failure.Error()
	if got := dependencyCheckFailed(failure).Error(); got != want {
		t.Fatalf("error text = %q, want %q", got, want)
	}
}
