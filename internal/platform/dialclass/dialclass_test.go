package dialclass

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
)

type fakeNetTimeout struct{}

func (fakeNetTimeout) Error() string   { return "i/o timeout" }
func (fakeNetTimeout) Timeout() bool   { return true }
func (fakeNetTimeout) Temporary() bool { return false }

func TestIsTransientTable(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"nil":                      {nil, false},
		"canceled":                 {context.Canceled, false},
		"context_deadline":         {fmt.Errorf("acquire: %w", context.DeadlineExceeded), false},
		"err_unreachable":          {fmt.Errorf("x: %w", ErrUnreachable), true},
		"econnrefused":             {&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, true},
		"econnreset":               {syscall.ECONNRESET, true},
		"epipe":                    {syscall.EPIPE, true},
		"ehostunreach":             {syscall.EHOSTUNREACH, true},
		"enetunreach":              {syscall.ENETUNREACH, true},
		"eof":                      {io.EOF, true},
		"unexpected_eof":           {io.ErrUnexpectedEOF, true},
		"dns":                      {&net.DNSError{Err: "no such host", Name: "db"}, true},
		"os_deadline":              {os.ErrDeadlineExceeded, true},
		"net_op_error_os_deadline": {&net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}, true},
		"net_error_fake_timeout":   {fakeNetTimeout{}, true},
		"pg_57P03":                 {&pgconn.PgError{Code: "57P03"}, true},
		"pg_08P01":                 {&pgconn.PgError{Code: "08P01"}, true},
		"pg_08006":                 {&pgconn.PgError{Code: "08006"}, true},
		"pg_53300":                 {&pgconn.PgError{Code: "53300"}, true},
		"pg_wrong_password":        {&pgconn.PgError{Code: "28P01"}, false},
		"pg_invalid_auth":          {&pgconn.PgError{Code: "28000"}, false},
		"pg_permission":            {&pgconn.PgError{Code: "42501"}, false},
		"plain":                    {errors.New("boom"), false},
	} {
		if got := IsTransient(tc.err); got != tc.want {
			t.Errorf("%s: IsTransient(%v) = %v, want %v", name, tc.err, got, tc.want)
		}
	}
}

// transientBit reads the method the health registry reads, through errors.As, without importing health.
func transientBit(err error) (answers bool, present bool) {
	var transient interface{ Transient() bool }
	if errors.As(err, &transient) {
		return transient.Transient(), true
	}
	return false, false
}

func TestMarkTransientMarksEveryTransientClass(t *testing.T) {
	t.Parallel()
	for name, failure := range map[string]error{
		"refused":         &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		"raw_errno":       syscall.ECONNREFUSED,
		"reset":           syscall.ECONNRESET,
		"eof":             io.EOF,
		"unexpected_eof":  io.ErrUnexpectedEOF,
		"dns":             &net.DNSError{Err: "no such host", Name: "db"},
		"pg_57P03":        &pgconn.PgError{Code: "57P03"},
		"pg_53300":        &pgconn.PgError{Code: "53300"},
		"err_unreachable": fmt.Errorf("x: %w", ErrUnreachable),
		"net_timeout":     fakeNetTimeout{},
		"os_deadline":     os.ErrDeadlineExceeded,
	} {
		marked := MarkTransient(failure)
		if answers, present := transientBit(marked); !present || !answers {
			t.Errorf("%s: MarkTransient(%v) answers Transient() = %v (method present %v), want true", name, failure, answers, present)
		}
		if !errors.Is(marked, failure) {
			t.Errorf("%s: MarkTransient(%v) lost its cause", name, failure)
		}
	}
}

func TestMarkTransientReturnsEverythingElseUnchanged(t *testing.T) {
	t.Parallel()
	if got := MarkTransient(nil); got != nil {
		t.Errorf("MarkTransient(nil) = %v, want nil", got)
	}
	plain := errors.New("boom")
	pgWrong := &pgconn.PgError{Code: "28P01"}
	for name, failure := range map[string]error{
		"canceled":         context.Canceled,
		"context_deadline": fmt.Errorf("acquire: %w", context.DeadlineExceeded),
		"pg_28P01":         pgWrong,
		"pg_28000":         &pgconn.PgError{Code: "28000"},
		"pg_42501":         &pgconn.PgError{Code: "42501"},
		"plain":            plain,
	} {
		got := MarkTransient(failure)
		if got != failure {
			t.Errorf("%s: MarkTransient(%v) = %v, want the same error back", name, failure, got)
		}
		if _, present := transientBit(got); present {
			t.Errorf("%s: MarkTransient(%v) carries a Transient() method", name, failure)
		}
	}
}

func TestMarkTransientAgreesWithIsTransientForEveryTableRow(t *testing.T) {
	t.Parallel()
	for name, failure := range map[string]error{
		"nil":                      nil,
		"canceled":                 context.Canceled,
		"context_deadline":         fmt.Errorf("acquire: %w", context.DeadlineExceeded),
		"err_unreachable":          fmt.Errorf("x: %w", ErrUnreachable),
		"econnrefused":             &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		"epipe":                    syscall.EPIPE,
		"ehostunreach":             syscall.EHOSTUNREACH,
		"enetunreach":              syscall.ENETUNREACH,
		"dns":                      &net.DNSError{Err: "no such host", Name: "db"},
		"net_op_error_os_deadline": &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded},
		"pg_08P01":                 &pgconn.PgError{Code: "08P01"},
		"pg_08006":                 &pgconn.PgError{Code: "08006"},
		"pg_wrong_password":        &pgconn.PgError{Code: "28P01"},
		"pg_permission":            &pgconn.PgError{Code: "42501"},
		"plain":                    errors.New("boom"),
	} {
		answers, present := transientBit(MarkTransient(failure))
		if got := present && answers; got != IsTransient(failure) {
			t.Errorf("%s: marked = %v, IsTransient = %v", name, got, IsTransient(failure))
		}
	}
}

func TestMarkTransientDoesNotStackOnAMarkedError(t *testing.T) {
	t.Parallel()
	failure := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	once := MarkTransient(failure)
	twice := MarkTransient(once)
	if once.Error() != twice.Error() || errors.Unwrap(twice) != failure {
		t.Errorf("a second mark changed the error: once %q, twice %q, unwrap %v", once.Error(), twice.Error(), errors.Unwrap(twice))
	}
}

// One table for acp condition 1: for the same input the marked error keeps the text, the
// Unwrap chain and the Transient() answer that a worker or reconciler caller relies on.
func TestMarkTransientPreservesTextUnwrapChainAndTransientBit(t *testing.T) {
	t.Parallel()
	pgRefused := &pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}
	opErr := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	wrappedPg := fmt.Errorf("check queue_postgres: %w", pgRefused)
	for name, tc := range map[string]struct {
		in            error
		wantTransient bool
		wantIs        error
	}{
		"refused_dial":         {opErr, true, syscall.ECONNREFUSED},
		"timeout":              {fakeNetTimeout{}, true, nil},
		"wrapped_pgconn_error": {wrappedPg, true, pgRefused},
		"non_transient_error":  {fmt.Errorf("check: %w", &pgconn.PgError{Code: "28P01"}), false, nil},
	} {
		got := MarkTransient(tc.in)
		if got.Error() != tc.in.Error() {
			t.Errorf("%s: text %q, want %q", name, got.Error(), tc.in.Error())
		}
		if !errors.Is(got, tc.in) {
			t.Errorf("%s: errors.Is(marked, input) is false", name)
		}
		if tc.wantIs != nil && !errors.Is(got, tc.wantIs) {
			t.Errorf("%s: errors.Is(marked, %v) is false", name, tc.wantIs)
		}
		var pg *pgconn.PgError
		if errors.As(tc.in, &pg) != errors.As(got, &pg) {
			t.Errorf("%s: errors.As(*pgconn.PgError) differs before and after", name)
		}
		answers, present := transientBit(got)
		if tc.wantTransient && (!present || !answers) {
			t.Errorf("%s: Transient() = %v (present %v), want true", name, answers, present)
		}
		if !tc.wantTransient && present {
			t.Errorf("%s: carries a Transient() method, want none", name)
		}
		if tc.wantTransient {
			if unwrapped := errors.Unwrap(got); unwrapped != tc.in {
				t.Errorf("%s: Unwrap() = %v, want the input", name, unwrapped)
			}
		}
	}
}
