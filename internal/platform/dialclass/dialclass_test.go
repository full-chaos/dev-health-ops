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
