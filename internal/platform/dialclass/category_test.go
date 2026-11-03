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

	"github.com/full-chaos/dev-health-ops/internal/platform/timeoutclass"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestCategoryTable(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"nil":             {nil, "none"},
		"refused":         {&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, "refused"},
		"dns":             {&net.DNSError{Err: "no such host", Name: "db.invalid"}, "dns"},
		"host_unreach":    {fmt.Errorf("dial: %w", syscall.EHOSTUNREACH), "no_route"},
		"net_unreach":     {syscall.ENETUNREACH, "no_route"},
		"reset":           {syscall.ECONNRESET, "reset"},
		"pipe":            {syscall.EPIPE, "reset"},
		"eof":             {io.EOF, "eof"},
		"unexpected_eof":  {io.ErrUnexpectedEOF, "eof"},
		"os_deadline":     {&net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}, "timeout"},
		"ctx_deadline":    {fmt.Errorf("timeout: %w", context.DeadlineExceeded), "timeout"},
		"timeout_class":   {timeoutclass.ErrTimeout, "timeout"},
		"net_timeout":     {fakeNetTimeout{}, "timeout"},
		"server_starting": {&pgconn.PgError{Code: "57P03"}, "server_starting"},
		"server_too_many": {&pgconn.PgError{Code: "53300"}, "server_unavailable"},
		"server_auth":     {&pgconn.PgError{Code: "28P01"}, "server"},
		"plain":           {errors.New("boom"), "other"},
	} {
		if got := Category(tc.err); got != tc.want {
			t.Errorf("%s: Category(%v) = %q, want %q", name, tc.err, got, tc.want)
		}
	}
}

// The category is read from the type and code only: a message that names a
// host or a password never reaches it.
func TestCategoryNeverReturnsErrorText(t *testing.T) {
	t.Parallel()
	err := fmt.Errorf("dial tcp db.example:5432 as login sekret: %w", syscall.ECONNREFUSED)
	if got := Category(err); got != "refused" {
		t.Fatalf("Category() = %q, want refused", got)
	}
}
