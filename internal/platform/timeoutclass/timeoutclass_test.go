package timeoutclass_test

import (
	"context"
	"errors"
	"fmt"
	"go/build"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/dialclass"
	"github.com/full-chaos/dev-health-ops/internal/platform/timeoutclass"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeNetTimeout struct{}

func (fakeNetTimeout) Error() string   { return "i/o timeout" }
func (fakeNetTimeout) Timeout() bool   { return true }
func (fakeNetTimeout) Temporary() bool { return false }

type fakeNetNoTimeout struct{}

func (fakeNetNoTimeout) Error() string   { return "reset" }
func (fakeNetNoTimeout) Timeout() bool   { return false }
func (fakeNetNoTimeout) Temporary() bool { return true }

func TestIsTimeoutTable(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"nil":                       {nil, false},
		"os_deadline":               {os.ErrDeadlineExceeded, true},
		"wrapped_os_deadline":       {fmt.Errorf("read: %w", os.ErrDeadlineExceeded), true},
		"net_op_error_os_deadline":  {&net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}, true},
		"wrapped_op_error":          {fmt.Errorf("connect: %w", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}), true},
		"net_error_fake_timeout":    {fakeNetTimeout{}, true},
		"wrapped_fake_timeout":      {fmt.Errorf("x: %w", fakeNetTimeout{}), true},
		"context_deadline":          {context.DeadlineExceeded, true},
		"wrapped_context_deadline":  {fmt.Errorf("acquire: %w", context.DeadlineExceeded), true},
		"err_timeout":               {timeoutclass.ErrTimeout, true},
		"wrapped_err_timeout":       {fmt.Errorf("x: %w", timeoutclass.ErrTimeout), true},
		"canceled":                  {context.Canceled, false},
		"canceled_over_timeout":     {errors.Join(context.Canceled, os.ErrDeadlineExceeded), false},
		"econnrefused_op_error":     {&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, false},
		"net_error_fake_no_timeout": {fakeNetNoTimeout{}, false},
		"pg_wrong_password":         {&pgconn.PgError{Code: "28P01"}, false},
		"plain":                     {errors.New("boom"), false},
	} {
		if got := timeoutclass.IsTimeout(tc.err); got != tc.want {
			t.Errorf("%s: IsTimeout(%v) = %v, want %v", name, tc.err, got, tc.want)
		}
	}
}

// ErrTimeout is what a sanitizer keeps once it drops an I/O timeout's text; the startup retry loops read
// it through dialclass, so it must stay transient there.
func TestErrTimeoutStaysTransientForTheDialRetryLoops(t *testing.T) {
	t.Parallel()
	if !dialclass.IsTransient(timeoutclass.ErrTimeout) || !dialclass.IsTransient(fmt.Errorf("x: %w", timeoutclass.ErrTimeout)) {
		t.Fatal("dialclass.IsTransient(ErrTimeout) = false, want true")
	}
	if dialclass.IsTransient(context.DeadlineExceeded) {
		t.Fatal("a context deadline must stay non-transient")
	}
}

func TestTimeoutclassIsStdlibOnly(t *testing.T) {
	t.Parallel()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := build.ImportDir(filepath.Join(root, "internal", "platform", "timeoutclass"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkg.GoFiles) == 0 {
		t.Fatal("no non-test Go files found: the guard measured nothing")
	}
	for _, path := range pkg.Imports {
		if first, _, _ := strings.Cut(path, "/"); strings.Contains(first, ".") {
			t.Errorf("timeoutclass imports %q: it must stay stdlib-only so health stays stdlib-only", path)
		}
	}
}
