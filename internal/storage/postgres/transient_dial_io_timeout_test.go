package postgres

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
)

type ioTimeoutFake struct{}

func (ioTimeoutFake) Error() string   { return "i/o timeout" }
func (ioTimeoutFake) Timeout() bool   { return true }
func (ioTimeoutFake) Temporary() bool { return false }

// The startup retry loops (IsTransientDialError) keep retrying an I/O timeout; a context deadline stays final.
func TestIsTransientDialErrorKeepsRetryingIOTimeouts(t *testing.T) {
	t.Parallel()
	for name, err := range map[string]error{
		"os_deadline":    os.ErrDeadlineExceeded,
		"wrapped":        fmt.Errorf("read: %w", os.ErrDeadlineExceeded),
		"net_op_error":   &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded},
		"net_error_fake": ioTimeoutFake{},
	} {
		if !IsTransientDialError(err) {
			t.Errorf("%s: IsTransientDialError = false, want true", name)
		}
	}
	if IsTransientDialError(context.DeadlineExceeded) {
		t.Error("a context deadline must stay a final error")
	}
}
