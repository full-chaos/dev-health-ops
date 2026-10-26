// Package timeoutclass classifies an error as a timeout by type only, never by
// text. It is a stdlib-only leaf so the readiness registry (health) and the
// Postgres dial classifier (dialclass) share one definition without health
// importing a database driver (CHAOS-11617).
package timeoutclass

import (
	"context"
	"errors"
	"net"
	"os"
)

// ErrTimeout is the bounded cause a sanitizer keeps when it drops an I/O
// timeout's text, so the classification survives the sanitizing.
var ErrTimeout = errors.New("postgres: endpoint timed out")

// IsTimeout reports whether err is a timeout: a context deadline, an OS socket
// deadline, a net.Error that says it timed out, or ErrTimeout. A cancellation
// is never a timeout, even when it also wraps one.
func IsTimeout(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, ErrTimeout) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
