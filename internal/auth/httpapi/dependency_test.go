package httpapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/valkey-io/valkey-go"
)

var errStore = errors.New("store unavailable")

func TestDependencyFailureClassifiesEachStoreErrorClass(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"pg connection exception", &pgconn.PgError{Code: "08006"}, true},
		{"pg out of resources", &pgconn.PgError{Code: "53300"}, true},
		{"pg shutting down", &pgconn.PgError{Code: "57P01"}, true},
		{"pg i/o error", &pgconn.PgError{Code: "58030"}, true},
		{"pg unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"pg syntax error", &pgconn.PgError{Code: "42601"}, false},
		{"pg empty code", &pgconn.PgError{}, false},
		{"pg connect error", &pgconn.ConnectError{}, true},
		{"pg wrapped dial", fmt.Errorf("%w: load: %w", errStore, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}), true},
		{"net timeout", &net.DNSError{IsTimeout: true}, true},
		{"unexpected eof", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), true},
		{"valkey closing", fmt.Errorf("rate limit script: %w", valkey.ErrClosing), true},
		{"valkey no address", valkey.ErrNoAddr, true},
		{"valkey server error", &valkey.ValkeyError{}, true},
		{"store deadline", fmt.Errorf("rate limit peek: %w", context.DeadlineExceeded), true},
		{"client cancel", fmt.Errorf("query: %w", context.Canceled), false},
		{"cancel inside a connect error", fmt.Errorf("%w", errors.Join(context.Canceled, &net.OpError{Op: "dial"})), false},
		{"plain error", errors.New("dial tcp: connection refused"), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := DependencyFailure(c.err); got != c.want {
				t.Fatalf("DependencyFailure(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}
