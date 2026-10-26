package httpapi

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/valkey-io/valkey-go"
)

// DependencyFailure reports whether err says a store the request needs
// (Postgres or the Valkey counter store) cannot answer: a refused or lost
// connection, a timeout, a closed client, or a server that is down, starting
// or out of resources. The answer is a 503 the client may retry. A client
// that went away (context.Canceled) is no dependency failure. A store call
// that ran out of time (context.DeadlineExceeded) is one. Classification is by
// error type and SQLSTATE class, never by message text.
func DependencyFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code[:min(2, len(pgErr.Code))] {
		case "08", "53", "57", "58":
			return true
		}
		return false
	}
	var connectErr *pgconn.ConnectError
	var valkeyErr *valkey.ValkeyError
	var netErr net.Error
	switch {
	case errors.As(err, &connectErr),
		errors.As(err, &valkeyErr),
		errors.As(err, &netErr),
		errors.Is(err, valkey.ErrClosing),
		errors.Is(err, valkey.ErrNoAddr),
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF):
		return true
	}
	return false
}
