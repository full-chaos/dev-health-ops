package httpapi

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
	"github.com/valkey-io/valkey-go"
)

// DependencyFailure reports whether err says a store the request needs
// (Postgres or the Valkey counter store) cannot answer: a refused or lost
// connection, a timeout, a closed client or pool, or a server that is down, starting
// or out of resources. A Valkey error reply counts only when its code says the
// server is not available (LOADING, BUSY, MASTERDOWN, CLUSTERDOWN, TRYAGAIN,
// READONLY); every other reply is a command error and no outage. The answer is a 503 the client may retry. A client
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
	var valkeyErr *valkey.ValkeyError
	if errors.As(err, &valkeyErr) {
		return valkeyAvailabilityReply(valkeyErr)
	}
	var connectErr *pgconn.ConnectError
	var netErr net.Error
	switch {
	case errors.As(err, &connectErr),
		errors.As(err, &netErr),
		errors.Is(err, valkey.ErrClosing),
		errors.Is(err, puddle.ErrClosedPool),
		errors.Is(err, valkey.ErrNoAddr),
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF):
		return true
	}
	return false
}

// valkeyAvailabilityReply reports whether the reply code (its first word, as
// the server sends it) says the server cannot serve the command now.
func valkeyAvailabilityReply(e *valkey.ValkeyError) bool {
	if e.IsLoading() || e.IsTryAgain() || e.IsClusterDown() {
		return true
	}
	code, _, _ := strings.Cut(e.Error(), " ")
	switch code {
	case "BUSY", "MASTERDOWN", "READONLY":
		return true
	}
	return false
}
