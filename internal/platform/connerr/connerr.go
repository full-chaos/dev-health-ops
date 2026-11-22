// Package connerr names a connection the server or the network dropped, by error type only, never by text. It is a
// stdlib-only leaf so the ClickHouse read retry (internal/storage/clickhouse retryConn) and the provider-sync effect
// committer (a dropped chunk write, CHAOS-14493) share ONE matcher without the committer importing the storage
// package and its dependencies.
package connerr

import (
	"errors"
	"io"
	"net"
	"syscall"
)

// IsDropped reports whether err is the error of a dropped connection: EOF, unexpected EOF, a closed connection, a reset
// or a broken pipe. Callers check ctx.Err() == nil first: a done context closes the connection itself, and that is no
// drop.
func IsDropped(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}
