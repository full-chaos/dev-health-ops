// Package connerr names a connection the server or the network dropped, by error type only, never by text. It is a
// stdlib-only leaf so the ClickHouse read retry (internal/storage/clickhouse retryConn) and the provider-sync effect
// committer (a dropped chunk write, CHAOS-14493) share ONE matcher without the committer importing the storage
// package and its dependencies. The storage conn marks the error of each INSERT call (AtWrite), so the committer
// names only a drop at the INSERT boundary (IsDroppedWrite).
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

// writeError is the error of a ClickHouse INSERT call: PrepareBatch, or Send, Flush or Close of its batch. Error() is
// the inner text unchanged and Unwrap keeps the chain.
type writeError struct{ err error }

func (err *writeError) Error() string { return err.err.Error() }

func (err *writeError) Unwrap() error { return err.err }

// AtWrite marks err as the error of an INSERT call. The ONE caller is the ClickHouse conn every service opens
// (internal/storage/clickhouse wrapConn); a read error is never marked.
func AtWrite(err error) error {
	if err == nil {
		return nil
	}
	return &writeError{err: err}
}

// IsDroppedWrite reports whether err holds an INSERT-call error (AtWrite) that is itself a dropped connection. A
// dropped read that runs before or after the INSERT is no dropped write, even when an INSERT error sits beside it in
// one errors.Join.
func IsDroppedWrite(err error) bool {
	switch wrapped := err.(type) {
	case *writeError:
		return IsDropped(wrapped.err)
	case interface{ Unwrap() error }:
		return IsDroppedWrite(wrapped.Unwrap())
	case interface{ Unwrap() []error }:
		for _, inner := range wrapped.Unwrap() {
			if IsDroppedWrite(inner) {
				return true
			}
		}
	}
	return false
}
