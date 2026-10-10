// Package chclient is the ONE constructor path of every query-API ClickHouse
// read client (CHAOS-9126, D5879): the REST routes, /query and the MCP class
// build their client from Options, so no route keeps its own result-row
// default. The reference (the Python API) set no row cap on its reads; the
// dev-health-go default of 1,000 rows was a port default no route chose.
package chclient

import (
	"context"
	"errors"
	"net"
	"os"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// MaxResultRows is the result-row bound of every query-API read: this process
// buffers a result before it writes the response. The value is /query's own
// workload derivation (4 x workgraph.MaxEdgesLimit + 100,000 = 500,000, pinned by a server test;
// PROVISIONAL, successor CHAOS-4654: re-derive from a declared memory budget).
const MaxResultRows uint = 500_000

// Options is the client options of a query-API read client: ClickHouse's own
// "unrestricted" MaxBytesToRead (a non-nil pointer to 0; nil would select the
// 64 MiB default, CHAOS-4647) and the MaxResultRows bound above.
func Options(dsn string) dhclickhouse.Options {
	maxBytesToRead := uint64(0)
	maxResultRows := MaxResultRows
	return dhclickhouse.Options{
		DSN:            dsn,
		MaxBytesToRead: &maxBytesToRead,
		MaxResultRows:  &maxResultRows,
	}
}

// New builds a query-API read client.
func New(dsn string) (*dhclickhouse.Client, error) {
	return dhclickhouse.NewClickHouseQueryClientWithOptions(Options(dsn))
}

// ClickHouse error codes that mean "a bound was hit", not "the store is down".
const (
	codeTooManyRows      = 158 // TOO_MANY_ROWS
	codeTimeoutExceeded  = 159 // TIMEOUT_EXCEEDED
	codeTooSlow          = 160 // TOO_SLOW (execution speed bound)
	codeTooManyBytes     = 307 // TOO_MANY_BYTES
	codeTooManyRowsBytes = 396 // TOO_MANY_ROWS_OR_BYTES (max_result_rows)
)

// Bound names the bound an error hit.
type Bound string

const (
	BoundNone  Bound = ""
	BoundRows  Bound = "result_rows"
	BoundBytes Bound = "bytes"
	BoundTime  Bound = "execution_time"
)

// BoundHit reports which bound a ClickHouse error hit, with its code; it is
// BoundNone for any other error.
func BoundHit(err error) (Bound, int32) {
	// The client sets the time bound itself (its own derived deadline and the
	// server's max_execution_time), so a time bound ends the read in one of
	// several shapes: the context deadline, the connection's socket deadline
	// (os.ErrDeadlineExceeded, a net.Error that says Timeout) or the server's
	// own exception (159/160, below). The shape is not the decision: a deadline
	// of any kind is the TIME BOUND when the CALLER's context is still live,
	// which is for the caller of BoundHit to say (the route knows its request).
	if isDeadline(err) {
		return BoundTime, codeTimeoutExceeded
	}
	var exception *chdriver.Exception
	if !errors.As(err, &exception) {
		return BoundNone, 0
	}
	switch exception.Code {
	case codeTooManyRows, codeTooManyRowsBytes:
		return BoundRows, exception.Code
	case codeTooManyBytes:
		return BoundBytes, exception.Code
	case codeTimeoutExceeded, codeTooSlow:
		return BoundTime, exception.Code
	}
	return BoundNone, 0
}

// isDeadline reports a deadline of any shape: a context deadline, a socket
// deadline, or any net.Error that says Timeout. A cancel is not a deadline.
func isDeadline(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var network net.Error
	return errors.As(err, &network) && network.Timeout()
}
