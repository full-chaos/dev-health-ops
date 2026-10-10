package chclient

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
)

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return false }

var _ net.Error = timeoutNetError{}

// CHAOS-9126: a bound is decided by what ended the read, not by one error text or one
// shape; a caller's cancel and any other failure are no bound.
func TestBoundHitDecidesByCause(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("ClickHouse row iteration failed: %w", err) }
	for _, tc := range []struct {
		name string
		err  error
		want Bound
	}{
		{"context deadline", wrap(context.DeadlineExceeded), BoundTime},
		{"socket deadline", wrap(os.ErrDeadlineExceeded), BoundTime},
		{"a net timeout", wrap(&net.OpError{Op: "read", Err: timeoutNetError{}}), BoundTime},
		{"server 159", wrap(&chdriver.Exception{Code: 159}), BoundTime},
		{"server 160", wrap(&chdriver.Exception{Code: 160}), BoundTime},
		{"server 396", wrap(&chdriver.Exception{Code: 396}), BoundRows},
		{"server 158", wrap(&chdriver.Exception{Code: 158}), BoundRows},
		{"server 307", wrap(&chdriver.Exception{Code: 307}), BoundBytes},
		{"a caller cancel", wrap(context.Canceled), BoundNone},
		{"another server error", wrap(&chdriver.Exception{Code: 62}), BoundNone},
		{"a plain error", wrap(fmt.Errorf("connection refused")), BoundNone},
		{"nil", nil, BoundNone},
	} {
		if got, _ := BoundHit(tc.err); got != tc.want {
			t.Errorf("%s: BoundHit = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// The shape a loaded CI runner showed: the connection's read deadline ends the read
// and the driver wraps the socket's own timeout twice ("query processing: failed to
// read packet ...: read: ... i/o timeout"). The error here comes from a REAL socket
// whose peer never answers, wrapped the way clickhouse-go wraps it.
func TestBoundHitSeesARealSocketReadTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			time.Sleep(2 * time.Second)
			_ = conn.Close()
		}
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	_, readErr := bufio.NewReader(conn).ReadByte()
	if readErr == nil {
		t.Fatal("the read did not time out")
	}
	wrapped := fmt.Errorf("ClickHouse row iteration failed: %w",
		fmt.Errorf("query processing: failed to read packet from %s (conn_id=%d): %w", conn.RemoteAddr(), 1, readErr))
	if bound, _ := BoundHit(wrapped); bound != BoundTime {
		t.Errorf("a real socket read timeout (%T %v): BoundHit = %q, want %q", errors.Unwrap(errors.Unwrap(wrapped)), readErr, bound, BoundTime)
	}
}
