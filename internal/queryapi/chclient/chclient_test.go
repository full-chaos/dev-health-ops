package chclient

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"

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
