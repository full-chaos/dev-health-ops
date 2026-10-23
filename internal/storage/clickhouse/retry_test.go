package clickhouse

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type flakyConn struct {
	driver.Conn
	pingErrs, execs, pings int
	err                    error
}

func (f *flakyConn) Ping(context.Context) error {
	f.pings++
	if f.pings <= f.pingErrs {
		return f.err
	}
	return nil
}

func (f *flakyConn) Exec(context.Context, string, ...any) error {
	f.execs++
	return io.EOF
}

func TestAReadRetriesOnceOnADroppedConnection(t *testing.T) {
	f := &flakyConn{pingErrs: 1, err: io.EOF}
	if err := (retryConn{f}).Ping(context.Background()); err != nil || f.pings != 2 {
		t.Fatalf("err %v pings %d, want nil and 2", err, f.pings)
	}
	f = &flakyConn{pingErrs: 2, err: io.EOF}
	if err := (retryConn{f}).Ping(context.Background()); err == nil || f.pings != 2 {
		t.Fatalf("err %v pings %d, want the error and exactly one retry", err, f.pings)
	}
	f = &flakyConn{pingErrs: 5, err: errors.New("code 60 unknown table")}
	if err := (retryConn{f}).Ping(context.Background()); err == nil || f.pings != 1 {
		t.Fatalf("a server error must not retry: err %v pings %d", err, f.pings)
	}
}

func TestAWriteIsNeverRetried(t *testing.T) {
	f := &flakyConn{}
	if err := (retryConn{f}).Exec(context.Background(), "INSERT"); !errors.Is(err, io.EOF) || f.execs != 1 {
		t.Fatalf("err %v execs %d, want io.EOF and 1", err, f.execs)
	}
}

func TestACancelledReadIsNotRetried(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := &flakyConn{pingErrs: 5, err: io.EOF}
	_ = (retryConn{f}).Ping(ctx)
	if f.pings != 1 {
		t.Fatalf("pings %d, want 1", f.pings)
	}
}
