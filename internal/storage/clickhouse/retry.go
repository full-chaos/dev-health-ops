package clickhouse

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// retryConn retries a READ once when a pooled connection turns out to be dead (the server or an idle-scaling Cloud
// service dropped it while it sat idle). Writes are never retried: whether a dropped write landed is unknown.
type retryConn struct{ driver.Conn }

func isDroppedConn(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}

func (c retryConn) Ping(ctx context.Context) error {
	err := c.Conn.Ping(ctx)
	if err != nil && ctx.Err() == nil && isDroppedConn(err) {
		return c.Conn.Ping(ctx)
	}
	return err
}

func (c retryConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	err := c.Conn.Select(ctx, dest, query, args...)
	if err != nil && ctx.Err() == nil && isDroppedConn(err) {
		return c.Conn.Select(ctx, dest, query, args...)
	}
	return err
}

func (c retryConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	rows, err := c.Conn.Query(ctx, query, args...)
	if err != nil && ctx.Err() == nil && isDroppedConn(err) {
		return c.Conn.Query(ctx, query, args...)
	}
	return rows, err
}
