package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/platform/connerr"
)

// droppedDriverError has the shape of the driver error of a connection the
// server closed (the clickhouse-go first-block line over ch-go "read: EOF").
func droppedDriverError() error {
	return fmt.Errorf("query processing: failed to read first block packet from %s (conn_id=%d): %w",
		"127.0.0.1:9000", 1, fmt.Errorf("read: %w", io.EOF))
}

// droppingDriverConn drops the connection on the calls a cell names; reads
// drop every time, so retryConn's one retry drops too.
type droppingDriverConn struct {
	driver.Conn
	prepareErr                  error
	sendErr, flushErr, closeErr error
	readErr                     error
	reads                       int
}

func (c *droppingDriverConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	return droppingDriverBatch{send: c.sendErr, flush: c.flushErr, close: c.closeErr}, nil
}

func (c *droppingDriverConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	c.reads++
	return nil, c.readErr
}

func (c *droppingDriverConn) Select(context.Context, any, string, ...any) error {
	c.reads++
	return c.readErr
}

func (c *droppingDriverConn) Exec(context.Context, string, ...any) error {
	return c.readErr
}

type droppingDriverBatch struct {
	driver.Batch
	send, flush, close error
}

func (b droppingDriverBatch) Send() error  { return b.send }
func (b droppingDriverBatch) Flush() error { return b.flush }
func (b droppingDriverBatch) Close() error { return b.close }
func (b droppingDriverBatch) Abort() error { return nil }

func requireDroppedWrite(t *testing.T, call string, err, driverErr error) {
	t.Helper()
	if !connerr.IsDroppedWrite(err) || !errors.Is(err, driverErr) || err.Error() != driverErr.Error() {
		t.Errorf("%s: IsDroppedWrite=%v errors.Is(driver)=%v text %q; want a marked drop with the driver chain and text",
			call, connerr.IsDroppedWrite(err), errors.Is(err, driverErr), err)
	}
}

func requireNotDroppedWrite(t *testing.T, call string, err error) {
	t.Helper()
	if !connerr.IsDropped(err) || connerr.IsDroppedWrite(err) {
		t.Errorf("%s: IsDropped=%v IsDroppedWrite=%v; want the drop, not marked as an INSERT call",
			call, connerr.IsDropped(err), connerr.IsDroppedWrite(err))
	}
}

// The conn every service opens (wrapConn, the one OpenWith returns:
// TestEveryProductionBatchGoesThroughTheDetachedBatchWrapper) is the INSERT
// boundary: a drop on PrepareBatch or on the batch's Send, Flush or Close is
// marked connerr.AtWrite; a drop on a read (Query, Select, after retryConn's
// one retry) or an Exec is not, so the provider-sync committer
// (sinkConnectionDropped) names only a dropped INSERT.
func TestTheServiceConnMarksOnlyItsInsertCalls(t *testing.T) {
	ctx := context.Background()
	driverErr := droppedDriverError()

	_, err := wrapConn(&droppingDriverConn{prepareErr: driverErr}, time.Minute).PrepareBatch(ctx, "INSERT INTO t")
	requireDroppedWrite(t, "PrepareBatch", err, driverErr)

	for name, inner := range map[string]*droppingDriverConn{
		"Send": {sendErr: driverErr}, "Flush": {flushErr: driverErr}, "Close": {closeErr: driverErr},
	} {
		batch, err := wrapConn(inner, time.Minute).PrepareBatch(ctx, "INSERT INTO t")
		if err != nil {
			t.Fatalf("%s: PrepareBatch: %v", name, err)
		}
		switch name {
		case "Send":
			err = batch.Send()
		case "Flush":
			err = batch.Flush()
		case "Close":
			err = batch.Close()
		}
		requireDroppedWrite(t, name, err, driverErr)
	}

	inner := &droppingDriverConn{readErr: driverErr}
	conn := wrapConn(inner, time.Minute)
	_, err = conn.Query(ctx, "SELECT 1")
	requireNotDroppedWrite(t, "Query", err)
	err = conn.Select(ctx, &[]string{}, "SELECT 1")
	requireNotDroppedWrite(t, "Select", err)
	if inner.reads != 4 {
		t.Fatalf("premise: %d driver reads, want 4 (each read retried once by retryConn)", inner.reads)
	}
	requireNotDroppedWrite(t, "Exec", conn.Exec(ctx, "ALTER TABLE t DELETE WHERE 1"))

}
