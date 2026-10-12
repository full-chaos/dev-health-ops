package clickhouse

import (
	"context"
	"errors"
	"strconv"
	"sync"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/platform/dbspan"
)

// spanConn decorates a driver.Conn with slow-call client spans (CHAOS-9364).
// It embeds the interface, so every method it does not override forwards
// untouched (the driver documents embedding as the supported way to decorate).
//
// What a span covers: Select, Exec and AsyncInsert cover the whole call. Query
// and QueryRow cover time until the first response (the driver returns before
// rows are streamed), so a slow row scan is not counted. PrepareBatch covers
// PrepareBatch through Send or Abort of the returned Batch; a batch that is
// never sent or aborted emits nothing.
type spanConn struct {
	driver.Conn
	spans dbspan.Emitter
}

func newSpanConn(conn driver.Conn, spans dbspan.Emitter) driver.Conn {
	return &spanConn{Conn: conn, spans: spans}
}

func (c *spanConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	probe := c.spans.Start(ctx)
	err := c.Conn.Select(ctx, dest, query, args...)
	probe.Finish(ctx, query, "", err, chErrorClass(err))
	return err
}

func (c *spanConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	probe := c.spans.Start(ctx)
	rows, err := c.Conn.Query(ctx, query, args...)
	probe.Finish(ctx, query, "", err, chErrorClass(err))
	return rows, err
}

func (c *spanConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	probe := c.spans.Start(ctx)
	row := c.Conn.QueryRow(ctx, query, args...)
	var err error
	if row != nil {
		err = row.Err()
	}
	probe.Finish(ctx, query, "", err, chErrorClass(err))
	return row
}

func (c *spanConn) Exec(ctx context.Context, query string, args ...any) error {
	probe := c.spans.Start(ctx)
	err := c.Conn.Exec(ctx, query, args...)
	probe.Finish(ctx, query, "", err, chErrorClass(err))
	return err
}

func (c *spanConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	probe := c.spans.Start(ctx)
	err := c.Conn.AsyncInsert(ctx, query, wait, args...)
	probe.Finish(ctx, query, "", err, chErrorClass(err))
	return err
}

func (c *spanConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	probe := c.spans.Start(ctx)
	batch, err := c.Conn.PrepareBatch(ctx, query, opts...)
	if err != nil || batch == nil {
		probe.Finish(ctx, query, dbspan.OpBatch, err, chErrorClass(err))
		return batch, err
	}
	if !probe.Active() {
		return batch, nil
	}
	return &spanBatch{Batch: batch, probe: probe, ctx: ctx, query: query}, nil
}

// spanBatch ends the PrepareBatch span at Send (with its error) or at Abort
// (error-free: an abort is the caller's choice, so only the threshold rule
// applies). The span is emitted at most once.
type spanBatch struct {
	driver.Batch
	probe dbspan.Probe
	ctx   context.Context
	query string
	once  sync.Once
}

func (b *spanBatch) finish(err error) {
	b.once.Do(func() { b.probe.Finish(b.ctx, b.query, dbspan.OpBatch, err, chErrorClass(err)) })
}

func (b *spanBatch) Send() error {
	err := b.Batch.Send()
	b.finish(err)
	return err
}

func (b *spanBatch) Abort() error {
	err := b.Batch.Abort()
	b.finish(nil)
	return err
}

// chErrorClass is a bounded class: the numeric server exception code
// ("ch_60"), never its message, or the generic class.
func chErrorClass(err error) string {
	var exception *chdriver.Exception
	if errors.As(err, &exception) && exception != nil {
		return "ch_" + strconv.Itoa(int(exception.Code))
	}
	return dbspan.ErrorClass(err)
}
