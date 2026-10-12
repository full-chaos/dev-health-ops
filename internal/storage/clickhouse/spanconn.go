package clickhouse

import (
	"context"
	"errors"
	"io"
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
	if err != nil || rows == nil || !probe.Active() {
		probe.Finish(ctx, query, "", err, chErrorClass(err))
		return rows, err
	}
	return &spanRows{Rows: rows, finishOnce: newOnce(probe, ctx, query)}, nil
}

func (c *spanConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	probe := c.spans.Start(ctx)
	row := c.Conn.QueryRow(ctx, query, args...)
	if row == nil || !probe.Active() {
		return row
	}
	if err := row.Err(); err != nil {
		probe.Finish(ctx, query, "", err, chErrorClass(err))
		return row
	}
	return &spanRow{Row: row, finishOnce: newOnce(probe, ctx, query)}
}

func (c *spanConn) InsertFormat(ctx context.Context, format string, query string, data io.Reader) error {
	probe := c.spans.Start(ctx)
	err := c.Conn.InsertFormat(ctx, format, query, data)
	probe.Finish(ctx, query, "", err, chErrorClass(err))
	return err
}

func (c *spanConn) QueryFormat(ctx context.Context, format string, query string, args ...any) (io.ReadCloser, error) {
	probe := c.spans.Start(ctx)
	stream, err := c.Conn.QueryFormat(ctx, format, query, args...)
	if err != nil || stream == nil || !probe.Active() {
		probe.Finish(ctx, query, "", err, chErrorClass(err))
		return stream, err
	}
	return &spanStream{ReadCloser: stream, finishOnce: newOnce(probe, ctx, query)}, nil
}

// finishOnce ends one probe at most once, whichever of Close / Scan / Send
// reaches it first.
type finishOnce struct {
	probe dbspan.Probe
	ctx   context.Context
	query string
	once  sync.Once
}

func newOnce(probe dbspan.Probe, ctx context.Context, query string) *finishOnce {
	return &finishOnce{probe: probe, ctx: ctx, query: query}
}

func (f *finishOnce) finish(op string, err error) {
	f.once.Do(func() { f.probe.Finish(f.ctx, f.query, op, err, chErrorClass(err)) })
}

// spanRows ends the Query span at Close, so the span covers row streaming and
// carries an error surfaced by Rows.Err. Rows that are never closed emit
// nothing (database/sql-style callers always Close).
type spanRows struct {
	driver.Rows
	*finishOnce
}

func (r *spanRows) Close() error {
	closeErr := r.Rows.Close()
	err := r.Rows.Err()
	if err == nil {
		err = closeErr
	}
	r.finish("", err)
	return closeErr
}

// spanRow ends the QueryRow span at Scan / ScanStruct.
type spanRow struct {
	driver.Row
	*finishOnce
}

func (r *spanRow) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)
	r.finish("", err)
	return err
}

func (r *spanRow) ScanStruct(dest any) error {
	err := r.Row.ScanStruct(dest)
	r.finish("", err)
	return err
}

// spanStream ends the QueryFormat span at Close, with the first read error.
type spanStream struct {
	io.ReadCloser
	*finishOnce
	readErr error
}

func (s *spanStream) Read(p []byte) (int, error) {
	n, err := s.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && s.readErr == nil {
		s.readErr = err
	}
	return n, err
}

func (s *spanStream) Close() error {
	err := s.ReadCloser.Close()
	if s.readErr != nil {
		err = s.readErr
	}
	s.finish("", err)
	return err
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
