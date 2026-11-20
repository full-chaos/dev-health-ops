package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DefaultBatchWriteBound is the floor of a batch's total bound when
// Config.BatchWriteBound is unset; the bound is then max(this, ReadTimeout).
// Measured prod ClickHouse BATCH spans (SigNoz, 7 d to 2026-11-19): p99 0.35 s,
// max 4.27 s (dbspan slow tail only, so conservative).
const DefaultBatchWriteBound = 15 * time.Second

// batchWriteBound is the bound a connection's batches get: the configured one,
// else max(DefaultBatchWriteBound, ReadTimeout), so a tool that allows long reads
// keeps that budget for one batch.
func batchWriteBound(configured, readTimeout time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return max(DefaultBatchWriteBound, readTimeout)
}

// batchWatchdogGrace is how long a batch's driver context stays uncancelled after
// Send returns, so the driver's late Send watchdog finds it still live.
const batchWatchdogGrace = 30 * time.Second

// ErrBatchWriteBound marks a batch ended by its own write bound.
var ErrBatchWriteBound = errors.New("clickhouse batch write bound exceeded")

// detachedBatchConn hands the driver's PrepareBatch a context that the caller
// cannot cancel (CHAOS-14054). clickhouse-go v2.49 batch.Send starts a goroutine
// that closes the TCP connection when the batch context ends and does not wait
// for it: run late, after the caller cancelled, it closed the pooled connection
// the next batch already held. The driver context keeps the caller's values and
// has its own deadline: min(caller deadline, bound). It is cancelled only
// batchWatchdogGrace after Send returns (or at once when no Send ran), never right
// after Send. A done caller context refuses PrepareBatch, Flush and Send.
type detachedBatchConn struct {
	driver.Conn
	bound time.Duration
}

func (c detachedBatchConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	deadline, cause := time.Now().Add(batchWriteBound(c.bound, 0)), ErrBatchWriteBound
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline, cause = callerDeadline, context.DeadlineExceeded
	}
	driverCtx, cancel := context.WithDeadlineCause(context.WithoutCancel(ctx), deadline, cause)
	batch, err := c.Conn.PrepareBatch(driverCtx, query, opts...)
	if err != nil || batch == nil {
		err = boundErr(driverCtx, cause, err)
		cancel()
		return batch, err
	}
	return &detachedBatch{Batch: batch, ctx: ctx, driverCtx: driverCtx, cause: cause, cancel: cancel}, nil
}

type detachedBatch struct {
	driver.Batch
	ctx       context.Context
	driverCtx context.Context
	cause     error
	cancel    context.CancelFunc
	sent      bool
}

// release cancels the driver context: at once when the driver never ran Send
// (no watchdog exists), after batchWatchdogGrace when it did.
func (b *detachedBatch) release() {
	if b.sent {
		time.AfterFunc(batchWatchdogGrace, b.cancel)
		return
	}
	b.cancel()
}

func (b *detachedBatch) Send() error {
	if err := b.ctx.Err(); err != nil {
		_ = b.Abort()
		return err
	}
	b.sent = true
	err := boundErr(b.driverCtx, b.cause, b.Batch.Send())
	b.release()
	return err
}

func (b *detachedBatch) Flush() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	return boundErr(b.driverCtx, b.cause, b.Batch.Flush())
}

func (b *detachedBatch) Abort() error {
	err := b.Batch.Abort()
	b.release()
	return err
}

func (b *detachedBatch) Close() error {
	err := boundErr(b.driverCtx, b.cause, b.Batch.Close())
	b.release()
	return err
}

// boundErr names the bound that ended a driver call: ErrBatchWriteBound, or the
// caller's own deadline. The driver sets the socket deadline to the driver
// context's deadline, so a socket deadline error at or after that deadline is the
// bound too, even when it beats the context's own timer (CHAOS-14255).
func boundErr(driverCtx context.Context, cause, err error) error {
	if err == nil || !deadlineEnded(driverCtx, err) {
		return err
	}
	if errors.Is(err, cause) {
		return err
	}
	return fmt.Errorf("%w: %w", cause, err)
}

func deadlineEnded(driverCtx context.Context, err error) bool {
	if errors.Is(driverCtx.Err(), context.DeadlineExceeded) {
		return true
	}
	deadline, ok := driverCtx.Deadline()
	return ok && errors.Is(err, os.ErrDeadlineExceeded) && !time.Now().Before(deadline)
}
