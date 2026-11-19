package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DefaultBatchWriteBound is the total bound of one batch when Config.BatchWriteBound
// is unset. NOT a measured p99 (no prod measure yet): it is the longest
// ReadTimeout an ops tool sets for one read (operationalbackfill, usersbackfill),
// so no batch that worked before is cut off by it.
const DefaultBatchWriteBound = 10 * time.Minute

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
	bound := c.bound
	if bound <= 0 {
		bound = DefaultBatchWriteBound
	}
	deadline, cause := time.Now().Add(bound), ErrBatchWriteBound
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline, cause = callerDeadline, context.DeadlineExceeded
	}
	driverCtx, cancel := context.WithDeadlineCause(context.WithoutCancel(ctx), deadline, cause)
	batch, err := c.Conn.PrepareBatch(driverCtx, query, opts...)
	if err != nil || batch == nil {
		err = boundErr(driverCtx, err)
		cancel()
		return batch, err
	}
	return &detachedBatch{Batch: batch, ctx: ctx, driverCtx: driverCtx, cancel: cancel}, nil
}

type detachedBatch struct {
	driver.Batch
	ctx       context.Context
	driverCtx context.Context
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
	err := boundErr(b.driverCtx, b.Batch.Send())
	b.release()
	return err
}

func (b *detachedBatch) Flush() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}
	return boundErr(b.driverCtx, b.Batch.Flush())
}

func (b *detachedBatch) Abort() error {
	err := b.Batch.Abort()
	b.release()
	return err
}

func (b *detachedBatch) Close() error {
	err := boundErr(b.driverCtx, b.Batch.Close())
	b.release()
	return err
}

// boundErr names the bound that ended a driver call: ErrBatchWriteBound, or the
// caller's own deadline.
func boundErr(driverCtx context.Context, err error) error {
	if err == nil || !errors.Is(driverCtx.Err(), context.DeadlineExceeded) {
		return err
	}
	cause := context.Cause(driverCtx)
	if errors.Is(err, cause) {
		return err
	}
	return fmt.Errorf("%w: %w", cause, err)
}
