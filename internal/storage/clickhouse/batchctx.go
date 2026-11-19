package clickhouse

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// detachedBatchConn hands the driver's PrepareBatch a context that is never
// cancelled (CHAOS-14054). clickhouse-go v2.49 batch.Send starts a goroutine
// that closes the TCP connection when the batch context ends and does not wait
// for it: run late, after the caller cancelled, it closed the pooled connection
// the next batch already held. A cancelled caller still writes nothing more: a
// done context refuses PrepareBatch and Send. An in-flight Send is not
// interrupted; the driver's ReadTimeout still ends a read the server stalls.
type detachedBatchConn struct{ driver.Conn }

func (c detachedBatchConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	batch, err := c.Conn.PrepareBatch(context.WithoutCancel(ctx), query, opts...)
	if err != nil || batch == nil {
		return batch, err
	}
	return &detachedBatch{Batch: batch, ctx: ctx}, nil
}

type detachedBatch struct {
	driver.Batch
	ctx context.Context
}

func (b *detachedBatch) Send() error {
	if err := b.ctx.Err(); err != nil {
		_ = b.Batch.Abort()
		return err
	}
	return b.Batch.Send()
}
