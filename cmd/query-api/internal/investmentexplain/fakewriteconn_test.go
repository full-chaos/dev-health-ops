package investmentexplain

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// capturingBatch is a driver.Batch that records every Append call's
// arguments and nothing else -- used to verify WriteInvestmentExplanation/
// WriteLLMTokenUsage build the right row without a live ClickHouse
// (bigboy testcontainer pause).
type capturingBatch struct {
	appended [][]any
}

func (b *capturingBatch) Abort() error                  { return nil }
func (b *capturingBatch) Append(v ...any) error         { b.appended = append(b.appended, v); return nil }
func (b *capturingBatch) AppendStruct(v any) error      { return nil }
func (b *capturingBatch) Column(int) driver.BatchColumn { return nil }
func (b *capturingBatch) Flush() error                  { return nil }
func (b *capturingBatch) Send() error                   { return nil }
func (b *capturingBatch) IsSent() bool                  { return true }
func (b *capturingBatch) Rows() int                     { return len(b.appended) }
func (b *capturingBatch) Columns() []column.Interface   { return nil }
func (b *capturingBatch) Close() error                  { return nil }

// capturingWriteConn is a writeConn that hands out one capturingBatch per
// PrepareBatch call, keyed by the query text, so a test can inspect what
// was appended to a specific INSERT.
type capturingWriteConn struct {
	batches map[string]*capturingBatch
}

func newCapturingWriteConn() *capturingWriteConn {
	return &capturingWriteConn{batches: map[string]*capturingBatch{}}
}

func (c *capturingWriteConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	batch := &capturingBatch{}
	c.batches[query] = batch
	return batch, nil
}
