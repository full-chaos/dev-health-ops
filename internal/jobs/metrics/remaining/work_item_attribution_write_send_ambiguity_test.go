package remaining

import (
	"context"
	"errors"
	"testing"
	"time"

	chcolumn "github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TestWriteAttributionsReportsTheTrueRowCountOnSendAmbiguity is the #2276
// confirmation-pass sweep fix (found independently before launching the
// pass, same class the F1 sweep already closed fleet-wide across
// internal/jobs/metrics/daily/*): this writer's batch.Send() branch used to
// return `0, err` on an ambiguous network failure, discarding the true
// attempted row count -- ClickHouse may have already committed the insert
// server-side, and 0 tells the caller (WorkItemAttributionExecutor.
// ComputeFamily's per-repo loop) the opposite of what may have happened.
// This file was never in scope for the original sweep -- it landed via
// #2246/CHAOS-5078 on main, merged into this branch after that sweep ran.
func TestWriteAttributionsReportsTheTrueRowCountOnSendAmbiguity(t *testing.T) {
	conn := &attributionSendFailingConn{}
	writer, err := NewWorkItemAttributionClickHouseWriter(conn)
	if err != nil {
		t.Fatal(err)
	}

	rows := []WorkItemAttributionRow{
		{WorkItemID: "wi-1", Provider: "github", Source: "codeowners", IsPrimary: 1,
			Confidence: "high", Evidence: "{}", ComputedAt: time.Now().UTC(), OrgID: "org-42"},
		{WorkItemID: "wi-2", Provider: "github", Source: "codeowners", IsPrimary: 1,
			Confidence: "high", Evidence: "{}", ComputedAt: time.Now().UTC(), OrgID: "org-42"},
	}

	written, err := writer.WriteAttributions(context.Background(), rows)
	if err == nil {
		t.Fatal("WriteAttributions err = nil, want the forced Send() failure to surface")
	}
	if written != 2 {
		t.Fatalf("written=%d, want 2 -- the true attempted row count on this ambiguous Send() "+
			"failure must not be discarded and reported as 0", written)
	}
}

type attributionSendFailingConn struct{}

func (c *attributionSendFailingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errors.New("unused")
}

func (c *attributionSendFailingConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &attributionSendFailingBatch{}, nil
}

type attributionSendFailingBatch struct {
	appended [][]any
}

func (batch *attributionSendFailingBatch) Append(values ...any) error {
	batch.appended = append(batch.appended, values)
	return nil
}
func (batch *attributionSendFailingBatch) Send() error {
	return errors.New("simulated ambiguous ClickHouse Send() failure")
}
func (batch *attributionSendFailingBatch) Abort() error                  { return nil }
func (batch *attributionSendFailingBatch) AppendStruct(any) error        { return errors.New("unused") }
func (batch *attributionSendFailingBatch) Column(int) driver.BatchColumn { return nil }
func (batch *attributionSendFailingBatch) Flush() error                  { return nil }
func (batch *attributionSendFailingBatch) IsSent() bool                  { return false }
func (batch *attributionSendFailingBatch) Rows() int                     { return len(batch.appended) }
func (batch *attributionSendFailingBatch) Columns() []chcolumn.Interface { return nil }
func (batch *attributionSendFailingBatch) Close() error                  { return nil }
