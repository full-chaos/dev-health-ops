package benchmarking

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TestWriteOutputsReportsTheFailingStepsOwnCountOnSendAmbiguity is the #2276
// confirmation-pass P1 fix (team-lead-required live proof before any fix):
// each write<Table> function already correctly reports its OWN true row
// count on a batch.Send() ambiguity (the F1 sweep), but WriteOutputs' loop
// only added a step's count to `total` AFTER a confirmed success (`total +=
// written` runs past the `if err != nil` return) -- discarding the failing
// step's own truthful count a second time, and using the WRONG total to
// decide the total==0 fail-open-vs-partial-write branch.
//
// This test forces testops_maturity_bands' Send() to fail ambiguously after
// 3 rows were appended, with testops_metric_baselines (1 row) already
// landed and every step after maturity_bands never reached.
func TestWriteOutputsReportsTheFailingStepsOwnCountOnSendAmbiguity(t *testing.T) {
	conn := &benchmarkingSendFailingConn{failTable: "testops_maturity_bands"}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}

	outputs := Outputs{
		Baselines:     make([]BenchmarkBaselineRecord, 1),
		MaturityBands: make([]MaturityBandRecord, 3),
	}

	written, err := writer.WriteOutputs(context.Background(), outputs, "org-42")
	if err == nil {
		t.Fatal("WriteOutputs err = nil, want the forced testops_maturity_bands Send() failure to surface")
	}
	if written != 4 {
		t.Fatalf("written=%d, want 4 (1 baseline row already landed + 3 maturity-band rows this "+
			"failing step itself attempted) -- the failing step's OWN true count must not be discarded", written)
	}
	if conn.anomaliesBatchPrepared {
		t.Fatal("testops_metric_anomalies batch must never be prepared -- WriteOutputs stops at the first failure")
	}
}

// TestWriteOutputsStaysFailOpenWhenTheFirstStepFailsWithNothingWritten is the
// companion positive control: the `total+written == 0` check (not `total ==
// 0`) must still correctly identify the "nothing landed at all" case and
// keep the ordinary fail-open path, not wrap ErrPartialWrite when the very
// FIRST step fails with zero rows ever appended.
func TestWriteOutputsStaysFailOpenWhenTheFirstStepFailsWithNothingWritten(t *testing.T) {
	conn := &benchmarkingSendFailingConn{failTable: "testops_metric_baselines"}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	outputs := Outputs{Baselines: make([]BenchmarkBaselineRecord, 3)}

	written, err := writer.WriteOutputs(context.Background(), outputs, "org-42")
	if err == nil {
		t.Fatal("expected the forced Send() failure to surface")
	}
	if written != 3 {
		t.Fatalf("written=%d, want 3 -- the first (and only) step's own true count", written)
	}
}

// benchmarkingSendFailingConn/benchmarkingSendFailingBatch: PrepareBatch's
// returned batch fails Send() when its table name matches failTable,
// succeeding for every other table.
type benchmarkingSendFailingConn struct {
	failTable              string
	anomaliesBatchPrepared bool
}

func (c *benchmarkingSendFailingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errors.New("unused")
}

func (c *benchmarkingSendFailingConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if benchmarkingContainsTable(query, "testops_metric_anomalies") {
		c.anomaliesBatchPrepared = true
	}
	fail := benchmarkingContainsTable(query, c.failTable)
	return &benchmarkingSendFailingBatch{fail: fail}, nil
}

func benchmarkingContainsTable(query, table string) bool {
	for i := 0; i+len(table) <= len(query); i++ {
		if query[i:i+len(table)] == table {
			return true
		}
	}
	return false
}

type benchmarkingSendFailingBatch struct {
	fail     bool
	appended int
	sent     bool
}

func (batch *benchmarkingSendFailingBatch) Append(...any) error {
	batch.appended++
	return nil
}
func (batch *benchmarkingSendFailingBatch) Send() error {
	if batch.fail {
		return errors.New("simulated ambiguous ClickHouse Send() failure")
	}
	batch.sent = true
	return nil
}
func (batch *benchmarkingSendFailingBatch) Abort() error                  { return nil }
func (batch *benchmarkingSendFailingBatch) AppendStruct(any) error        { return errors.New("unused") }
func (batch *benchmarkingSendFailingBatch) Column(int) driver.BatchColumn { return nil }
func (batch *benchmarkingSendFailingBatch) Flush() error                  { return nil }
func (batch *benchmarkingSendFailingBatch) IsSent() bool                  { return batch.sent }
func (batch *benchmarkingSendFailingBatch) Rows() int                     { return batch.appended }
func (batch *benchmarkingSendFailingBatch) Columns() []column.Interface   { return nil }
func (batch *benchmarkingSendFailingBatch) Close() error                  { return nil }
