package benchmarking

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/familyerr"
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
//
// Codex confirmation-pass P3 (this test's own first draft, self-caught on
// re-review): failTable alone fails at Send() time, AFTER every row in that
// table's batch has already been appended -- so a fixture with 3 rows
// reported written=3, exercising the SAME ambiguous-positive-count path as
// TestWriteOutputsReportsTheFailingStepsOwnCountOnSendAmbiguity above, never
// the total+written==0 branch this test's own doc comment claims to cover,
// and never asserting ErrPartialWrite is ABSENT. Fixed with failPrepare: a
// genuine pre-network PrepareBatch failure, nothing ever appended.
func TestWriteOutputsStaysFailOpenWhenTheFirstStepFailsWithNothingWritten(t *testing.T) {
	conn := &benchmarkingSendFailingConn{failTable: "testops_metric_baselines", failPrepare: true}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	outputs := Outputs{Baselines: make([]BenchmarkBaselineRecord, 3)}

	written, err := writer.WriteOutputs(context.Background(), outputs, "org-42")
	if err == nil {
		t.Fatal("expected the forced PrepareBatch failure to surface")
	}
	if written != 0 {
		t.Fatalf("written=%d, want 0 -- PrepareBatch never crosses the network, so nothing was "+
			"ever appended or sent", written)
	}
	if errors.Is(err, familyerr.ErrPartialWrite) {
		t.Fatalf("err = %v, must NOT wrap ErrPartialWrite -- nothing was written, the ordinary "+
			"fail-open path is still correct", err)
	}
	if conn.anomaliesBatchPrepared {
		t.Fatal("testops_metric_anomalies batch must never be prepared -- WriteOutputs stops at the first failure")
	}
}

// benchmarkingSendFailingConn/benchmarkingSendFailingBatch: PrepareBatch's
// returned batch fails Send() when its table name matches failTable,
// succeeding for every other table. If failPrepare is set instead,
// PrepareBatch itself fails for failTable -- a genuine PRE-NETWORK failure
// (nothing appended, nothing sent), unlike failTable/Send() which fails
// only after every row in that table's batch has already been appended.
type benchmarkingSendFailingConn struct {
	failTable              string
	failPrepare            bool
	anomaliesBatchPrepared bool
}

func (c *benchmarkingSendFailingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errors.New("unused")
}

func (c *benchmarkingSendFailingConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if benchmarkingContainsTable(query, "testops_metric_anomalies") {
		c.anomaliesBatchPrepared = true
	}
	matches := benchmarkingContainsTable(query, c.failTable)
	if matches && c.failPrepare {
		return nil, errors.New("simulated pre-network PrepareBatch failure")
	}
	return &benchmarkingSendFailingBatch{fail: matches}, nil
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
