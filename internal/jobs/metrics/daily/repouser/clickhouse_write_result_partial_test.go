package repouser

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TestWriteResultReportsTheFailingWritesOwnCountOnSendAmbiguity is the #2276
// confirmation-pass P1 fix (team-lead-required live proof before any fix):
// writeRepoMetrics/writeUserMetrics/writeCommitMetrics each correctly report
// their OWN true row count on a batch.Send() ambiguity (the F1 sweep fixed
// this innermost layer already, confirmed by the Send() branches in
// clickhouse.go). But WriteResult -- the OUTER layer that sequences the
// three writes -- hard-codes 0 for the CURRENTLY-failing write's slot
// instead of using the just-assigned named return value, discarding that
// truthful count a second time.
//
// This test forces user_metrics_daily's Send() to fail ambiguously after 3
// rows were appended (repo_metrics_daily succeeds first, commit_metrics is
// never reached). The correct behavior: userRows == 3 (the rows really
// might be on disk), matching work_graph_edges_native_executor.go's
// established idiom (`written += writtenX` BEFORE the error check, every
// time) -- not userRows == 0, which tells an operator the opposite of what
// may have happened and licenses an unsafe fail-open duplicate write.
func TestWriteResultReportsTheFailingWritesOwnCountOnSendAmbiguity(t *testing.T) {
	conn := &sendFailingConn{failTable: "user_metrics_daily"}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}

	result := Result{
		RepoMetrics: []RepoMetric{
			{RepoID: repoA, Day: day, ComputedAt: time.Now().UTC()},
		},
		UserMetrics: []UserMetric{
			{RepoID: repoA, Day: day, AuthorEmail: "alice@example.com", ComputedAt: time.Now().UTC()},
			{RepoID: repoA, Day: day, AuthorEmail: "bob@example.com", ComputedAt: time.Now().UTC()},
			{RepoID: repoA, Day: day, AuthorEmail: "carol@example.com", ComputedAt: time.Now().UTC()},
		},
		CommitMetrics: []CommitMetric{
			{RepoID: repoA, CommitHash: "c1", Day: day, AuthorEmail: "alice@example.com", ComputedAt: time.Now().UTC()},
		},
	}

	repoRows, userRows, commitRows, err := writer.WriteResult(context.Background(), result, "org-42")
	if err == nil {
		t.Fatal("WriteResult err = nil, want the forced user_metrics_daily Send() failure to surface")
	}
	if repoRows != 1 {
		t.Fatalf("repoRows=%d, want 1 (this write fully succeeded before user_metrics_daily failed)", repoRows)
	}
	if userRows != 3 {
		t.Fatalf("userRows=%d, want 3 -- the failing write's OWN true row count (3 rows were appended "+
			"before the ambiguous Send() failure) must not be discarded and reported as 0", userRows)
	}
	if commitRows != 0 {
		t.Fatalf("commitRows=%d, want 0 -- commit_metrics is never reached once user_metrics_daily fails", commitRows)
	}
	if conn.commitBatchPrepared {
		t.Fatal("commit_metrics batch must never be prepared -- WriteResult stops at the first failure")
	}
}

// sendFailingConn/sendFailingBatch mirror orgIDRecordingConn/
// orgIDRecordingBatch's exact shape (clickhouse_write_org_id_test.go), with
// one addition: PrepareBatch's returned batch fails Send() when its table
// name matches failTable, succeeding for every other table.
type sendFailingConn struct {
	failTable           string
	commitBatchPrepared bool
}

func (c *sendFailingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errors.New("unused")
}

func (c *sendFailingConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if containsTable(query, "commit_metrics") {
		c.commitBatchPrepared = true
	}
	fail := containsTable(query, c.failTable)
	return &sendFailingBatch{fail: fail}, nil
}

type sendFailingBatch struct {
	fail     bool
	appended [][]any
	sent     bool
}

func (batch *sendFailingBatch) Append(values ...any) error {
	batch.appended = append(batch.appended, values)
	return nil
}
func (batch *sendFailingBatch) Send() error {
	if batch.fail {
		return errors.New("simulated ambiguous ClickHouse Send() failure")
	}
	batch.sent = true
	return nil
}
func (batch *sendFailingBatch) Abort() error                  { return nil }
func (batch *sendFailingBatch) AppendStruct(any) error        { return errors.New("unused") }
func (batch *sendFailingBatch) Column(int) driver.BatchColumn { return nil }
func (batch *sendFailingBatch) Flush() error                  { return nil }
func (batch *sendFailingBatch) IsSent() bool                  { return batch.sent }
func (batch *sendFailingBatch) Rows() int                     { return len(batch.appended) }
func (batch *sendFailingBatch) Columns() []column.Interface   { return nil }
func (batch *sendFailingBatch) Close() error                  { return nil }
