package repouser

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TestWriteResultWritesRealOrgIDNotEmpty is CHAOS-4341's red-on-baseline
// proof: before this ticket's fix, writeRepoMetrics/writeUserMetrics/
// writeCommitMetrics hard-coded "" as the last (org_id) Append() argument on
// all three tables regardless of what org the partition belonged to. This
// test fails against that code (asserting "" == orgID, a mismatch) and
// passes once WriteResult threads the real org_id through to every row.
func TestWriteResultWritesRealOrgIDNotEmpty(t *testing.T) {
	const orgID = "c6a38355-dad6-42e4-8cc9-4c712450827d"
	conn := &orgIDRecordingConn{}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}

	result := Result{
		RepoMetrics:   []RepoMetric{{RepoID: repoA, Day: day, ComputedAt: time.Now().UTC()}},
		UserMetrics:   []UserMetric{{RepoID: repoA, Day: day, AuthorEmail: "alice@example.com", ComputedAt: time.Now().UTC()}},
		CommitMetrics: []CommitMetric{{RepoID: repoA, CommitHash: "c1", Day: day, AuthorEmail: "alice@example.com", ComputedAt: time.Now().UTC()}},
	}

	repoRows, userRows, commitRows, err := writer.WriteResult(context.Background(), result, orgID)
	if err != nil {
		t.Fatal(err)
	}
	if repoRows != 1 || userRows != 1 || commitRows != 1 {
		t.Fatalf("rows written repo=%d user=%d commit=%d, want 1/1/1", repoRows, userRows, commitRows)
	}

	for table, batch := range map[string]*orgIDRecordingBatch{
		"repo_metrics_daily": conn.repoBatch,
		"user_metrics_daily": conn.userBatch,
		"commit_metrics":     conn.commitBatch,
	} {
		if batch == nil || len(batch.appended) != 1 {
			t.Fatalf("%s: expected exactly one appended row, got %v", table, batch)
		}
		gotOrgID, ok := batch.appended[0][len(batch.appended[0])-1].(string)
		if !ok {
			t.Fatalf("%s: last append argument is not a string: %#v", table, batch.appended[0][len(batch.appended[0])-1])
		}
		if gotOrgID != orgID {
			t.Fatalf("%s: org_id column = %q, want the partition's real org_id %q (not empty)", table, gotOrgID, orgID)
		}
	}
}

// TestWriteResultRejectsEmptyOrgID: the writer must fail closed rather than
// silently repeat CHAOS-4341 -- an empty org_id must never reach ClickHouse
// again, not even via a caller that forgot to plumb run.OrganizationID
// through.
func TestWriteResultRejectsEmptyOrgID(t *testing.T) {
	conn := &orgIDRecordingConn{}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	result := Result{RepoMetrics: []RepoMetric{{RepoID: repoA, Day: day, ComputedAt: time.Now().UTC()}}}
	if _, _, _, err := writer.WriteResult(context.Background(), result, ""); err == nil {
		t.Fatal("expected an error for empty orgID, got nil")
	}
	if conn.repoBatch != nil {
		t.Fatal("expected no ClickHouse write attempt for an empty orgID")
	}
}

// orgIDRecordingConn/orgIDRecordingBatch capture exactly what WriteResult
// appends per table, mirroring wellbeing_native_clickhouse_test.go's
// recordingBatchConn/recordingBatch pattern in the sibling daily package.
type orgIDRecordingConn struct {
	repoBatch, userBatch, commitBatch *orgIDRecordingBatch
}

func (c *orgIDRecordingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errors.New("unused")
}

func (c *orgIDRecordingConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	batch := &orgIDRecordingBatch{}
	switch {
	case containsTable(query, "repo_metrics_daily"):
		c.repoBatch = batch
	case containsTable(query, "user_metrics_daily"):
		c.userBatch = batch
	case containsTable(query, "commit_metrics"):
		c.commitBatch = batch
	}
	return batch, nil
}

func containsTable(query, table string) bool {
	for i := 0; i+len(table) <= len(query); i++ {
		if query[i:i+len(table)] == table {
			return true
		}
	}
	return false
}

type orgIDRecordingBatch struct {
	appended [][]any
	sent     bool
}

func (batch *orgIDRecordingBatch) Append(values ...any) error {
	batch.appended = append(batch.appended, values)
	return nil
}
func (batch *orgIDRecordingBatch) Send() error                   { batch.sent = true; return nil }
func (batch *orgIDRecordingBatch) Abort() error                  { return nil }
func (batch *orgIDRecordingBatch) AppendStruct(any) error        { return errors.New("unused") }
func (batch *orgIDRecordingBatch) Column(int) driver.BatchColumn { return nil }
func (batch *orgIDRecordingBatch) Flush() error                  { return nil }
func (batch *orgIDRecordingBatch) IsSent() bool                  { return batch.sent }
func (batch *orgIDRecordingBatch) Rows() int                     { return len(batch.appended) }
func (batch *orgIDRecordingBatch) Columns() []column.Interface   { return nil }
func (batch *orgIDRecordingBatch) Close() error                  { return nil }
