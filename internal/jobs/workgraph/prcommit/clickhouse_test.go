package prcommit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// stubConn is the narrow ClickHouse seam, same shape as
// issueprlinks' stubConn: answers each read in the order Loader issues it and
// captures every insert.
type stubConn struct {
	responses [][][]any
	queries   []string
	queryArgs [][]any
	inserted  [][]any
	queryErr  error
	batchErr  error
}

func (stub *stubConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	if stub.queryErr != nil {
		return nil, stub.queryErr
	}
	stub.queries = append(stub.queries, query)
	stub.queryArgs = append(stub.queryArgs, args)
	if len(stub.responses) == 0 {
		return &stubRows{}, nil
	}
	next := stub.responses[0]
	stub.responses = stub.responses[1:]
	return &stubRows{rows: next}, nil
}

func (stub *stubConn) PrepareBatch(_ context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if stub.batchErr != nil {
		return nil, stub.batchErr
	}
	return &stubBatch{conn: stub}, nil
}

type stubRows struct {
	rows   [][]any
	cursor int
}

func (rows *stubRows) Next() bool {
	if rows.cursor >= len(rows.rows) {
		return false
	}
	rows.cursor++
	return true
}

// Scan handles every destination type this package's loaders use, including
// **string for the nullable `message` column -- a nil entry in the stub row
// leaves the pointer nil, exactly like a live NULL.
func (rows *stubRows) Scan(dest ...any) error {
	row := rows.rows[rows.cursor-1]
	if len(row) != len(dest) {
		return fmt.Errorf("stub row has %d values, scan wants %d", len(row), len(dest))
	}
	for index, value := range row {
		switch target := dest[index].(type) {
		case *string:
			*target = value.(string)
		case **string:
			if value == nil {
				*target = nil
			} else {
				s := value.(string)
				*target = &s
			}
		case *uint32:
			*target = value.(uint32)
		case *int:
			*target = value.(int)
		case *float32:
			*target = value.(float32)
		case *uuid.UUID:
			*target = value.(uuid.UUID)
		case *time.Time:
			*target = value.(time.Time)
		default:
			return fmt.Errorf("stub cannot scan into %T", dest[index])
		}
	}
	return nil
}

func (rows *stubRows) Close() error                     { return nil }
func (rows *stubRows) Err() error                       { return nil }
func (rows *stubRows) ScanStruct(any) error             { return errors.New("unused") }
func (rows *stubRows) ColumnTypes() []driver.ColumnType { return nil }
func (rows *stubRows) Totals(...any) error              { return nil }
func (rows *stubRows) Columns() []string                { return nil }
func (rows *stubRows) HasData() bool                    { return len(rows.rows) > 0 }

type stubBatch struct{ conn *stubConn }

func (batch *stubBatch) Abort() error { return nil }
func (batch *stubBatch) Append(values ...any) error {
	batch.conn.inserted = append(batch.conn.inserted, values)
	return nil
}
func (batch *stubBatch) AppendStruct(any) error        { return errors.New("unused") }
func (batch *stubBatch) Column(int) driver.BatchColumn { return nil }
func (batch *stubBatch) Flush() error                  { return nil }
func (batch *stubBatch) Send() error                   { return nil }
func (batch *stubBatch) IsSent() bool                  { return true }
func (batch *stubBatch) Rows() int                     { return len(batch.conn.inserted) }
func (batch *stubBatch) Close() error                  { return nil }
func (batch *stubBatch) Columns() []column.Interface   { return nil }

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

const testOrg = "org-test"

var testRepoID = uuid.MustParse("11111111-1111-1111-1111-111111111111")

func TestNewLoaderRefusesNilConnection(t *testing.T) {
	if _, err := NewLoader(nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

func TestNewWriterRefusesNilConnection(t *testing.T) {
	if _, err := NewWriter(nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

func TestLoadRefusesEmptyOrganizationID(t *testing.T) {
	loader, err := NewLoader(&stubConn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loader.Load(context.Background(), "", Window{}); !errors.Is(err, ErrScopeRequired) {
		t.Fatalf("got %v, want ErrScopeRequired", err)
	}
}

func TestWriteRefusesEmptyOrganizationID(t *testing.T) {
	writer, err := NewWriter(&stubConn{})
	if err != nil {
		t.Fatal(err)
	}
	links := []Link{{RepoID: testRepoID, PRNumber: 1, CommitHash: "abc"}}
	if err := writer.Write(context.Background(), "", links); !errors.Is(err, ErrScopeRequired) {
		t.Fatalf("got %v, want ErrScopeRequired", err)
	}
}

func TestLoadFastPathRefusesEmptyOrganizationID(t *testing.T) {
	loader, err := NewLoader(&stubConn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loader.LoadFastPath(context.Background(), "", Window{}); !errors.Is(err, ErrScopeRequired) {
		t.Fatalf("got %v, want ErrScopeRequired", err)
	}
}

// TestLoadReadsPullRequestsThenCommits pins the read order and the nullable
// `message` handling: a NULL message row must never reach Derive as a nil
// pointer dereference, and a non-null one must arrive verbatim.
func TestLoadReadsPullRequestsThenCommits(t *testing.T) {
	conn := &stubConn{responses: [][][]any{
		{{testOrg, testRepoID, uint32(42)}},                                // pull requests
		{{testOrg, testRepoID, "abc123", "Merge pull request #42 from x"}}, // commits
	}}
	loader, err := NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}

	inputs, err := loader.Load(context.Background(), testOrg, Window{})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(inputs.PullRequests) != 1 || inputs.PullRequests[0].Number != 42 {
		t.Fatalf("unexpected pull requests: %+v", inputs.PullRequests)
	}
	if len(inputs.Commits) != 1 || inputs.Commits[0].Message != "Merge pull request #42 from x" {
		t.Fatalf("unexpected commits: %+v", inputs.Commits)
	}
	if len(conn.queries) != 2 {
		t.Fatalf("issued %d queries, want 2 (pull requests, commits): %v", len(conn.queries), conn.queries)
	}
}

// TestWriteInsertsColumnsInDeclaredOrder guards against a positional insert
// silently shifting values -- same reasoning as issueprlinks'
// TestProduceWritesAndReportsTheOutcome.
func TestWriteInsertsColumnsInDeclaredOrder(t *testing.T) {
	conn := &stubConn{}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	synced := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	links := []Link{{
		RepoID: testRepoID, PRNumber: 42, CommitHash: "abc123",
		Confidence: 0.9, Provenance: "explicit_text", Evidence: "commit_message_pr_ref",
		LastSynced: synced,
	}}

	if err := writer.Write(context.Background(), testOrg, links); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(conn.inserted) != 1 {
		t.Fatalf("inserted %d rows, want 1", len(conn.inserted))
	}
	want := []any{testRepoID, uint32(42), "abc123", float32(0.9), "explicit_text", "commit_message_pr_ref", synced, testOrg}
	for index, value := range want {
		if conn.inserted[0][index] != value {
			t.Errorf("insert column %d = %v (%T), want %v (%T)",
				index, conn.inserted[0][index], conn.inserted[0][index], value, value)
		}
	}
}

func TestWriteIsNoopForEmptyLinks(t *testing.T) {
	conn := &stubConn{}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(context.Background(), testOrg, nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(conn.inserted) != 0 {
		t.Fatalf("inserted %d rows for an empty batch, want 0", len(conn.inserted))
	}
}

func TestServiceProduceLinksEndToEnd(t *testing.T) {
	conn := &stubConn{responses: [][][]any{
		{{testOrg, testRepoID, uint32(42)}},
		{{testOrg, testRepoID, "abc123", "Merge pull request #42 from x"}},
	}}
	loader, err := NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(loader, writer, &stubDriverConn{}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	frozen := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	service.SetClock(func() time.Time { return frozen })

	outcome, err := service.ProduceLinks(context.Background(), testOrg, Window{})
	if err != nil {
		t.Fatalf("ProduceLinks: %v", err)
	}
	if outcome.LinksWritten != 1 || outcome.CommitsScanned != 1 {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if len(conn.inserted) != 1 {
		t.Fatalf("inserted %d rows, want 1", len(conn.inserted))
	}
	if got := conn.inserted[0][6]; got != frozen {
		t.Fatalf("last_synced = %v, want the clock %v", got, frozen)
	}
}

// stubDriverConn satisfies driver.Conn's fuller surface for Service's rawConn
// field. Only Query and PrepareBatch are exercised (via LoadFastPath and
// edges.WriteEdges); every other method is unused by this package's tests and
// left to the embedded nil driver.Conn, which panics if called -- a call this
// stub does not expect fails loudly rather than silently.
type stubDriverConn struct {
	driver.Conn
	inner *stubConn
}

func newStubDriverConn(responses [][][]any) *stubDriverConn {
	return &stubDriverConn{inner: &stubConn{responses: responses}}
}

func (stub *stubDriverConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return stub.inner.Query(ctx, query, args...)
}

func (stub *stubDriverConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return stub.inner.PrepareBatch(ctx, query, opts...)
}

func TestServiceProduceEdgesEndToEnd(t *testing.T) {
	authorWhen := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	rawConn := newStubDriverConn([][][]any{
		{{testRepoID, uint32(42), "abc123", float32(0.9), "explicit_text", "commit_message_pr_ref", authorWhen}},
	})
	loader, err := NewLoader(rawConn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(&stubConn{})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(loader, writer, rawConn, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	buildTime := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	service.SetClock(func() time.Time { return buildTime })

	outcome, err := service.ProduceEdges(context.Background(), testOrg, Window{})
	if err != nil {
		t.Fatalf("ProduceEdges: %v", err)
	}
	if outcome.RowsRead != 1 || outcome.EdgesWritten != 1 {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if len(rawConn.inner.inserted) != 1 {
		t.Fatalf("inserted %d edges, want 1", len(rawConn.inner.inserted))
	}
	// work_graph_edges' column order: edge_id, source_type, source_id,
	// target_type, target_id, edge_type, repo_id, provider, provenance,
	// confidence, evidence, discovered_at, last_synced, event_ts, day, org_id.
	row := rawConn.inner.inserted[0]
	if row[8] != "explicit_text" || row[9] != float32(0.9) {
		t.Fatalf("provenance/confidence not carried through: %+v", row)
	}
	if row[13] != authorWhen {
		t.Fatalf("event_ts should be the row's author_when, got %v", row[13])
	}
}
