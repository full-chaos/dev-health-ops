package issuepredges

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

// stubConn is the narrow ClickHouse seam this package's Loader/Service tests
// drive without a container -- same shape as prcommit/issueprlinks' own
// stubConn (CHAOS-5341's F4: Loader.LoadFastPath and Service.
// ProduceFastPathEdges had zero coverage; every other exercised path in this
// package is a pure function, but the loader/write seam itself was never
// hermetically driven).
type stubConn struct {
	responses [][][]any
	queries   []string
	queryArgs [][]any
	inserted  [][]any
	queryErr  error
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

// Scan handles every destination type LoadFastPath's row shape uses.
func (rows *stubRows) Scan(dest ...any) error {
	row := rows.rows[rows.cursor-1]
	if len(row) != len(dest) {
		return fmt.Errorf("stub row has %d values, scan wants %d", len(row), len(dest))
	}
	for index, value := range row {
		switch target := dest[index].(type) {
		case *string:
			*target = value.(string)
		case *uint32:
			*target = value.(uint32)
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
func (batch *stubBatch) Columns() []column.Interface   { return nil }
func (batch *stubBatch) Close() error                  { return nil }

// stubDriverConn satisfies driver.Conn's fuller surface for Service's
// rawConn field -- same pattern as prcommit's stubDriverConn. Only Query and
// PrepareBatch are exercised; every other method is left to the embedded nil
// driver.Conn, which panics if called, so an unexpected call fails loudly.
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

// TestLoaderLoadFastPathReadsTheJoin drives Loader.LoadFastPath (0% coverage
// before this test) against a stubbed fast-path join row, proving the Scan
// order matches the query's column order.
func TestLoaderLoadFastPathReadsTheJoin(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-0000000000f4")
	prCreated := time.Date(2026, 7, 15, 9, 0, 0, 0, time.UTC)
	stub := &stubConn{responses: [][][]any{
		{{repoID, "jira:F4-1", uint32(7), float32(1.0), "native", "closing_reference", prCreated}},
	}}
	loader, err := NewLoader(stub)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := loader.LoadFastPath(context.Background(), "org-f4", Window{})
	if err != nil {
		t.Fatalf("LoadFastPath: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	row := rows[0]
	if row.RepoID != repoID || row.WorkItemID != "jira:F4-1" || row.PRNumber != 7 ||
		row.Confidence != 1.0 || row.Provenance != "native" || row.Evidence != "closing_reference" ||
		!row.PRCreatedAt.Equal(prCreated) {
		t.Fatalf("unexpected row: %+v", row)
	}
	if len(stub.queries) != 1 || len(stub.queryArgs[0]) != 1 {
		t.Fatalf("expected exactly one query with one arg (org_id), got queries=%v args=%v", stub.queries, stub.queryArgs)
	}
}

// TestLoaderLoadFastPathRequiresScope proves ErrScopeRequired fires without
// ever reaching the connection -- the same contract LoadHeuristicInputs/
// LoadTextParseInputs document, now actually exercised for LoadFastPath too.
func TestLoaderLoadFastPathRequiresScope(t *testing.T) {
	loader, err := NewLoader(&stubConn{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loader.LoadFastPath(context.Background(), "", Window{}); !errors.Is(err, ErrScopeRequired) {
		t.Fatalf("LoadFastPath(\"\") error = %v, want ErrScopeRequired", err)
	}
}

// TestServiceProduceFastPathEdgesEndToEnd drives Service.ProduceFastPathEdges
// (0% coverage before this test) through a full stubbed read+derive+write
// round trip: Loader.LoadFastPath -> DeriveFastPathEdges -> edges.WriteEdges.
func TestServiceProduceFastPathEdgesEndToEnd(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-4000-8000-0000000000f5")
	prCreated := time.Date(2026, 7, 15, 9, 0, 0, 0, time.UTC)
	rawConn := newStubDriverConn([][][]any{
		{{repoID, "jira:F4-2", uint32(9), float32(0.9), "native", "closing_reference", prCreated}},
	})
	loader, err := NewLoader(rawConn)
	if err != nil {
		t.Fatal(err)
	}
	linksWriter, err := issueprlinks.NewWriter(&issueprlinksStubConn{})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(loader, rawConn, linksWriter, nil)
	if err != nil {
		t.Fatal(err)
	}
	buildTime := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	service.SetClock(func() time.Time { return buildTime })

	outcome, err := service.ProduceFastPathEdges(context.Background(), "org-f4", Window{})
	if err != nil {
		t.Fatalf("ProduceFastPathEdges: %v", err)
	}
	if outcome.RowsRead != 1 || outcome.EdgesWritten != 1 {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if len(rawConn.inner.inserted) != 1 {
		t.Fatalf("inserted %d edges, want 1", len(rawConn.inner.inserted))
	}
}

// issueprlinksStubConn satisfies issueprlinks.NewWriter's own conn interface
// (Query + PrepareBatch) without needing a container; ProduceFastPathEdges
// never calls the links writer, so a no-op stub is sufficient here.
type issueprlinksStubConn struct{}

func (issueprlinksStubConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return &stubRows{}, nil
}
func (issueprlinksStubConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &stubBatch{conn: &stubConn{}}, nil
}
