package edges

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment"
)

const testScope = "70d529e0-3c06-4597-8480-794fd02328b6"

// TestWrittenEdgesCarryTheRunsScope is a REGRESSION test for a defect this
// package shipped and I found by tracing the field rather than trusting the
// comment on it.
//
// DeriveIssueIssueEdges sets Row.OrgID to "" and its comment said the writer
// stamped the scope. The writer did not: it appended row.OrgID, so every edge
// would have been inserted with org_id=”. That is Python's gate-4 hazard --
// rows untargetable by any later scoped delete -- except reached on a correctly
// scoped run rather than an unscoped one, so no scope guard anywhere would have
// caught it.
//
// It asserts the value handed to the batch, not the value on the struct,
// because the struct was right and the argument was wrong.
func TestWrittenEdgesCarryTheRunsScope(t *testing.T) {
	conn := &scopeStubConn{}
	rows := []Row{{EdgeID: "a", EdgeType: "blocks", Confidence: 1.0}}
	if _, err := WriteEdges(context.Background(), conn, testScope, rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(conn.inserted) != 1 {
		t.Fatalf("expected one appended row, got %d", len(conn.inserted))
	}
	values := conn.inserted[0]
	orgID, ok := values[len(values)-1].(string)
	if !ok {
		t.Fatalf("last insert column is %T, not the org id string", values[len(values)-1])
	}
	if orgID == "" {
		t.Fatal("edge written with an empty org id: the row would be invisible to every " +
			"scoped query and untargetable by every scoped delete")
	}
	if orgID != testScope {
		t.Fatalf("edge written under org %q, run was scoped to %q", orgID, testScope)
	}
}

// TestBothDatabasePathsRefuseAnUnscopedRun. The read and the write are guarded
// separately because they fail differently: an unscoped read returns every
// tenant's rows, while a write under a scope that merely LOOKS valid stamps
// real edges under a tenant that does not exist.
func TestBothDatabasePathsRefuseAnUnscopedRun(t *testing.T) {
	for _, scope := range []struct{ name, value string }{
		{"empty", ""},
		{"blank", "   "},
		// Python's str.strip() removes this; Go's unicode.IsSpace does not. The
		// shared guard handles it, and this asserts we actually defer to it.
		{"unit separator", "\x1f"},
		{"nil uuid", nilOrganizationID},
		{"nil uuid uppercase", "00000000-0000-0000-0000-000000000000"},
	} {
		t.Run(scope.name, func(t *testing.T) {
			conn := &scopeStubConn{}
			if _, err := ReadDependencies(context.Background(), conn, scope.value); err == nil {
				t.Error("read accepted an unscoped run")
			} else if !errors.Is(err, investment.ErrOrganizationScopeRequired) {
				t.Errorf("read refused with %v, not the shared scope error", err)
			}
			if _, err := WriteEdges(context.Background(), conn, scope.value,
				[]Row{{EdgeID: "a", Confidence: 1.0}}); err == nil {
				t.Error("write accepted an unscoped run")
			}
			// The guard must run BEFORE any statement, not after one came back
			// empty. A refusal that still issued the query has already leaked
			// the read across tenants.
			if len(conn.queries) != 0 || conn.prepared != "" {
				t.Errorf("a statement was issued despite refusal: %d queries, prepared=%q",
					len(conn.queries), conn.prepared)
			}
		})
	}
}

// TestAnEmptyWriteIsStillScopeChecked. The length check is after the guard on
// purpose: otherwise the guard's coverage would depend on how many edges the
// derivation happened to produce, and a bad scope would pass on any quiet org.
func TestAnEmptyWriteIsStillScopeChecked(t *testing.T) {
	if _, err := WriteEdges(context.Background(), &scopeStubConn{}, "", nil); err == nil {
		t.Fatal("a zero-row write under an empty scope was accepted")
	}
}

func TestAValidScopeIsAccepted(t *testing.T) {
	if err := requireEdgeScope(testScope); err != nil {
		t.Fatalf("a real org id was refused: %v", err)
	}
}

// --- narrow ClickHouse stub: records what was issued, executes nothing ---

type scopeStubConn struct {
	driver.Conn
	queries  []string
	prepared string
	inserted [][]any
}

func (stub *scopeStubConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	stub.queries = append(stub.queries, query)
	return &scopeStubRows{}, nil
}

func (stub *scopeStubConn) PrepareBatch(
	_ context.Context, query string, _ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	stub.prepared = query
	return &scopeStubBatch{conn: stub}, nil
}

type scopeStubRows struct{ driver.Rows }

func (rows *scopeStubRows) Next() bool    { return false }
func (rows *scopeStubRows) Close() error  { return nil }
func (rows *scopeStubRows) Err() error    { return nil }
func (rows *scopeStubRows) HasData() bool { return false }

type scopeStubBatch struct {
	driver.Batch
	conn *scopeStubConn
}

func (batch *scopeStubBatch) Append(values ...any) error {
	batch.conn.inserted = append(batch.conn.inserted, values)
	return nil
}
func (batch *scopeStubBatch) Send() error                   { return nil }
func (batch *scopeStubBatch) Abort() error                  { return nil }
func (batch *scopeStubBatch) Flush() error                  { return nil }
func (batch *scopeStubBatch) Rows() int                     { return len(batch.conn.inserted) }
func (batch *scopeStubBatch) Close() error                  { return nil }
func (batch *scopeStubBatch) Column(int) driver.BatchColumn { return nil }
func (batch *scopeStubBatch) Columns() []column.Interface   { return nil }
func (batch *scopeStubBatch) IsSent() bool                  { return true }
