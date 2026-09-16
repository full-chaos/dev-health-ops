package edges

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// TestBothCleanupPathsFailLoudOnAWriteFailure pins a failure-symmetry invariant: on a
// sink that cannot execute the tombstone write, DeleteEdgesByID (the blocker-
// family cleanup) and DeleteStalePRDependencyIssueEdges (the stale PR-
// dependency cleanup) must both return a non-nil error -- never a silent
// success -- so the caller's own error handling (which logs at ERROR before
// a permanent cancel, see buildHandler.work) fires identically for both.
//
// Both retire edges the same way -- one tombstone INSERT via WriteTombstones,
// see tombstone.go's package doc comment -- so a batch failure on either path
// exercises the SAME write primitive. A sink lacking write support is
// therefore symmetric by construction: there is no separate ALTER TABLE ...
// DELETE / `sink.client.command` seam here for the two paths to disagree
// over, unlike the two Python functions this ports.
func TestBothCleanupPathsFailLoudOnAWriteFailure(t *testing.T) {
	clock := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)

	t.Run("DeleteEdgesByID", func(t *testing.T) {
		row := Row{
			EdgeID:     EdgeID(NodeTypeIssue, "gh:acme/app#1", EdgeTypeRelates, NodeTypeIssue, "gh:acme/app#2"),
			SourceType: NodeTypeIssue, SourceID: "gh:acme/app#1",
			TargetType: NodeTypeIssue, TargetID: "gh:acme/app#2",
			EdgeType: EdgeTypeRelates, Provenance: ProvenanceNative, Confidence: AssociativeConfidence,
			DiscoveredAt: clock, EventTs: clock, Day: clock, LastSynced: clock,
		}
		versions := EdgeVersions{identityOf(row): EdgeVersion{Latest: row, MaxLastSynced: clock}}
		plan := CleanupPlan{CandidateIDs: []string{row.EdgeID}}

		conn := &batchFailingConn{}
		err := DeleteEdgesByID(context.Background(), conn, testScope, plan, versions, nil, clock)
		if err == nil {
			t.Fatal("DeleteEdgesByID returned nil on a sink that cannot write; want a loud error")
		}
		if !conn.batchAttempted {
			t.Fatal("DeleteEdgesByID never reached the write -- this scenario proves nothing")
		}
	})

	t.Run("DeleteStalePRDependencyIssueEdges", func(t *testing.T) {
		conn := &batchFailingConn{staleRow: true}
		err := DeleteStalePRDependencyIssueEdges(context.Background(), conn, testScope, clock)
		if err == nil {
			t.Fatal("DeleteStalePRDependencyIssueEdges returned nil on a sink that cannot write; want a loud error")
		}
		if !conn.batchAttempted {
			t.Fatal("DeleteStalePRDependencyIssueEdges never reached the write -- this scenario proves nothing")
		}
	})
}

// batchFailingConn is a driver.Conn whose PrepareBatch succeeds (so the
// caller believes it has a batch to fill) but whose Append always fails,
// modelling a sink that cannot execute the write at all -- the closest
// analogue in this port's tombstone-based design to Python's
// `sink.client.command` being unavailable.
type batchFailingConn struct {
	driver.Conn
	// staleRow, when true, makes Query return one row shaped as the stale
	// PR-dependency identity DeleteStalePRDependencyIssueEdges's own read
	// targets, so its write step is actually reached.
	staleRow       bool
	batchAttempted bool
}

func (c *batchFailingConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	if !c.staleRow {
		return &emptyRows{}, nil
	}
	return &staleShapedRows{remaining: 1}, nil
}

func (c *batchFailingConn) PrepareBatch(
	_ context.Context, _ string, _ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	return &failingBatch{conn: c}, nil
}

type emptyRows struct{ driver.Rows }

func (*emptyRows) Next() bool   { return false }
func (*emptyRows) Close() error { return nil }
func (*emptyRows) Err() error   { return nil }

// staleShapedRows yields one row matching edgeVersionsSQL's column order,
// shaped as the stale PR-dependency identity (isStalePRDependency): a
// mislabelled source_type='issue' PR source, a linear_attachment evidence
// tag, and a Linear-prefixed target.
type staleShapedRows struct {
	driver.Rows
	remaining int
}

func (r *staleShapedRows) Next() bool {
	if r.remaining <= 0 {
		return false
	}
	r.remaining--
	return true
}
func (*staleShapedRows) Close() error { return nil }
func (*staleShapedRows) Err() error   { return nil }
func (*staleShapedRows) Scan(dest ...any) error {
	// source_type, source_id, edge_type, target_type, target_id,
	// edge_id, repo_id, provider, provenance, confidence, evidence,
	// discovered_at, event_ts, day, is_deleted, max_last_synced
	if len(dest) != 16 {
		return errors.New("staleShapedRows: unexpected column count")
	}
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	*dest[0].(*string) = NodeTypeIssue
	*dest[1].(*string) = "ghpr:acme/app#1"
	*dest[2].(*string) = EdgeTypeRelates
	*dest[3].(*string) = NodeTypeIssue
	*dest[4].(*string) = "linear:ACME-1"
	*dest[5].(*string) = "stale-ghpr-source"
	*dest[8].(*string) = ProvenanceNative
	*dest[9].(*float32) = AssociativeConfidence
	*dest[10].(*string) = "linear_attachment"
	*dest[11].(*time.Time) = clock
	*dest[12].(*time.Time) = clock
	*dest[13].(*time.Time) = clock
	*dest[14].(*uint8) = 0
	*dest[15].(*time.Time) = clock
	return nil
}

// failingBatch accepts Append (so the row count the caller believes it built
// is real) but fails on Send -- the point at which a sink lacking write
// support would actually surface its inability to execute the statement.
type failingBatch struct {
	driver.Batch
	conn *batchFailingConn
}

func (b *failingBatch) Append(...any) error {
	b.conn.batchAttempted = true
	return nil
}
func (b *failingBatch) Send() error                   { return errors.New("sink cannot execute this write") }
func (b *failingBatch) Abort() error                  { return nil }
func (b *failingBatch) Flush() error                  { return nil }
func (b *failingBatch) Rows() int                     { return 0 }
func (b *failingBatch) Close() error                  { return nil }
func (b *failingBatch) Column(int) driver.BatchColumn { return nil }
func (b *failingBatch) Columns() []column.Interface   { return nil }
func (b *failingBatch) IsSent() bool                  { return false }
