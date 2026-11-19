package syncdispatchruntime

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// snapshotTx is a pass transaction whose commit snapshot read fails with err every time. Each read runs inside a
// savepoint (Begin); the counters say how often the fence read, rolled back to and released a savepoint, and whether it
// committed the pass.
type snapshotTx struct {
	pgx.Tx
	err                                  error
	savepoints, reads, rollbacks, commit int
}

func (tx *snapshotTx) Begin(context.Context) (pgx.Tx, error) {
	tx.savepoints++
	return &snapshotSavepoint{parent: tx}, nil
}

func (tx *snapshotTx) QueryRow(context.Context, string, ...any) pgx.Row {
	tx.reads++
	return errRow{tx.err}
}

func (tx *snapshotTx) Commit(context.Context) error {
	tx.commit++
	return nil
}

type snapshotSavepoint struct {
	pgx.Tx
	parent *snapshotTx
}

func (savepoint *snapshotSavepoint) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return savepoint.parent.QueryRow(ctx, sql, args...)
}

func (savepoint *snapshotSavepoint) Rollback(context.Context) error {
	savepoint.parent.rollbacks++
	return nil
}

func (savepoint *snapshotSavepoint) Commit(context.Context) error { return nil }

type errRow struct{ err error }

func (row errRow) Scan(...any) error { return row.err }

// TestTheFencedCommitRetriesOnlyALockNotAvailable: only SQLSTATE 55P03 on the commit snapshot is read again; any other
// error (another SQLSTATE, a dropped connection, a canceled read) returns the unavailable error after ONE read, with
// nothing committed.
func TestTheFencedCommitRetriesOnlyALockNotAvailable(t *testing.T) {
	previous := commitSnapshotLockBackoff
	commitSnapshotLockBackoff = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { commitSnapshotLockBackoff = previous })
	args := DispatchSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: "org-1", RunID: "00000000-0000-4000-8000-000000000001",
		DispatchOutbox: "00000000-0000-4000-8000-000000000002", RouteGeneration: 1, DeliveryAttempt: 1,
	}}
	for _, c := range []struct {
		name  string
		err   error
		reads int
	}{
		{"lock not available", &pgconn.PgError{Code: "55P03"}, len(commitSnapshotLockBackoff) + 1},
		{"serialization failure", &pgconn.PgError{Code: "40001"}, 1},
		{"deadlock detected", &pgconn.PgError{Code: "40P01"}, 1},
		{"query canceled", &pgconn.PgError{Code: "57014"}, 1},
		{"in failed transaction", &pgconn.PgError{Code: "25P02"}, 1},
		{"connection lost", io.ErrUnexpectedEOF, 1},
		{"context canceled", context.Canceled, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			tx := &snapshotTx{err: c.err}
			fence := &transportFence{args: args, kind: outboxKindDispatchSyncRun, unavailable: ErrDispatchSyncRunUnavailable}
			outcome, err := fence.commit(context.Background(), tx)
			if !errors.Is(err, ErrDispatchSyncRunUnavailable) || outcome.committed {
				t.Fatalf("commit = %+v, %v; want not committed and %v", outcome, err, ErrDispatchSyncRunUnavailable)
			}
			if tx.reads != c.reads || tx.savepoints != c.reads {
				t.Fatalf("snapshot reads = %d in %d savepoints, want %d each", tx.reads, tx.savepoints, c.reads)
			}
			if tx.commit != 0 {
				t.Fatal("the pass committed")
			}
			if c.reads > 1 && tx.rollbacks != c.reads {
				t.Fatalf("rollbacks to savepoint = %d, want %d (one per 55P03 read)", tx.rollbacks, c.reads)
			}
		})
	}
}
