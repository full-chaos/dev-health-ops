package syncdispatchruntime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// fakeSnapshotClock is an injected clock for the commit snapshot's retry window: sleep advances it at once, and a
// test hook can advance it to stand for a slow database round trip. onAdvance sees every new offset from its start.
type fakeSnapshotClock struct {
	mu        sync.Mutex
	base      time.Time
	offset    time.Duration
	sleeps    int
	onAdvance func(offset time.Duration)
}

func (clock *fakeSnapshotClock) now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.base.Add(clock.offset)
}

func (clock *fakeSnapshotClock) advance(by time.Duration) {
	clock.mu.Lock()
	clock.offset += by
	offset, onAdvance := clock.offset, clock.onAdvance
	clock.mu.Unlock()
	if onAdvance != nil {
		onAdvance(offset)
	}
}

func (clock *fakeSnapshotClock) sleep(ctx context.Context, wait time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clock.mu.Lock()
	clock.sleeps++
	clock.mu.Unlock()
	clock.advance(wait)
	return nil
}

func (clock *fakeSnapshotClock) sleepCount() int {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.sleeps
}

func installFakeSnapshotClock(t *testing.T) *fakeSnapshotClock {
	t.Helper()
	clock := &fakeSnapshotClock{base: time.Unix(0, 0)}
	previous := commitSnapshotClock
	commitSnapshotClock.now, commitSnapshotClock.sleep = clock.now, clock.sleep
	t.Cleanup(func() { commitSnapshotClock = previous })
	return clock
}

func installBusyMeter(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	return reader
}

// busyCounts reads FenceCommitSnapshotBusyCounter by "kind/reason".
func busyCounts(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var resource metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resource); err != nil {
		t.Fatal(err)
	}
	counted := map[string]int64{}
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != FenceCommitSnapshotBusyCounter {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				kind, _ := point.Attributes.Value("kind")
				reason, _ := point.Attributes.Value("reason")
				counted[kind.AsString()+"/"+reason.AsString()] += point.Value
			}
		}
	}
	return counted
}

// snapshotTx is a pass transaction whose commit snapshot read number n (from 1) returns result(n): nil is a current
// dispatched row, anything else is the read's error. Each read runs inside a savepoint (Begin); onRead and onRollback
// run at the read and at the rollback to its savepoint.
type snapshotTx struct {
	pgx.Tx
	result                                                  func(read int) error
	onRead, onRollback                                      func(read int)
	savepoints, reads, rollbacks, released, commit, readSQL int
}

func (tx *snapshotTx) Begin(context.Context) (pgx.Tx, error) {
	tx.savepoints++
	return &snapshotSavepoint{parent: tx}, nil
}

func (tx *snapshotTx) Commit(context.Context) error {
	tx.commit++
	return nil
}

type snapshotSavepoint struct {
	pgx.Tx
	parent *snapshotTx
}

func (savepoint *snapshotSavepoint) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	tx := savepoint.parent
	tx.reads++
	if sql == commitSnapshotSQL {
		tx.readSQL++
	}
	if tx.onRead != nil {
		tx.onRead(tx.reads)
	}
	return snapshotRow{tx.result(tx.reads)}
}

func (savepoint *snapshotSavepoint) Rollback(context.Context) error {
	savepoint.parent.rollbacks++
	if savepoint.parent.onRollback != nil {
		savepoint.parent.onRollback(savepoint.parent.reads)
	}
	return nil
}

func (savepoint *snapshotSavepoint) Commit(context.Context) error {
	savepoint.parent.released++
	return nil
}

// snapshotRow scans a row dispatched to River at generation 1 and attempt 1 (decideCommit commits it) or fails with err.
type snapshotRow struct{ err error }

func (row snapshotRow) Scan(dest ...any) error {
	if row.err != nil {
		return row.err
	}
	river, generation := "river", int64(1)
	paused := false
	*dest[0].(*string) = "dispatched"
	*dest[1].(**string) = &river
	*dest[2].(**int64) = &generation
	*dest[3].(*int64) = 1
	*dest[4].(*bool) = true
	*dest[5].(**string) = &river
	*dest[6].(**int64) = &generation
	*dest[7].(**bool) = &paused
	return nil
}

func lockNotAvailable(int) error { return &pgconn.PgError{Code: "55P03"} }

func testFence(logs io.Writer) *transportFence {
	return &transportFence{
		args: DispatchSyncRunArgs{TransportArgs: TransportArgs{
			Version: ContractVersionV1, OrgID: "org-1", RunID: "00000000-0000-4000-8000-000000000001",
			DispatchOutbox: "00000000-0000-4000-8000-000000000002", RouteGeneration: 1, DeliveryAttempt: 1,
		}},
		kind: outboxKindDispatchSyncRun, unavailable: ErrDispatchSyncRunUnavailable,
		logger: synclog.New(slog.New(slog.NewTextHandler(logs, nil))),
	}
}

// assertGaveUp: the fenced commit returned the unavailable error naming the busy reason, committed nothing, logged one
// busy WARN and counted one give-up.
func assertGaveUp(t *testing.T, outcome commitOutcome, err error, tx *snapshotTx, logs *bytes.Buffer, reader *sdkmetric.ManualReader) {
	t.Helper()
	if !errors.Is(err, ErrDispatchSyncRunUnavailable) || !strings.Contains(err.Error(), commitSnapshotBusyReason) || outcome.committed || tx.commit != 0 {
		t.Fatalf("commit = %+v, %v (commits %d); want not committed and %v naming %s", outcome, err, tx.commit, ErrDispatchSyncRunUnavailable, commitSnapshotBusyReason)
	}
	if warns := strings.Count(logs.String(), "level=WARN msg=sync_dispatch_fence.commit_snapshot_busy"); warns != 1 {
		t.Fatalf("busy WARN lines = %d, want 1:\n%s", warns, logs.String())
	}
	if want := outboxKindDispatchSyncRun + "/" + commitSnapshotBusyReason; busyCounts(t, reader)[want] != 1 {
		t.Fatalf("%s = %v, want {%s: 1}", FenceCommitSnapshotBusyCounter, busyCounts(t, reader), want)
	}
}

// TestTheFencedCommitRetriesOnlyALockNotAvailable: only SQLSTATE 55P03 on the commit snapshot is read again; any other
// error (another SQLSTATE, a dropped connection, a canceled read) returns the unavailable error after ONE read, with
// nothing committed.
func TestTheFencedCommitRetriesOnlyALockNotAvailable(t *testing.T) {
	previous, previousWindow := commitSnapshotLockBackoff, commitSnapshotWindow
	commitSnapshotLockBackoff, commitSnapshotWindow = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}, time.Second
	t.Cleanup(func() { commitSnapshotLockBackoff, commitSnapshotWindow = previous, previousWindow })
	installFakeSnapshotClock(t)
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
			tx := &snapshotTx{result: func(int) error { return c.err }}
			outcome, err := testFence(io.Discard).commit(context.Background(), tx)
			if !errors.Is(err, ErrDispatchSyncRunUnavailable) || outcome.committed {
				t.Fatalf("commit = %+v, %v; want not committed and %v", outcome, err, ErrDispatchSyncRunUnavailable)
			}
			if tx.reads != c.reads || tx.savepoints != c.reads || tx.readSQL != c.reads {
				t.Fatalf("snapshot reads = %d (%d of commitSnapshotSQL) in %d savepoints, want %d each", tx.reads, tx.readSQL, tx.savepoints, c.reads)
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

// TestTheCommitWindowSendsNoReadOnceTheDeadlineIsReached: the injected clock reaches the deadline between reads 3
// and 4 (a slow round trip), at it or past it. Read 4 is not sent; the pass gives up with the reason, one WARN and the
// counter at 1. The rollback to the savepoint of read 3 is still sent.
func TestTheCommitWindowSendsNoReadOnceTheDeadlineIsReached(t *testing.T) {
	for _, c := range []struct {
		name string
		slow time.Duration // added at the rollback of read 3, which starts at 15 ms
	}{
		{"wait cut to land on the deadline", commitSnapshotWindow - 15*time.Millisecond - 10*time.Millisecond},
		{"round trip ends on the deadline", commitSnapshotWindow - 15*time.Millisecond},
		{"round trip ends past the deadline", commitSnapshotWindow},
	} {
		t.Run(c.name, func(t *testing.T) {
			reader := installBusyMeter(t)
			clock := installFakeSnapshotClock(t)
			var logs bytes.Buffer
			tx := &snapshotTx{result: lockNotAvailable, onRollback: func(read int) {
				if read == 3 {
					clock.advance(c.slow)
				}
			}}
			outcome, err := testFence(&logs).commit(context.Background(), tx)
			if tx.reads != 3 || tx.rollbacks != 3 {
				t.Fatalf("reads = %d, rollbacks = %d; want 3 and 3 (read 4 would start at or after the deadline)", tx.reads, tx.rollbacks)
			}
			assertGaveUp(t, outcome, err, tx, &logs, reader)
		})
	}
}

// TestACommitReadStartedBeforeTheDeadlineCountsWhenItEndsAfterIt: read 2 starts at 5 ms and its round trip moves the
// injected clock past the deadline; it finds the row free. It counts: the pass commits, with no busy WARN or counter.
func TestACommitReadStartedBeforeTheDeadlineCountsWhenItEndsAfterIt(t *testing.T) {
	reader := installBusyMeter(t)
	clock := installFakeSnapshotClock(t)
	var logs bytes.Buffer
	tx := &snapshotTx{
		result: func(read int) error {
			if read == 1 {
				return &pgconn.PgError{Code: "55P03"}
			}
			return nil
		},
		onRead: func(read int) {
			if read == 2 {
				clock.advance(2 * commitSnapshotWindow)
			}
		},
	}
	outcome, err := testFence(&logs).commit(context.Background(), tx)
	if err != nil || !outcome.committed || tx.commit != 1 || tx.released != 1 || tx.reads != 2 {
		t.Fatalf("commit = %+v, %v (reads %d, released %d, commits %d); want committed after 2 reads", outcome, err, tx.reads, tx.released, tx.commit)
	}
	if strings.Contains(logs.String(), "commit_snapshot_busy") || len(busyCounts(t, reader)) != 0 {
		t.Fatalf("an in-flight read that succeeded reported busy: %v\n%s", busyCounts(t, reader), logs.String())
	}
}

// TestTheCommitReadCapEndsAFastWindowFirst: with reads that take no time on the injected clock, the 7th read goes out
// at 310 ms (inside the 315 ms window) and the pass gives up on the read cap: 7 reads, 6 waits, no wait after the last.
func TestTheCommitReadCapEndsAFastWindowFirst(t *testing.T) {
	reader := installBusyMeter(t)
	clock := installFakeSnapshotClock(t)
	var logs bytes.Buffer
	tx := &snapshotTx{result: lockNotAvailable}
	outcome, err := testFence(&logs).commit(context.Background(), tx)
	if want := len(commitSnapshotLockBackoff) + 1; tx.reads != want || clock.sleepCount() != want-1 {
		t.Fatalf("reads = %d, waits = %d; want %d and %d (the cap, not the deadline, ends the window)", tx.reads, clock.sleepCount(), want, want-1)
	}
	assertGaveUp(t, outcome, err, tx, &logs, reader)
}
