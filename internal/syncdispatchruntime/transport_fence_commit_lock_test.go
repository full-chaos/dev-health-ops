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
// dispatched row, anything else is the read's error. Each read runs inside a savepoint (Begin); onBegin, onRead and
// onRollback run when savepoint n opens, at read n and at the rollback after read n.
type snapshotTx struct {
	pgx.Tx
	result                                                  func(read int) error
	onBegin, onRead, onRollback                             func(n int)
	savepoints, reads, rollbacks, released, commit, readSQL int
	// barrier is the result of route barrier statement n (from 1); nil takes the lock. statements is every statement
	// the fence sent, with the savepoint it ran in.
	barrier    func(n int) error
	barriers   int
	statements []snapshotStatement
}

type snapshotStatement struct {
	savepoint int
	sql       string
}

func (tx *snapshotTx) Begin(context.Context) (pgx.Tx, error) {
	tx.savepoints++
	if tx.onBegin != nil {
		tx.onBegin(tx.savepoints)
	}
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

func (savepoint *snapshotSavepoint) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	tx := savepoint.parent
	tx.barriers++
	tx.statements = append(tx.statements, snapshotStatement{tx.savepoints, sql})
	if tx.barrier != nil {
		return pgconn.CommandTag{}, tx.barrier(tx.barriers)
	}
	return pgconn.CommandTag{}, nil
}

func (savepoint *snapshotSavepoint) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	tx := savepoint.parent
	tx.statements = append(tx.statements, snapshotStatement{tx.savepoints, sql})
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
	assertGaveUpNaming(t, commitSnapshotBusyReason, outcome, err, tx, logs, reader)
}

// assertGaveUpNaming is assertGaveUp for one give-up reason: the error, the WARN and the counter name it, and no other
// reason is counted.
func assertGaveUpNaming(t *testing.T, reason string, outcome commitOutcome, err error, tx *snapshotTx, logs *bytes.Buffer, reader *sdkmetric.ManualReader) {
	t.Helper()
	if !errors.Is(err, ErrDispatchSyncRunUnavailable) || !strings.Contains(err.Error(), reason) || outcome.committed || tx.commit != 0 {
		t.Fatalf("commit = %+v, %v (commits %d); want not committed and %v naming %s", outcome, err, tx.commit, ErrDispatchSyncRunUnavailable, reason)
	}
	if warns := strings.Count(logs.String(), "level=WARN msg=sync_dispatch_fence.commit_snapshot_busy"); warns != 1 || !strings.Contains(logs.String(), "reason="+reason) {
		t.Fatalf("busy WARN lines = %d, want 1 naming reason=%s:\n%s", warns, reason, logs.String())
	}
	if want := outboxKindDispatchSyncRun + "/" + reason; busyCounts(t, reader)[want] != 1 || len(busyCounts(t, reader)) != 1 {
		t.Fatalf("%s = %v, want exactly {%s: 1}", FenceCommitSnapshotBusyCounter, busyCounts(t, reader), want)
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
// counter at 1. Every savepoint opened is rolled back, the one opened for read 4 included.
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
			if tx.reads != 3 || tx.rollbacks != tx.savepoints {
				t.Fatalf("reads = %d, rollbacks = %d of %d savepoints; want 3 reads (read 4 would start at or after the deadline), every savepoint rolled back", tx.reads, tx.rollbacks, tx.savepoints)
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

// TestASlowRetrySavepointPastTheDeadlineSendsNoQuery: the deadline is checked after a retry savepoint opens, right
// before its query. When opening savepoint 2 (or savepoint 7, at 310 ms) moves the injected clock to or past the
// deadline, that savepoint is rolled back, its query is not sent, and the pass gives up with the reason, one WARN and
// the counter at 1.
func TestASlowRetrySavepointPastTheDeadlineSendsNoQuery(t *testing.T) {
	for _, c := range []struct {
		name      string
		savepoint int
		slow      time.Duration
	}{
		{"second savepoint takes 400 ms", 2, 400 * time.Millisecond},
		{"seventh savepoint opens at the deadline", 7, 5 * time.Millisecond},
		{"seventh savepoint opens past the deadline", 7, 10 * time.Millisecond},
	} {
		t.Run(c.name, func(t *testing.T) {
			reader := installBusyMeter(t)
			clock := installFakeSnapshotClock(t)
			var logs bytes.Buffer
			tx := &snapshotTx{result: lockNotAvailable, onBegin: func(n int) {
				if n == c.savepoint {
					clock.advance(c.slow)
				}
			}}
			outcome, err := testFence(&logs).commit(context.Background(), tx)
			if tx.savepoints != c.savepoint || tx.reads != c.savepoint-1 || tx.rollbacks != tx.savepoints {
				t.Fatalf("savepoints = %d, queries = %d, rollbacks = %d; want %d, %d and every savepoint rolled back", tx.savepoints, tx.reads, tx.rollbacks, c.savepoint, c.savepoint-1)
			}
			assertGaveUp(t, outcome, err, tx, &logs, reader)
		})
	}
}

// TestASlowFirstSavepointDoesNotShiftTheWindow: the window starts when the first query is sent, after its savepoint
// opened. Opening savepoint 1 takes 12 ms on the injected clock; the retry due 310 ms after the first query is still
// sent, finds the row free, and the pass commits.
func TestASlowFirstSavepointDoesNotShiftTheWindow(t *testing.T) {
	reader := installBusyMeter(t)
	clock := installFakeSnapshotClock(t)
	var logs bytes.Buffer
	last := len(commitSnapshotLockBackoff) + 1
	tx := &snapshotTx{
		result: func(read int) error {
			if read < last {
				return &pgconn.PgError{Code: "55P03"}
			}
			return nil
		},
		onBegin: func(n int) {
			if n == 1 {
				clock.advance(12 * time.Millisecond)
			}
		},
	}
	outcome, err := testFence(&logs).commit(context.Background(), tx)
	if err != nil || !outcome.committed || tx.reads != last || tx.commit != 1 {
		t.Fatalf("commit = %+v, %v (queries %d, commits %d); want committed on query %d", outcome, err, tx.reads, tx.commit, last)
	}
	if strings.Contains(logs.String(), "commit_snapshot_busy") || len(busyCounts(t, reader)) != 0 {
		t.Fatalf("a slow first savepoint cost the last retry: %v\n%s", busyCounts(t, reader), logs.String())
	}
}

// The route mutation barrier (CHAOS-14523). syncroute.Controller takes SHARE ROW EXCLUSIVE on sync_dispatch_outbox
// before it pauses, resumes or moves a route; the commit snapshot takes ROW EXCLUSIVE on the same table, NOWAIT, so
// a route mutation cannot commit between the snapshot and the pass's COMMIT. The literals pin the wire values.
const (
	testRouteBarrierSQL        = "LOCK TABLE public.sync_dispatch_outbox IN ROW EXCLUSIVE MODE NOWAIT"
	testRouteBarrierBusyReason = "outbox_route_barrier_held_past_bound"
)

// TestTheRouteBarrierGoesOutBeforeTheSnapshotInItsSavepoint: each read sends the barrier lock first, then the snapshot,
// in the same savepoint; the savepoint is released (the lock stays with the pass) and the pass commits.
func TestTheRouteBarrierGoesOutBeforeTheSnapshotInItsSavepoint(t *testing.T) {
	installFakeSnapshotClock(t)
	tx := &snapshotTx{result: func(read int) error {
		if read == 1 {
			return &pgconn.PgError{Code: "55P03"}
		}
		return nil
	}}
	outcome, err := testFence(io.Discard).commit(context.Background(), tx)
	if err != nil || !outcome.committed || tx.commit != 1 || tx.released != 1 {
		t.Fatalf("commit = %+v, %v (released %d, commits %d); want committed after 2 reads", outcome, err, tx.released, tx.commit)
	}
	want := []snapshotStatement{{1, testRouteBarrierSQL}, {1, commitSnapshotSQL}, {2, testRouteBarrierSQL}, {2, commitSnapshotSQL}}
	if len(tx.statements) != len(want) {
		t.Fatalf("statements = %+v, want %+v", tx.statements, want)
	}
	for index := range want {
		if tx.statements[index] != want[index] {
			t.Fatalf("statement %d = %+v, want %+v (all: %+v)", index, tx.statements[index], want[index], tx.statements)
		}
	}
}

// TestARouteBarrierHeldPastTheWindowNamesTheBarrierReason: a route mutation holds (or waits for) SHARE ROW EXCLUSIVE,
// so every barrier lock fails 55P03. It is polled like a held row: one savepoint per read, the snapshot never sent,
// and the pass gives up with the barrier reason, one WARN and the counter at 1.
func TestARouteBarrierHeldPastTheWindowNamesTheBarrierReason(t *testing.T) {
	reader := installBusyMeter(t)
	installFakeSnapshotClock(t)
	var logs bytes.Buffer
	tx := &snapshotTx{result: func(int) error { return nil }, barrier: lockNotAvailable}
	outcome, err := testFence(&logs).commit(context.Background(), tx)
	if want := len(commitSnapshotLockBackoff) + 1; tx.barriers != want || tx.savepoints != want || tx.rollbacks != want || tx.reads != 0 {
		t.Fatalf("barrier locks = %d, savepoints = %d, rollbacks = %d, snapshot reads = %d; want %d, %d, %d and 0",
			tx.barriers, tx.savepoints, tx.rollbacks, tx.reads, want, want, want)
	}
	assertGaveUpNaming(t, testRouteBarrierBusyReason, outcome, err, tx, &logs, reader)
}

// TestARouteBarrierErrorOtherThanLockNotAvailableIsNotRetried: any other barrier error returns the unavailable error
// after one savepoint, with nothing committed and no busy line.
func TestARouteBarrierErrorOtherThanLockNotAvailableIsNotRetried(t *testing.T) {
	installFakeSnapshotClock(t)
	var logs bytes.Buffer
	tx := &snapshotTx{result: func(int) error { return nil }, barrier: func(int) error { return &pgconn.PgError{Code: "42501"} }}
	outcome, err := testFence(&logs).commit(context.Background(), tx)
	if !errors.Is(err, ErrDispatchSyncRunUnavailable) || outcome.committed || tx.commit != 0 || tx.barriers != 1 || tx.reads != 0 {
		t.Fatalf("commit = %+v, %v (barrier locks %d, reads %d, commits %d); want one barrier lock, no read, not committed", outcome, err, tx.barriers, tx.reads, tx.commit)
	}
	if strings.Contains(logs.String(), "commit_snapshot_busy") {
		t.Fatalf("a non-55P03 barrier error logged the busy line:\n%s", logs.String())
	}
}
