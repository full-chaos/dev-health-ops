//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// The cells below hold the delivery's outbox row with a REAL row lock: a wake upsert of the same run, left open in a
// transaction on a second pool (the shape that made main red in CHAOS-14265). A tracer on the service's pool sees every
// 55P03 of the commit snapshot; where a cell releases the row, it does so synchronously on the first 55P03, so the
// overlap is proven and no timing decides the outcome.

type commitLockSQLKey struct{}

type commitLockTracer struct {
	mu                  sync.Mutex
	busy                int
	onBusy              func()
	rollbacks           int
	onSavepointRollback func()
	// aborted counts every statement on the traced pool that failed with SQLSTATE 25P02 (in failed transaction).
	aborted int
}

func (tracer *commitLockTracer) abortedStatements() int {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	return tracer.aborted
}

func (tracer *commitLockTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, commitLockSQLKey{}, data.SQL)
}

func (tracer *commitLockTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	sql, _ := ctx.Value(commitLockSQLKey{}).(string)
	if data.Err == nil && strings.HasPrefix(sql, "rollback to savepoint") {
		tracer.mu.Lock()
		tracer.rollbacks++
		first, onRollback := tracer.rollbacks == 1, tracer.onSavepointRollback
		tracer.mu.Unlock()
		if first && onRollback != nil {
			onRollback()
		}
		return
	}
	var pgErr *pgconn.PgError
	if errors.As(data.Err, &pgErr) && pgErr.Code == "25P02" {
		tracer.mu.Lock()
		tracer.aborted++
		tracer.mu.Unlock()
	}
	if !isCommitCheck(sql) || !errors.As(data.Err, &pgErr) || pgErr.Code != "55P03" {
		return
	}
	tracer.mu.Lock()
	tracer.busy++
	first := tracer.busy == 1
	onBusy := tracer.onBusy
	tracer.mu.Unlock()
	if first && onBusy != nil {
		onBusy()
	}
}

func (tracer *commitLockTracer) busyReads() int {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	return tracer.busy
}

// withCommitLockPools gives a cell the service's pool (traced, the production domain pool size 4) and a second,
// untraced pool for the row holder and the assertions.
func withCommitLockPools(t *testing.T, fn func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	other, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 4
	tracer := &commitLockTracer{}
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgseed.Org(ctx, t, other, discoveryTestOrg, "community")
	fn(ctx, pool, other, tracer)
}

// holdWithOwnRunWake runs the wake upsert of runID in an open transaction on other: it holds the run's dispatch
// outbox row until the returned transaction ends. The caller rolls it back before other closes.
func holdWithOwnRunWake(t *testing.T, ctx context.Context, other *pgxpool.Pool, runID string) pgx.Tx {
	t.Helper()
	holder, err := other.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tag, err := holder.Exec(ctx, syncrunrollup.WakeDispatchSQL, uuid.New(), runID, time.Now().UTC())
	if err != nil {
		_ = holder.Rollback(context.Background())
		t.Fatal(err)
	}
	if tag.RowsAffected() != 1 {
		_ = holder.Rollback(context.Background())
		t.Fatalf("own-run wake upsert rows = %d, want 1 (the row is not held)", tag.RowsAffected())
	}
	return holder
}

func runStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id = $1::uuid`, runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

// TestDispatchCommitsAfterAWakeOfItsOwnRunReleasesTheOutboxRow: a wake of the same run holds the run's dispatch
// outbox row when the pass reaches its commit snapshot; the wake commits right after the first 55P03. The pass reads
// the snapshot again (the wake left the row 'pending', unclaimed, same attempt: decideCommit accepts it) and commits.
//
// RED CONTROL: before CHAOS-14265 the pass returned ErrDispatchSyncRunUnavailable and the run stayed 'running'.
func TestDispatchCommitsAfterAWakeOfItsOwnRunReleasesTheOutboxRow(t *testing.T) {
	withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer) {
		args := seedContentionRuns(t, ctx, other, 1)
		service, logs := newLoggedContentionDispatchService(t, pool)
		holder := holdWithOwnRunWake(t, ctx, other, args[0].RunID)
		defer func() { _ = holder.Rollback(context.Background()) }()
		var released error
		tracer.onBusy = func() { released = holder.Commit(ctx) }

		err := service.Dispatch(ctx, args[0])
		if busy := tracer.busyReads(); busy != 1 {
			t.Fatalf("commit snapshot 55P03 reads = %d, want 1 (the wake must hold the row when the pass reaches it)", busy)
		}
		if released != nil {
			t.Fatalf("release the wake: %v", released)
		}
		if err != nil {
			t.Fatalf("pass after the wake released the row: %v, want nil", err)
		}
		if got := runStatus(t, ctx, other, args[0].RunID); got != syncRunStatusDispatching {
			t.Fatalf("run status = %s, want %s", got, syncRunStatusDispatching)
		}
		if line := strings.Join(logs.lines(), "\n"); strings.Contains(line, "sync_dispatch_fence.commit_snapshot_busy") {
			t.Fatalf("a released row logged the give-up line:\n%s", line)
		}
	})
}

// TestDispatchGivesUpOnAnOutboxRowHeldPastTheRetryBound: the same-run wake never commits. The pass reads the snapshot
// once per backoff step, then returns ErrDispatchSyncRunUnavailable naming the reason, logs one WARN line and counts
// one give-up, with nothing of the pass committed.
func TestDispatchGivesUpOnAnOutboxRowHeldPastTheRetryBound(t *testing.T) {
	reader := installBusyMeter(t)
	withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer) {
		args := seedContentionRuns(t, ctx, other, 1)
		service, logs := newLoggedContentionDispatchService(t, pool)
		holder := holdWithOwnRunWake(t, ctx, other, args[0].RunID)
		defer func() { _ = holder.Rollback(context.Background()) }()
		installFakeSnapshotClock(t)

		passCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		err := service.Dispatch(passCtx, args[0])

		if !errors.Is(err, ErrDispatchSyncRunUnavailable) || !strings.Contains(err.Error(), commitSnapshotBusyReason) {
			t.Fatalf("pass on a row held past the bound: %v, want %v naming %s", err, ErrDispatchSyncRunUnavailable, commitSnapshotBusyReason)
		}
		if want := len(commitSnapshotLockBackoff) + 1; tracer.busyReads() != want {
			t.Fatalf("commit snapshot 55P03 reads = %d, want %d (one per backoff step and the first)", tracer.busyReads(), want)
		}
		assertOneDispatchGiveUp(t, ctx, other, reader, logs, args[0].RunID)
	})
}

// assertOneDispatchGiveUp: one WARN naming the run, outbox kind and reason, the counter at exactly 1 for
// dispatch_sync_run, and nothing of the pass committed.
func assertOneDispatchGiveUp(t *testing.T, ctx context.Context, other *pgxpool.Pool, reader *sdkmetric.ManualReader, logs *lockedLogBuffer, runID string) {
	t.Helper()
	var warns []string
	for _, line := range logs.lines() {
		if strings.Contains(line, "msg=sync_dispatch_fence.commit_snapshot_busy") {
			warns = append(warns, line)
		}
	}
	if len(warns) != 1 || !strings.Contains(warns[0], "level=WARN") ||
		!strings.Contains(warns[0], "reason="+commitSnapshotBusyReason) || !strings.Contains(warns[0], "outbox_kind="+outboxKindDispatchSyncRun) ||
		!strings.Contains(warns[0], "sync_run_id="+runID) {
		t.Fatalf("give-up lines = %q, want one WARN naming the run, outbox kind and reason", warns)
	}
	counted := busyCounts(t, reader)
	if want := outboxKindDispatchSyncRun + "/" + commitSnapshotBusyReason; counted[want] != 1 || len(counted) != 1 {
		t.Fatalf("%s = %v, want exactly {%s: 1}", FenceCommitSnapshotBusyCounter, counted, want)
	}
	if got := runStatus(t, ctx, other, runID); got == syncRunStatusDispatching {
		t.Fatal("the pass committed while the row was held")
	}
	var unitStatus string
	if err := other.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE sync_run_id = $1::uuid`, runID).Scan(&unitStatus); err != nil {
		t.Fatal(err)
	}
	if unitStatus != "planned" {
		t.Fatalf("unit status = %s, want planned (the pass rolled back)", unitStatus)
	}
}

// TestDispatchGivesUpWhenTheRetryWindowEndsBeforeALateRelease is the round 1 repro shape: the same-run wake holds the
// row until 365 ms into the window, and the first rollback to the savepoint takes 125 ms (a query hook advances the
// injected window clock; no product sleep). Reads 2..6 go out at 130, 140, 160, 200 and 280 ms; the last wait is cut
// at the deadline (315 ms), where no read is sent. The pass gives up with the reason, one WARN and the counter at 1,
// and the late release never happens inside the pass.
//
// RED CONTROL (round 1 tip): the waits did not count the round trips; read 7 went out at 440 ms, after the release,
// and the pass committed.
func TestDispatchGivesUpWhenTheRetryWindowEndsBeforeALateRelease(t *testing.T) {
	reader := installBusyMeter(t)
	withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer) {
		args := seedContentionRuns(t, ctx, other, 1)
		service, logs := newLoggedContentionDispatchService(t, pool)
		holder := holdWithOwnRunWake(t, ctx, other, args[0].RunID)
		defer func() { _ = holder.Rollback(context.Background()) }()
		clock := installFakeSnapshotClock(t)
		released := false
		var releaseErr error
		clock.onAdvance = func(offset time.Duration) {
			if !released && offset >= 365*time.Millisecond {
				released = true
				releaseErr = holder.Commit(ctx)
			}
		}
		tracer.onSavepointRollback = func() { clock.advance(125 * time.Millisecond) }

		passCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		err := service.Dispatch(passCtx, args[0])

		if releaseErr != nil {
			t.Fatalf("late release: %v", releaseErr)
		}
		if released {
			t.Fatalf("the window ran past the late release (pass err=%v): a read was sent after the deadline", err)
		}
		if !errors.Is(err, ErrDispatchSyncRunUnavailable) || !strings.Contains(err.Error(), commitSnapshotBusyReason) {
			t.Fatalf("pass whose window ended with the row held: %v, want %v naming %s", err, ErrDispatchSyncRunUnavailable, commitSnapshotBusyReason)
		}
		if tracer.busyReads() != 6 {
			t.Fatalf("commit snapshot 55P03 reads = %d, want 6 (none at the deadline)", tracer.busyReads())
		}
		assertOneDispatchGiveUp(t, ctx, other, reader, logs, args[0].RunID)
	})
}

// TestFinalizeFirstPostSyncArmKeepsTheBusyReason: a finalize that arms post_sync for the first time (no
// sync_run_post_dispatches row yet) and finds its outbox row held to the end of the window returns
// ErrFinalizeSyncRunUnavailable WITH the busy reason, and commits nothing.
//
// RED CONTROL (round 1 tip): that branch replaced the fence's error with the bare ErrFinalizeSyncRunUnavailable.
func TestFinalizeFirstPostSyncArmKeepsTheBusyReason(t *testing.T) {
	reader := installBusyMeter(t)
	kind := fenceKinds()[0]
	withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer) {
		kind.seed(t, ctx, other)
		if _, err := other.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`, kind.outbox); err != nil {
			t.Fatal(err)
		}
		var armed int
		if err := other.QueryRow(ctx, `SELECT count(*) FROM sync_run_post_dispatches WHERE sync_run_id = $1::uuid`, kind.run).Scan(&armed); err != nil {
			t.Fatal(err)
		}
		if armed != 0 {
			t.Fatalf("post_sync already armed (%d rows): the cell must take the first-arm branch", armed)
		}
		holder, err := other.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Rollback(context.Background()) }()
		if _, err := holder.Exec(ctx, rearmOutboxSQL, uuid.NewString(), kind.org, kind.run, kind.kind); err != nil {
			t.Fatal(err)
		}
		installFakeSnapshotClock(t)

		err = kind.pass(t, ctx, pool, 1)
		if !errors.Is(err, ErrFinalizeSyncRunUnavailable) || !strings.Contains(err.Error(), commitSnapshotBusyReason) {
			t.Fatalf("finalize first arm on a held row: %v, want %v naming %s", err, ErrFinalizeSyncRunUnavailable, commitSnapshotBusyReason)
		}
		if want := len(commitSnapshotLockBackoff) + 1; tracer.busyReads() != want {
			t.Fatalf("commit snapshot 55P03 reads = %d, want %d", tracer.busyReads(), want)
		}
		if want := outboxKindFinalizeSyncRun + "/" + commitSnapshotBusyReason; busyCounts(t, reader)[want] != 1 {
			t.Fatalf("%s = %v, want {%s: 1}", FenceCommitSnapshotBusyCounter, busyCounts(t, reader), want)
		}
		if err := other.QueryRow(ctx, `SELECT count(*) FROM sync_run_post_dispatches WHERE sync_run_id = $1::uuid`, kind.run).Scan(&armed); err != nil {
			t.Fatal(err)
		}
		if armed != 0 || kind.wrote(t, ctx, other) {
			t.Fatalf("finalize committed on a held row: post_dispatches=%d wrote=%t", armed, kind.wrote(t, ctx, other))
		}
	})
}

// TestFinalizeCommitsAfterAWakeOfItsOwnRunReleasesTheOutboxRow: the finalize fence takes the same retry. A wake of
// the run's finalize row holds it at the commit snapshot and commits on the first 55P03; the finalize commits.
//
// RED CONTROL: before CHAOS-14265 Finalize returned ErrFinalizeSyncRunUnavailable and the run stayed 'dispatching'.
func TestFinalizeCommitsAfterAWakeOfItsOwnRunReleasesTheOutboxRow(t *testing.T) {
	kind := fenceKinds()[0]
	withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer) {
		kind.seed(t, ctx, other)
		if _, err := other.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`, kind.outbox); err != nil {
			t.Fatal(err)
		}
		holder, err := other.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = holder.Rollback(context.Background()) }()
		if _, err := holder.Exec(ctx, rearmOutboxSQL, uuid.NewString(), kind.org, kind.run, kind.kind); err != nil {
			t.Fatal(err)
		}
		var released error
		tracer.onBusy = func() { released = holder.Commit(ctx) }

		err = kind.pass(t, ctx, pool, 1)
		if busy := tracer.busyReads(); busy != 1 {
			t.Fatalf("commit snapshot 55P03 reads = %d, want 1 (the wake must hold the row when the finalize reaches it)", busy)
		}
		if released != nil {
			t.Fatalf("release the wake: %v", released)
		}
		if err != nil || !kind.wrote(t, ctx, other) {
			t.Fatalf("finalize after the wake released the row: err=%v wrote=%t, want nil/true", err, kind.wrote(t, ctx, other))
		}
	})
}
