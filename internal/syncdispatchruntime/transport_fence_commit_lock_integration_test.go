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
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

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
	mu     sync.Mutex
	busy   int
	onBusy func()
}

func (tracer *commitLockTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, commitLockSQLKey{}, data.SQL)
}

func (tracer *commitLockTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	sql, _ := ctx.Value(commitLockSQLKey{}).(string)
	var pgErr *pgconn.PgError
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
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	withCommitLockPools(t, func(ctx context.Context, pool, other *pgxpool.Pool, tracer *commitLockTracer) {
		args := seedContentionRuns(t, ctx, other, 1)
		service, logs := newLoggedContentionDispatchService(t, pool)
		holder := holdWithOwnRunWake(t, ctx, other, args[0].RunID)
		defer func() { _ = holder.Rollback(context.Background()) }()

		var bound time.Duration
		for _, wait := range commitSnapshotLockBackoff {
			bound += wait
		}
		passCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		start := time.Now()
		err := service.Dispatch(passCtx, args[0])
		elapsed := time.Since(start)

		if !errors.Is(err, ErrDispatchSyncRunUnavailable) || !strings.Contains(err.Error(), commitSnapshotBusyReason) {
			t.Fatalf("pass on a row held past the bound: %v, want %v naming %s", err, ErrDispatchSyncRunUnavailable, commitSnapshotBusyReason)
		}
		if want := len(commitSnapshotLockBackoff) + 1; tracer.busyReads() != want {
			t.Fatalf("commit snapshot 55P03 reads = %d, want %d (one per backoff step and the first)", tracer.busyReads(), want)
		}
		if elapsed < bound || elapsed > bound+5*time.Second {
			t.Fatalf("pass took %s, want at least the bound %s and well under the pass deadline", elapsed, bound)
		}
		var warns []string
		for _, line := range logs.lines() {
			if strings.Contains(line, "msg=sync_dispatch_fence.commit_snapshot_busy") {
				warns = append(warns, line)
			}
		}
		if len(warns) != 1 || !strings.Contains(warns[0], "level=WARN") ||
			!strings.Contains(warns[0], "reason="+commitSnapshotBusyReason) || !strings.Contains(warns[0], "outbox_kind="+outboxKindDispatchSyncRun) ||
			!strings.Contains(warns[0], "sync_run_id="+args[0].RunID) {
			t.Fatalf("give-up lines = %q, want one WARN naming the run, outbox kind and reason", warns)
		}
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
		if want := outboxKindDispatchSyncRun + "/" + commitSnapshotBusyReason; counted[want] != 1 || len(counted) != 1 {
			t.Fatalf("%s = %v, want exactly {%s: 1}", FenceCommitSnapshotBusyCounter, counted, want)
		}
		if got := runStatus(t, ctx, other, args[0].RunID); got == syncRunStatusDispatching {
			t.Fatal("the pass committed while the row was held")
		}
		var unitStatus string
		if err := other.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE sync_run_id = $1::uuid`, args[0].RunID).Scan(&unitStatus); err != nil {
			t.Fatal(err)
		}
		if unitStatus != "planned" {
			t.Fatalf("unit status = %s, want planned (the pass rolled back)", unitStatus)
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
