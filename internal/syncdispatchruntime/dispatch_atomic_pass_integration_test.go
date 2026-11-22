//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

// Atomic pass cells (CHAOS-14479): every end state of a pass either commits with a wake armed for the work it left,
// or returns an error for a pass that committed nothing. One run, one bucket (github/commits/rest_core).

const (
	atomicPassRunning  = "00000000-0000-4000-8000-000000144790"
	atomicPassPlannedA = "00000000-0000-4000-8000-000000144791"
	atomicPassPlannedB = "00000000-0000-4000-8000-000000144792"
	atomicPassRetrying = "00000000-0000-4000-8000-000000144793"
)

func atomicPassArgs() DispatchSyncRunArgs {
	args := dispatchTestArgs()
	args.DeliveryAttempt = 1
	return args
}

// seedAtomicPassRun seeds the delivered row at attempt 1 and the units: running holds R under a live lease.
func seedAtomicPassRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, running bool, planned []string, retryingAt *time.Time) {
	t.Helper()
	seedDispatchRoute(t, ctx, pool)
	markReferenceDiscoverySucceeded(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET attempts = 1 WHERE id = $1`, dispatchServiceTestOutbox); err != nil {
		t.Fatal(err)
	}
	now := pgNow()
	if running {
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,lease_owner,lease_expires_at,last_heartbeat_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'running',$5,'live-worker',$6,$5,(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'rest_core','incremental',1,now())`,
			atomicPassRunning, discoveryTestOrg, discoveryTestRun, dispatchTestSource, now, now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range planned {
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'planned',$5,(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'rest_core','incremental',0,now())`,
			id, discoveryTestOrg, discoveryTestRun, dispatchTestSource, now); err != nil {
			t.Fatal(err)
		}
	}
	if retryingAt != nil {
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,available_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'retrying',$5,$6,(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'rest_core','incremental',1,now())`,
			atomicPassRetrying, discoveryTestOrg, discoveryTestRun, dispatchTestSource, now, *retryingAt); err != nil {
			t.Fatal(err)
		}
	}
}

// refuseRunWakeWrites makes every write that would leave the run's dispatch_sync_run row 'pending' fail: the re-arm's
// upsert (an INSERT .. ON CONFLICT, whose BEFORE INSERT trigger fires first) and its overwrite. Other kinds and
// other runs are not touched. The returned func drops the trigger.
func refuseRunWakeWrites(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (drop func()) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
CREATE FUNCTION atomic_pass_refuse_run_wake() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.kind = 'dispatch_sync_run' AND NEW.status = 'pending' AND NEW.sync_run_id = '`+discoveryTestRun+`'::uuid THEN
    RAISE EXCEPTION 'atomic pass cell: run wake write refused';
  END IF;
  RETURN NEW;
END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
CREATE TRIGGER atomic_pass_refuse_run_wake BEFORE INSERT OR UPDATE ON sync_dispatch_outbox
FOR EACH ROW EXECUTE FUNCTION atomic_pass_refuse_run_wake()`); err != nil {
		t.Fatal(err)
	}
	return func() {
		if _, err := pool.Exec(ctx, `DROP TRIGGER atomic_pass_refuse_run_wake ON sync_dispatch_outbox`); err != nil {
			t.Fatal(err)
		}
	}
}

func atomicPassRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (string, int64) {
	t.Helper()
	var status string
	var attempts int64
	if err := pool.QueryRow(ctx, `SELECT status, attempts FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = $2`,
		discoveryTestRun, outboxKindDispatchSyncRun).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	return status, attempts
}

func atomicPassPublishes(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE job_kind = $1`, jobcontract.KindSyncProviderUnit).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func atomicPassPlannedCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_run_units WHERE sync_run_id = $1 AND status = $2`,
		discoveryTestRun, syncRunUnitStatusPlanned).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

type atomicPassShape struct {
	name          string
	cap           string
	running       bool
	planned       []string
	retryingIn    time.Duration
	wantPublished int
}

// atomicPassShapes are the three arms that re-arm the run: a pass that queued a unit and left one capped, a pass
// that queued nothing with a capped unit (countdown), and a pass that queued nothing with a future retry (deferral).
var atomicPassShapes = []atomicPassShape{
	{name: "queued_and_capped", cap: "2", running: true, planned: []string{atomicPassPlannedA, atomicPassPlannedB}, wantPublished: 1},
	{name: "nothing_queued_capped", cap: "1", running: true, planned: []string{atomicPassPlannedA}},
	{name: "nothing_queued_deferred", cap: "1", retryingIn: 30 * time.Minute},
}

func (shape atomicPassShape) seed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", shape.cap)
	var retryingAt *time.Time
	if shape.retryingIn > 0 {
		at := pgNow().Add(shape.retryingIn)
		retryingAt = &at
	}
	seedAtomicPassRun(t, ctx, pool, shape.running, shape.planned, retryingAt)
}

// TestAtomicPassFailedRearmIsNotASuccessWithNoWake: the run's wake write fails. The pass must not end as a success
// with the row still 'dispatched' and no wake. An error is right only for a pass that committed nothing (no unit
// published, every planned unit still planned), and River's retry of that same attempt then completes the pass.
func TestAtomicPassFailedRearmIsNotASuccessWithNoWake(t *testing.T) {
	for _, shape := range atomicPassShapes {
		t.Run(shape.name, func(t *testing.T) {
			withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				shape.seed(t, ctx, pool)
				planned := atomicPassPlannedCount(t, ctx, pool)
				drop := refuseRunWakeWrites(t, ctx, pool)

				err := newTestDispatchService(t, pool).Dispatch(ctx, atomicPassArgs())
				status, attempts := atomicPassRow(t, ctx, pool)
				published := atomicPassPublishes(t, ctx, pool)
				t.Logf("failed wake write: err=%v row=%s/%d published=%d", err, status, attempts, published)
				if err == nil {
					if status != "pending" {
						t.Fatalf("Dispatch returned nil with row status=%s: a failed re-arm ended the pass as a success with no wake", status)
					}
					return
				}
				if published != 0 || atomicPassPlannedCount(t, ctx, pool) != planned || status != "dispatched" || attempts != 1 {
					t.Fatalf("Dispatch returned %v after a commit (published=%d row=%s/%d): an error is right only for a pass that committed nothing", err, published, status, attempts)
				}

				drop()
				if err := newTestDispatchService(t, pool).Dispatch(ctx, atomicPassArgs()); err != nil {
					t.Fatalf("retry of the uncommitted attempt: %v, want nil", err)
				}
				if status, _ := atomicPassRow(t, ctx, pool); status != "pending" {
					t.Fatalf("after the retry row status=%s, want pending", status)
				}
				if got := atomicPassPublishes(t, ctx, pool); got != shape.wantPublished {
					t.Fatalf("after the retry published=%d, want %d", got, shape.wantPublished)
				}
			})
		})
	}
}

// TestAtomicPassRearmArmsAWake is the control: no failure, every shape ends nil with a pending wake.
func TestAtomicPassRearmArmsAWake(t *testing.T) {
	for _, shape := range atomicPassShapes {
		t.Run(shape.name, func(t *testing.T) {
			withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				shape.seed(t, ctx, pool)
				if err := newTestDispatchService(t, pool).Dispatch(ctx, atomicPassArgs()); err != nil {
					t.Fatalf("Dispatch: %v, want nil", err)
				}
				if status, attempts := atomicPassRow(t, ctx, pool); status != "pending" || attempts != 1 {
					t.Fatalf("row %s/%d, want pending/1", status, attempts)
				}
				if got := atomicPassPublishes(t, ctx, pool); got != shape.wantPublished {
					t.Fatalf("published=%d, want %d", got, shape.wantPublished)
				}
			})
		})
	}
}

// TestAtomicPassTailReadAfterTheCommitPointDoesNotFailACommittedPass: a pass that queues nothing (the one slot is
// R's). From the first service clock read after the commit snapshot on, another session tries to take
// sync_run_units ACCESS EXCLUSIVE (NOWAIT); the service's lock_timeout fails any unit read it then makes. A committed
// pass must not return that error (River would retry a committed attempt), and the capped unit must have a wake.
// When the read sits inside the pass transaction the NOWAIT lock is refused (the pass holds the table) and nothing
// fails.
func TestAtomicPassTailReadAfterTheCommitPointDoesNotFailACommittedPass(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
		seedAtomicPassRun(t, ctx, pool, true, []string{atomicPassPlannedA}, nil)
		service := newTestDispatchService(t, atomicPassLockTimeoutPool(t, ctx, pool, "300ms"))

		var snapshotTaken, fired, held atomic.Bool
		release := func() {}
		saved := commitSnapshotClock
		t.Cleanup(func() { commitSnapshotClock = saved })
		commitSnapshotClock.now = func() time.Time {
			snapshotTaken.Store(true)
			return time.Now()
		}
		service.now = func() time.Time {
			if snapshotTaken.Load() && fired.CompareAndSwap(false, true) {
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Error(err)
					return time.Now()
				}
				if _, err := tx.Exec(ctx, `LOCK TABLE sync_run_units IN ACCESS EXCLUSIVE MODE NOWAIT`); err != nil {
					_ = tx.Rollback(ctx)
					var pgErr *pgconn.PgError
					if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
						t.Errorf("hold: %v, want nil or 55P03", err)
					}
				} else {
					held.Store(true)
					release = func() { _ = tx.Rollback(ctx) }
				}
			}
			return time.Now()
		}

		err := service.Dispatch(ctx, atomicPassArgs())
		release()
		status, attempts := atomicPassRow(t, ctx, pool)
		t.Logf("tail read: err=%v fired=%v held=%v row=%s/%d", err, fired.Load(), held.Load(), status, attempts)
		if !fired.Load() {
			t.Fatal("precondition: no service clock read after the commit snapshot")
		}
		if got := atomicPassPublishes(t, ctx, pool); got != 0 {
			t.Fatalf("precondition: published=%d, want 0 (the bucket is full)", got)
		}
		if err != nil {
			t.Fatalf("Dispatch returned %v after its commit: a failed tail read failed a committed pass", err)
		}
		if status != "pending" {
			t.Fatalf("row status=%s, want pending: the capped unit has no wake", status)
		}
	})
}

// atomicPassLockTimeoutPool is a second pool on the same database whose sessions give up a lock wait after timeout.
func atomicPassLockTimeoutPool(t *testing.T, ctx context.Context, pool *pgxpool.Pool, timeout string) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["lock_timeout"] = timeout
	limited, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(limited.Close)
	return limited
}
