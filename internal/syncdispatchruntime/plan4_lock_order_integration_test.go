//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncbucket"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-14250 acp lock-1 rule 2: ONE lock order everywhere, the bucket lock first, then rows. On the migrated schema,
// each iteration runs four slot writers of one medium bucket (cap 2) at once, each on its own connection: a unit
// writer's release by the REAL providersync repository (Complete or Fail: LockCommitPath, the unit's write, Bump,
// ArmFinalize, ReserveHeads, commit, then the wakes and the release), a dispatch pass of a waiting run (the service's
// Dispatch), a bucket sweep (runBucketSweep) and the reconciler's lease repair of an expired lease in the bucket
// (syncreconciler.LeaseRepair.Step). No goroutine may end in a deadlock (40P01) or a lock-not-available (55P03); no
// statement may hit 40P01 and pg_stat_database.deadlocks may not move. Once quiescent, every free slot is reserved: a
// probe ReserveHeads (rolled back) reserves no new run while eligible waiters exist, and every live reservation is
// an open run's with a dispatchable unit in the bucket.

// b4Sweeps is a goroutine-safe BucketSweepEnqueuer that only counts.
type b4Sweeps struct {
	mu       sync.Mutex
	enqueues int
	ensures  int
}

func (sweeps *b4Sweeps) EnqueueBucketSweep(context.Context, string, string, string) error {
	sweeps.mu.Lock()
	defer sweeps.mu.Unlock()
	sweeps.enqueues++
	return nil
}

func (sweeps *b4Sweeps) EnsureBucketSweep(context.Context, string, string, string) error {
	sweeps.mu.Lock()
	defer sweeps.mu.Unlock()
	sweeps.ensures++
	return nil
}

// b4ErrorClass is the class of an error a goroutine returned: its SQLSTATE when it carries one (as a PgError or in
// its text), else the dispatch lock-busy sentinel, else "other".
func b4ErrorClass(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	text := err.Error()
	switch {
	case strings.Contains(text, "40P01") || strings.Contains(text, "deadlock"):
		return "40P01"
	case strings.Contains(text, "55P03"):
		return "55P03"
	case errors.Is(err, ErrDispatchLockBusy):
		return "lock_busy"
	}
	return "other"
}

func b4Deadlocks(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var deadlocks int64
	if _, err := pool.Exec(ctx, `SELECT pg_stat_clear_snapshot()`); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()`).Scan(&deadlocks); err != nil {
		t.Fatal(err)
	}
	return deadlocks
}

// b4DeliverAndPass delivers runID's dispatch row now, runs its pass, and starts what it claimed (a worker's claim).
func b4DeliverAndPass(ctx context.Context, pool *pgxpool.Pool, service *NativeDispatchSyncRunService, runID string, now time.Time) (int, error) {
	var outboxID string
	if err := pool.QueryRow(ctx, `UPDATE sync_dispatch_outbox SET status = 'dispatched', dispatched_at = $2, dispatched_transport = 'river',
	dispatched_route_generation = $3, available_at = LEAST(available_at, $2), updated_at = $2
WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run' RETURNING id::text`, runID, now, dispatchRouteGeneration).Scan(&outboxID); err != nil {
		return 0, fmt.Errorf("deliver %s: %w", runID, err)
	}
	args := DispatchSyncRunArgs{TransportArgs: TransportArgs{Version: ContractVersionV1, OrgID: rqOrg, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: dispatchRouteGeneration}}
	if err := service.Dispatch(ctx, args); err != nil {
		return 0, fmt.Errorf("pass of %s: %w", runID, err)
	}
	command, err := pool.Exec(ctx, `UPDATE sync_run_units SET status = 'running', lease_owner = 'worker-14250', lease_expires_at = $2,
	last_heartbeat_at = $3, updated_at = $3 WHERE sync_run_id = $1::uuid AND status = 'dispatching'`, runID, now.Add(20*time.Hour), now)
	if err != nil {
		return 0, fmt.Errorf("claim of %s: %w", runID, err)
	}
	return int(command.RowsAffected()), nil
}

// b4EligibleWaiters lists the open runs with a planned medium unit and a successful discovery, oldest first.
func b4EligibleWaiters(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT run.id::text FROM sync_runs AS run
WHERE run.org_id = $1 AND run.status NOT IN ('success', 'partial_failed', 'failed')
  AND EXISTS (SELECT 1 FROM sync_run_units AS unit WHERE unit.sync_run_id = run.id AND unit.cost_class = 'medium' AND unit.status = 'planned')
  AND EXISTS (SELECT 1 FROM sync_run_reference_discoveries AS discovery WHERE discovery.sync_run_id = run.id AND discovery.status = 'success')
ORDER BY run.created_at, run.id`, rqOrg)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func TestB4OneLockOrderNoDeadlockNoLostReservation(t *testing.T) {
	const iterations, minWaiters = 100, 6
	withRearmQueuePool(t, func(ctx context.Context, base *pgxpool.Pool) {
		rqSetEnv(t)
		bucket := syncbucket.Bucket{OrgID: rqOrg, Provider: "github", CostClass: "medium"}
		tracer := newB4Tracer(bucket)
		pool := b4TracedPool(t, ctx, base, tracer, 24)
		poolOpen := true
		defer func() {
			if poolOpen {
				pool.Close()
			}
		}()
		deadlocksBefore := b4Deadlocks(t, ctx, base)
		sim := newRQSim(t, ctx, pool, pgNow(), time.Hour)
		sweeps := &b4Sweeps{}
		sim.service.WithBucketSweeps(sweeps)
		repair, err := syncreconciler.NewLeaseRepair(pool)
		if err != nil {
			t.Fatal(err)
		}
		repair.WithBucketSweeps(sweeps)
		if capValue := syncbucket.Cap("medium"); capValue != 2 {
			t.Fatalf("premise: medium cap = %d, want 2", capValue)
		}
		start := sim.now
		holder := sim.seed(rqRunSpec{index: 0, createdAt: start.Add(-40 * time.Hour), running: 1, runningEnd: start.Add(10 * time.Hour)})
		next := 1
		far := start.Add(50 * time.Minute)
		seedWaiter := func() {
			sim.seed(rqRunSpec{index: next, createdAt: start.Add(-30*time.Hour + time.Duration(next)*time.Second), medium: 2, wakeAt: &far})
			next++
		}
		for range minWaiters {
			seedWaiter()
		}

		names := [4]string{"writer", "pass", "sweep", "lease_repair"}
		classes := map[string]map[string]int{}
		var others []string
		var writes, claims, passes, woken, repaired, overlapped int
		// The real writer's claim reads the run's credential binding (providersync claimUnitSQL).
		const writerCredential = "00000000-0000-4000-8000-0000001425c1"
		if _, err := pool.Exec(ctx, `INSERT INTO integration_credentials (id, provider, name, created_at, updated_at)
VALUES ($1::uuid, 'github', 'b4-writer', now(), now())`, writerCredential); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE sync_runs SET credential_id = $2::uuid, credential_fingerprint = 'b4-fingerprint',
	auth_source = 'integration_credential' WHERE id = $1::uuid`, holder, writerCredential); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE integrations SET credential_id = $2::uuid WHERE id = $1::uuid`, rqIntegration, writerCredential); err != nil {
			t.Fatal(err)
		}
		writer, err := providersync.NewPostgresRepository(pool)
		if err != nil {
			t.Fatal(err)
		}
		writer.BucketSweeps = sweeps
		for iteration := 0; iteration < iterations; iteration++ {
			sim.now = start.Add(time.Duration(iteration+1) * time.Second)
			now := sim.now
			waiters := b4EligibleWaiters(t, ctx, pool)
			for len(waiters) < minWaiters {
				seedWaiter()
				waiters = b4EligibleWaiters(t, ctx, pool)
			}
			// (a) the REAL providersync writer: a new unit of the holder, claimed through the repository (dispatching ->
			// running with a lease), then ended concurrently by Complete (even iterations) or Fail (odd).
			unitID := rqID(3, 0, 100+iteration)
			b4InsertRunningUnit(t, ctx, pool, unitID, holder, now)
			if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status = 'dispatching', lease_owner = NULL, lease_expires_at = NULL WHERE id = $1::uuid`, unitID); err != nil {
				t.Fatal(err)
			}
			claim, err := writer.Claim(ctx, providersync.ClaimRequest{UnitID: unitID, OrgID: rqOrg, Owner: uuid.NewString(), Now: now,
				LeaseDuration: time.Minute})
			if err != nil {
				t.Fatalf("iteration %d: claim the writer's unit: %v", iteration, err)
			}
			// (d) a running unit whose lease expired a second ago, in its own run.
			sim.seed(rqRunSpec{index: 5000 + iteration, createdAt: now.Add(-50 * time.Hour), running: 1, runningEnd: now.Add(-10*time.Hour - time.Second)})
			// (b) even iterations: the oldest reserved run (its pass claims); odd: the youngest waiter (capped).
			passRun := waiters[len(waiters)-1]
			if iteration%2 == 0 {
				var reservedRun string
				err := pool.QueryRow(ctx, `SELECT reservation.sync_run_id::text FROM sync_dispatch_bucket_reservations AS reservation
JOIN sync_runs AS run ON run.id = reservation.sync_run_id
WHERE reservation.org_id = $1 AND reservation.provider = 'github' AND reservation.cost_class = 'medium'
ORDER BY run.created_at, run.id LIMIT 1`, rqOrg).Scan(&reservedRun)
				switch {
				case err == nil:
					passRun = reservedRun
				case errors.Is(err, pgx.ErrNoRows):
					passRun = waiters[0]
				default:
					t.Fatal(err)
				}
			}

			var errs [4]error
			var claimed int
			var sweepResult bucketSweepResult
			var repairResult syncreconciler.LeaseRepairResult
			var ready, done sync.WaitGroup
			gate := make(chan struct{})
			jobs := [4]func() error{
				func() error {
					if iteration%2 == 0 {
						return writer.Complete(ctx, claim, map[string]any{"records": 1}, nil, now, now)
					}
					return writer.Fail(ctx, claim, "provider_error", now, now)
				},
				func() (err error) {
					claimed, err = b4DeliverAndPass(ctx, pool, sim.service, passRun, now)
					return err
				},
				func() (err error) {
					sweepResult, err = runBucketSweep(ctx, pool, dispatchBucket{orgID: rqOrg, provider: "github", costClass: "medium"},
						func() time.Time { return now })
					return err
				},
				func() (err error) {
					repairResult, err = repair.Step(ctx, now, 10)
					return err
				},
			}
			var running, peak int
			var peakMu sync.Mutex
			for index, job := range jobs {
				ready.Add(1)
				done.Add(1)
				go func() {
					defer done.Done()
					ready.Done()
					<-gate
					peakMu.Lock()
					running++
					peak = max(peak, running)
					peakMu.Unlock()
					errs[index] = job()
					peakMu.Lock()
					running--
					peakMu.Unlock()
				}()
			}
			ready.Wait()
			close(gate)
			done.Wait()
			if peak == len(jobs) {
				overlapped++
			}
			for index, err := range errs {
				if err == nil {
					continue
				}
				class := b4ErrorClass(err)
				if classes[names[index]] == nil {
					classes[names[index]] = map[string]int{}
				}
				classes[names[index]][class]++
				if class == "other" && len(others) < 10 {
					others = append(others, fmt.Sprintf("iteration %d %s: %v", iteration, names[index], err))
				}
			}
			if errs[0] == nil {
				writes++
			}
			passes++
			claims += claimed
			woken += len(sweepResult.woken)
			repaired += repairResult.Failed + repairResult.Retried
		}
		codes, samples := tracer.codeCounts()
		holds, waits := tracer.take()
		contended := 0
		for _, wait := range waits {
			if wait >= time.Millisecond {
				contended++
			}
		}

		// Quiescent: the probe, in a rolled-back transaction under the bucket lock.
		now := sim.now
		staleCutoff := now.Add(-syncbucket.StaleWindow())
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, syncbucket.LockKey(bucket)); err != nil {
			t.Fatal(err)
		}
		active, _, err := syncbucket.CountActive(ctx, tx, bucket, staleCutoff, now)
		if err != nil {
			t.Fatal(err)
		}
		reservedCount, _, err := syncbucket.CountReservations(ctx, tx, bucket, "", now)
		if err != nil {
			t.Fatal(err)
		}
		var eligible []string
		for _, backfill := range []bool{true, false} {
			heads, err := syncbucket.ReadHeads(ctx, tx, bucket, backfill, now, staleCutoff, 1000, nil)
			if err != nil {
				t.Fatal(err)
			}
			eligible = append(eligible, heads...)
		}
		probe, err := syncbucket.ReserveHeads(ctx, tx, bucket, now, "")
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		rows, err := pool.Query(ctx, `
SELECT reservation.sync_run_id::text, run.id IS NOT NULL AND run.status NOT IN ('success', 'partial_failed', 'failed'),
	EXISTS (SELECT 1 FROM sync_run_units AS unit WHERE unit.sync_run_id = reservation.sync_run_id
		AND unit.provider = reservation.provider AND unit.cost_class = reservation.cost_class AND `+syncbucket.DispatchableUnitSQL+`)
FROM sync_dispatch_bucket_reservations AS reservation
LEFT JOIN sync_runs AS run ON run.id = reservation.sync_run_id
WHERE reservation.org_id = $3 AND reservation.provider = 'github' AND reservation.cost_class = 'medium'
	AND reservation.reserved_at > $1::timestamptz - make_interval(secs => $4::double precision)
ORDER BY 1`, now, staleCutoff, rqOrg, syncbucket.ReservationTTL().Seconds())
		if err != nil {
			t.Fatal(err)
		}
		var reservations []string
		var badReservations []string
		for rows.Next() {
			var runID string
			var open, dispatchable bool
			if err := rows.Scan(&runID, &open, &dispatchable); err != nil {
				t.Fatal(err)
			}
			reservations = append(reservations, runID)
			if !open || !dispatchable {
				badReservations = append(badReservations, fmt.Sprintf("%s (open=%t dispatchable=%t)", runID, open, dispatchable))
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}

		// Close the traced pool so its backends exit and flush their statistics, then read the deadlock counter.
		pool.Close()
		poolOpen = false
		time.Sleep(2 * time.Second)
		deadlocks := b4Deadlocks(t, ctx, base) - deadlocksBefore

		classLines := make([]string, 0, len(classes))
		for name, counts := range classes {
			classLines = append(classLines, fmt.Sprintf("%s=%v", name, counts))
		}
		sort.Strings(classLines)
		t.Logf("INFO %d iterations (%d with all 4 writers running at once): writer releases ok %d, passes %d (units claimed %d), sweep wakes %d, lease repairs %d, sweep enqueues %d",
			iterations, overlapped, writes, passes, claims, woken, repaired, sweeps.enqueues)
		t.Logf("INFO goroutine errors by class: %v", classLines)
		t.Logf("INFO contention: %d of %d blocking bucket-lock statements (writer, lease repair) waited >= 1ms, max wait %s; %d bucket-locked commits, max hold %s",
			contended, len(waits), b4Max(waits), len(holds), b4Max(holds))
		t.Logf("INFO statement SQLSTATEs (handled or not): %v", codes)
		for code, sample := range samples {
			t.Logf("INFO first %s statement: %s", code, sample)
		}
		t.Logf("INFO pg_stat_database.deadlocks delta = %d", deadlocks)
		t.Logf("INFO final: active %d, live reservations %d %v, eligible waiters %d, probe reserved %v (still waiting %t)",
			active, reservedCount, reservations, len(eligible), probe.Reserved, probe.StillWaiting)

		escaped := 0
		for _, counts := range classes {
			escaped += counts["40P01"] + counts["55P03"]
		}
		if escaped > 0 || codes["40P01"] > 0 || deadlocks > 0 {
			t.Errorf("deadlock/lock-not-available: %d escaped from a goroutine (%v), %d statements hit 40P01, pg_stat_database.deadlocks +%d",
				escaped, classLines, codes["40P01"], deadlocks)
		}
		for _, other := range others {
			t.Errorf("unexpected goroutine error: %s", other)
		}
		if len(eligible) == 0 {
			t.Fatalf("premise: no eligible waiter is left, the lost-reservation probe proves nothing")
		}
		if len(probe.Reserved) > 0 {
			t.Errorf("lost reservation: with everything quiescent a probe ReserveHeads reserved %v (active %d, live reservations %d, cap 2): a freed slot was left unreserved",
				probe.Reserved, active, reservedCount)
		}
		if len(badReservations) > 0 {
			t.Errorf("live reservations that no open run with a dispatchable unit holds: %v", badReservations)
		}
	})
}
