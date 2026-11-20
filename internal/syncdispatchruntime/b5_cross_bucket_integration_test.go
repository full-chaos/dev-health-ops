//go:build integration

package syncdispatchruntime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncbucket"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const b5Credential = "00000000-0000-4000-8000-0000001425c9"

// b5Writer is the real providersync writer on the rearm-queue fixture, with a credential the claims can bind.
func b5Writer(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *providersync.PostgresRepository {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO integration_credentials (id, provider, name, created_at, updated_at)
VALUES ($1::uuid, 'github', 'verify', now(), now())`, b5Credential); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE integrations SET credential_id = $2::uuid WHERE id = $1::uuid`, rqIntegration, b5Credential); err != nil {
		t.Fatal(err)
	}
	writer, err := providersync.NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	writer.BucketSweeps = &b4Sweeps{}
	return writer
}

// b5ClaimUnit inserts a dispatching unit of runID in the github cost class and claims it through the real writer.
func b5ClaimUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, writer *providersync.PostgresRepository, runID, unitID, costClass string,
	now time.Time) providersync.Claim {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE sync_runs SET credential_id = $2::uuid, credential_fingerprint = 'v-fp',
	auth_source = 'integration_credential' WHERE id = $1::uuid`, runID, b5Credential); err != nil {
		t.Fatal(err)
	}
	dataset := "commits"
	if costClass == "light" {
		dataset = "repo-metadata"
	}
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: unitID, RunID: runID, OrgID: rqOrg, IntegrationID: rqIntegration,
		SourceID: rqSource, Provider: "github", DatasetKey: dataset, CostClass: costClass, Mode: "incremental",
		UpdatedAt: &now, Status: syncRunUnitStatusDispatching})
	claim, err := writer.Claim(ctx, providersync.ClaimRequest{UnitID: unitID, OrgID: rqOrg, Owner: uuid.NewString(), Now: now,
		LeaseDuration: 10 * time.Minute})
	if err != nil {
		t.Fatalf("claim %s: %v", unitID, err)
	}
	return claim
}

func b5Deadlocks(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	_, _ = pool.Exec(ctx, `SELECT pg_stat_clear_snapshot()`)
	var deadlocks int64
	if err := pool.QueryRow(ctx, `SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()`).Scan(&deadlocks); err != nil {
		t.Fatal(err)
	}
	return deadlocks
}

// Verifier repro (verify-14250, cell bc3a2f23678a) at 100 iterations: run A has a running unit in github:medium and a
// planned unit in github:light, run B the reverse. Both units end at once through the real providersync Complete; each
// locks its run row, then reserves its bucket's slot for the OTHER run (the reservation's FK takes FOR KEY SHARE on it).
func TestB5CrossBucketCompletesDoNotDeadlock(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		rqSetEnv(t)
		writer := b5Writer(t, ctx, pool)
		sim := newRQSim(t, ctx, pool, pgNow(), time.Hour)
		deadlocksBefore := b5Deadlocks(t, ctx, pool)
		const iterations = 100
		failures, notTerminal := 0, 0
		var sample error
		for it := 0; it < iterations; it++ {
			now := sim.now.Add(time.Duration(it) * time.Second)
			created := now.Add(-time.Duration(200-it) * time.Hour)
			runA := sim.seed(rqRunSpec{index: 100 + 2*it, createdAt: created, lightFirst: 1})
			runB := sim.seed(rqRunSpec{index: 101 + 2*it, createdAt: created.Add(time.Second), medium: 1})
			claims := []providersync.Claim{
				b5ClaimUnit(t, ctx, pool, writer, runA, rqID(7, it, 0), "medium", now),
				b5ClaimUnit(t, ctx, pool, writer, runB, rqID(7, it, 1), "light", now),
			}
			var errs [2]error
			var ready, done sync.WaitGroup
			gate := make(chan struct{})
			for i, claim := range claims {
				ready.Add(1)
				done.Add(1)
				go func() {
					defer done.Done()
					ready.Done()
					<-gate
					errs[i] = writer.Complete(ctx, claim, map[string]any{"records": 1}, nil, now, now)
				}()
			}
			ready.Wait()
			close(gate)
			done.Wait()
			if errs[0] != nil || errs[1] != nil {
				failures++
				if sample == nil {
					sample = errs[0]
					if sample == nil {
						sample = errs[1]
					}
				}
			}
			var terminal int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_run_units WHERE id IN ($1::uuid, $2::uuid) AND status = 'success'`,
				rqID(7, it, 0), rqID(7, it, 1)).Scan(&terminal); err != nil {
				t.Fatal(err)
			}
			if terminal != 2 {
				notTerminal++
			}
			if _, err := pool.Exec(ctx, `DELETE FROM sync_dispatch_bucket_reservations`); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `UPDATE sync_runs SET status = 'success' WHERE id IN ($1::uuid, $2::uuid)`, runA, runB); err != nil {
				t.Fatal(err)
			}
		}
		pool.Reset()
		time.Sleep(2 * time.Second)
		deadlocks := b5Deadlocks(t, ctx, pool) - deadlocksBefore
		t.Logf("cross-bucket Complete pairs: iterations=%d failed=%d not_terminal=%d pg_deadlocks_delta=%d first_error=%v",
			iterations, failures, notTerminal, deadlocks, sample)
		if failures > 0 || notTerminal > 0 || deadlocks > 0 {
			t.Fatalf("cross-bucket Complete pairs: %d of %d failed, %d left a unit non-terminal, pg deadlocks +%d (first error %v)",
				failures, iterations, notTerminal, deadlocks, sample)
		}
	})
}

// Deterministic replica of the same order with the pieces Complete runs: bucket lock, LockRun (Bump's first
// statement), a barrier so both hold their run row, then ReserveHeads for the other run.
func TestB5CrossBucketOrderDeterministic(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		rqSetEnv(t)
		sim := newRQSim(t, ctx, pool, pgNow(), time.Hour)
		now := sim.now
		runA := sim.seed(rqRunSpec{index: 1, createdAt: now.Add(-2 * time.Hour), lightFirst: 1})
		runB := sim.seed(rqRunSpec{index: 2, createdAt: now.Add(-time.Hour), medium: 1})
		deadlocksBefore := b5Deadlocks(t, ctx, pool)
		var barrier sync.WaitGroup
		barrier.Add(2)
		var errs [2]error
		var reserved [2][]string
		var done sync.WaitGroup
		for i, spec := range []struct {
			run    string
			bucket syncbucket.Bucket
		}{{runA, syncbucket.Bucket{OrgID: rqOrg, Provider: "github", CostClass: "medium"}},
			{runB, syncbucket.Bucket{OrgID: rqOrg, Provider: "github", CostClass: "light"}}} {
			done.Add(1)
			go func() {
				defer done.Done()
				arrived := false
				arrive := func() {
					if !arrived {
						arrived = true
						barrier.Done()
					}
				}
				defer arrive()
				tx, err := pool.Begin(ctx)
				if err != nil {
					errs[i] = err
					return
				}
				defer func() { _ = tx.Rollback(ctx) }()
				if err := syncbucket.LockCommitPath(ctx, tx, []syncbucket.Bucket{spec.bucket}, syncbucket.LockWaitPathProviderUnit); err != nil {
					errs[i] = err
					return
				}
				if err := syncrunrollup.LockRun(ctx, tx, spec.run); err != nil {
					errs[i] = err
					return
				}
				arrive()
				barrier.Wait()
				heads, err := syncbucket.ReserveHeads(ctx, tx, spec.bucket, now, "")
				if err != nil {
					errs[i] = err
					return
				}
				reserved[i] = heads.Reserved
				errs[i] = tx.Commit(ctx)
			}()
		}
		done.Wait()
		pool.Reset()
		time.Sleep(2 * time.Second)
		deadlocks := b5Deadlocks(t, ctx, pool) - deadlocksBefore
		t.Logf("deterministic: A(medium) err=%v reserved=%v; B(light) err=%v reserved=%v; pg deadlocks +%d",
			errs[0], reserved[0], errs[1], reserved[1], deadlocks)
		if errs[0] != nil || errs[1] != nil || deadlocks > 0 {
			t.Fatalf("cross-bucket lock cycle: A err=%v, B err=%v, pg deadlocks +%d", errs[0], errs[1], deadlocks)
		}
		if len(reserved[0]) != 1 || reserved[0][0] != runB || len(reserved[1]) != 1 || reserved[1][0] != runA {
			t.Fatalf("premise: each writer reserves for the other run, got A=%v B=%v", reserved[0], reserved[1])
		}
	})
}

// N1 through the real providersync writer: the holder's unit Completes in a full medium bucket while an older run is
// parked. The commit reserves the slot for the older run AND wakes its dispatch row; a younger pass then claims nothing.
func TestB5RealWriterReleaseReservesAndWakesTheOlderRun(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		sim, _ := newP3Sim(t, ctx, pool)
		writer := b5Writer(t, ctx, pool)
		holder := sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 1, runningEnd: sim.now.Add(10 * time.Hour)})
		claim := b5ClaimUnit(t, ctx, pool, writer, holder, rqID(8, 0, 0), "medium", sim.now)
		due := sim.now
		older := sim.seed(rqRunSpec{index: 1, createdAt: sim.now.Add(-20 * time.Hour), medium: 1, wakeAt: &due})
		far := sim.now.Add(50 * time.Minute)
		younger := sim.seed(rqRunSpec{index: 2, createdAt: sim.now.Add(-time.Hour), medium: 1, wakeAt: &far})
		if claimed := p3Pass(t, sim, older); claimed != 0 {
			t.Fatalf("premise: the older run's pass claimed %d in a full bucket", claimed)
		}
		if status, lastError, _ := p4RowOf(t, ctx, pool, older); status != "dispatched" || lastError == nil || *lastError != bucketParkedError {
			t.Fatalf("premise: the capped pass did not park the older run's row (status %s, last_error %v)", status, lastError)
		}
		sim.now = sim.now.Add(time.Minute)
		if err := writer.Complete(ctx, claim, map[string]any{"records": 1}, nil, sim.now, sim.now); err != nil {
			t.Fatal(err)
		}
		status, lastError, availableAt := p4RowOf(t, ctx, pool, older)
		_, reserved, err := rqReservation(ctx, pool, rqOrg, "github", "medium", older)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("after the real Complete: older reserved=%v, row status=%s last_error=%v available_at=%s", reserved, status, lastError, availableAt.Sub(sim.now))
		if !reserved || status != "pending" || lastError != nil || availableAt.After(sim.now) {
			t.Fatalf("the writer's release did not reserve and wake the older run (reserved=%v, row status=%s last_error=%v available_at=%s)",
				reserved, status, lastError, availableAt)
		}
		if youngerClaimed := p3Pass(t, sim, younger); youngerClaimed != 0 {
			t.Fatalf("the younger pass took the slot the real writer released (claimed %d), want 0", youngerClaimed)
		}
	})
}
