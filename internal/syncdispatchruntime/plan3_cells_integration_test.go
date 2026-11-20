//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PLAN-14250-3 cells (r1 63a6f74b782e P1 classes 1-7). Red on the base tree c9d30583; the build makes them green.

// p3Sweeps stands in for River's one sweep per bucket, with the running state: an enqueue or ensure while the
// bucket's sweep runs is a duplicate River accepts and drops.
type p3Sweeps struct {
	now     func() time.Time
	due     map[string]time.Time
	running map[string]bool
	dropped int
}

func newP3Sweeps(now func() time.Time) *p3Sweeps {
	return &p3Sweeps{now: now, due: map[string]time.Time{}, running: map[string]bool{}}
}

func p3Key(orgID, provider, costClass string) string { return orgID + "|" + provider + "|" + costClass }

func (sweeps *p3Sweeps) EnqueueBucketSweep(_ context.Context, orgID, provider, costClass string) error {
	key := p3Key(orgID, provider, costClass)
	if sweeps.running[key] {
		sweeps.dropped++
		return nil
	}
	if due, ok := sweeps.due[key]; !ok || due.After(sweeps.now()) {
		sweeps.due[key] = sweeps.now()
	}
	return nil
}

func (sweeps *p3Sweeps) EnsureBucketSweep(_ context.Context, orgID, provider, costClass string) error {
	key := p3Key(orgID, provider, costClass)
	if sweeps.running[key] {
		sweeps.dropped++
		return nil
	}
	if _, ok := sweeps.due[key]; !ok {
		sweeps.due[key] = sweeps.now()
	}
	return nil
}

// run runs the medium bucket's sweep with hooks and applies River's outcome: run again at once, re-arm, or end.
func (sweeps *p3Sweeps) run(t *testing.T, sim *rqSim, hooks p3Hooks) []string {
	key := p3Key(rqOrg, "github", "medium")
	delete(sweeps.due, key)
	sweeps.running[key] = true
	waiting, woken, again, err := p3SweepHooked(sim.ctx, sim.pool, rqOrg, "github", "medium", sim.now, hooks)
	sweeps.running[key] = false
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case again:
		sweeps.due[key] = sim.now
	case waiting:
		sweeps.due[key] = sim.now.Add(time.Hour)
	}
	return woken
}

func (sweeps *p3Sweeps) dueIn(now time.Time) (time.Duration, bool) {
	due, ok := sweeps.due[p3Key(rqOrg, "github", "medium")]
	return due.Sub(now), ok
}

func newP3Sim(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (*rqSim, *p3Sweeps) {
	rqSetEnv(t)
	sim := newRQSim(t, ctx, pool, pgNow(), time.Hour)
	sweeps := newP3Sweeps(func() time.Time { return sim.now })
	p3Attach(sim.service, sweeps)
	return sim, sweeps
}

// p3Pass delivers runID's dispatch row now and runs its pass; it returns the units the pass claimed.
func p3Pass(t *testing.T, sim *rqSim, runID string) int {
	t.Helper()
	ctx, pool := sim.ctx, sim.pool
	var outboxID string
	if err := pool.QueryRow(ctx, `UPDATE sync_dispatch_outbox SET status = 'dispatched', dispatched_at = $2, dispatched_transport = 'river',
	dispatched_route_generation = $3, available_at = LEAST(available_at, $2), updated_at = $2
WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run' RETURNING id::text`, runID, sim.now, dispatchRouteGeneration).Scan(&outboxID); err != nil {
		t.Fatalf("deliver %s: %v", runID, err)
	}
	args := DispatchSyncRunArgs{TransportArgs: TransportArgs{Version: ContractVersionV1, OrgID: rqOrg, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: dispatchRouteGeneration}}
	if err := sim.service.Dispatch(ctx, args); err != nil {
		t.Fatalf("pass of %s: %v", runID, err)
	}
	command, err := pool.Exec(ctx, `UPDATE sync_run_units SET status = 'running', lease_owner = 'worker-14250', lease_expires_at = $2,
	last_heartbeat_at = $3, updated_at = $3 WHERE sync_run_id = $1::uuid AND status = 'dispatching'`, runID, sim.now.Add(10*time.Hour), sim.now)
	if err != nil {
		t.Fatal(err)
	}
	return int(command.RowsAffected())
}

// p3EndUnit ends one running unit of runID (Complete's statements) and runs its release through sweeps.
func p3EndUnit(t *testing.T, sim *rqSim, sweeps syncrunrollup.BucketSweepEnqueuer, unitID, runID, path, status string, availableAt *time.Time) {
	t.Helper()
	ctx, pool := sim.ctx, sim.pool
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE sync_run_units SET status = $3, available_at = $4, lease_owner = NULL, lease_expires_at = NULL, updated_at = $2
WHERE id = $1::uuid AND status = 'running'`, unitID, sim.now, status, availableAt); err != nil {
		t.Fatal(err)
	}
	if status != syncRunUnitStatusRetrying {
		if err := syncrunrollup.ArmFinalize(ctx, tx, runID, rqOrg, sim.now); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	p3Release(ctx, pool, sweeps, syncrunrollup.SlotFree{RunID: runID, OrgID: rqOrg, Provider: "github", CostClass: "medium"}, sim.now, path)
}

// P1-1a: a pass the cap holds back parks its own dispatch row (dispatched, last_error 'bucket_parked').
func TestP3CappedPassParksItsDispatchRow(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		sim, _ := newP3Sim(t, ctx, pool)
		sim.seed(rqRunSpec{index: 0, backfill: true, createdAt: sim.now.Add(-40 * time.Hour), running: 2, runningEnd: sim.now.Add(10 * time.Hour)})
		due := sim.now
		waiter := sim.seed(rqRunSpec{index: 1, createdAt: sim.now.Add(-time.Hour), medium: 2, wakeAt: &due})
		if claimed := p3Pass(t, sim, waiter); claimed != 0 {
			t.Fatalf("premise: the capped pass claimed %d", claimed)
		}
		var status string
		var lastError *string
		if err := pool.QueryRow(ctx, `SELECT status, last_error FROM sync_dispatch_outbox WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run'`, waiter).Scan(&status, &lastError); err != nil {
			t.Fatal(err)
		}
		got := "<nil>"
		if lastError != nil {
			got = *lastError
		}
		t.Logf("capped pass left its row: status=%s last_error=%s", status, got)
		if status != "dispatched" || got != "bucket_parked" {
			t.Fatalf("capped pass left status=%s last_error=%s, want dispatched / bucket_parked", status, got)
		}
	})
}

// P1-2: the head read (limit 1) and the guard's older-run read visit a number of rows that does not grow with the
// queue: at depth 70, 140 and 350 the head read visits <= 8 sync_runs + sync_run_units rows and the older-run read <= 64.
func TestP3BucketReadsAreBoundedAtAnyDepth(t *testing.T) {
	const headsK, holdBackK = 8, 64
	for _, depth := range []int{70, 140, 350} {
		withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
			now := pgNow()
			for _, statement := range []string{
				`INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
				 SELECT ('00000000-0000-4000-9006-' || lpad(to_hex(n), 12, '0'))::uuid, $1, $2::uuid, 'schedule', 'incremental', 'running', 1, 0, 0,
					$4::timestamptz - make_interval(mins => $3::int - n)
				 FROM generate_series(1, $3::int) AS n`,
				`INSERT INTO sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at)
				 SELECT ('00000000-0000-4000-9007-' || lpad(to_hex(n), 12, '0'))::uuid, ('00000000-0000-4000-9006-' || lpad(to_hex(n), 12, '0'))::uuid,
					$1, 'success', 1, $4::timestamptz, $4::timestamptz, $4::timestamptz
				 FROM generate_series(1, $3::int) AS n WHERE $2::text IS NOT NULL`,
				`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, created_at, updated_at)
				 SELECT ('00000000-0000-4000-9008-' || lpad(to_hex(n), 12, '0'))::uuid, $1, ('00000000-0000-4000-9006-' || lpad(to_hex(n), 12, '0'))::uuid,
					$2::uuid, $5::uuid, 'github', 'commits', 'medium', 'incremental', 'planned', 0, $4::timestamptz, $4::timestamptz
				 FROM generate_series(1, $3::int) AS n`,
				`ANALYZE`,
			} {
				var args []any
				switch {
				case statement == `ANALYZE`:
				default:
					args = []any{rqOrg, rqIntegration, depth, now, rqSource}
					if !containsPlaceholder(statement, "$5") {
						args = args[:4]
					}
				}
				if _, err := pool.Exec(ctx, statement, args...); err != nil {
					t.Fatalf("seed depth %d: %v", depth, err)
				}
			}
			self := fmt.Sprintf("00000000-0000-4000-9006-%012x", depth)
			heads, err := p3HeadsRowsRead(ctx, pool, rqOrg, "github", "medium", false, now)
			if err != nil {
				t.Fatal(err)
			}
			holdBack, err := p3HoldBackRowsRead(ctx, pool, rqOrg, "github", "medium", self, now)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("depth %d: head read visits %.0f rows (k %d); older-run read visits %.0f rows (k %d)", depth, heads, headsK, holdBack, holdBackK)
			if heads > headsK || holdBack > holdBackK {
				t.Errorf("depth %d: head read %.0f rows (want <= %d), older-run read %.0f rows (want <= %d)", depth, heads, headsK, holdBack, holdBackK)
			}
		})
	}
}

func containsPlaceholder(statement, placeholder string) bool {
	for i := 0; i+len(placeholder) <= len(statement); i++ {
		if statement[i:i+len(placeholder)] == placeholder {
			return true
		}
	}
	return false
}

// P1-3: an older run still in reference discovery (live by its recent creation) must not hold back the slot the
// sweep gave to a newer eligible run.
func TestP3UndiscoveredOlderRunDoesNotHoldTheSweptSlot(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		sim, sweeps := newP3Sim(t, ctx, pool)
		sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 1, runningEnd: sim.now.Add(10 * time.Hour)})
		sim.seed(rqRunSpec{index: 1, createdAt: sim.now.Add(-2 * time.Minute), medium: 2, noDiscovery: true})
		far := sim.now.Add(50 * time.Minute)
		newer := sim.seed(rqRunSpec{index: 2, createdAt: sim.now.Add(-time.Minute), medium: 2, wakeAt: &far})
		woken := sweeps.run(t, sim, p3Hooks{})
		claimed := p3Pass(t, sim, newer)
		t.Logf("sweep woke %v; the newer run's pass claimed %d", woken, claimed)
		if len(woken) != 1 || woken[0] != newer || claimed != 1 {
			t.Fatalf("the slot the sweep gave the newer run went unused: woken=%v claimed=%d (want [%s] and 1)", woken, claimed, newer)
		}
	})
}

// P1-4: a signal that reaches a running sweep (River drops the duplicate) is not lost: when the sweep ends, the
// bucket's next sweep is due at once, for each wake source.
func TestP3SignalDuringARunningSweepIsNotLost(t *testing.T) {
	for _, source := range []string{"unit_end", "refusal", "lease_repair_retrying", "late_capped_ensure"} {
		t.Run(source, func(t *testing.T) {
			withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				sim, sweeps := newP3Sim(t, ctx, pool)
				far := sim.now.Add(50 * time.Minute)
				var hook func()
				switch source {
				case "unit_end", "lease_repair_retrying":
					holder := sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 2, runningEnd: sim.now.Add(10 * time.Hour)})
					sim.seed(rqRunSpec{index: 1, createdAt: sim.now.Add(-time.Hour), medium: 2, wakeAt: &far})
					status, path := "success", syncrunrollup.DispatchWakePathProviderUnit
					var availableAt *time.Time
					if source == "lease_repair_retrying" {
						due := sim.now.Add(-time.Second)
						status, path, availableAt = syncRunUnitStatusRetrying, syncrunrollup.DispatchWakePathLeaseRepair, &due
					}
					hook = func() { p3EndUnit(t, sim, sweeps, rqID(3, 0, 0), holder, path, status, availableAt) }
				case "refusal":
					sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 1, runningEnd: sim.now.Add(10 * time.Hour)})
					refused := sim.seed(rqRunSpec{index: 1, createdAt: sim.now.Add(-2 * time.Hour), medium: 1, wakeAt: &far})
					sim.seed(rqRunSpec{index: 2, createdAt: sim.now.Add(-time.Hour), medium: 1, wakeAt: &far})
					sim.refuse(1)
					hook = func() {
						if claimed := p3Pass(t, sim, refused); claimed != 0 {
							t.Fatalf("premise: the refused run claimed %d", claimed)
						}
					}
				case "late_capped_ensure":
					sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 2, runningEnd: sim.now.Add(10 * time.Hour)})
					due := sim.now
					late := sim.seed(rqRunSpec{index: 1, createdAt: sim.now.Add(-time.Hour), medium: 2, wakeAt: &due})
					if _, err := pool.Exec(ctx, `UPDATE sync_run_reference_discoveries SET status = 'planned' WHERE sync_run_id = $1::uuid`, late); err != nil {
						t.Fatal(err)
					}
					hook = func() {
						if _, err := pool.Exec(ctx, `UPDATE sync_run_reference_discoveries SET status = 'success' WHERE sync_run_id = $1::uuid`, late); err != nil {
							t.Fatal(err)
						}
						if claimed := p3Pass(t, sim, late); claimed != 0 {
							t.Fatalf("premise: the late run claimed %d", claimed)
						}
					}
				}
				sweeps.run(t, sim, p3Hooks{beforeFinish: hook})
				next, pending := sweeps.dueIn(sim.now)
				t.Logf("%s during a running sweep: dropped duplicates=%d; next sweep pending=%v in %s", source, sweeps.dropped, pending, next)
				if !pending || next > time.Minute {
					t.Fatalf("%s reached a running sweep and was lost: next sweep pending=%v in %s, want due within a minute", source, pending, next)
				}
			})
		})
	}
}

// P1-5: between the sweep's pick and its mark, a younger run's pass must not take the slot picked for the older run.
func TestP3YoungerPassCannotTakeThePickedSlot(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		sim, sweeps := newP3Sim(t, ctx, pool)
		sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 1, runningEnd: sim.now.Add(10 * time.Hour)})
		older := sim.seed(rqRunSpec{index: 1, createdAt: sim.now.Add(-20 * time.Hour), medium: 2})
		far := sim.now.Add(50 * time.Minute)
		younger := sim.seed(rqRunSpec{index: 2, createdAt: sim.now.Add(-time.Hour), medium: 2, wakeAt: &far})
		var youngerClaimed int
		woken := sweeps.run(t, sim, p3Hooks{afterPick: func() { youngerClaimed = p3Pass(t, sim, younger) }})
		t.Logf("sweep picked %v; the younger pass in the pick-to-mark window claimed %d", woken, youngerClaimed)
		if len(woken) != 1 || woken[0] != older || youngerClaimed != 0 {
			t.Fatalf("younger pass took the slot picked for the older run: woken=%v younger claimed=%d (want [%s] and 0)", woken, youngerClaimed, older)
		}
	})
}

// P1-6: cap 2, a non-backfill unit runs. Round 1 gives the backfill share to the oldest backfill waiter. The
// non-backfill unit ends during the sweep: round 2 must give that slot to the non-backfill waiter, not to a second
// backfill waiter (the first one's reservation already holds the backfill share).
func TestP3SecondRoundCountsReservationsPerClass(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		sim, sweeps := newP3Sim(t, ctx, pool)
		holder := sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 1, runningEnd: sim.now.Add(10 * time.Hour)})
		oldBackfill := sim.seed(rqRunSpec{index: 1, backfill: true, createdAt: sim.now.Add(-30 * time.Hour), medium: 2})
		nextBackfill := sim.seed(rqRunSpec{index: 2, backfill: true, createdAt: sim.now.Add(-29 * time.Hour), medium: 2})
		nonBackfill := sim.seed(rqRunSpec{index: 3, createdAt: sim.now.Add(-time.Hour), medium: 2})
		woken := sweeps.run(t, sim, p3Hooks{betweenRounds: func() {
			p3EndUnit(t, sim, newP3Sweeps(func() time.Time { return sim.now }), rqID(3, 0, 0), holder, syncrunrollup.DispatchWakePathProviderUnit, "success", nil)
		}})
		t.Logf("woken across the two rounds: %v (backfill %s, %s; non-backfill %s)", woken, oldBackfill, nextBackfill, nonBackfill)
		if len(woken) != 2 || woken[0] != oldBackfill || woken[1] != nonBackfill {
			t.Fatalf("second round miscounted the reservation: woken=%v, want [%s %s]", woken, oldBackfill, nonBackfill)
		}
	})
}

// P1-7: lease repair moves the run's last in-flight unit to retrying with a future available_at. The sweep finds no
// due waiter; the run's dispatch wakeup must still be armed at the retry deadline.
func TestP3LeaseRepairToAFutureRetryKeepsTheRunsWake(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		sim, sweeps := newP3Sim(t, ctx, pool)
		runID := sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-time.Hour), running: 1, runningEnd: sim.now.Add(10 * time.Hour)})
		deadline := sim.now.Add(time.Minute)
		p3EndUnit(t, sim, sweeps, rqID(3, 0, 0), runID, syncrunrollup.DispatchWakePathLeaseRepair, syncRunUnitStatusRetrying, &deadline)
		woken := sweeps.run(t, sim, p3Hooks{})
		var status string
		var availableAt time.Time
		err := pool.QueryRow(ctx, `SELECT status, available_at FROM sync_dispatch_outbox WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run'`, runID).Scan(&status, &availableAt)
		t.Logf("lease repair to a retry at +1m: sweep woke %v; run's dispatch row status=%q available_at=+%s (err %v)", woken, status, availableAt.Sub(sim.now), err)
		if err != nil || status != "pending" || availableAt.After(deadline) {
			t.Fatalf("the run's wake at its retry deadline is lost: row status=%q available_at=%s err=%v, want pending at or before +1m", status, availableAt, err)
		}
	})
}
