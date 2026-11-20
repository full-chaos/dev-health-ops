//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PLAN-14250-4 cells (r2 fd6c29d7d881 P1 classes N1, C1, C2, C7). Red on 4272f088 (plan4-red-2.log); the build makes
// N1, C1 and C7 green. C2 is cut from this PR (acp plan4-build-1, ticket 14338 (filed separately)): its cell logs the
// head read's rows as the follow-up's baseline and asserts nothing. They drive the code only through the shims.

var p4Buckets = []struct {
	costClass string
	capValue  int
}{{"heavy", 1}, {"medium", 2}, {"light", 4}}

func p4SetCostClass(t *testing.T, ctx context.Context, pool *pgxpool.Pool, costClass string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET cost_class = $1`, costClass); err != nil {
		t.Fatal(err)
	}
}

func p4RowOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string) (string, *string, time.Time) {
	t.Helper()
	var status string
	var lastError *string
	var availableAt time.Time
	if err := pool.QueryRow(ctx, `SELECT status, last_error, available_at FROM sync_dispatch_outbox
WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run'`, runID).Scan(&status, &lastError, &availableAt); err != nil {
		t.Fatal(err)
	}
	return status, lastError, availableAt
}

// p4EndUnitAsWriter ends one running unit the way a slot writer does (unit write + ArmFinalize in its transaction,
// then the release), through the seam.
func p4EndUnitAsWriter(t *testing.T, sim *rqSim, sweeps syncrunrollup.BucketSweepEnqueuer, unitID, runID, costClass string) {
	t.Helper()
	slot := syncrunrollup.SlotFree{RunID: runID, OrgID: rqOrg, Provider: "github", CostClass: costClass}
	if err := p4WriterRelease(sim.ctx, sim.pool, sweeps, slot, sim.now, syncrunrollup.DispatchWakePathProviderUnit, func(tx pgx.Tx) error {
		if _, err := tx.Exec(sim.ctx, `UPDATE sync_run_units SET status = 'success', lease_owner = NULL, lease_expires_at = NULL, updated_at = $2
WHERE id = $1::uuid AND status = 'running'`, unitID, sim.now); err != nil {
			return err
		}
		return syncrunrollup.ArmFinalize(sim.ctx, tx, runID, rqOrg, sim.now)
	}); err != nil {
		t.Fatal(err)
	}
}

// N1: a slot freed while an older eligible run waits (parked) goes to that run. A younger run's own pass that reaches
// the bucket lock after the release commits and before any sweep runs finds the slot reserved. 12 cells: bucket
// heavy/medium/light x older class (non-backfill, backfill) x older unit (planned, due retrying).
func TestP4ReleaseReservesTheSlotBeforeAYoungerPass(t *testing.T) {
	for _, bucket := range p4Buckets {
		for _, backfill := range []bool{false, true} {
			for _, oldest := range []string{"planned", "retrying"} {
				name := fmt.Sprintf("%s/backfill=%t/%s", bucket.costClass, backfill, oldest)
				t.Run(name, func(t *testing.T) {
					withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
						sim, _ := newP3Sim(t, ctx, pool)
						noSweep := newP3Sweeps(func() time.Time { return sim.now })
						holder := sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: bucket.capValue, runningEnd: sim.now.Add(10 * time.Hour)})
						due := sim.now
						older := sim.seed(rqRunSpec{index: 1, backfill: backfill, createdAt: sim.now.Add(-20 * time.Hour), medium: 1, wakeAt: &due})
						far := sim.now.Add(50 * time.Minute)
						younger := sim.seed(rqRunSpec{index: 2, createdAt: sim.now.Add(-time.Hour), medium: 1, wakeAt: &far})
						p4SetCostClass(t, ctx, pool, bucket.costClass)
						if oldest == "retrying" {
							if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status = 'retrying', available_at = $2 WHERE sync_run_id = $1::uuid`,
								older, sim.now.Add(-time.Second)); err != nil {
								t.Fatal(err)
							}
						}
						if claimed := p3Pass(t, sim, older); claimed != 0 {
							t.Fatalf("premise: the older run's pass claimed %d in a full bucket", claimed)
						}
						sim.now = sim.now.Add(time.Minute)
						p4EndUnitAsWriter(t, sim, noSweep, rqID(3, 0, 0), holder, bucket.costClass)
						if status, lastError, availableAt := p4RowOf(t, ctx, pool, older); status != "pending" || lastError != nil || availableAt.After(sim.now) {
							t.Fatalf("%s: the release did not wake the older run (row status=%s last_error=%v available_at=%s)", name, status, lastError, availableAt)
						}
						youngerClaimed := p3Pass(t, sim, younger)
						_, reserved, err := rqReservation(ctx, pool, rqOrg, "github", bucket.costClass, older)
						if err != nil {
							t.Fatal(err)
						}
						t.Logf("%s: after the release the younger pass claimed %d; the older run reserved=%v", name, youngerClaimed, reserved)
						if youngerClaimed != 0 || !reserved {
							t.Fatalf("%s: the younger pass took the released slot (claimed %d, older reserved=%v), want 0 and a reservation for the older run",
								name, youngerClaimed, reserved)
						}
					})
				})
			}
		}
	}
}

// N1 (no writer): a running unit whose lease expires frees its slot by time, before any repair writes it. A younger
// run's pass then must leave the slot to the older eligible run.
func TestP4SlotFreedByLeaseExpiryGoesToTheOlderRun(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		sim, _ := newP3Sim(t, ctx, pool)
		sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 2, runningEnd: sim.now.Add(10 * time.Hour)})
		due := sim.now
		older := sim.seed(rqRunSpec{index: 1, createdAt: sim.now.Add(-20 * time.Hour), medium: 1, wakeAt: &due})
		far := sim.now.Add(50 * time.Minute)
		younger := sim.seed(rqRunSpec{index: 2, createdAt: sim.now.Add(-time.Hour), medium: 1, wakeAt: &far})
		if claimed := p3Pass(t, sim, older); claimed != 0 {
			t.Fatalf("premise: the older run's pass claimed %d", claimed)
		}
		sim.now = sim.now.Add(time.Minute)
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET lease_expires_at = $2 WHERE id = $1::uuid`, rqID(3, 0, 0), sim.now.Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		youngerClaimed := p3Pass(t, sim, younger)
		_, reserved, err := rqReservation(ctx, pool, rqOrg, "github", "medium", older)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("lease expired, no repair yet: the younger pass claimed %d; the older run reserved=%v", youngerClaimed, reserved)
		if youngerClaimed != 0 || !reserved {
			t.Fatalf("the younger pass took the slot an expired lease freed (claimed %d, older reserved=%v), want 0 and a reservation", youngerClaimed, reserved)
		}
	})
}

// C1: a capped pass whose only candidate is a stale dispatching unit parks its row. When a slot frees, that run is
// eligible by the same dispatchable rule the materializer's stale recovery uses, and its row is woken.
func TestP4ParkedStaleDispatchingRunIsRecovered(t *testing.T) {
	for _, backfill := range []bool{false, true} {
		t.Run(fmt.Sprintf("backfill=%t", backfill), func(t *testing.T) {
			withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				sim, sweeps := newP3Sim(t, ctx, pool)
				holder := sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 2, runningEnd: sim.now.Add(10 * time.Hour)})
				due := sim.now
				stale := sim.seed(rqRunSpec{index: 1, backfill: backfill, createdAt: sim.now.Add(-20 * time.Hour), medium: 1, wakeAt: &due})
				if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status = 'dispatching', updated_at = $2 WHERE sync_run_id = $1::uuid`,
					stale, sim.now.Add(-staleDispatchSeconds()-time.Second).Truncate(time.Microsecond)); err != nil {
					t.Fatal(err)
				}
				staleAt := sim.now.Add(-staleDispatchSeconds() - time.Second).Truncate(time.Microsecond)
				p4PassOnly(t, sim, stale)
				var unitStatus string
				var unitUpdated time.Time
				if err := pool.QueryRow(ctx, `SELECT status, updated_at FROM sync_run_units WHERE sync_run_id = $1::uuid`, stale).Scan(&unitStatus, &unitUpdated); err != nil {
					t.Fatal(err)
				}
				if unitStatus != "dispatching" || !unitUpdated.Equal(staleAt) {
					t.Fatalf("premise: the capped pass reclaimed the stale unit (status %s, updated_at %s)", unitStatus, unitUpdated)
				}
				if status, lastError, _ := p4RowOf(t, ctx, pool, stale); status != "dispatched" || lastError == nil || *lastError != bucketParkedError {
					t.Fatalf("premise: the capped pass did not park its row (status %s, last_error %v)", status, lastError)
				}
				sim.now = sim.now.Add(time.Minute)
				p4EndUnitAsWriter(t, sim, sweeps, rqID(3, 0, 0), holder, "medium")
				woken := sweeps.run(t, sim, p3Hooks{})
				status, lastError, availableAt := p4RowOf(t, ctx, pool, stale)
				t.Logf("after a release: sweep woke %v; stale run's row status=%s last_error=%v available_at=%s", woken, status, lastError, availableAt.Sub(sim.now))
				if status != "pending" || lastError != nil || availableAt.After(sim.now) {
					t.Fatalf("the parked stale-dispatching run was not recovered: row status=%s last_error=%v available_at=%s", status, lastError, availableAt)
				}
			})
		})
	}
}

// C2 (Postgres, INFO only): the head read (limit 1) at depth 70, 140 and 350 with depth-1 runs of the same org and class ahead
// of one eligible target. Bound: rows examined <= 2 when nothing ahead is in backoff (eligible, other-bucket and
// in-discovery prefixes), <= b + 2 when b same-bucket runs ahead wait for a future retry.
func TestP4HeadReadRowsDoNotGrowWithDepth(t *testing.T) {
	for _, prefix := range []string{"eligible", "other_bucket", "discovering", "backoff"} {
		for _, depth := range []int{70, 140, 350} {
			t.Run(fmt.Sprintf("%s/%d", prefix, depth), func(t *testing.T) {
				withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
					now := pgNow()
					costClass, status, discovery := "medium", "planned", "success"
					availableAt := now
					switch prefix {
					case "other_bucket":
						costClass = "light"
					case "discovering":
						discovery = "planned"
					case "backoff":
						status, availableAt = "retrying", now.Add(time.Hour)
					}
					for _, statement := range []struct {
						sql  string
						args []any
					}{
						{`INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
						  SELECT ('00000000-0000-4000-900f-' || lpad(to_hex(n), 12, '0'))::uuid, $1, $2::uuid, 'schedule', 'incremental', 'running', 1, 0, 0,
							$4::timestamptz - make_interval(mins => $3::int + 1 - n)
						  FROM generate_series(1, $3::int) AS n`, []any{rqOrg, rqIntegration, depth, now}},
						{`INSERT INTO sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at)
						  SELECT ('00000000-0000-4000-9010-' || lpad(to_hex(n), 12, '0'))::uuid, ('00000000-0000-4000-900f-' || lpad(to_hex(n), 12, '0'))::uuid,
							$1, CASE WHEN n = $2::int THEN 'success' ELSE $3 END, 1, $4::timestamptz, $4::timestamptz, $4::timestamptz
						  FROM generate_series(1, $2::int) AS n`, []any{rqOrg, depth, discovery, now}},
						{`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts,
							available_at, created_at, updated_at)
						  SELECT ('00000000-0000-4000-9011-' || lpad(to_hex(n), 12, '0'))::uuid, $1, ('00000000-0000-4000-900f-' || lpad(to_hex(n), 12, '0'))::uuid,
							$2::uuid, $3::uuid, 'github', 'commits', CASE WHEN n = $4::int THEN 'medium' ELSE $5 END, 'incremental',
							CASE WHEN n = $4::int THEN 'planned' ELSE $6 END, 0,
							CASE WHEN n = $4::int THEN NULL ELSE $7::timestamptz END, $8::timestamptz, $8::timestamptz
						  FROM generate_series(1, $4::int) AS n`, []any{rqOrg, rqIntegration, rqSource, depth, costClass, status, availableAt, now}},
					} {
						if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
							t.Fatalf("seed: %v", err)
						}
					}
					if _, err := pool.Exec(ctx, `ANALYZE`); err != nil {
						t.Fatal(err)
					}
					rows, err := p4HeadReadRows(ctx, pool, rqOrg, "github", "medium", false, now)
					if err != nil {
						t.Fatal(err)
					}
					bound := 2.0
					if prefix == "backoff" {
						bound = float64(depth-1) + 2
					}
					t.Logf("INFO %s prefix, depth %d: head read examined %.0f rows (ticket 14338 target %.0f)", prefix, depth, rows, bound)
				})
			})
		}
	}
}

// C7: lease repair moves a unit to a retry 60 s ahead while its run's dispatch row is dispatched and long consumed
// (available_at 120 s in the past) and another unit of the run runs. (a) The wake must not take the consumed instant;
// (b) a pass delivered early (nothing dispatchable, one unit in flight) must leave a wakeup at the retry deadline.
func TestP4FutureRetryWakeIsNeverAConsumedInstant(t *testing.T) {
	for _, step := range []string{"wake", "early_pass"} {
		t.Run(step, func(t *testing.T) {
			withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				sim, sweeps := newP3Sim(t, ctx, pool)
				consumed := sim.now.Add(-120 * time.Second)
				runID := sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-time.Hour), running: 2, runningEnd: sim.now.Add(10 * time.Hour), wakeAt: &consumed})
				if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET status = 'dispatched', dispatched_at = available_at, dispatched_transport = 'river',
	dispatched_route_generation = $2 WHERE sync_run_id = $1::uuid`, runID, dispatchRouteGeneration); err != nil {
					t.Fatal(err)
				}
				deadline := sim.now.Add(time.Minute)
				p3EndUnit(t, sim, sweeps, rqID(3, 0, 0), runID, syncrunrollup.DispatchWakePathLeaseRepair, syncRunUnitStatusRetrying, &deadline)
				status, _, availableAt := p4RowOf(t, ctx, pool, runID)
				if step == "wake" {
					t.Logf("after the repair's wake: row status=%s available_at=%s (deadline +1m)", status, availableAt.Sub(sim.now))
					if status != "pending" || availableAt.Before(deadline.Add(-time.Second)) {
						t.Fatalf("the future-retry wake took the consumed instant: status=%s available_at=%s, want pending at the deadline %s",
							status, availableAt, deadline)
					}
					return
				}
				p3Pass(t, sim, runID)
				status, _, availableAt = p4RowOf(t, ctx, pool, runID)
				t.Logf("early pass with one unit in flight: row status=%s available_at=%s (deadline +1m)", status, availableAt.Sub(sim.now))
				if status != "pending" || availableAt.After(deadline.Add(time.Second)) || availableAt.Before(deadline.Add(-time.Second)) {
					t.Fatalf("the early pass left no wakeup at the retry deadline: status=%s available_at=%s, want pending at %s", status, availableAt, deadline)
				}
			})
		})
	}
}

// p4PassOnly delivers runID's dispatch row now and runs its pass, without starting what it claimed.
func p4PassOnly(t *testing.T, sim *rqSim, runID string) {
	t.Helper()
	var outboxID string
	if err := sim.pool.QueryRow(sim.ctx, `UPDATE sync_dispatch_outbox SET status = 'dispatched', dispatched_at = $2, dispatched_transport = 'river',
	dispatched_route_generation = $3, available_at = LEAST(available_at, $2), updated_at = $2
WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run' RETURNING id::text`, runID, sim.now, dispatchRouteGeneration).Scan(&outboxID); err != nil {
		t.Fatalf("deliver %s: %v", runID, err)
	}
	args := DispatchSyncRunArgs{TransportArgs: TransportArgs{Version: ContractVersionV1, OrgID: rqOrg, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: dispatchRouteGeneration}}
	if err := sim.service.Dispatch(sim.ctx, args); err != nil {
		t.Fatalf("pass of %s: %v", runID, err)
	}
}
