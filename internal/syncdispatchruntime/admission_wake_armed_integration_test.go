//go:build integration

package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-14394 wake-armed liveness cells. Bucket github/medium (cap 2). Run A (older, 2 h, scheduled incremental) was
// never claimed; run B (newer) has one finished unit, one running unit with a live lease (the busy slot) and 3 planned
// units. One slot is free. The only difference between cells is A's units and A's dispatch_sync_run outbox row.

type olderUnits int

const (
	olderPlanned olderUnits = iota
	olderRetryingFuture
	olderTerminal
)

type wakeRow struct {
	status    string
	lastError *string
	available time.Time
	updated   time.Time
	attempts  int
}

type olderRunShape struct {
	units    olderUnits
	backfill bool
	wake     *wakeRow
	liveUnit bool
}

func seedWakeRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string, row wakeRow) {
	t.Helper()
	if row.status == "dispatched" {
		pgseed.SyncDispatchOutbox(ctx, t, pool, "00000000-0000-4000-8000-000000143c01", runID, guardTestOrg, "dispatch_sync_run", "dispatched", "river", 1)
		return
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_dispatch_outbox (id, org_id, sync_run_id, kind, status, available_at, attempts, last_error, created_at, updated_at)
VALUES ('00000000-0000-4000-8000-000000143c02'::uuid, $1, $2::uuid, 'dispatch_sync_run', $3, $4, $5, $6, $7, $7)`,
		guardTestOrg, runID, row.status, row.available, row.attempts, row.lastError, row.updated); err != nil {
		t.Fatalf("seed wake row: %v", err)
	}
}

func seedOlderRunCase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time, shape olderRunShape) {
	t.Helper()
	old, recent := now.Add(-2*time.Hour), now.Add(-time.Minute)
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: admissionOlderRun, OrgID: guardTestOrg})
	trigger, mode := "schedule", "incremental"
	if shape.backfill {
		trigger, mode = "backfill", "backfill"
	}
	for _, args := range [][]any{{admissionOlderRun, old, trigger, mode}, {guardTestRun, now.Add(-time.Hour), "schedule", "incremental"}} {
		if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET triggered_by = $3, mode = $4, status = 'dispatching', created_at = $2 WHERE id = $1::uuid`, args...); err != nil {
			t.Fatal(err)
		}
	}
	future := now.Add(time.Hour)
	for i := 0; i < 2; i++ {
		unit := pgseed.SyncRunUnit{
			ID: guardUnitID("00000143a0", i), RunID: admissionOlderRun, OrgID: guardTestOrg, Provider: "github", CostClass: "medium",
			Status: syncRunUnitStatusPlanned, UpdatedAt: &old,
		}
		switch shape.units {
		case olderRetryingFuture:
			unit.Status, unit.AvailableAt = syncRunUnitStatusRetrying, &future
		case olderTerminal:
			unit.Status = "success"
		}
		pgseed.InsertSyncRunUnit(ctx, t, pool, unit)
	}
	if shape.liveUnit {
		lease := now.Add(time.Hour)
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
			ID: guardUnitID("00000143a1", 0), RunID: admissionOlderRun, OrgID: guardTestOrg, Provider: "github", CostClass: "light",
			Status: syncRunUnitStatusRunning, UpdatedAt: &recent, LeaseExpiresAt: &lease, LeaseOwner: ptrString("a-owner"),
		})
		if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET last_heartbeat_at = $2 WHERE id = $1::uuid`, guardUnitID("00000143a1", 0), recent); err != nil {
			t.Fatal(err)
		}
	}
	lease := now.Add(time.Hour)
	insertGuardUnit(t, ctx, pool, guardUnitID("00000143b0", 0), "github", "medium", "success", old, nil, nil, nil)
	insertGuardUnit(t, ctx, pool, guardUnitID("00000143b1", 0), "github", "medium", syncRunUnitStatusRunning, recent, nil, &lease, ptrString("b-owner"))
	for i := 0; i < 3; i++ {
		insertGuardUnit(t, ctx, pool, guardUnitID("00000143b2", i), "github", "medium", syncRunUnitStatusPlanned, now, nil, nil, nil)
	}
	if shape.wake != nil {
		seedWakeRow(t, ctx, pool, admissionOlderRun, *shape.wake)
	}
}

// capRearm is the row a capped pass of A leaves: pending, no error, due one countdown ahead.
func capRearm(now time.Time) *wakeRow {
	return &wakeRow{status: "pending", available: now.Add(150 * time.Second), updated: now.Add(-time.Minute), attempts: 3}
}

func passFor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, run string, now time.Time, name string) (guardDecision, int) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d, err := authorizeRun(ctx, tx, nil, guardTestOrg, run, now)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("%s: %v", name, err)
	}
	capped := map[string]bool{}
	for _, id := range d.cappedUnitIDs {
		capped[id] = true
	}
	admitted := 0
	for id := range d.authorizedUnitIDs {
		if capped[id] {
			continue
		}
		admitted++
		if _, err := tx.Exec(ctx, `UPDATE public.sync_run_units SET status = 'dispatching', updated_at = $2 WHERE id = $1::uuid AND status = 'planned'`, id, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: run=%s authorized=%d capped=%d admitted=%d held_back_run_ids=%v", name, run, len(d.authorizedUnitIDs), len(d.cappedUnitIDs), admitted, d.heldBackRunIDs)
	return d, admitted
}

// assertBHeldForA: B's pass leaves the free slot to A (held back for A, nothing admitted for B).
func assertBHeldForA(t *testing.T, shape func(now time.Time) olderRunShape) {
	t.Helper()
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedOlderRunCase(t, ctx, pool, now, shape(now))
		b, admitted := passFor(t, ctx, pool, guardTestRun, now, "B pass (wake)")
		if !reflect.DeepEqual(b.heldBackRunIDs, []string{admissionOlderRun}) || admitted != 0 {
			t.Fatalf("want B held back for A and 0 admitted; got heldBack=%v admitted=%d", b.heldBackRunIDs, admitted)
		}
	})
}

// assertBTakesSlot: B's pass is not held back and takes the free slot.
func assertBTakesSlot(t *testing.T, shape func(now time.Time) olderRunShape) {
	t.Helper()
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedOlderRunCase(t, ctx, pool, now, shape(now))
		b, admitted := passFor(t, ctx, pool, guardTestRun, now, "B pass (wake)")
		if len(b.heldBackRunIDs) != 0 || admitted != 1 {
			t.Fatalf("want no hold-back and B admitted 1; got heldBack=%v admitted=%d", b.heldBackRunIDs, admitted)
		}
	})
}

// RED at base (2c881514 and main): A never claimed, 2 h old, its cap re-arm row armed -> B takes the freed slot.
func TestNeverClaimedOlderRunWithCapRearmHoldsBackNewerRun(t *testing.T) {
	assertBHeldForA(t, func(now time.Time) olderRunShape { return olderRunShape{wake: capRearm(now)} })
}

// Control: A has a live unit (heartbeat + lease) and no wake row -> A holds B back by the old signs.
func TestLiveOlderRunHoldsBackNewerRunControl(t *testing.T) {
	assertBHeldForA(t, func(now time.Time) olderRunShape { return olderRunShape{liveUnit: true} })
}

// Bound edges: the upper edge is inclusive.
func TestWakeArmedAtTheUpperEdgeHoldsBack(t *testing.T) {
	assertBHeldForA(t, func(now time.Time) olderRunShape {
		row := capRearm(now)
		row.available = now.Add(admissionLivenessBound())
		return olderRunShape{wake: row}
	})
}

// (i-0) no wake row at all and no sign: today's rule, A holds nothing (CHAOS-8880 "every sign past the bound").
func TestNeverClaimedOlderRunWithoutWakeRowHoldsNothing(t *testing.T) {
	assertBTakesSlot(t, func(now time.Time) olderRunShape { return olderRunShape{} })
}

// (i-1) deferral: A's units are retrying with a future available_at (budget / cooldown); the row is armed for that
// time. A is not waiting, so it holds nothing.
func TestOwnRefusalRetryingFutureHoldsNothing(t *testing.T) {
	assertBTakesSlot(t, func(now time.Time) olderRunShape {
		return olderRunShape{units: olderRetryingFuture, wake: capRearm(now)}
	})
}

// (i-2) terminal: A has no waiting unit.
func TestOwnRefusalTerminalUnitsHoldNothing(t *testing.T) {
	assertBTakesSlot(t, func(now time.Time) olderRunShape {
		return olderRunShape{units: olderTerminal, wake: capRearm(now)}
	})
}

// (i-3) discovery block: the pass left the row 'dispatched', not pending.
func TestOwnRefusalDiscoveryBlockRowDispatchedHoldsNothing(t *testing.T) {
	assertBTakesSlot(t, func(now time.Time) olderRunShape {
		return olderRunShape{wake: &wakeRow{status: "dispatched"}}
	})
}

// (i-4) publish failure: pending, due inside the window, last_error set.
func TestOwnRefusalPublishFailureRowHoldsNothing(t *testing.T) {
	assertBTakesSlot(t, func(now time.Time) olderRunShape {
		row := capRearm(now)
		row.lastError = ptrString("publish_failed")
		return olderRunShape{wake: row}
	})
}

// (i-5) paused route: pending, no error, due longer ago than the bound; the relay rewrote updated_at a minute ago
// (never a sign), so only available_at tells it is dead.
func TestOwnRefusalPausedRouteRowPastLowerEdgeHoldsNothing(t *testing.T) {
	assertBTakesSlot(t, func(now time.Time) olderRunShape {
		return olderRunShape{wake: &wakeRow{status: "pending", available: now.Add(-admissionLivenessBound() - time.Second), updated: now.Add(-time.Second), attempts: 40}}
	})
}

// (i-6) a deferral further out than the bound does not hold slots.
func TestOwnRefusalRowPastUpperEdgeHoldsNothing(t *testing.T) {
	assertBTakesSlot(t, func(now time.Time) olderRunShape {
		row := capRearm(now)
		row.available = now.Add(admissionLivenessBound() + time.Second)
		return olderRunShape{wake: row}
	})
}

// B' (CHAOS-8880/14211): an older never-claimed BACKFILL never holds a newer scheduled run, wake row or not.
func TestOlderBackfillWithWakeArmedNeverHoldsNewerScheduledRun(t *testing.T) {
	assertBTakesSlot(t, func(now time.Time) olderRunShape {
		return olderRunShape{backfill: true, wake: capRearm(now)}
	})
}

// R2_documented: the reconciler backstop (syncreconciler/materializer.go) re-arms a run whose wake was lost: pending,
// no error, due now, attempts high. A run whose passes keep failing holds newer runs while that row is in the window.
// NOT pinned as wanted: this documents the accepted behavior (acp ruling 3a942baa0279).
func TestR2_documentedBackstopArmedRunWithFailingPassesHolds(t *testing.T) {
	assertBHeldForA(t, func(now time.Time) olderRunShape {
		return olderRunShape{wake: &wakeRow{status: "pending", available: now, updated: now, attempts: 900}}
	})
}

// (ii) throughput by operation count (D6418, no wall clock): the passes run in prod order. B's pass holds the free
// slot for A and arms A's wake row due now; A's very next pass claims the slot. Two passes, A's unit claimed in the second.
func TestFreedSlotGoesToTheWaitingOlderRunWithinItsNextPass(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedOlderRunCase(t, ctx, pool, now, olderRunShape{wake: capRearm(now)})
		passes := 0
		b, bAdmitted := passFor(t, ctx, pool, guardTestRun, now, "B pass (wake)")
		passes++
		wakeHeldBackRuns(ctx, pool, b.heldBackRunIDs, now)
		var due int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_dispatch_outbox WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run' AND status = 'pending' AND available_at <= $2`,
			admissionOlderRun, now).Scan(&due); err != nil {
			t.Fatal(err)
		}
		_, aAdmitted := passFor(t, ctx, pool, admissionOlderRun, now.Add(time.Second), "A pass (poll)")
		passes++
		if bAdmitted != 0 || due != 1 || aAdmitted != 1 || passes != 2 {
			t.Fatalf("passes=%d: B admitted=%d (want 0), A wake rows due=%d (want 1), A admitted=%d (want 1)", passes, bAdmitted, due, aAdmitted)
		}
	})
}

// TestWakeArmedProbeReadsOneOutboxRowOfALargeOpenRun (CHAOS-14394): an open older run of 10,000 planned medium units,
// live by no sign but its armed wake row, among 3,000 finished runs that each keep a dispatch row. The read must visit
// a bounded number of units and at most a couple of outbox rows, and the run must read as an older waiting run.
func TestWakeArmedProbeReadsOneOutboxRowOfALargeOpenRun(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		insertGuardUnit(t, ctx, pool, guardUnitID("00000143d0", 0), "github", "medium", syncRunUnitStatusPlanned, now, nil, nil, nil)
		pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: admissionOlderRun, OrgID: guardTestOrg})
		for _, statement := range []struct {
			sql  string
			args []any
		}{
			{`UPDATE public.sync_runs SET triggered_by = 'schedule', mode = 'incremental', status = 'dispatching', created_at = now() - interval '3 hours' WHERE id = $1::uuid`, []any{admissionOlderRun}},
			{`UPDATE public.sync_runs SET triggered_by = 'schedule', mode = 'incremental', status = 'dispatching', created_at = now() WHERE id = $1::uuid`, []any{guardTestRun}},
			{`INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, created_at, updated_at)
			 SELECT md5('large-wake-14394-' || k)::uuid, run.org_id, run.id, run.integration_id,
				(SELECT source_id FROM public.sync_run_units WHERE sync_run_id = $2::uuid LIMIT 1),
				'github', 'dataset-' || k, 'medium', 'incremental', 'planned', 0,
				now() - interval '3 hours', now() - interval '3 hours'
			 FROM public.sync_runs AS run CROSS JOIN generate_series(1, 10000) AS k
			 WHERE run.id = $1::uuid`, []any{admissionOlderRun, guardTestRun}},
			{`INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
			 SELECT md5('history-14394-' || n)::uuid, $1, (SELECT integration_id FROM public.sync_runs WHERE id = $2::uuid),
				'schedule', 'incremental', 'success', 10, 10, 0, now() - make_interval(hours => n + 5)
			 FROM generate_series(1, 3000) AS n`, []any{guardTestOrg, guardTestRun}},
		} {
			if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		if _, err := pool.Exec(ctx, historyOutboxSQL, guardTestOrg); err != nil {
			t.Fatalf("seed history outbox: %v", err)
		}
		seedWakeRow(t, ctx, pool, admissionOlderRun, *capRearm(now))
		if _, err := pool.Exec(ctx, `ANALYZE`); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		self, err := readRunOrder(ctx, tx, guardTestRun)
		if err != nil {
			t.Fatal(err)
		}
		bucket := dispatchBucket{orgID: guardTestOrg, provider: "github", costClass: "medium"}
		var raw string
		if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+olderRunsInBucketSQL,
			bucket.orgID, bucket.provider, bucket.costClass, self.createdAt, self.id, self.backfill, now,
			concurrencyCapForCostClass(bucket.costClass), admissionLivenessBound().Seconds(), olderRunsReadLimit, olderRunWaitingScanLimit,
		).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var plans []struct {
			Plan planNode `json:"Plan"`
		}
		if err := json.Unmarshal([]byte(raw), &plans); err != nil || len(plans) != 1 {
			t.Fatalf("parse plan: %v (%d plans)", err, len(plans))
		}
		units, outbox := rowVisits(plans[0].Plan, "sync_run_units"), rowVisits(plans[0].Plan, "sync_dispatch_outbox")
		t.Logf("rows visited: sync_run_units=%.0f sync_dispatch_outbox=%.0f", units, outbox)
		if units > 2*olderRunWaitingScanLimit || outbox > 2 {
			t.Fatalf("wake-armed read visits units=%.0f (bound %d) outbox=%.0f (bound 2):\n%s", units, 2*olderRunWaitingScanLimit, outbox, raw)
		}
		older, err := readOlderRuns(ctx, tx, bucket, self, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(older) != 1 || older[0].id != admissionOlderRun || older[0].waiting == 0 {
			t.Fatalf("older runs = %+v, want the armed large run, waiting > 0", older)
		}
	})
}
