//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncbucket"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PLAN-14250-4 build cells (acp plan4-build-1): green-only cells beside the plan's red cells.

// C1-eq: the one dispatchable rule agrees in its three forms: the materializer's SQL (syncbucket.DispatchableUnitSQL),
// the guard's Go classify (syncbucket.Dispatchable) and the bucket's head read (a run is a head iff its unit is
// dispatchable and its discovery succeeded), over every unit state at the edges.
func TestB4DispatchableRuleAgreesInItsThreeForms(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		staleCutoff := now.Add(-staleDispatchSeconds())
		past, future := now.Add(-time.Second), now.Add(time.Second)
		cases := []struct {
			status      string
			updatedAt   time.Time
			availableAt *time.Time
		}{
			{"planned", now, nil},
			{"retrying", now, &past}, {"retrying", now, &now}, {"retrying", now, &future}, {"retrying", now, nil},
			{"dispatching", now, nil}, {"dispatching", staleCutoff, nil}, {"dispatching", staleCutoff.Add(-time.Second), nil},
			{"dispatching", staleCutoff.Add(time.Second), nil},
			{"running", now, nil}, {"success", now, nil}, {"failed", now, nil},
		}
		for index, tc := range cases {
			for _, discovered := range []bool{true, false} {
				name := fmt.Sprintf("%s/updated%+.0fs/available=%v/discovered=%t", tc.status, tc.updatedAt.Sub(now).Seconds(), tc.availableAt, discovered)
				if _, err := pool.Exec(ctx, `TRUNCATE sync_dispatch_bucket_reservations, sync_run_units, sync_run_reference_discoveries, sync_dispatch_outbox, sync_runs CASCADE`); err != nil {
					t.Fatal(err)
				}
				sim := newRQSim(t, ctx, pool, now, time.Hour)
				runID := sim.seed(rqRunSpec{index: index + 1, createdAt: now.Add(-time.Hour), medium: 1, noDiscovery: !discovered})
				if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status = $2, updated_at = $3, available_at = $4 WHERE sync_run_id = $1::uuid`,
					runID, tc.status, tc.updatedAt, tc.availableAt); err != nil {
					t.Fatal(err)
				}
				var sqlRule bool
				if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM sync_run_units AS unit WHERE unit.sync_run_id = $3::uuid AND `+
					syncbucket.DispatchableUnitSQL+`)`, now, staleCutoff, runID).Scan(&sqlRule); err != nil {
					t.Fatal(err)
				}
				goRule := syncbucket.Dispatchable(tc.status, tc.updatedAt, tc.availableAt, now, staleCutoff)
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				heads, err := syncbucket.ReadHeads(ctx, tx, syncbucket.Bucket{OrgID: rqOrg, Provider: "github", CostClass: "medium"}, false, now, staleCutoff, 1, nil)
				_ = tx.Rollback(ctx)
				if err != nil {
					t.Fatal(err)
				}
				isHead := len(heads) == 1
				if sqlRule != goRule || isHead != (goRule && discovered) {
					t.Errorf("%s: materializer SQL %t, guard Go %t, head read %t (want SQL == Go and head == Go && discovered)", name, sqlRule, goRule, isHead)
				}
			}
		}
	})
}

// N1 TTL: a reservation whose run never claims is void after the TTL; the sweep then picks the slot again by
// eligibility (here the same oldest run, afresh), and snoozes to the earliest reservation's expiry, not the 1 h re-arm.
func TestB4VoidReservationIsPickedAgainByTheSweep(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		rqSetEnv(t)
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		older := sim.seed(rqRunSpec{index: 1, createdAt: t0.Add(-5 * time.Hour), medium: 1})
		sim.seed(rqRunSpec{index: 2, createdAt: t0.Add(-time.Hour), medium: 1})
		ttl := syncbucket.ReservationTTL()
		if err := rqSetReservation(ctx, pool, rqOrg, "github", "medium", older, false, t0.Add(-ttl-time.Second)); err != nil {
			t.Fatal(err)
		}
		result, err := runBucketSweep(ctx, pool, dispatchBucket{orgID: rqOrg, provider: "github", costClass: "medium"}, func() time.Time { return t0 })
		if err != nil {
			t.Fatal(err)
		}
		at, reserved, err := rqReservation(ctx, pool, rqOrg, "github", "medium", older)
		if err != nil {
			t.Fatal(err)
		}
		rearm := sweepRearm(t0, result.nextAt)
		t.Logf("void reservation: sweep woke %v; older reserved=%v at %s; next sweep in %s (TTL %s)", result.woken, reserved, at.Sub(t0), rearm, ttl)
		if !reserved || !at.Equal(t0) || len(result.woken) < 1 || result.woken[0] != older {
			t.Fatalf("the sweep did not pick the void reservation's slot again by eligibility: woken=%v reserved=%v at=%s", result.woken, reserved, at)
		}
		if rearm > ttl+time.Second || rearm >= cappedRearmMax() {
			t.Fatalf("the sweep re-arms in %s, want the reservation's expiry (<= TTL %s + 1 s), not the %s re-arm", rearm, ttl, cappedRearmMax())
		}
	})
}

// N1 on the pass: a pass granted a slot that it does not fill (its unit is refused by the budget) reserves that slot,
// in its own transaction, for the bucket's next eligible waiter, before any sweep runs.
func TestB4PassReservesTheSlotItLeavesUnused(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		sim, _ := newP3Sim(t, ctx, pool)
		due := sim.now
		refusing := sim.seed(rqRunSpec{index: 1, createdAt: sim.now.Add(-2 * time.Hour), medium: 1, wakeAt: &due})
		next := sim.seed(rqRunSpec{index: 2, createdAt: sim.now.Add(-time.Hour), medium: 1})
		p4SetCostClass(t, ctx, pool, "heavy")
		sim.refuse(1)
		if claimed := p3Pass(t, sim, refusing); claimed != 0 {
			t.Fatalf("premise: the refused pass claimed %d", claimed)
		}
		_, reserved, err := rqReservation(ctx, pool, rqOrg, "github", "heavy", next)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("refused pass in a heavy bucket (cap 1): the next eligible run reserved=%v right after the pass's commit", reserved)
		if !reserved {
			t.Fatal("the pass left its granted slot unreserved: the next eligible run holds no reservation after the pass's commit")
		}
	})
}
