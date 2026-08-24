//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestComputePendingUnitCountsClassifiesEveryNonTerminalStatus pins the
// full classification: PLANNED and due RETRYING count as dispatchable;
// stale DISPATCHING counts as dispatchable (a reclaimable claim); fresh
// DISPATCHING and RUNNING count as in-flight; a not-yet-due RETRYING unit
// contributes to nextDeferredAt instead of either count; SUCCESS/FAILED
// units are excluded entirely.
func TestComputePendingUnitCountsClassifiesEveryNonTerminalStatus(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		past := now.Add(-time.Hour)
		future := now.Add(time.Hour)
		staleCutoff := staleDispatchCutoff(now)

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000801", status: syncRunUnitStatusPlanned, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000802", status: syncRunUnitStatusRetrying, availableAt: &past, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000803", status: syncRunUnitStatusDispatching, updatedAt: staleCutoff.Add(-time.Minute)})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000804", status: syncRunUnitStatusDispatching, updatedAt: now})
		// Exact-boundary fixture: Python's predicate is updated_at <=
		// stale_dispatch_cutoff (inclusive), so a row exactly AT the cutoff
		// must count as stale/dispatchable -- a fixture using only
		// clearly-past/clearly-fresh values passes vacuously against a
		// <= -> < mutation.
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000809", status: syncRunUnitStatusDispatching, updatedAt: staleCutoff})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000805", status: syncRunUnitStatusRunning, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000806", status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000807", status: syncRunUnitStatusSuccess, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000808", status: syncRunUnitStatusFailed, updatedAt: now})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		counts, err := computePendingUnitCounts(ctx, tx, budgetCandidatesRunID, now)
		if err != nil {
			t.Fatalf("computePendingUnitCounts: %v", err)
		}

		if counts.dispatchable != 4 {
			t.Fatalf("dispatchable=%d want=4 (planned + due-retrying + stale-dispatching + exact-boundary-dispatching)", counts.dispatchable)
		}
		if counts.inFlight != 2 {
			t.Fatalf("inFlight=%d want=2 (fresh-dispatching + running)", counts.inFlight)
		}
		if counts.nextDeferredAt == nil || !counts.nextDeferredAt.Equal(future) {
			t.Fatalf("nextDeferredAt=%v want=%s (the not-yet-due retrying unit)", counts.nextDeferredAt, future)
		}
	})
}

// TestComputePendingUnitCountsTracksTheEarliestDeferral pins the
// earliest-wins aggregation across multiple not-yet-due RETRYING units.
func TestComputePendingUnitCountsTracksTheEarliestDeferral(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		soon := now.Add(10 * time.Minute)
		later := now.Add(2 * time.Hour)

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000811", status: syncRunUnitStatusRetrying, availableAt: &later, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: "00000000-0000-4000-8000-000000000812", status: syncRunUnitStatusRetrying, availableAt: &soon, updatedAt: now})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		counts, err := computePendingUnitCounts(ctx, tx, budgetCandidatesRunID, now)
		if err != nil {
			t.Fatalf("computePendingUnitCounts: %v", err)
		}
		if counts.dispatchable != 0 || counts.inFlight != 0 {
			t.Fatalf("dispatchable=%d inFlight=%d want both 0", counts.dispatchable, counts.inFlight)
		}
		if counts.nextDeferredAt == nil || !counts.nextDeferredAt.Equal(soon) {
			t.Fatalf("nextDeferredAt=%v want=%s (the SOONER of the two)", counts.nextDeferredAt, soon)
		}
	})
}
