//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type claimedUnitRowResult struct {
	status         string
	availableAt    *time.Time
	firstBlockedAt *time.Time
	updatedAt      time.Time
}

func claimedUnitRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) claimedUnitRowResult {
	t.Helper()
	var result claimedUnitRowResult
	if err := pool.QueryRow(ctx, `SELECT status, available_at, first_blocked_at, updated_at FROM sync_run_units WHERE id=$1`, id).
		Scan(&result.status, &result.availableAt, &result.firstBlockedAt, &result.updatedAt); err != nil {
		t.Fatal(err)
	}
	return result
}

// TestClaimUnitsClaimsFreshPlannedUnits pins the PLANNED claim path: status
// moves to DISPATCHING and first_blocked_at is cleared (CHAOS-3412: a
// successful claim is the moment a unit stops being blocked).
func TestClaimUnitsClaimsFreshPlannedUnits(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		past := now.Add(-time.Hour)
		unitID := "00000000-0000-4000-8000-000000000601"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusPlanned, updatedAt: now, firstBlockedAt: &past})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, err := claimUnits(ctx, tx, budgetCandidatesRunID, nil, now)
		if err != nil {
			t.Fatalf("claimUnits: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if len(units) != 1 || units[0].id != unitID {
			t.Fatalf("got=%v want exactly [%s]", idsOf(units), unitID)
		}

		row := claimedUnitRow(t, ctx, pool, unitID)
		if row.status != syncRunUnitStatusDispatching {
			t.Fatalf("status=%q want=dispatching", row.status)
		}
		if row.firstBlockedAt != nil {
			t.Fatalf("firstBlockedAt=%v want=nil (cleared on claim)", row.firstBlockedAt)
		}
	})
}

// TestClaimUnitsClaimsDueRetryingUnitsAndClearsAvailableAt pins the due
// RETRYING claim path: status moves to DISPATCHING, available_at is
// cleared (not left dangling with a past timestamp), first_blocked_at is
// cleared.
func TestClaimUnitsClaimsDueRetryingUnitsAndClearsAvailableAt(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		due := now.Add(-time.Minute)
		blocked := now.Add(-2 * time.Hour)
		unitID := "00000000-0000-4000-8000-000000000602"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusRetrying, availableAt: &due, updatedAt: now, firstBlockedAt: &blocked})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, err := claimUnits(ctx, tx, budgetCandidatesRunID, nil, now)
		if err != nil {
			t.Fatalf("claimUnits: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if len(units) != 1 || units[0].id != unitID {
			t.Fatalf("got=%v want exactly [%s]", idsOf(units), unitID)
		}

		row := claimedUnitRow(t, ctx, pool, unitID)
		if row.status != syncRunUnitStatusDispatching {
			t.Fatalf("status=%q want=dispatching", row.status)
		}
		if row.availableAt != nil {
			t.Fatalf("availableAt=%v want=nil (cleared on claim)", row.availableAt)
		}
		if row.firstBlockedAt != nil {
			t.Fatalf("firstBlockedAt=%v want=nil (cleared on claim)", row.firstBlockedAt)
		}
	})
}

// TestClaimUnitsSkipsNotYetDueRetryingUnits pins the negative case: a
// RETRYING unit whose available_at has not arrived must be left exactly
// as-is.
func TestClaimUnitsSkipsNotYetDueRetryingUnits(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		future := now.Add(time.Hour)
		unitID := "00000000-0000-4000-8000-000000000603"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, err := claimUnits(ctx, tx, budgetCandidatesRunID, nil, now)
		if err != nil {
			t.Fatalf("claimUnits: %v", err)
		}
		if len(units) != 0 {
			t.Fatalf("got=%v want none claimed (not yet due)", idsOf(units))
		}
	})
}

// TestClaimUnitsReclaimsStaleDispatchingButNeverRunning pins the F2
// property: a stale DISPATCHING unit is reclaimed (updated_at refreshed,
// status stays DISPATCHING -- a re-enqueue, not a new claim), while a
// RUNNING unit is NEVER reclaimed by dispatch regardless of age --
// re-dispatching a RUNNING unit would duplicate provider writes; that
// recovery belongs to reconcile_sync_dispatch, not this function.
func TestClaimUnitsReclaimsStaleDispatchingButNeverRunning(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		staleCutoff := staleDispatchCutoff(now)
		staleDispatching := "00000000-0000-4000-8000-000000000604"
		veryOldRunning := "00000000-0000-4000-8000-000000000605"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: staleDispatching, status: syncRunUnitStatusDispatching, updatedAt: staleCutoff.Add(-time.Minute)})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: veryOldRunning, status: syncRunUnitStatusRunning, updatedAt: staleCutoff.Add(-24 * time.Hour)})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, err := claimUnits(ctx, tx, budgetCandidatesRunID, nil, now)
		if err != nil {
			t.Fatalf("claimUnits: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		got := idsOf(units)
		if len(got) != 1 || !got[staleDispatching] {
			t.Fatalf("got=%v want exactly [%s] (the stale dispatching unit)", got, staleDispatching)
		}

		staleRow := claimedUnitRow(t, ctx, pool, staleDispatching)
		if staleRow.status != syncRunUnitStatusDispatching {
			t.Fatalf("staleDispatching status=%q want=dispatching (re-enqueued, status unchanged)", staleRow.status)
		}
		if !staleRow.updatedAt.After(staleCutoff) {
			t.Fatalf("staleDispatching updated_at=%s want refreshed past the stale cutoff=%s", staleRow.updatedAt, staleCutoff)
		}

		runningRow := claimedUnitRow(t, ctx, pool, veryOldRunning)
		if runningRow.status != syncRunUnitStatusRunning {
			t.Fatalf("veryOldRunning status=%q want unchanged (running) -- F2: dispatch never reclaims RUNNING", runningRow.status)
		}
	})
}

// TestClaimUnitsExcludesCappedUnitIDsFromEveryClaimPath pins the
// concurrency-guard exclusion across ALL THREE claim statements -- a unit
// the guard deferred must stay untouched whether it would otherwise match
// the planned, due-retrying, or stale-dispatching branch.
func TestClaimUnitsExcludesCappedUnitIDsFromEveryClaimPath(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		due := now.Add(-time.Minute)
		staleCutoff := staleDispatchCutoff(now)

		cappedPlanned := "00000000-0000-4000-8000-000000000611"
		cappedRetrying := "00000000-0000-4000-8000-000000000612"
		cappedStale := "00000000-0000-4000-8000-000000000613"
		uncappedPlanned := "00000000-0000-4000-8000-000000000614"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: cappedPlanned, status: syncRunUnitStatusPlanned, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: cappedRetrying, status: syncRunUnitStatusRetrying, availableAt: &due, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: cappedStale, status: syncRunUnitStatusDispatching, updatedAt: staleCutoff.Add(-time.Minute)})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: uncappedPlanned, status: syncRunUnitStatusPlanned, updatedAt: now})

		capped := map[string]bool{cappedPlanned: true, cappedRetrying: true, cappedStale: true}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, err := claimUnits(ctx, tx, budgetCandidatesRunID, capped, now)
		if err != nil {
			t.Fatalf("claimUnits: %v", err)
		}
		got := idsOf(units)
		if len(got) != 1 || !got[uncappedPlanned] {
			t.Fatalf("got=%v want exactly [%s] -- every capped id must be excluded from every claim path", got, uncappedPlanned)
		}
	})
}

// TestClaimUnitsReturnsEmptyWhenNothingClaimable pins the empty-result
// contract: no candidates means an empty (nil) slice, not an error.
func TestClaimUnitsReturnsEmptyWhenNothingClaimable(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, err := claimUnits(ctx, tx, budgetCandidatesRunID, nil, time.Now())
		if err != nil {
			t.Fatalf("claimUnits: %v", err)
		}
		if len(units) != 0 {
			t.Fatalf("got=%v want empty", units)
		}
	})
}
