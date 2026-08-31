//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/providerfamilycontract"
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

// authorized builds claimUnits' CHAOS-4605 allow-list. Every test below
// authorizes EVERY unit it seeds, so what each case still exercises is
// claimUnits' own status/timing predicates and the capped exclusion --
// unchanged by the allow-list. The allow-list's own behavior is pinned
// separately in dispatch_guard_claim_snapshot_integration_test.go.
func authorized(ids ...string) map[string]bool {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}

// TestClaimUnitsClaimsFreshPlannedUnits pins the PLANNED claim path: status
// moves to DISPATCHING and first_blocked_at is cleared (CHAOS-3412: a
// successful claim is the moment a unit stops being blocked).
func TestClaimUnitsClaimsFreshPlannedUnits(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		past := now.Add(-time.Hour)
		unitID := "00000000-0000-4000-8000-000000000601"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusPlanned, updatedAt: now, firstBlockedAt: &past})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, _, err := claimUnits(ctx, tx, budgetCandidatesRunID, authorized(unitID), nil, now)
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
		now := pgNow()
		due := now.Add(-time.Minute)
		blocked := now.Add(-2 * time.Hour)
		unitID := "00000000-0000-4000-8000-000000000602"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusRetrying, availableAt: &due, updatedAt: now, firstBlockedAt: &blocked})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, _, err := claimUnits(ctx, tx, budgetCandidatesRunID, authorized(unitID), nil, now)
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
		now := pgNow()
		future := now.Add(time.Hour)
		unitID := "00000000-0000-4000-8000-000000000603"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, _, err := claimUnits(ctx, tx, budgetCandidatesRunID, authorized(unitID), nil, now)
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
		now := pgNow()
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
		units, _, err := claimUnits(ctx, tx, budgetCandidatesRunID, authorized(staleDispatching, veryOldRunning), nil, now)
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
		now := pgNow()
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
		units, _, err := claimUnits(ctx, tx, budgetCandidatesRunID, authorized(cappedPlanned, cappedRetrying, cappedStale, uncappedPlanned), capped, now)
		if err != nil {
			t.Fatalf("claimUnits: %v", err)
		}
		got := idsOf(units)
		if len(got) != 1 || !got[uncappedPlanned] {
			t.Fatalf("got=%v want exactly [%s] -- every capped id must be excluded from every claim path", got, uncappedPlanned)
		}
	})
}

// TestClaimUnitsRoundTripsProcessorFlagsForValidateClaim is the
// mismatched-value fixture team-lead asked for: a claimed unit's
// processor_flags column must decode into EXACTLY the shape
// providerfamilycontract.ValidateClaim consumes -- proven here by feeding
// a claimed unit's own processorFlags into ValidateClaim directly, both a
// COMPLETE atomic-family flag set (must be admitted) and one missing a
// required flag (must be refused), rather than asserting on the decoded
// map's shape in isolation.
func TestClaimUnitsRoundTripsProcessorFlagsForValidateClaim(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		complete := "00000000-0000-4000-8000-000000000621"
		incomplete := "00000000-0000-4000-8000-000000000622"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{
			id: complete, status: syncRunUnitStatusPlanned, updatedAt: now, datasetKey: "work-items",
			processorFlagsJSON: `{"family_dataset_work_items":true,"family_dataset_work_item_labels":true,` +
				`"family_dataset_work_item_projects":true,"family_dataset_work_item_history":true,` +
				`"family_dataset_work_item_comments":true}`,
		})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{
			id: incomplete, status: syncRunUnitStatusPlanned, updatedAt: now, datasetKey: "work-items",
			// Missing family_dataset_work_item_comments -- an incomplete
			// family claim, the exact malformed-persisted-state case
			// ValidateClaim exists to reject.
			processorFlagsJSON: `{"family_dataset_work_items":true,"family_dataset_work_item_labels":true,` +
				`"family_dataset_work_item_projects":true,"family_dataset_work_item_history":true}`,
		})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, _, err := claimUnits(ctx, tx, budgetCandidatesRunID, authorized(complete, incomplete), nil, now)
		if err != nil {
			t.Fatalf("claimUnits: %v", err)
		}
		byID := map[string]budgetUnit{}
		for _, unit := range units {
			byID[unit.id] = unit
		}
		if len(byID) != 2 {
			t.Fatalf("got %d claimed units, want 2", len(byID))
		}

		if err := providerfamilycontract.ValidateClaim(byID[complete].provider, byID[complete].datasetKey, byID[complete].processorFlags, true); err != nil {
			t.Fatalf("ValidateClaim(complete): %v, want nil -- the full family flag set was round-tripped correctly", err)
		}
		if err := providerfamilycontract.ValidateClaim(byID[incomplete].provider, byID[incomplete].datasetKey, byID[incomplete].processorFlags, true); err == nil {
			t.Fatal("ValidateClaim(incomplete): want an error -- a claimed unit missing one family flag must be refused, not silently admitted")
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
		units, _, err := claimUnits(ctx, tx, budgetCandidatesRunID, nil, nil, pgNow())
		if err != nil {
			t.Fatalf("claimUnits: %v", err)
		}
		if len(units) != 0 {
			t.Fatalf("got=%v want empty", units)
		}
	})
}
