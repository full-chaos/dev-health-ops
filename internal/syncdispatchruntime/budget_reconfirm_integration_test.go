//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestReconfirmCooldownsReturnsEmptyWhenAllUnitsAlreadyExcluded pins the
// short-circuit: nothing left to check means no cooldown query at all --
// proven here by NOT creating the provider_rate_limit_observations table,
// so a query against it would fail loudly rather than silently succeed.
func TestReconfirmCooldownsReturnsEmptyWhenAllUnitsAlreadyExcluded(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		unitID := "00000000-0000-4000-8000-000000000501"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusPlanned, updatedAt: now})
		unit := budgetUnit{id: unitID, orgID: "org-1", provider: "github"}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		result, err := reconfirmCooldowns(
			ctx, tx, nil, budgetCandidatesRunID, []budgetUnit{unit},
			map[string][]budgetEstimate{unitID: estimateFor(10, "github", "rest_core", "work-items")},
			map[string]bool{unitID: true}, // already excluded
			5, nil, now, now,
		)
		if err != nil {
			t.Fatalf("reconfirmCooldowns: %v", err)
		}
		if len(result.excludedUnitIDs) != 0 || result.nextDeferredAt != nil {
			t.Fatalf("got=%+v want empty result", result)
		}
	})
}

// TestReconfirmCooldownsDefersAFreshlyObservedCooldown pins the ordinary
// (non-surplus) TOCTOU-closure path: a cooldown observation that landed
// AFTER enforceRun's own snapshot (simulated by simply inserting it before
// calling reconfirmCooldowns, which re-reads fresh) must defer the unit
// through the SAME resolveCooldownBlockedUnit path enforceRun itself uses.
func TestReconfirmCooldownsDefersAFreshlyObservedCooldown(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		unitID := "00000000-0000-4000-8000-000000000502"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusPlanned, updatedAt: now})
		routeFamily := "work-items"
		insertObservation(t, ctx, pool, "00000000-0000-4000-8000-000000000511", func(o *insertObservationOpts) {
			o.orgID = "org-1"
			o.integrationID = "00000000-0000-4000-8000-000000000010"
			o.routeFamily = &routeFamily
			o.retryAfterSeconds = floatPtr(3600)
			o.observedAt = now
		})
		unit := budgetUnit{id: unitID, orgID: "org-1", provider: "github", integrationID: "00000000-0000-4000-8000-000000000010"}
		estimates := map[string][]budgetEstimate{unitID: estimateFor(10, "github", "rest_core", "work-items")}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		result, err := reconfirmCooldowns(
			ctx, tx, nil, budgetCandidatesRunID, []budgetUnit{unit}, estimates,
			map[string]bool{}, 5, nil, now, now,
		)
		if err != nil {
			t.Fatalf("reconfirmCooldowns: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		if !result.excludedUnitIDs[unitID] {
			t.Fatal("want unitID excluded (it matched a cooldown)")
		}
		if result.nextDeferredAt == nil {
			t.Fatal("want nextDeferredAt set (a fresh deferral, not a terminal verdict)")
		}
		var status, errorCategory string
		if err := pool.QueryRow(ctx, `SELECT status, result->>'error_category' FROM sync_run_units WHERE id=$1`, unitID).
			Scan(&status, &errorCategory); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusRetrying || errorCategory != rateLimitCooldownDeferredCategory {
			t.Fatalf("status=%q error_category=%q, want retrying/%q", status, errorCategory, rateLimitCooldownDeferredCategory)
		}
	})
}

// TestReconfirmCooldownsWithdrawsASurplusAdmittedUnitInsteadOfDeferring
// pins the CHAOS-3465 CRITICAL finding: a unit the surplus phase pulled
// forward, now caught by a freshly observed cooldown, must be WITHDRAWN
// (available_at restored to its pre-promotion value, episode columns
// untouched) rather than routed through the ordinary cooldown deferral,
// which would fabricate an episode reset.
func TestReconfirmCooldownsWithdrawsASurplusAdmittedUnitInsteadOfDeferring(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		enforcedAt := pgNow().Add(-time.Second) // enforceRun's OWN now, earlier than this check
		checkedAt := pgNow()
		priorAvailableAt := checkedAt.Add(2 * time.Hour) // where the unit was BEFORE the surplus promotion
		unitID := "00000000-0000-4000-8000-000000000503"

		// The row as it stands AFTER enforceRun's surplus promotion:
		// status=retrying, available_at pulled forward to enforcedAt, and a
		// pre-existing budget episode that must survive the withdrawal.
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{
			id: unitID, status: syncRunUnitStatusRetrying, availableAt: &enforcedAt, updatedAt: enforcedAt,
			resultJSON: `{"error_category":"budget_deferred"}`,
		})
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET budget_deferrals=3 WHERE id=$1`, unitID); err != nil {
			t.Fatal(err)
		}

		routeFamily := "work-items"
		insertObservation(t, ctx, pool, "00000000-0000-4000-8000-000000000512", func(o *insertObservationOpts) {
			o.orgID = "org-1"
			o.integrationID = "00000000-0000-4000-8000-000000000010"
			o.routeFamily = &routeFamily
			o.retryAfterSeconds = floatPtr(3600)
			o.observedAt = checkedAt
		})

		unit := budgetUnit{id: unitID, orgID: "org-1", provider: "github", integrationID: "00000000-0000-4000-8000-000000000010", budgetDeferrals: 3}
		estimates := map[string][]budgetEstimate{unitID: estimateFor(10, "github", "rest_core", "work-items")}
		surplusPriorAvailableAt := map[string]time.Time{unitID: priorAvailableAt}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		result, err := reconfirmCooldowns(
			ctx, tx, nil, budgetCandidatesRunID, []budgetUnit{unit}, estimates,
			map[string]bool{}, 5, surplusPriorAvailableAt, enforcedAt, checkedAt,
		)
		if err != nil {
			t.Fatalf("reconfirmCooldowns: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		if !result.excludedUnitIDs[unitID] {
			t.Fatal("want unitID excluded")
		}
		if result.nextDeferredAt != nil {
			t.Fatal("want nextDeferredAt UNSET -- a withdrawn unit is back on its ORIGINAL countdown, not a new deferral this pass caused")
		}

		var status string
		var availableAt time.Time
		var budgetDeferrals int
		var errorCategory string
		if err := pool.QueryRow(ctx, `SELECT status, available_at, budget_deferrals, result->>'error_category' FROM sync_run_units WHERE id=$1`, unitID).
			Scan(&status, &availableAt, &budgetDeferrals, &errorCategory); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusRetrying {
			t.Fatalf("status=%q, want unchanged (retrying)", status)
		}
		if !availableAt.Equal(priorAvailableAt) {
			t.Fatalf("available_at=%s want=%s (restored to pre-promotion value)", availableAt, priorAvailableAt)
		}
		if budgetDeferrals != 3 || errorCategory != "budget_deferred" {
			t.Fatalf("budgetDeferrals=%d errorCategory=%q, want unchanged (3/budget_deferred) -- withdrawal must not touch the episode", budgetDeferrals, errorCategory)
		}
	})
}

// TestReconfirmCooldownsExcludesASurplusUnitEvenWhenTheWithdrawalCASLoses
// pins the "UNCONDITIONAL" property from the review finding: even when the
// withdrawal's own CAS loses the race (something else already moved the
// row), the unit must STILL be excluded from this pass's claim -- a lost
// withdrawal proves nothing here (unlike the ordinary path), since the row
// is left due with a cooldown this check just matched.
func TestReconfirmCooldownsExcludesASurplusUnitEvenWhenTheWithdrawalCASLoses(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		enforcedAt := pgNow().Add(-time.Second)
		checkedAt := pgNow()
		actualAvailableAt := checkedAt.Add(30 * time.Minute) // something else already moved it
		unitID := "00000000-0000-4000-8000-000000000504"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{
			id: unitID, status: syncRunUnitStatusRetrying, availableAt: &actualAvailableAt, updatedAt: checkedAt,
		})

		routeFamily := "work-items"
		insertObservation(t, ctx, pool, "00000000-0000-4000-8000-000000000513", func(o *insertObservationOpts) {
			o.orgID = "org-1"
			o.integrationID = "00000000-0000-4000-8000-000000000010"
			o.routeFamily = &routeFamily
			o.retryAfterSeconds = floatPtr(3600)
			o.observedAt = checkedAt
		})

		unit := budgetUnit{id: unitID, orgID: "org-1", provider: "github", integrationID: "00000000-0000-4000-8000-000000000010"}
		estimates := map[string][]budgetEstimate{unitID: estimateFor(10, "github", "rest_core", "work-items")}
		surplusPriorAvailableAt := map[string]time.Time{unitID: checkedAt.Add(2 * time.Hour)}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		result, err := reconfirmCooldowns(
			ctx, tx, nil, budgetCandidatesRunID, []budgetUnit{unit}, estimates,
			map[string]bool{}, 5, surplusPriorAvailableAt, enforcedAt, checkedAt,
		)
		if err != nil {
			t.Fatalf("reconfirmCooldowns: %v", err)
		}
		if !result.excludedUnitIDs[unitID] {
			t.Fatal("want unitID excluded EVEN THOUGH the withdrawal's own CAS lost the race")
		}
	})
}
