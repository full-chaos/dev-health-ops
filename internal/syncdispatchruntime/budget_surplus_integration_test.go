//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func createBudgetSurplusTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE public.sync_run_units (
 id uuid PRIMARY KEY, status text NOT NULL, available_at timestamptz NULL,
 updated_at timestamptz NOT NULL DEFAULT now(), error text NULL, result json NULL,
 budget_deferrals int NOT NULL DEFAULT 0
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const budgetSurplusTestUnit = "00000000-0000-4000-8000-0000000002a0"

func withBudgetSurplusPool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createBudgetSurplusTables(t, ctx, pool)
	fn(ctx, pool)
}

func insertSurplusUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, status string, availableAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (id, status, available_at, updated_at)
VALUES ($1::uuid, $2, $3, now())`, budgetSurplusTestUnit, status, availableAt); err != nil {
		t.Fatal(err)
	}
}

// TestAdmitUnitFromSurplusPullsAvailableAtForward pins the CAS write's
// whole contract: only available_at (and updated_at) move; status stays
// RETRYING; the predicate re-asserts "still not due" so a concurrent
// promotion or terminalization wins the race.
func TestAdmitUnitFromSurplusPullsAvailableAtForward(t *testing.T) {
	withBudgetSurplusPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		future := now.Add(time.Hour)
		insertSurplusUnit(t, ctx, pool, syncRunUnitStatusRetrying, future)
		unit := budgetUnit{id: budgetSurplusTestUnit}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		ok, err := admitUnitFromSurplus(ctx, tx, nil, budgetCandidatesRunID, unit, now)
		if err != nil {
			t.Fatalf("admitUnitFromSurplus: %v", err)
		}
		if !ok {
			t.Fatal("want ok=true")
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status string
		var availableAt time.Time
		if err := pool.QueryRow(ctx, `SELECT status, available_at FROM sync_run_units WHERE id=$1`, budgetSurplusTestUnit).
			Scan(&status, &availableAt); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusRetrying {
			t.Fatalf("status=%q, want unchanged (retrying) -- surplus promotion must not reassign status", status)
		}
		if !availableAt.Equal(now) {
			t.Fatalf("available_at=%s want=%s (pulled forward to now)", availableAt, now)
		}
	})
}

// TestAdmitUnitFromSurplusLosesTheRaceWhenAlreadyDue pins the CAS
// predicate: a unit that is no longer "not yet due" (e.g. a concurrent pass
// already claimed it, or its available_at is already <= now) must not be
// promoted again. The exact-boundary case (available_at == now) is the
// discriminating one: Python's predicate is available_at > now (strict), so
// a unit exactly AT now must NOT be treated as still-surplus-eligible --
// without this boundary fixture, a > -> >= mutation passes vacuously
// against a fixture that only tests strictly-past values.
func TestAdmitUnitFromSurplusLosesTheRaceWhenAlreadyDue(t *testing.T) {
	withBudgetSurplusPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		past := now.Add(-time.Minute)
		insertSurplusUnit(t, ctx, pool, syncRunUnitStatusRetrying, past)
		unit := budgetUnit{id: budgetSurplusTestUnit}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		ok, err := admitUnitFromSurplus(ctx, tx, nil, budgetCandidatesRunID, unit, now)
		if err != nil {
			t.Fatalf("admitUnitFromSurplus: %v", err)
		}
		if ok {
			t.Fatal("want ok=false -- the unit is already due, not a live surplus candidate")
		}
	})
}

// TestAdmitUnitFromSurplusLosesTheRaceAtTheExactBoundary is the
// discriminating boundary case: available_at == now must NOT be promoted
// (Python's predicate is strictly available_at > now).
func TestAdmitUnitFromSurplusLosesTheRaceAtTheExactBoundary(t *testing.T) {
	withBudgetSurplusPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		insertSurplusUnit(t, ctx, pool, syncRunUnitStatusRetrying, now)
		unit := budgetUnit{id: budgetSurplusTestUnit}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		ok, err := admitUnitFromSurplus(ctx, tx, nil, budgetCandidatesRunID, unit, now)
		if err != nil {
			t.Fatalf("admitUnitFromSurplus: %v", err)
		}
		if ok {
			t.Fatal("want ok=false -- available_at == now is not STRICTLY in the future")
		}
	})
}

// TestWithdrawSurplusAdmissionRestoresThePriorAvailableAt pins the
// CHAOS-3465 CRITICAL finding's fix: a withdrawal restores EXACTLY the
// pre-promotion available_at, and nothing else moves.
func TestWithdrawSurplusAdmissionRestoresThePriorAvailableAt(t *testing.T) {
	withBudgetSurplusPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		priorAvailableAt := now.Add(45 * time.Minute)
		promotedAvailableAt := now
		insertSurplusUnit(t, ctx, pool, syncRunUnitStatusRetrying, promotedAvailableAt)
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET budget_deferrals=2 WHERE id=$1`, budgetSurplusTestUnit); err != nil {
			t.Fatal(err)
		}
		unit := budgetUnit{id: budgetSurplusTestUnit, budgetDeferrals: 2}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		ok, err := withdrawSurplusAdmission(ctx, tx, nil, budgetCandidatesRunID, unit, promotedAvailableAt, priorAvailableAt, now)
		if err != nil {
			t.Fatalf("withdrawSurplusAdmission: %v", err)
		}
		if !ok {
			t.Fatal("want ok=true")
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status string
		var availableAt time.Time
		var budgetDeferrals int
		if err := pool.QueryRow(ctx, `SELECT status, available_at, budget_deferrals FROM sync_run_units WHERE id=$1`, budgetSurplusTestUnit).
			Scan(&status, &availableAt, &budgetDeferrals); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusRetrying {
			t.Fatalf("status=%q, want unchanged (retrying)", status)
		}
		if !availableAt.Equal(priorAvailableAt) {
			t.Fatalf("available_at=%s want=%s (restored to the pre-promotion value)", availableAt, priorAvailableAt)
		}
		if budgetDeferrals != 2 {
			t.Fatalf("budgetDeferrals=%d, want unchanged (2) -- withdrawal must not touch the episode columns", budgetDeferrals)
		}
	})
}

// TestWithdrawSurplusAdmissionLosesTheRaceIfAvailableAtMoved pins the CAS
// predicate on the OTHER side: if anything moved available_at away from the
// promoted value between the promotion and the withdrawal, the withdrawal
// must not stomp on it.
func TestWithdrawSurplusAdmissionLosesTheRaceIfAvailableAtMoved(t *testing.T) {
	withBudgetSurplusPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		promotedAvailableAt := now
		actualAvailableAt := now.Add(10 * time.Minute) // something else moved it
		insertSurplusUnit(t, ctx, pool, syncRunUnitStatusRetrying, actualAvailableAt)
		unit := budgetUnit{id: budgetSurplusTestUnit}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		ok, err := withdrawSurplusAdmission(ctx, tx, nil, budgetCandidatesRunID, unit, promotedAvailableAt, now.Add(45*time.Minute), now)
		if err != nil {
			t.Fatalf("withdrawSurplusAdmission: %v", err)
		}
		if ok {
			t.Fatal("want ok=false -- available_at no longer matches the promoted value the CAS pins on")
		}

		var availableAt time.Time
		if err := tx.QueryRow(ctx, `SELECT available_at FROM sync_run_units WHERE id=$1`, budgetSurplusTestUnit).Scan(&availableAt); err != nil {
			t.Fatal(err)
		}
		if !availableAt.Equal(actualAvailableAt) {
			t.Fatalf("available_at=%s want=%s (untouched -- a lost race must never overwrite)", availableAt, actualAvailableAt)
		}
	})
}
