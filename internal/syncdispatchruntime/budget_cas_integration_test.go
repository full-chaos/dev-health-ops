//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func createBudgetCASTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE public.sync_run_units (
 id uuid PRIMARY KEY, status text NOT NULL, available_at timestamptz NULL,
 updated_at timestamptz NOT NULL DEFAULT now(), error text NULL, result json NULL,
 lease_owner text NULL, lease_expires_at timestamptz NULL, last_heartbeat_at timestamptz NULL,
 rate_limit_deferrals int NOT NULL DEFAULT 0, rate_limit_first_seen_at timestamptz NULL,
 budget_deferrals int NOT NULL DEFAULT 0, budget_first_deferred_at timestamptz NULL,
 first_blocked_at timestamptz NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const budgetCASTestUnit = "00000000-0000-4000-8000-0000000000f0"

func withBudgetCASPool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
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
	createBudgetCASTables(t, ctx, pool)
	fn(ctx, pool)
}

func insertBudgetCASUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, status string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (id, status, available_at, updated_at)
VALUES ($1::uuid, $2, now(), now())`, budgetCASTestUnit, status); err != nil {
		t.Fatal(err)
	}
}

// TestApplyCooldownDeferralWritesTheStampAndClearsTheBudgetEpisode pins the
// happy path plus the episode-pair symmetry: applying a cooldown deferral
// must clear the OTHER episode's (budget) counters, matching
// deferUnitForBudget's own symmetric clear of the rate-limit pair.
func TestApplyCooldownDeferralWritesTheStampAndClearsTheBudgetEpisode(t *testing.T) {
	withBudgetCASPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		insertBudgetCASUnit(t, ctx, pool, syncRunUnitStatusPlanned)
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET budget_deferrals=3, budget_first_deferred_at=now() WHERE id=$1`, budgetCASTestUnit); err != nil {
			t.Fatal(err)
		}
		unit := budgetUnit{id: budgetCASTestUnit}
		now := time.Now().UTC()
		firstSeenAt := now.Add(-time.Minute)
		deferral := rateLimitDeferralPlan{notBefore: now.Add(30 * time.Second), attempts: 1, firstSeenAt: firstSeenAt}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		availableAt, ok, err := applyCooldownDeferral(ctx, tx, nil, unit, deferral, 0, now)
		if err != nil {
			t.Fatalf("applyCooldownDeferral: %v", err)
		}
		if !ok {
			t.Fatal("want ok=true (successful CAS)")
		}
		if !availableAt.Equal(deferral.notBefore) {
			t.Fatalf("availableAt=%s want=%s (zero jitter)", availableAt, deferral.notBefore)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status string
		var rateLimitDeferrals, budgetDeferrals int
		var budgetFirstDeferredAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT status, rate_limit_deferrals, budget_deferrals, budget_first_deferred_at FROM sync_run_units WHERE id=$1`, budgetCASTestUnit).
			Scan(&status, &rateLimitDeferrals, &budgetDeferrals, &budgetFirstDeferredAt); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusRetrying || rateLimitDeferrals != 1 {
			t.Fatalf("status=%q rateLimitDeferrals=%d, want retrying/1", status, rateLimitDeferrals)
		}
		if budgetDeferrals != 0 || budgetFirstDeferredAt != nil {
			t.Fatalf("budgetDeferrals=%d budgetFirstDeferredAt=%v, want cleared (episode-pair symmetry)", budgetDeferrals, budgetFirstDeferredAt)
		}
	})
}

// TestApplyCooldownDeferralClampsToTheWallClockDeadline pins the
// double-clamp: jitter added on top of an already-clamped not_before must
// not push available_at past firstSeenAt+RATE_LIMIT_MAX_TOTAL_WAIT_SECONDS.
func TestApplyCooldownDeferralClampsToTheWallClockDeadline(t *testing.T) {
	withBudgetCASPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		insertBudgetCASUnit(t, ctx, pool, syncRunUnitStatusPlanned)
		unit := budgetUnit{id: budgetCASTestUnit}
		now := time.Now().UTC()
		firstSeenAt := now.Add(-time.Hour)
		deadline := firstSeenAt.Add(rateLimitMaxTotalWaitSecondsBudget * time.Second)
		// notBefore is already AT the deadline -- any jitter must be clamped away.
		deferral := rateLimitDeferralPlan{notBefore: deadline, attempts: 1, firstSeenAt: firstSeenAt}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		availableAt, ok, err := applyCooldownDeferral(ctx, tx, nil, unit, deferral, 3600, now)
		if err != nil {
			t.Fatalf("applyCooldownDeferral: %v", err)
		}
		if !ok {
			t.Fatal("want ok=true")
		}
		if availableAt.After(deadline) {
			t.Fatalf("availableAt=%s exceeds the wall-clock deadline=%s", availableAt, deadline)
		}
	})
}

// TestDeferUnitForBudgetIncrementsInSQLAndClearsTheRateLimitEpisode pins the
// budget-episode CAS write: the SQL-side increment (not a client-computed
// value, so concurrent passes cannot both write the same count) and the
// symmetric clear of the rate-limit episode pair.
func TestDeferUnitForBudgetIncrementsInSQLAndClearsTheRateLimitEpisode(t *testing.T) {
	withBudgetCASPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		insertBudgetCASUnit(t, ctx, pool, syncRunUnitStatusPlanned)
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET budget_deferrals=2, rate_limit_deferrals=4, rate_limit_first_seen_at=now() WHERE id=$1`, budgetCASTestUnit); err != nil {
			t.Fatal(err)
		}
		unit := budgetUnit{id: budgetCASTestUnit}
		now := time.Now().UTC()
		availableAt := now.Add(time.Minute)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		ok, err := deferUnitForBudget(ctx, tx, unit, availableAt, now, []map[string]any{{"decision": "would_defer"}})
		if err != nil {
			t.Fatalf("deferUnitForBudget: %v", err)
		}
		if !ok {
			t.Fatal("want ok=true")
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status string
		var budgetDeferrals, rateLimitDeferrals int
		var rateLimitFirstSeenAt *time.Time
		if err := pool.QueryRow(ctx, `SELECT status, budget_deferrals, rate_limit_deferrals, rate_limit_first_seen_at FROM sync_run_units WHERE id=$1`, budgetCASTestUnit).
			Scan(&status, &budgetDeferrals, &rateLimitDeferrals, &rateLimitFirstSeenAt); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusRetrying || budgetDeferrals != 3 {
			t.Fatalf("status=%q budgetDeferrals=%d, want retrying/3 (SQL-side increment from 2)", status, budgetDeferrals)
		}
		if rateLimitDeferrals != 0 || rateLimitFirstSeenAt != nil {
			t.Fatalf("rateLimitDeferrals=%d rateLimitFirstSeenAt=%v, want cleared (episode-pair symmetry)", rateLimitDeferrals, rateLimitFirstSeenAt)
		}
	})
}

// TestResolveCooldownBlockedUnitDefersANormalUnit pins the common case:
// no exhaustion, so the unit is simply deferred via applyCooldownDeferral.
func TestResolveCooldownBlockedUnitDefersANormalUnit(t *testing.T) {
	withBudgetCASPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		insertBudgetCASUnit(t, ctx, pool, syncRunUnitStatusPlanned)
		unit := budgetUnit{id: budgetCASTestUnit, result: map[string]any{}}
		now := time.Now().UTC()
		cooldownExpiry := now.Add(5 * time.Minute)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		at, terminalized, ok, err := resolveCooldownBlockedUnit(ctx, tx, nil, unit, cooldownExpiry, 0, now)
		if err != nil {
			t.Fatalf("resolveCooldownBlockedUnit: %v", err)
		}
		if !ok || terminalized {
			t.Fatalf("ok=%v terminalized=%v, want ok=true/terminalized=false (a fresh unit is deferred, not terminalized)", ok, terminalized)
		}
		if at.IsZero() {
			t.Fatal("want a non-zero deferred available_at")
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, budgetCASTestUnit).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusRetrying {
			t.Fatalf("status=%q want=retrying", status)
		}
	})
}

// TestResolveCooldownBlockedUnitTerminalizesAnExhaustedEpisode pins the
// ordering contract's first rung: a unit whose rate-limit episode is
// already exhausted terminalizes with the rate-limit-specific category,
// never falls through to a fresh deferral.
func TestResolveCooldownBlockedUnitTerminalizesAnExhaustedEpisode(t *testing.T) {
	withBudgetCASPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		insertBudgetCASUnit(t, ctx, pool, syncRunUnitStatusPlanned)
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET rate_limit_deferrals=$1 WHERE id=$2`, rateLimitMaxDeferralsBudget, budgetCASTestUnit); err != nil {
			t.Fatal(err)
		}
		unit := budgetUnit{
			id:                 budgetCASTestUnit,
			result:             map[string]any{"error_category": rateLimitEpisodeErrorCategory},
			rateLimitDeferrals: rateLimitMaxDeferralsBudget,
		}
		now := time.Now().UTC()
		cooldownExpiry := now.Add(5 * time.Minute)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		_, terminalized, ok, err := resolveCooldownBlockedUnit(ctx, tx, nil, unit, cooldownExpiry, 0, now)
		if err != nil {
			t.Fatalf("resolveCooldownBlockedUnit: %v", err)
		}
		if !ok || !terminalized {
			t.Fatalf("ok=%v terminalized=%v, want both true -- an exhausted episode must terminalize", ok, terminalized)
		}

		// Read through the SAME transaction: the write is not committed yet,
		// so a separate pool connection would not see it under READ COMMITTED.
		var status, errorCategory string
		if err := tx.QueryRow(ctx, `SELECT status, result->>'error_category' FROM sync_run_units WHERE id=$1`, budgetCASTestUnit).
			Scan(&status, &errorCategory); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusFailed || errorCategory != rateLimitCooldownExhaustedCategory {
			t.Fatalf("status=%q error_category=%q, want failed/%q", status, errorCategory, rateLimitCooldownExhaustedCategory)
		}
	})
}
