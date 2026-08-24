//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func createChokepointTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE public.sync_run_units (
 id uuid PRIMARY KEY, status text NOT NULL, available_at timestamptz NULL,
 updated_at timestamptz NOT NULL DEFAULT now(), error text NULL, result json NULL,
 lease_owner text NULL, lease_expires_at timestamptz NULL, last_heartbeat_at timestamptz NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const chokepointTestUnit = "00000000-0000-4000-8000-0000000000e0"

func withChokepointPool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
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
	createChokepointTables(t, ctx, pool)
	fn(ctx, pool)
}

func insertChokepointUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, status string, resultJSON string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (id, status, available_at, updated_at, result)
VALUES ($1::uuid, $2, now(), now(), $3::json)`,
		chokepointTestUnit, status, resultJSON); err != nil {
		t.Fatal(err)
	}
}

// TestTerminalizeUnitWritesTheVerdict pins the happy path: a well-formed,
// evidence-licensed verdict against a claimable unit writes status=FAILED
// with the verdict's error text and evidence.
func TestTerminalizeUnitWritesTheVerdict(t *testing.T) {
	withChokepointPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		insertChokepointUnit(t, ctx, pool, syncRunUnitStatusPlanned, `{}`)
		unit := budgetUnit{id: chokepointTestUnit, result: map[string]any{}}
		now := time.Now().UTC()

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		decision, err := terminalizeUnit(ctx, tx, nil, unit, terminalVerdict{
			errorCategory: deferralExhaustedCategory,
			errorText:     "blocked too long",
			evidence:      map[string]any{"blocked_seconds": 100},
		}, now)
		if err != nil {
			t.Fatalf("terminalizeUnit: %v", err)
		}
		if decision.outcome != terminalOutcomeTerminalized {
			t.Fatalf("outcome=%v want=%v", decision.outcome, terminalOutcomeTerminalized)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status, errorText string
		if err := pool.QueryRow(ctx, `SELECT status, error FROM sync_run_units WHERE id=$1`, chokepointTestUnit).
			Scan(&status, &errorText); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusFailed || errorText != "blocked too long" {
			t.Fatalf("status=%q error=%q, want failed/'blocked too long'", status, errorText)
		}
	})
}

// TestTerminalizeUnitRefusesAnUnlicensedEpisode is the chokepoint's core
// invariant: a verdict naming an episode ("rate_limit") the unit's own
// last-recorded cause does NOT evidence must be REFUSED, not written --
// counters alone are never sufficient evidence.
func TestTerminalizeUnitRefusesAnUnlicensedEpisode(t *testing.T) {
	withChokepointPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		insertChokepointUnit(t, ctx, pool, syncRunUnitStatusPlanned, `{"error_category":"budget_deferred"}`)
		unit := budgetUnit{id: chokepointTestUnit, result: map[string]any{"error_category": budgetDeferredCategory}}
		now := time.Now().UTC()

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		decision, err := terminalizeUnit(ctx, tx, nil, unit, terminalVerdict{
			errorCategory: rateLimitCooldownExhaustedCategory,
			errorText:     "rate limit cooldown deferral budget exhausted",
			evidence:      map[string]any{"rate_limit_deferrals": 5},
			episode:       "rate_limit",
		}, now)
		if err != nil {
			t.Fatalf("terminalizeUnit: %v", err)
		}
		if decision.outcome != terminalOutcomeRefused {
			t.Fatalf("outcome=%v want=%v -- the unit's last cause is budget_deferred, not rate-limit evidence", decision.outcome, terminalOutcomeRefused)
		}

		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, chokepointTestUnit).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusPlanned {
			t.Fatalf("status=%q, want unchanged (planned) -- a refused verdict must not write anything", status)
		}
	})
}

// TestTerminalizeUnitLosesTheRaceWhenTheUnitAlreadyMovedOn pins the CAS
// semantics: a unit already terminal (or otherwise outside the claim
// predicate) reports CAS_LOST, not an error and not a silent overwrite.
func TestTerminalizeUnitLosesTheRaceWhenTheUnitAlreadyMovedOn(t *testing.T) {
	withChokepointPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		insertChokepointUnit(t, ctx, pool, syncRunUnitStatusSuccess, `{}`)
		unit := budgetUnit{id: chokepointTestUnit, result: map[string]any{}}
		now := time.Now().UTC()

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		decision, err := terminalizeUnit(ctx, tx, nil, unit, terminalVerdict{
			errorCategory: deferralExhaustedCategory,
			errorText:     "blocked too long",
			evidence:      map[string]any{"blocked_seconds": 100},
		}, now)
		if err != nil {
			t.Fatalf("terminalizeUnit: %v", err)
		}
		if decision.outcome != terminalOutcomeCASLost {
			t.Fatalf("outcome=%v want=%v -- a SUCCESS unit is outside the claim predicate", decision.outcome, terminalOutcomeCASLost)
		}

		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, chokepointTestUnit).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusSuccess {
			t.Fatalf("status=%q, want unchanged (success) -- a lost race must never overwrite a winner", status)
		}
	})
}
