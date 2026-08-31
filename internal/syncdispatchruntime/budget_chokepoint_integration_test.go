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
	// sync_runs backs bumpSyncRunRollup's seam (CHAOS-4586): terminalizeUnit
	// recomputes this row's completed_units/failed_units in the same
	// transaction as every terminal write it makes.
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.sync_runs (
 id uuid PRIMARY KEY, completed_units int NOT NULL DEFAULT 0,
 failed_units int NOT NULL DEFAULT 0, total_units int NOT NULL DEFAULT 0
);
INSERT INTO public.sync_runs (id, total_units) VALUES ('`+chokepointTestRunID+`', 1);`); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `
CREATE TABLE public.sync_run_units (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL, status text NOT NULL, available_at timestamptz NULL,
 updated_at timestamptz NOT NULL DEFAULT now(), error text NULL, result json NULL,
 lease_owner text NULL, lease_expires_at timestamptz NULL, last_heartbeat_at timestamptz NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const chokepointTestUnit = "00000000-0000-4000-8000-0000000000e0"
const chokepointTestRunID = "00000000-0000-4000-8000-0000000000e1"

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
INSERT INTO public.sync_run_units (id, sync_run_id, status, available_at, updated_at, result)
VALUES ($1::uuid, $4::uuid, $2, now(), now(), $3::json)`,
		chokepointTestUnit, status, resultJSON, chokepointTestRunID); err != nil {
		t.Fatal(err)
	}
}

// TestTerminalizeUnitWritesTheVerdict pins the happy path: a well-formed,
// evidence-licensed verdict against a claimable unit writes status=FAILED
// with the verdict's error text and evidence.
func TestTerminalizeUnitWritesTheVerdict(t *testing.T) {
	withChokepointPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		insertChokepointUnit(t, ctx, pool, syncRunUnitStatusPlanned, `{}`)
		unit := budgetUnit{id: chokepointTestUnit, syncRunID: chokepointTestRunID, result: map[string]any{}}
		now := pgNow()

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

		// CHAOS-4586: terminalizeUnit is THE single budget/rate-limit/
		// aggregate-deferral-exhaustion terminal-fail path, and the parent
		// run commonly stays active with other units still dispatching --
		// it must recompute sync_runs.failed_units in the SAME transaction,
		// not leave it stale until finalize_sync_run.
		var runFailedUnits int
		if err := pool.QueryRow(ctx, `SELECT failed_units FROM public.sync_runs WHERE id=$1`, chokepointTestRunID).Scan(&runFailedUnits); err != nil {
			t.Fatal(err)
		}
		if runFailedUnits != 1 {
			t.Fatalf("sync_runs.failed_units=%d, want 1", runFailedUnits)
		}
	})
}

// TestTerminalizeUnitRollupSurvivesConcurrentTerminalizationOfSiblingUnits
// proves the lock-first ordering (CHAOS-4559's own pattern,
// TestPostgresRollupCountsBothUnitsUnderConcurrentCompletion): bumpSyncRunRollupSQL's
// two COUNT(*) subqueries are InitPlans, evaluated once per statement against
// the snapshot at statement start -- without locking sync_runs FIRST, a
// second terminalizeUnit call that blocked on the first's row lock would
// resume and overwrite with its own stale pre-wait count, silently dropping
// the first unit's contribution.
//
// codex round 11 (void checkpoint, CHAOS-4586) found this test's ORIGINAL
// form -- synchronizing two goroutines' START via a channel close, then
// checking only the FINAL count -- never forced or OBSERVED real lock
// contention on the sync_runs row: it could pass by pure scheduling luck
// even with the lock-first protection entirely removed, if one goroutine's
// whole transaction happened to finish before the other's even began.
// Confirmed by mutation: temporarily removed syncrunrollup.Bump's LockRun
// call and ran the ORIGINAL form of this test 10x in isolation -- 10/10
// PASS, proving the weakness exactly as described (the same "observes the
// outcome, not the mechanism" class this session already fixed once for
// TestTerminalizeFeatureDisabledRunWaitsForAConcurrentRollupWriterThenSeesItsResult).
// Team-lead's ruling: fix in this PR (the weak test is this PR's own round-1
// diff, not pre-existing main code).
//
// Redesigned to force and OBSERVE genuine contention, same
// pg_stat_activity-polling technique as the feature-disabled concurrency
// proof: goroutine A's terminalizeUnit call is allowed to return WITHOUT
// committing -- its own Bump call already took and is still holding the
// sync_runs row's FOR UPDATE lock. Goroutine B's terminalizeUnit call is
// then started; its own Bump call's LockRun MUST block on A's held lock.
// waitForBlockedSyncRunsLock confirms B is provably blocked (not just
// scheduled) before A's transaction commits. With LockRun removed, B's
// unprotected recompute would never need to block on anything -- there is
// nothing for waitForBlockedSyncRunsLock to observe -- so this redesigned
// test fails loudly (a timeout, not a silent pass) when the fix is missing,
// closing exactly the gap codex found.
func TestTerminalizeUnitRollupSurvivesConcurrentTerminalizationOfSiblingUnits(t *testing.T) {
	withChokepointPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		secondUnit := "00000000-0000-4000-8000-0000000000e2"
		insertChokepointUnit(t, ctx, pool, syncRunUnitStatusPlanned, `{}`)
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (id, sync_run_id, status, available_at, updated_at, result)
VALUES ($1::uuid, $2::uuid, $3, now(), now(), '{}'::json)`,
			secondUnit, chokepointTestRunID, syncRunUnitStatusPlanned); err != nil {
			t.Fatal(err)
		}
		// The fixture pins total_units=1 (one unit); a second sibling was
		// just added, so correct it to match reality for this test.
		if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET total_units = 2 WHERE id=$1`, chokepointTestRunID); err != nil {
			t.Fatal(err)
		}

		now := pgNow()
		verdict := terminalVerdict{
			errorCategory: deferralExhaustedCategory,
			errorText:     "blocked too long",
			evidence:      map[string]any{"blocked_seconds": 100},
		}
		firstUnit := budgetUnit{id: chokepointTestUnit, syncRunID: chokepointTestRunID, result: map[string]any{}}
		secondUnitArg := budgetUnit{id: secondUnit, syncRunID: chokepointTestRunID, result: map[string]any{}}

		// Goroutine A (run inline, not concurrently): terminalize the first
		// unit and return WITHOUT committing -- its own Bump call's LockRun
		// now holds the sync_runs row's FOR UPDATE lock until txA.Commit
		// fires below.
		txA, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = txA.Rollback(ctx) }()
		decisionA, err := terminalizeUnit(ctx, txA, nil, firstUnit, verdict, now)
		if err != nil {
			t.Fatalf("terminalizeUnit (first unit): %v", err)
		}
		if decisionA.outcome != terminalOutcomeTerminalized {
			t.Fatalf("first unit outcome=%v want=%v", decisionA.outcome, terminalOutcomeTerminalized)
		}

		// Goroutine B: started concurrently while A's lock is still held --
		// its own Bump call's LockRun must block on it.
		type unitResult struct {
			decision terminalDecision
			err      error
		}
		bDone := make(chan unitResult, 1)
		txB, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = txB.Rollback(ctx) }()
		go func() {
			decisionB, err := terminalizeUnit(ctx, txB, nil, secondUnitArg, verdict, now)
			bDone <- unitResult{decision: decisionB, err: err}
		}()

		waitForBlockedSyncRunsLock(t, ctx, pool, "the sibling terminalizeUnit call")

		// B is now PROVABLY blocked on A's row lock (pg_stat_activity
		// confirmed a waiter) -- only now commit A, unblocking B.
		if err := txA.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		select {
		case r := <-bDone:
			if r.err != nil {
				t.Fatalf("terminalizeUnit (second unit): %v", r.err)
			}
			if r.decision.outcome != terminalOutcomeTerminalized {
				t.Fatalf("second unit outcome=%v want=%v", r.decision.outcome, terminalOutcomeTerminalized)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the second unit's terminalizeUnit never unblocked after the first transaction committed")
		}
		if err := txB.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var runFailedUnits int
		if err := pool.QueryRow(ctx, `SELECT failed_units FROM public.sync_runs WHERE id=$1`, chokepointTestRunID).Scan(&runFailedUnits); err != nil {
			t.Fatal(err)
		}
		if runFailedUnits != 2 {
			t.Fatalf("sync_runs.failed_units=%d, want 2 -- lock-first ordering must prevent either concurrent bump from overwriting the other with a stale count", runFailedUnits)
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
		unit := budgetUnit{id: chokepointTestUnit, syncRunID: chokepointTestRunID, result: map[string]any{"error_category": budgetDeferredCategory}}
		now := pgNow()

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
		unit := budgetUnit{id: chokepointTestUnit, syncRunID: chokepointTestRunID, result: map[string]any{}}
		now := pgNow()

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
