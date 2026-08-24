//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgNow returns the current time truncated to microsecond precision, the
// finest Postgres timestamptz actually stores. A bare time.Now().UTC() has
// nanosecond resolution on Linux (never on macOS, in practice, which is why
// this was never caught locally): compare an untruncated value against one
// that round-tripped through a write and read-back and the comparison fails
// on the sub-microsecond remainder essentially every time it runs on Linux
// CI. Every fixture and assertion in this file's family of integration
// tests that seeds or checks a timestamp uses this, not a bare time.Now().
func pgNow() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func withBudgetEnforcePool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
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
	createBudgetCandidatesTables(t, ctx, pool)
	createCooldownTables(t, ctx, pool)
	fn(ctx, pool)
}

func enforceUnitStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id=$1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

// TestEnforceRunReturnsEmptyResultWithoutCallingTheBridge pins the
// short-circuit: no candidates and no surplus candidates means nothing to
// estimate, so enforceRun must return immediately without ever calling the
// estimator.
func TestEnforceRunReturnsEmptyResultWithoutCallingTheBridge(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		estimator := &fakeBudgetEstimator{}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		result, err := enforceRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, nil, time.Now(), nil)
		if err != nil {
			t.Fatalf("enforceRun: %v", err)
		}
		if len(result.observations) != 0 || len(result.deferredUnitIDs) != 0 || result.nextDeferredAt != nil {
			t.Fatalf("got=%+v want a zero-value result", result)
		}
		if len(estimator.calls) != 0 {
			t.Fatalf("estimator called %d times, want 0", len(estimator.calls))
		}
	})
}

// TestEnforceRunAdmitsWhatFitsAndDefersWhatDoesNot pins the core admission
// loop's sequencing: units are processed in id order, and a unit admitted
// EARLIER in the same pass actually charges consumedByBucket so a LATER
// unit in the same bucket can be pushed over the limit by it -- proving the
// running total is shared across the whole pass, not per-unit.
func TestEnforceRunAdmitsWhatFitsAndDefersWhatDoesNot(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_BUDGET_DEFAULT_LIMIT", "100")
		// The host shell may have SYNC_BUDGET_BUCKET_LIMITS set for local dev
		// convenience (a real per-bucket override) -- neutralize it so this
		// test's math is deterministic regardless of the ambient environment.
		t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
		now := pgNow()

		unitA := "00000000-0000-4000-8000-000000000301" // fits alone: 60 <= 100
		unitB := "00000000-0000-4000-8000-000000000302" // 60 (A) + 50 (B) = 110 > 100

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitA, status: syncRunUnitStatusPlanned, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitB, status: syncRunUnitStatusPlanned, updatedAt: now})

		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			unitA: estimateFor(60, "github", "rest_core", "work-items"),
			unitB: estimateFor(50, "github", "rest_core", "work-items"),
		}}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		result, err := enforceRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, nil, now, nil)
		if err != nil {
			t.Fatalf("enforceRun: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		if result.deferredUnitIDs[unitA] {
			t.Fatal("unitA must NOT be deferred -- it fits alone")
		}
		if !result.deferredUnitIDs[unitB] {
			t.Fatal("unitB must be deferred -- 60 (A, admitted first) + 50 (B) exceeds the 100 limit")
		}
		if result.nextDeferredAt == nil {
			t.Fatal("nextDeferredAt must be set")
		}
		// The "allowed" path never writes to the DB -- claiming is a
		// separate, later stage (_claim_units, not yet ported). Only the
		// deferred unit's row moves.
		if got := enforceUnitStatus(t, ctx, pool, unitA); got != syncRunUnitStatusPlanned {
			t.Fatalf("unitA status=%q, want unchanged (planned) -- admission does not claim", got)
		}
		if got := enforceUnitStatus(t, ctx, pool, unitB); got != syncRunUnitStatusRetrying {
			t.Fatalf("unitB status=%q, want retrying", got)
		}
	})
}

// TestEnforceRunTerminalizesAnExhaustedPermanentMisfit pins the exhaustion
// wiring end to end: a unit whose OWN budget_deferrals is already at the
// cap, and whose estimate alone exceeds its bucket's limit (a PERMANENT
// misfit against the baseline), must terminalize rather than defer again.
func TestEnforceRunTerminalizesAnExhaustedPermanentMisfit(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_BUDGET_DEFAULT_LIMIT", "100")
		// The host shell may have SYNC_BUDGET_BUCKET_LIMITS set for local dev
		// convenience (a real per-bucket override) -- neutralize it so this
		// test's math is deterministic regardless of the ambient environment.
		t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
		now := pgNow()
		unitID := "00000000-0000-4000-8000-000000000311"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusPlanned, updatedAt: now})
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET budget_deferrals=$1, result='{"error_category":"budget_deferred"}'::json WHERE id=$2`,
			budgetMaxDeferrals(), unitID); err != nil {
			t.Fatal(err)
		}

		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			unitID: estimateFor(500, "github", "rest_core", "work-items"), // alone exceeds the 100 limit -- permanent
		}}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		result, err := enforceRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, nil, now, nil)
		if err != nil {
			t.Fatalf("enforceRun: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		if result.deferredUnitIDs[unitID] {
			t.Fatal("an exhausted, permanently-oversized unit must NOT be in deferredUnitIDs -- it terminalized instead")
		}
		if got := enforceUnitStatus(t, ctx, pool, unitID); got != syncRunUnitStatusFailed {
			t.Fatalf("status=%q, want failed", got)
		}
		found := false
		for _, observation := range result.observations {
			if observation["decision"] == "exhausted" {
				found = true
			}
		}
		if !found {
			t.Fatal("want at least one observation with decision=exhausted")
		}
	})
}

// TestEnforceRunDivertsACooldownGatedUnitBeforeBudgetAdmission pins the
// ordering contract itself (CHAOS-2760): a unit gated by an active shared
// cooldown must be resolved through the cooldown path and NEVER reach
// budget admission at all -- proven here by a second unit sharing the same
// bucket that only fits if the cooldown-gated unit's estimate was never
// charged against it.
func TestEnforceRunDivertsACooldownGatedUnitBeforeBudgetAdmission(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_BUDGET_DEFAULT_LIMIT", "100")
		// The host shell may have SYNC_BUDGET_BUCKET_LIMITS set for local dev
		// convenience (a real per-bucket override) -- neutralize it so this
		// test's math is deterministic regardless of the ambient environment.
		t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
		now := pgNow()

		cooldownGated := "00000000-0000-4000-8000-000000000321"
		fitsIfNotCharged := "00000000-0000-4000-8000-000000000322"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: cooldownGated, status: syncRunUnitStatusPlanned, updatedAt: now})
		// A DIFFERENT integration id: the budget bucket does not key on
		// integration_id (so this unit still shares cooldownGated's bucket
		// for the purposes of this test's proof), but the cooldown DOES --
		// without this, fitsIfNotCharged would legitimately match the same
		// active cooldown too (correct production behavior for a genuinely
		// shared integration), which would defeat this test's whole point.
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{
			id: fitsIfNotCharged, status: syncRunUnitStatusPlanned, updatedAt: now,
			integrationID: "00000000-0000-4000-8000-000000000099",
		})

		// insertCandidateUnit hardcodes org_id="org-1", integration_id
		// "...0010", provider "github" -- match the observation to those so
		// it actually gates this unit's bucket.
		routeFamily := "work-items"
		insertObservation(t, ctx, pool, "00000000-0000-4000-8000-000000000331", func(o *insertObservationOpts) {
			o.orgID = "org-1"
			o.integrationID = "00000000-0000-4000-8000-000000000010"
			o.routeFamily = &routeFamily
			o.retryAfterSeconds = floatPtr(3600)
			o.observedAt = now
		})

		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			cooldownGated:    estimateFor(60, "github", "rest_core", "work-items"),
			fitsIfNotCharged: estimateFor(50, "github", "rest_core", "work-items"), // 60+50=110>100 IF cooldownGated were charged
		}}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		result, err := enforceRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, nil, now, nil)
		if err != nil {
			t.Fatalf("enforceRun: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		var status, errorCategory string
		if err := pool.QueryRow(ctx, `SELECT status, result->>'error_category' FROM sync_run_units WHERE id=$1`, cooldownGated).
			Scan(&status, &errorCategory); err != nil {
			t.Fatal(err)
		}
		if status != syncRunUnitStatusRetrying || errorCategory != rateLimitCooldownDeferredCategory {
			t.Fatalf("cooldownGated status=%q error_category=%q, want retrying/%q (the COOLDOWN path, not budget)", status, errorCategory, rateLimitCooldownDeferredCategory)
		}
		if result.deferredUnitIDs[fitsIfNotCharged] {
			t.Fatal("fitsIfNotCharged must NOT be deferred -- proves the cooldown-gated unit's 60 units were never charged against the shared bucket")
		}
		if got := enforceUnitStatus(t, ctx, pool, fitsIfNotCharged); got != syncRunUnitStatusPlanned {
			t.Fatalf("fitsIfNotCharged status=%q, want unchanged (planned/allowed)", got)
		}
	})
}

// TestEnforceRunAdmitsASurplusCandidateWhenHeadroomAllows pins the surplus
// phase's wiring into the top-level orchestrator: a not-yet-due
// budget-deferred unit (not a real candidate this pass) is pulled forward
// when slotHeadroom has room, and its prior available_at is preserved on
// the result for a later withdrawal.
func TestEnforceRunAdmitsASurplusCandidateWhenHeadroomAllows(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_BUDGET_DEFAULT_LIMIT", "100")
		// The host shell may have SYNC_BUDGET_BUCKET_LIMITS set for local dev
		// convenience (a real per-bucket override) -- neutralize it so this
		// test's math is deterministic regardless of the ambient environment.
		t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
		now := pgNow()
		future := now.Add(time.Hour)
		unitID := "00000000-0000-4000-8000-000000000341"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{
			id: unitID, status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now,
			resultJSON: `{"error_category":"budget_deferred"}`,
		})

		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			unitID: estimateFor(10, "github", "rest_core", "work-items"),
		}}
		slotHeadroom := map[dispatchBucket]int{{orgID: "org-1", provider: "github", costClass: "rest_core"}: 1}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		result, err := enforceRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, slotHeadroom, now, nil)
		if err != nil {
			t.Fatalf("enforceRun: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		if !result.surplusAdmittedUnitIDs[unitID] {
			t.Fatal("want unitID in surplusAdmittedUnitIDs")
		}
		priorAt, ok := result.surplusPriorAvailableAt[unitID]
		if !ok || !priorAt.Equal(future) {
			t.Fatalf("surplusPriorAvailableAt[unitID]=%v ok=%v, want=%s/true", priorAt, ok, future)
		}
		var availableAt time.Time
		if err := pool.QueryRow(ctx, `SELECT available_at FROM sync_run_units WHERE id=$1`, unitID).Scan(&availableAt); err != nil {
			t.Fatal(err)
		}
		if !availableAt.Equal(now) {
			t.Fatalf("available_at=%s want=%s (pulled forward to now)", availableAt, now)
		}
		// slotHeadroom passed in must be untouched (enforceRun clones it
		// before handing it to admitSurplusRetries).
		if slotHeadroom[dispatchBucket{orgID: "org-1", provider: "github", costClass: "rest_core"}] != 1 {
			t.Fatal("caller's slotHeadroom map must not be mutated by enforceRun")
		}
	})
}
