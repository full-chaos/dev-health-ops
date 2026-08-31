//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-4605 -- authorizeRun decides capacity from a snapshot read at the
// TOP of the dispatch transaction; claimUnits, many statements later in
// that SAME READ COMMITTED transaction, re-derives its own claim set from a
// FRESH read. Any row that becomes claimable between those two reads is
// claimed without its bucket's concurrency cap having been evaluated for
// this pass.
//
// Three reachability shapes are pinned here, weakest premise first:
//
//  1. TestClaimUnitsClaimsARetryingUnitThatBecameDueAfterTheGuardSnapshot --
//     NO concurrency at all. authorizeRun runs on Dispatch()'s `now`
//     (native_dispatch_sync_run_service.go:244) and claimUnits on a FRESH
//     service.nowUTC() (:384), separated by the feature gate, the
//     reference-discovery gate, invalidClaimsAmongDispatchCandidates,
//     observeRun, enforceRun and reconfirmCooldowns. A RETRYING unit whose
//     available_at lands inside that window is a `deferredBuckets` entry to
//     the guard (locked and cap-checked, but NOT a candidate, so it consumes
//     no slot and the bucket reports zero headroom) and a claim to
//     claimUnits. This is the shape reachable in today's wiring.
//
//  2. TestClaimUnitsClaimsAUnitLeaseRepairMadeClaimableInAnUnlockedBucket --
//     the ticket's named mechanism. A run whose only unit in a bucket is
//     RUNNING contributes that bucket to NEITHER the lock set NOR the
//     cap-check set (dispatch_guard.go:194-196, codex round 8), so a
//     concurrent LeaseRepair.Step takes the bucket lock uncontested and
//     converts the unit to RETRYING underneath the open dispatch
//     transaction.
//
//  3. TestGuardBucketLockDoesNotProtectTheClaimBecauseTheSnapshotPrecedesIt
//     -- proves widening the lock set is NOT a sufficient fix.
//     loadDispatchGuardUnits (dispatch_guard.go:107) runs BEFORE
//     acquireBucketAdvisoryLocks (:200), so a writer that already holds the
//     bucket lock commits into the window between them and the guard's
//     freshly-acquired lock certifies a snapshot that is already stale.
//
// Every case asserts the same harm: countActiveBucketUnits for the bucket,
// read back after the claim commits, exceeds syncUnitConcurrencyPerBucket().

const (
	claimSnapshotOrg         = "00000000-0000-4000-8000-0000000004a0"
	claimSnapshotRun         = "00000000-0000-4000-8000-0000000004a1"
	claimSnapshotOtherRun    = "00000000-0000-4000-8000-0000000004a2"
	claimSnapshotIntegration = "00000000-0000-4000-8000-0000000004a3"
	claimSnapshotSource      = "00000000-0000-4000-8000-0000000004a4"
	claimSnapshotProvider    = "github"
	claimSnapshotCostClass   = "rest_core"
)

func claimSnapshotBucket() dispatchBucket {
	return dispatchBucket{orgID: claimSnapshotOrg, provider: claimSnapshotProvider, costClass: claimSnapshotCostClass}
}

// createClaimSnapshotTables builds the budget-candidates sync_run_units
// projection (every column budgetUnitSelectColumns reads, plus the lease
// columns countActiveBucketUnits needs) AND the three entitlement tables
// ResolveMaxSyncUnitsCap queries -- without them a uuid org id makes
// loadPlanLimits' SELECT fail, and a failed statement poisons the whole
// dispatch transaction rather than falling back to the env default.
func createClaimSnapshotTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.organizations (id uuid PRIMARY KEY, tier text);
CREATE TABLE public.org_licenses (
 org_id uuid PRIMARY KEY, tier text NOT NULL, limits_override json NOT NULL,
 features_override json NOT NULL
);
CREATE TABLE public.tier_limits (
 tier text NOT NULL, limit_key text NOT NULL, limit_value text, UNIQUE(tier, limit_key)
);
CREATE TABLE public.sync_runs (
 id uuid PRIMARY KEY, completed_units int NOT NULL DEFAULT 0,
 failed_units int NOT NULL DEFAULT 0, total_units int NOT NULL DEFAULT 0
);
CREATE TABLE public.sync_run_units (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL, org_id text NOT NULL,
 integration_id uuid NOT NULL, source_id uuid NOT NULL, provider text NOT NULL,
 dataset_key text NOT NULL, cost_class text NOT NULL,
 since_at timestamptz NULL, before_at timestamptz NULL,
 status text NOT NULL, available_at timestamptz NULL,
 updated_at timestamptz NOT NULL DEFAULT now(), error text NULL, result json NULL,
 lease_owner text NULL, lease_expires_at timestamptz NULL, last_heartbeat_at timestamptz NULL,
 rate_limit_deferrals int NOT NULL DEFAULT 0, rate_limit_first_seen_at timestamptz NULL,
 budget_deferrals int NOT NULL DEFAULT 0, budget_first_deferred_at timestamptz NULL,
 first_blocked_at timestamptz NULL, last_retry_reason text NULL, processor_flags json NULL
)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO public.organizations (id, tier) VALUES ($1::uuid, 'community')`, claimSnapshotOrg); err != nil {
		t.Fatal(err)
	}
	for _, runID := range []string{claimSnapshotRun, claimSnapshotOtherRun} {
		if _, err := pool.Exec(ctx, `INSERT INTO public.sync_runs (id) VALUES ($1::uuid)`, runID); err != nil {
			t.Fatal(err)
		}
	}
}

func withClaimSnapshotPool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	createClaimSnapshotTables(t, ctx, pool)
	fn(ctx, pool)
}

type claimSnapshotUnitFixture struct {
	id             string
	runID          string
	status         string
	availableAt    *time.Time
	updatedAt      time.Time
	leaseOwner     *string
	leaseExpiresAt *time.Time
}

func insertClaimSnapshotUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, f claimSnapshotUnitFixture) {
	t.Helper()
	if f.runID == "" {
		f.runID = claimSnapshotRun
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units
 (id, sync_run_id, org_id, integration_id, source_id, provider, dataset_key, cost_class,
  status, available_at, updated_at, result, lease_owner, lease_expires_at)
VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5::uuid, $6, 'commits', $7,
        $8, $9, $10, '{}'::json, $11, $12)`,
		f.id, f.runID, claimSnapshotOrg, claimSnapshotIntegration, claimSnapshotSource,
		claimSnapshotProvider, claimSnapshotCostClass,
		f.status, f.availableAt, f.updatedAt, f.leaseOwner, f.leaseExpiresAt); err != nil {
		t.Fatal(err)
	}
}

// saturateClaimSnapshotBucket fills the bucket to exactly `count` capacity
// CONSUMERS from OTHER runs -- live RUNNING units (lease_expires_at in the
// future), which is what countActiveBucketUnits counts.
func saturateClaimSnapshotBucket(t *testing.T, ctx context.Context, pool *pgxpool.Pool, count int, now time.Time) {
	t.Helper()
	owner := "other-run-worker"
	live := now.Add(time.Hour)
	for index := 0; index < count; index++ {
		insertClaimSnapshotUnit(t, ctx, pool, claimSnapshotUnitFixture{
			id:             fmt.Sprintf("00000000-0000-4000-8000-0000000004b%x", index),
			runID:          claimSnapshotOtherRun,
			status:         syncRunUnitStatusRunning,
			updatedAt:      now,
			leaseOwner:     &owner,
			leaseExpiresAt: &live,
		})
	}
}

// activeBucketUnitsNow reads countActiveBucketUnits' own predicate back in a
// fresh transaction -- the occupancy claim is made with the production
// function, never a hand-written copy of its SQL.
func activeBucketUnitsNow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time) int {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	count, err := countActiveBucketUnits(ctx, tx, claimSnapshotBucket(), now.Add(-staleDispatchSeconds()), now)
	if err != nil {
		t.Fatalf("countActiveBucketUnits: %v", err)
	}
	return count
}

func unitStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM public.sync_run_units WHERE id = $1::uuid`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

// TestClaimUnitsClaimsARetryingUnitThatBecameDueAfterTheGuardSnapshot is the
// zero-concurrency instance of CHAOS-4605: the guard's `now` and
// claimUnits' `now` are two different timestamps taken at two different
// points of the SAME transaction, so a RETRYING unit can be "not yet due"
// to the capacity decision and "due" to the claim.
func TestClaimUnitsClaimsARetryingUnitThatBecameDueAfterTheGuardSnapshot(t *testing.T) {
	withClaimSnapshotPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		bucketCap := syncUnitConcurrencyPerBucket()
		guardNow := pgNow()
		// The window between authorizeRun's `now` and claimUnits' own
		// service.nowUTC(): in production it is the runtime of the feature
		// gate, the reference-discovery gate, the pre-capacity claim
		// validation, observeRun, enforceRun and reconfirmCooldowns.
		becameDueAt := guardNow.Add(200 * time.Millisecond)
		claimNow := guardNow.Add(400 * time.Millisecond)

		saturateClaimSnapshotBucket(t, ctx, pool, bucketCap, guardNow)
		lateUnit := "00000000-0000-4000-8000-0000000004c1"
		insertClaimSnapshotUnit(t, ctx, pool, claimSnapshotUnitFixture{
			id: lateUnit, status: syncRunUnitStatusRetrying,
			availableAt: &becameDueAt, updatedAt: guardNow,
		})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		decision, err := authorizeRun(ctx, tx, nil, claimSnapshotOrg, claimSnapshotRun, guardNow)
		if err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		if !decision.allowed {
			t.Fatalf("guard denied the run outright: %+v", decision)
		}
		// The bucket IS visited (a not-yet-due RETRYING unit makes it a
		// deferred bucket) and reports ZERO headroom -- the guard's own
		// account of this pass is "this bucket admits nothing".
		if headroom, ok := decision.slotHeadroom[claimSnapshotBucket()]; !ok || headroom != 0 {
			t.Fatalf("slotHeadroom[bucket]=%d ok=%v want 0/true (bucket already at cap)", headroom, ok)
		}
		if len(decision.cappedUnitIDs) != 0 {
			t.Fatalf("cappedUnitIDs=%v want empty (the late unit was never a candidate)", decision.cappedUnitIDs)
		}

		claimed, deferred, err := claimUnits(ctx, tx, claimSnapshotRun, decision.authorizedUnitIDs, nil, claimNow)
		if err != nil {
			t.Fatalf("claimUnits: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		if got := idsOf(claimed); len(got) != 0 {
			t.Fatalf("claimUnits claimed %v; want nothing -- the guard authorized no unit in a bucket it reported at zero headroom", got)
		}
		// The skip is REPORTED, not silent: the caller re-arms a dispatch
		// for exactly these ids rather than stranding them.
		if len(deferred) != 1 || deferred[0] != lateUnit {
			t.Fatalf("deferredOutsideSnapshot=%v want exactly [%s]", deferred, lateUnit)
		}
		if status := unitStatus(t, ctx, pool, lateUnit); status != syncRunUnitStatusRetrying {
			t.Fatalf("late unit status=%q want=retrying (unclaimed, left for the next pass)", status)
		}
		if active := activeBucketUnitsNow(t, ctx, pool, claimNow); active > bucketCap {
			t.Fatalf("bucket occupancy=%d exceeds SYNC_UNIT_CONCURRENCY_PER_BUCKET=%d", active, bucketCap)
		}
	})
}

// leaseRepairFlipToRetrying reproduces the committed state transition
// LeaseRepair.Step performs (internal/syncreconciler/lease_repair.go:
// acquireLeaseRepairBucketLocks then markExpiredLeaseRetryingSQL), narrowed
// to the columns this fixture schema carries. The advisory key is taken
// from THIS package's bucketAdvisoryLockKey; the two packages' formulas are
// pinned identical by
// internal/syncreconciler/bucket_advisory_lock_contract_test.go, so this is
// the same lock LeaseRepair.Step would take.
//
// availableAt is the caller's choice: LeaseRepair writes now+RetryBackoff,
// and LeaseRepairConfig.valid() admits RetryBackoff == 0 (the shipped
// DefaultLeaseRepairConfig is 60s).
func leaseRepairFlipToRetrying(t *testing.T, ctx context.Context, pool *pgxpool.Pool, unitID string, availableAt, now time.Time) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, bucketAdvisoryLockKey(claimSnapshotBucket())); err != nil {
		t.Fatalf("lease repair bucket lock: %v", err)
	}
	tag, err := tx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = 'retrying', available_at = $2, updated_at = $3,
    error = 'sync unit lease expired', last_retry_reason = 'expired_lease',
    lease_owner = NULL, lease_expires_at = NULL
WHERE id = $1::uuid AND status = 'running'
  AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL AND lease_expires_at <= $3`,
		unitID, availableAt, now)
	if err != nil {
		t.Fatalf("lease repair retry stamp: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("lease repair retry stamp affected %d rows, want 1", tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestClaimUnitsClaimsAUnitLeaseRepairMadeClaimableInAnUnlockedBucket is the
// ticket's named mechanism. This run's ONLY unit in the bucket is RUNNING,
// so dispatch_guard.go's classification loop puts the bucket in neither
// lockBucketsByUnit nor candidatesByBucket: the guard never locks it and
// never checks its cap. LeaseRepair.Step then takes that bucket's lock
// UNCONTESTED and converts the unit to a due RETRYING row underneath the
// still-open dispatch transaction.
func TestClaimUnitsClaimsAUnitLeaseRepairMadeClaimableInAnUnlockedBucket(t *testing.T) {
	withClaimSnapshotPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		bucketCap := syncUnitConcurrencyPerBucket()
		guardNow := pgNow()
		claimNow := guardNow.Add(400 * time.Millisecond)

		saturateClaimSnapshotBucket(t, ctx, pool, bucketCap, guardNow)
		owner := "dead-worker"
		expired := guardNow.Add(-time.Minute)
		lostUnit := "00000000-0000-4000-8000-0000000004d1"
		insertClaimSnapshotUnit(t, ctx, pool, claimSnapshotUnitFixture{
			id: lostUnit, status: syncRunUnitStatusRunning, updatedAt: guardNow,
			leaseOwner: &owner, leaseExpiresAt: &expired,
		})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		decision, err := authorizeRun(ctx, tx, nil, claimSnapshotOrg, claimSnapshotRun, guardNow)
		if err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		if !decision.allowed {
			t.Fatalf("guard denied the run outright: %+v", decision)
		}
		if _, ok := decision.slotHeadroom[claimSnapshotBucket()]; ok {
			t.Fatalf("slotHeadroom has an entry for a bucket whose only unit is RUNNING: %+v", decision.slotHeadroom)
		}

		// Uncontested: authorizeRun asked for no lock on this bucket.
		leaseRepairFlipToRetrying(t, ctx, pool, lostUnit, guardNow.Add(200*time.Millisecond), guardNow)

		claimed, deferred, err := claimUnits(ctx, tx, claimSnapshotRun, decision.authorizedUnitIDs, nil, claimNow)
		if err != nil {
			t.Fatalf("claimUnits: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		if got := idsOf(claimed); len(got) != 0 {
			t.Fatalf("claimUnits claimed %v; want nothing -- the guard neither locked nor cap-checked this bucket", got)
		}
		if len(deferred) != 1 || deferred[0] != lostUnit {
			t.Fatalf("deferredOutsideSnapshot=%v want exactly [%s]", deferred, lostUnit)
		}
		if active := activeBucketUnitsNow(t, ctx, pool, claimNow); active > bucketCap {
			t.Fatalf("bucket occupancy=%d exceeds SYNC_UNIT_CONCURRENCY_PER_BUCKET=%d", active, bucketCap)
		}
	})
}

// TestGuardBucketLockDoesNotProtectTheClaimBecauseTheSnapshotPrecedesIt
// closes the "just widen the lock set" fix shape. Here the bucket IS locked
// and IS cap-checked (the run has a PLANNED unit in it), and the guard still
// admits one unit over cap -- because loadDispatchGuardUnits runs BEFORE
// acquireBucketAdvisoryLocks, so a writer holding the lock first commits
// into the gap and the lock the guard finally acquires certifies an
// already-stale snapshot.
func TestGuardBucketLockDoesNotProtectTheClaimBecauseTheSnapshotPrecedesIt(t *testing.T) {
	withClaimSnapshotPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		bucketCap := syncUnitConcurrencyPerBucket()
		guardNow := pgNow()
		claimNow := guardNow.Add(400 * time.Millisecond)

		// One free slot in the bucket.
		saturateClaimSnapshotBucket(t, ctx, pool, bucketCap-1, guardNow)
		plannedUnit := "00000000-0000-4000-8000-0000000004e1"
		insertClaimSnapshotUnit(t, ctx, pool, claimSnapshotUnitFixture{
			id: plannedUnit, status: syncRunUnitStatusPlanned, updatedAt: guardNow,
		})
		owner := "dead-worker"
		expired := guardNow.Add(-time.Minute)
		lostUnit := "00000000-0000-4000-8000-0000000004e2"
		insertClaimSnapshotUnit(t, ctx, pool, claimSnapshotUnitFixture{
			id: lostUnit, status: syncRunUnitStatusRunning, updatedAt: guardNow,
			leaseOwner: &owner, leaseExpiresAt: &expired,
		})

		// LeaseRepair.Step's transaction takes the bucket lock FIRST and
		// holds it while the dispatch pass takes its snapshot.
		repairTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = repairTx.Rollback(ctx) }()
		if _, err := repairTx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, bucketAdvisoryLockKey(claimSnapshotBucket())); err != nil {
			t.Fatal(err)
		}

		var (
			wg       sync.WaitGroup
			decision guardDecision
			claimed  []budgetUnit
			deferred []string
			guardErr error
			claimErr error
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := pool.Begin(ctx)
			if err != nil {
				guardErr = err
				return
			}
			defer func() { _ = tx.Rollback(ctx) }()
			// Snapshot is taken here, then this blocks on the bucket lock.
			decision, guardErr = authorizeRun(ctx, tx, nil, claimSnapshotOrg, claimSnapshotRun, guardNow)
			if guardErr != nil {
				return
			}
			capped := map[string]bool{}
			for _, id := range decision.cappedUnitIDs {
				capped[id] = true
			}
			claimed, deferred, claimErr = claimUnits(ctx, tx, claimSnapshotRun, decision.authorizedUnitIDs, capped, claimNow)
			if claimErr != nil {
				return
			}
			claimErr = tx.Commit(ctx)
		}()

		// Deterministic happens-after edge: Postgres itself reports the
		// dispatch transaction waiting on the held bucket lock, which it can
		// only reach AFTER loadDispatchGuardUnits (dispatch_guard.go:107)
		// already took its snapshot.
		waitForBlockedAdvisoryLock(t, ctx, pool, "authorizeRun")
		// The dispatch snapshot is already taken and already says RUNNING.
		if _, err := repairTx.Exec(ctx, `
UPDATE public.sync_run_units
SET status = 'retrying', available_at = $2, updated_at = $3,
    error = 'sync unit lease expired', last_retry_reason = 'expired_lease',
    lease_owner = NULL, lease_expires_at = NULL
WHERE id = $1::uuid AND status = 'running'`,
			lostUnit, guardNow.Add(200*time.Millisecond), guardNow); err != nil {
			t.Fatal(err)
		}
		if err := repairTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		wg.Wait()

		if guardErr != nil {
			t.Fatalf("authorizeRun: %v", guardErr)
		}
		if claimErr != nil {
			t.Fatalf("claimUnits: %v", claimErr)
		}
		// The guard held the bucket lock and cap-checked the bucket: one
		// free slot, one PLANNED candidate, nothing capped.
		if len(decision.cappedUnitIDs) != 0 {
			t.Fatalf("cappedUnitIDs=%v want empty (one candidate, one free slot)", decision.cappedUnitIDs)
		}
		got := idsOf(claimed)
		if len(got) != 1 || !got[plannedUnit] {
			t.Fatalf("claimUnits claimed %v; want exactly [%s] -- the lease-repaired unit was never authorized by this pass", got, plannedUnit)
		}
		if len(deferred) != 1 || deferred[0] != lostUnit {
			t.Fatalf("deferredOutsideSnapshot=%v want exactly [%s]", deferred, lostUnit)
		}
		if active := activeBucketUnitsNow(t, ctx, pool, claimNow); active > bucketCap {
			t.Fatalf("bucket occupancy=%d exceeds SYNC_UNIT_CONCURRENCY_PER_BUCKET=%d despite the guard holding this bucket's advisory lock", active, bucketCap)
		}
	})
}
