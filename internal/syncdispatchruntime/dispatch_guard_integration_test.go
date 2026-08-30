//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func createDispatchGuardTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE public.organizations (id uuid PRIMARY KEY, tier text);
CREATE TABLE public.org_licenses (
 org_id uuid PRIMARY KEY, tier text NOT NULL, limits_override json NOT NULL,
 features_override json NOT NULL
);
CREATE TABLE public.tier_limits (
 tier text NOT NULL, limit_key text NOT NULL, limit_value text,
 UNIQUE(tier,limit_key)
);
CREATE TABLE public.sync_runs (
 id uuid PRIMARY KEY, org_id text NOT NULL
);
CREATE TABLE public.sync_run_units (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_run_id uuid NOT NULL, provider text NOT NULL,
 cost_class text NOT NULL, status text NOT NULL, available_at timestamptz NULL,
 lease_owner text NULL, lease_expires_at timestamptz NULL,
 updated_at timestamptz NOT NULL DEFAULT now()
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const (
	guardTestOrg = "00000000-0000-4000-8000-0000000000e1"
	guardTestRun = "00000000-0000-4000-8000-0000000000e2"
)

func seedGuardRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO public.organizations (id, tier) VALUES ($1::uuid, 'community')`, guardTestOrg); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO public.sync_runs (id, org_id) VALUES ($1::uuid, $2)`, guardTestRun, guardTestOrg); err != nil {
		t.Fatal(err)
	}
}

func insertGuardUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, provider, costClass, status string, updatedAt time.Time, availableAt, leaseExpiresAt *time.Time, leaseOwner *string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (id, org_id, sync_run_id, provider, cost_class, status, updated_at, available_at, lease_expires_at, lease_owner)
VALUES ($1::uuid, $2, $3::uuid, $4, $5, $6, $7, $8, $9, $10)`,
		id, guardTestOrg, guardTestRun, provider, costClass, status, updatedAt, availableAt, leaseExpiresAt, leaseOwner); err != nil {
		t.Fatal(err)
	}
}

func withGuardPool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
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
	createDispatchGuardTables(t, ctx, pool)
	seedGuardRun(t, ctx, pool)
	fn(ctx, pool)
}

// TestAuthorizeRunAllowsWhenUnderEveryCap pins the full-allow shape: no
// total-cap or concurrency-cap hit, slotHeadroom still populated per bucket.
func TestAuthorizeRunAllowsWhenUnderEveryCap(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		insertGuardUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000000f1", "github", "standard", syncRunUnitStatusPlanned, now, nil, nil, nil)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		decision, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now)
		if err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		if !decision.allowed || decision.concurrencyCapped || len(decision.cappedUnitIDs) != 0 {
			t.Fatalf("decision=%+v, want full allow", decision)
		}
		bucket := dispatchBucket{orgID: guardTestOrg, provider: "github", costClass: "standard"}
		if headroom, ok := decision.slotHeadroom[bucket]; !ok || headroom != 7 {
			t.Fatalf("slotHeadroom[bucket]=%d ok=%v, want 7 (cap 8 - 1 candidate)", headroom, ok)
		}
	})
}

// TestAuthorizeRunHardDeniesOverTheTotalUnitCap pins the total-cap
// hard-deny shape: a run with more units than the org's resolved cap is
// denied outright, capped_unit_ids is the STABLE id-ordered suffix past the
// cap, and NO bucket is ever visited (mismatched-value fixture: the org's
// tier_limits row is deliberately set below the unit count).
func TestAuthorizeRunHardDeniesOverTheTotalUnitCap(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		if _, err := pool.Exec(ctx, `INSERT INTO public.tier_limits (tier, limit_key, limit_value) VALUES ('community', 'max_sync_units', '1')`); err != nil {
			t.Fatal(err)
		}
		now := pgNow()
		insertGuardUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000000f1", "github", "standard", syncRunUnitStatusPlanned, now, nil, nil, nil)
		insertGuardUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000000f2", "github", "standard", syncRunUnitStatusPlanned, now, nil, nil, nil)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		decision, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now)
		if err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		if decision.allowed || decision.concurrencyCapped {
			t.Fatalf("decision=%+v, want total-cap hard-deny (allowed=false, concurrencyCapped=false)", decision)
		}
		if len(decision.cappedUnitIDs) != 1 || decision.cappedUnitIDs[0] != "00000000-0000-4000-8000-0000000000f2" {
			t.Fatalf("cappedUnitIDs=%v, want the id-ordered suffix past cap 1", decision.cappedUnitIDs)
		}
		if len(decision.slotHeadroom) != 0 {
			t.Fatalf("slotHeadroom=%v, want empty -- hard-deny returns before any bucket is visited", decision.slotHeadroom)
		}
	})
}

// TestAuthorizeRunFallsBackToTheDefaultCapOnAnOrgResolutionFailure pins the
// mismatched-value fixture team-lead flagged explicitly: an org_id that
// resolves to NO organizations row (deleted/never-synced) must not error
// out of authorizeRun -- Python's _resolve_total_unit_cap catches every
// resolution failure and falls back to the env default unconditionally
// (CHAOS-2580), and ResolveMaxSyncUnitsCap's own doc warns this port must
// replicate that fallback itself rather than propagate the error.
func TestAuthorizeRunFallsBackToTheDefaultCapOnAnOrgResolutionFailure(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_RUN_MAX_UNITS", "1")
		missingOrgRun := "00000000-0000-4000-8000-0000000000e9"
		missingOrg := "00000000-0000-4000-8000-0000000000e8"
		if _, err := pool.Exec(ctx, `INSERT INTO public.sync_runs (id, org_id) VALUES ($1::uuid, $2)`, missingOrgRun, missingOrg); err != nil {
			t.Fatal(err)
		}
		now := pgNow()
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (id, org_id, sync_run_id, provider, cost_class, status, updated_at)
VALUES ($1::uuid, $2, $3::uuid, 'github', 'standard', $4, $5)`,
			"00000000-0000-4000-8000-0000000000f3", missingOrg, missingOrgRun, syncRunUnitStatusPlanned, now); err != nil {
			t.Fatal(err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		decision, err := authorizeRun(ctx, tx, nil, missingOrg, missingOrgRun, now)
		if err != nil {
			t.Fatalf("authorizeRun: %v, want no error -- a missing org must fall back to the default cap, not fail the whole guard", err)
		}
		// SYNC_RUN_MAX_UNITS=1, exactly 1 unit -> under cap, not over it.
		if !decision.allowed || decision.concurrencyCapped {
			t.Fatalf("decision=%+v, want full allow at the fallback default cap", decision)
		}
	})
}

// TestAuthorizeRunConcurrencyCapsOverflowAndOrdersReclaimsFirst pins the
// concurrency partial-cap shape AND the CHAOS-3990 starvation fix in one
// test: a bucket already at its concurrency cap (via a live consumer) with
// both a stale-DISPATCHING reclaim candidate and fresh PLANNED candidates
// must cap the PLANNED unit, not the reclaim -- reclaims are prioritized
// ahead of new work.
func TestAuthorizeRunConcurrencyCapsOverflowAndOrdersReclaimsFirst(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "2")
		now := pgNow()
		staleCutoff := now.Add(-16 * time.Minute) // default stale window is 900s=15m

		// One live consumer occupies one of the bucket's two slots, leaving
		// exactly one slot for the two candidates below to compete over.
		consumerID := "00000000-0000-4000-8000-0000000000f4"
		liveLeaseExpiry := now.Add(time.Hour)
		insertGuardUnit(t, ctx, pool, consumerID, "github", "standard", syncRunUnitStatusRunning, now, nil, &liveLeaseExpiry, ptrString("consumer-owner"))

		// A stale DISPATCHING reclaim candidate -- must win the one slot.
		reclaimID := "00000000-0000-4000-8000-0000000000f5"
		insertGuardUnit(t, ctx, pool, reclaimID, "github", "standard", syncRunUnitStatusDispatching, staleCutoff, nil, nil, nil)

		// A PLANNED candidate -- must be the one capped.
		plannedID := "00000000-0000-4000-8000-0000000000f6"
		insertGuardUnit(t, ctx, pool, plannedID, "github", "standard", syncRunUnitStatusPlanned, now, nil, nil, nil)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		decision, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now)
		if err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		if !decision.allowed || !decision.concurrencyCapped {
			t.Fatalf("decision=%+v, want concurrency partial-cap", decision)
		}
		if len(decision.cappedUnitIDs) != 1 || decision.cappedUnitIDs[0] != plannedID {
			t.Fatalf("cappedUnitIDs=%v, want [%s] (the PLANNED unit, not the reclaim)", decision.cappedUnitIDs, plannedID)
		}
	})
}

// TestAuthorizeRunTreatsAnExpiredLeaseAsNotConsuming pins the lease-state
// split in the consumer set: a RUNNING unit with an EXPIRED lease is dead
// (the reconciler's job to terminalize) and must NOT count against the
// concurrency cap, unlike a NULL or live lease.
func TestAuthorizeRunTreatsAnExpiredLeaseAsNotConsuming(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "1")
		now := pgNow()
		expiredLeaseID := "00000000-0000-4000-8000-0000000000f7"
		expiredLease := now.Add(-time.Hour)
		insertGuardUnit(t, ctx, pool, expiredLeaseID, "github", "standard", syncRunUnitStatusRunning, now, nil, &expiredLease, ptrString("dead-owner"))

		plannedID := "00000000-0000-4000-8000-0000000000f8"
		insertGuardUnit(t, ctx, pool, plannedID, "github", "standard", syncRunUnitStatusPlanned, now, nil, nil, nil)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		decision, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now)
		if err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		if !decision.allowed || decision.concurrencyCapped {
			t.Fatalf("decision=%+v, want full allow -- an expired lease must not consume a slot", decision)
		}
	})
}

// TestAuthorizeRunPopulatesSlotHeadroomForADeferredOnlyBucket pins the
// CHAOS-3465 fix: a bucket holding ONLY a not-yet-due RETRYING unit has no
// candidate this pass, but must still get an active-count read (and
// slotHeadroom entry) -- omitting it would report "no slots" for exactly
// the case surplus retry exists to unblock.
func TestAuthorizeRunPopulatesSlotHeadroomForADeferredOnlyBucket(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		notYetDue := now.Add(time.Hour)
		deferredID := "00000000-0000-4000-8000-0000000000f9"
		insertGuardUnit(t, ctx, pool, deferredID, "github", "standard", syncRunUnitStatusRetrying, now, &notYetDue, nil, nil)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		decision, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now)
		if err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		bucket := dispatchBucket{orgID: guardTestOrg, provider: "github", costClass: "standard"}
		if headroom, ok := decision.slotHeadroom[bucket]; !ok || headroom != 8 {
			t.Fatalf("slotHeadroom[bucket]=%d ok=%v, want 8 (full cap, zero candidates this pass)", headroom, ok)
		}
	})
}

func ptrString(value string) *string { return &value }

// TestAuthorizeRunLocksEveryUnitsBucketEvenOnesExcludedFromCandidateSelection
// pins codex round 7's finding (CHAOS-4586, P2): the bucket lock set
// authorizeRun acquires must cover EVERY unit's bucket, not just the ones
// eligible for the concurrency-cap candidate loop further down.
//
// A RETRYING unit with a nil available_at is silently skipped by that
// candidate loop -- no switch case in it matches (syncRunUnitStatusRetrying
// only adds to candidatesByBucket/deferredBuckets when availableAt != nil)
// -- but IS matched by denyRun's failPlannedUnits, whose bare `status IN
// (planned, retrying)` predicate has no such carve-out. Before this fix,
// such a unit's bucket would never be locked by authorizeRun at all, even
// though a hard-cap denial's bulk write could still touch it.
//
// This is a genuine dynamic proof, not a source-text scan: it opens
// authorizeRun's transaction, deliberately never commits or rolls it back
// before probing, and uses a SEPARATE pool connection's
// pg_try_advisory_xact_lock (a single implicit-transaction statement, so it
// acquires-and-immediately-releases when free, or fails when someone else
// already holds the key) to observe whether the excluded unit's bucket is
// actually locked from OUTSIDE the transaction that (should have) taken it.
func TestAuthorizeRunLocksEveryUnitsBucketEvenOnesExcludedFromCandidateSelection(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		// An ordinary candidate, plus a RETRYING unit with available_at
		// NULL in its OWN distinct bucket -- excluded from
		// candidatesByBucket/deferredBuckets by construction, but present
		// in `units` and therefore in scope for failPlannedUnits's bulk
		// UPDATE if this run were denied.
		insertGuardUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000000f1", "github", "standard", syncRunUnitStatusPlanned, now, nil, nil, nil)
		insertGuardUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000000f2", "linear", "excluded-bucket", syncRunUnitStatusRetrying, now, nil, nil, nil)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now); err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		// authorizeRun's own transaction is still OPEN -- deliberately not
		// committed or rolled back yet -- so any lock it took is still held.

		excludedKey := bucketAdvisoryLockKey(dispatchBucket{orgID: guardTestOrg, provider: "linear", costClass: "excluded-bucket"})
		var excludedLocked bool
		if err := pool.QueryRow(ctx, `SELECT NOT pg_try_advisory_xact_lock($1)`, excludedKey).Scan(&excludedLocked); err != nil {
			t.Fatal(err)
		}
		if !excludedLocked {
			t.Fatal("the excluded-bucket unit's advisory lock is NOT held by authorizeRun's transaction -- " +
				"its lock set is not covering every unit's bucket (codex round 7, CHAOS-4586)")
		}

		// Control: a bucket NO unit belongs to must NOT be held -- proves
		// the probe technique itself (and this fixture) actually works,
		// rather than pg_try_advisory_xact_lock always returning "held".
		unrelatedKey := bucketAdvisoryLockKey(dispatchBucket{orgID: guardTestOrg, provider: "nonexistent", costClass: "nonexistent"})
		var unrelatedLocked bool
		if err := pool.QueryRow(ctx, `SELECT NOT pg_try_advisory_xact_lock($1)`, unrelatedKey).Scan(&unrelatedLocked); err != nil {
			t.Fatal(err)
		}
		if unrelatedLocked {
			t.Fatal("an unrelated bucket's advisory lock reads as held -- the probe technique or fixture is broken")
		}
	})
}

// TestAuthorizeRunDoesNotLockBucketsWithOnlyTerminalOrRunningUnits pins
// codex round 8's narrowing (CHAOS-4586, P2): a bucket whose only units in
// this run are terminal (SUCCESS/FAILED) or live RUNNING must NOT be
// locked. Round 7 fixed the nil-available_at RETRYING gap by locking
// EVERY unit's bucket unconditionally -- correct, but over-broad: neither
// denyRun's bulk writes (failPlannedUnits/failStaleDispatchingUnits) nor
// claimUnits ever touch a terminal or RUNNING row, so holding that
// bucket's lock for this whole transaction needlessly blocks an unrelated
// run sharing it. Uses the same open-transaction probe technique as
// TestAuthorizeRunLocksEveryUnitsBucketEvenOnesExcludedFromCandidateSelection.
func TestAuthorizeRunDoesNotLockBucketsWithOnlyTerminalOrRunningUnits(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		// A real candidate in its own bucket -- must be locked.
		insertGuardUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000000f1", "github", "standard", syncRunUnitStatusPlanned, now, nil, nil, nil)
		// A separate bucket whose only units are terminal or RUNNING --
		// never written by anything in this transaction, so it must NOT
		// be locked.
		insertGuardUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000000f2", "linear", "untouched-bucket", syncRunUnitStatusSuccess, now, nil, nil, nil)
		insertGuardUnit(t, ctx, pool, "00000000-0000-4000-8000-0000000000f3", "linear", "untouched-bucket", syncRunUnitStatusRunning, now, nil, nil, nil)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now); err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		// authorizeRun's own transaction is still OPEN.

		candidateKey := bucketAdvisoryLockKey(dispatchBucket{orgID: guardTestOrg, provider: "github", costClass: "standard"})
		var candidateLocked bool
		if err := pool.QueryRow(ctx, `SELECT NOT pg_try_advisory_xact_lock($1)`, candidateKey).Scan(&candidateLocked); err != nil {
			t.Fatal(err)
		}
		if !candidateLocked {
			t.Fatal("the real candidate's bucket is NOT locked -- authorizeRun's lock set regressed")
		}

		untouchedKey := bucketAdvisoryLockKey(dispatchBucket{orgID: guardTestOrg, provider: "linear", costClass: "untouched-bucket"})
		var untouchedLocked bool
		if err := pool.QueryRow(ctx, `SELECT NOT pg_try_advisory_xact_lock($1)`, untouchedKey).Scan(&untouchedLocked); err != nil {
			t.Fatal(err)
		}
		if untouchedLocked {
			t.Fatal("a bucket with only terminal/RUNNING units is locked -- authorizeRun over-locks again (codex round 8, CHAOS-4586)")
		}
	})
}

// TestAuthorizeRunLockSetMatchesExactlyTheStatusesItsMutatorsCanTouch is
// the contract test team-lead required after the lock set flip-flopped
// twice in two rounds (round 7 widened it to every unit unconditionally;
// round 8 narrowed it back down): rather than pin authorizeRun's OWN
// classification against itself, this derives "should this status be
// locked" independently from the five SQL predicates that can ever write
// public.sync_run_units for a run authorizeRun/denyRun/claimUnits touches,
// and asserts the real lock set matches that derivation exactly, status by
// status, in one pass. A future change to any of those five predicates
// that isn't mirrored in authorizeRun's lock-set loop fails THIS test,
// not a production deadlock discovered by codex nine rounds later.
//
// The five predicates (verbatim, CHAOS-4586):
//   - claimUnits' plain-claim UPDATE (claim_units.go): `status = planned`
//   - claimUnits' due-retry UPDATE (claim_units.go): `status = retrying
//     AND available_at IS NOT NULL AND available_at <= now`
//   - claimUnits' stale-reclaim UPDATE (claim_units.go): `status =
//     dispatching AND updated_at <= staleDispatchCutoff(now)`
//   - denyRun's failPlannedUnits (dispatch_denial.go): `status IN
//     (planned, retrying)` -- no available_at condition at all, so this
//     ALONE requires locking every retrying row regardless of
//     available_at, wider than claimUnits' own due-retry predicate above.
//   - denyRun's failStaleDispatchingUnits (dispatch_denial.go): `status =
//     dispatching AND updated_at <= staleDispatchCutoff(now)` -- same
//     cutoff formula as claimUnits' stale-reclaim (both call
//     staleDispatchCutoff, confirmed identical).
//
// Union of statuses these five predicates can EVER match, independent of
// available_at/updated_at except where a predicate itself conditions on
// it: PLANNED (unconditional), RETRYING (unconditional, from
// failPlannedUnits), DISPATCHING-if-stale. RUNNING and terminal
// (SUCCESS/FAILED) match none of the five under any available_at/
// updated_at value -- they must NOT be locked.
func TestAuthorizeRunLockSetMatchesExactlyTheStatusesItsMutatorsCanTouch(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		future := now.Add(time.Hour)
		past := now.Add(-time.Hour)
		staleUpdatedAt := now.Add(-(staleDispatchSeconds() + time.Minute))
		freshUpdatedAt := now

		type scenario struct {
			name        string
			costClass   string // distinct bucket per scenario -- provider stays "github"
			status      string
			updatedAt   time.Time
			availableAt *time.Time
			wantLocked  bool // independently derived from the 5 predicates above, NOT from authorizeRun's own source
		}
		scenarios := []scenario{
			{"planned", "s-planned", syncRunUnitStatusPlanned, freshUpdatedAt, nil, true},
			{"retrying nil available_at", "s-retrying-nil", syncRunUnitStatusRetrying, freshUpdatedAt, nil, true},
			{"retrying not yet due", "s-retrying-future", syncRunUnitStatusRetrying, freshUpdatedAt, &future, true},
			{"retrying due", "s-retrying-due", syncRunUnitStatusRetrying, freshUpdatedAt, &past, true},
			{"dispatching fresh", "s-dispatching-fresh", syncRunUnitStatusDispatching, freshUpdatedAt, nil, false},
			{"dispatching stale", "s-dispatching-stale", syncRunUnitStatusDispatching, staleUpdatedAt, nil, true},
			{"running", "s-running", syncRunUnitStatusRunning, freshUpdatedAt, nil, false},
			{"success (terminal)", "s-success", syncRunUnitStatusSuccess, freshUpdatedAt, nil, false},
			{"failed (terminal)", "s-failed", syncRunUnitStatusFailed, freshUpdatedAt, nil, false},
		}

		for index, sc := range scenarios {
			id := fmt.Sprintf("00000000-0000-4000-8000-0000000001%02d", index)
			insertGuardUnit(t, ctx, pool, id, "github", sc.costClass, sc.status, sc.updatedAt, sc.availableAt, nil, nil)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now); err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		// authorizeRun's own transaction is still OPEN.

		for _, sc := range scenarios {
			key := bucketAdvisoryLockKey(dispatchBucket{orgID: guardTestOrg, provider: "github", costClass: sc.costClass})
			var heldByOther bool
			if err := pool.QueryRow(ctx, `SELECT NOT pg_try_advisory_xact_lock($1)`, key).Scan(&heldByOther); err != nil {
				t.Fatalf("%s: probe: %v", sc.name, err)
			}
			if heldByOther != sc.wantLocked {
				t.Errorf("%s (status=%s): bucket locked=%v, want %v (derived from claimUnits'/denyRun's own predicates)",
					sc.name, sc.status, heldByOther, sc.wantLocked)
			}
		}
	})
}
