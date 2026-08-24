//go:build integration

package syncdispatchruntime

import (
	"context"
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
		now := time.Now().UTC()
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
		now := time.Now().UTC()
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
		now := time.Now().UTC()
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
		now := time.Now().UTC()
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
		now := time.Now().UTC()
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
		now := time.Now().UTC()
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
