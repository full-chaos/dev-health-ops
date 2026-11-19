//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-14211 on Postgres: authorizeRun with the real active count, run order and older-run reads.

const (
	shareActiveRunA   = "00000000-0000-4000-8000-0000000142a1"
	shareActiveRunB   = "00000000-0000-4000-8000-0000000142b1"
	shareQueuedRun    = "00000000-0000-4000-8000-0000000142c1"
	shareLeaseOwner   = "worker-14211"
	shareMediumBucket = "medium"
)

func seedShareRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID, triggeredBy, mode string, createdAt time.Time) {
	t.Helper()
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: guardTestOrg})
	if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET triggered_by = $2, mode = $3, status = 'running', created_at = $4 WHERE id = $1::uuid`,
		runID, triggeredBy, mode, createdAt); err != nil {
		t.Fatalf("seed run %s: %v", runID, err)
	}
}

func seedShareUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, runID, mode, status string, now time.Time) {
	t.Helper()
	unit := pgseed.SyncRunUnit{ID: id, RunID: runID, OrgID: guardTestOrg, Provider: "github", CostClass: shareMediumBucket, Mode: mode,
		Status: status, UpdatedAt: &now}
	if status == syncRunUnitStatusRunning {
		lease, owner := now.Add(10*time.Minute), shareLeaseOwner
		unit.LeaseExpiresAt, unit.LeaseOwner = &lease, &owner
	}
	pgseed.InsertSyncRunUnit(ctx, t, pool, unit)
}

// sharePassAdmits runs one guard pass of runID in its own rolled-back transaction and returns how many of its
// candidate units it admitted.
func sharePassAdmits(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string, now time.Time) int {
	t.Helper()
	decision := sharePass(t, ctx, pool, runID, now)
	return len(decision.authorizedUnitIDs) - len(decision.cappedUnitIDs)
}

func sharePass(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string, now time.Time) guardDecision {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	decision, err := authorizeRun(ctx, tx, nil, guardTestOrg, runID, now)
	if err != nil {
		t.Fatalf("authorizeRun(%s): %v", runID, err)
	}
	if !decision.allowed {
		t.Fatalf("authorizeRun(%s) = %+v, want allowed", runID, decision)
	}
	return decision
}

// The prod shape: two backfill runs each hold one running medium unit (both slots), a third backfill run waits with
// two planned units and an incremental run waits with one. Nothing is admitted while both run. When one ends, the
// incremental unit gets the free slot and the waiting backfill does not take it (before the fix it took it).
func TestBackfillShareProdShapeOnPostgres(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedShareRun(t, ctx, pool, shareActiveRunA, "backfill", "backfill", now.Add(-20*time.Hour))
		seedShareRun(t, ctx, pool, shareActiveRunB, "backfill", "backfill", now.Add(-19*time.Hour))
		seedShareRun(t, ctx, pool, shareQueuedRun, "backfill", "backfill", now.Add(-18*time.Hour))
		seedShareRun(t, ctx, pool, guardTestRun, "schedule", "incremental", now.Add(-time.Minute))
		seedShareUnit(t, ctx, pool, guardUnitID("00000142a0", 0), shareActiveRunA, "backfill", syncRunUnitStatusRunning, now)
		seedShareUnit(t, ctx, pool, guardUnitID("00000142b0", 0), shareActiveRunB, "backfill", syncRunUnitStatusRunning, now)
		for i := 0; i < 2; i++ {
			seedShareUnit(t, ctx, pool, guardUnitID("00000142c0", i), shareQueuedRun, "backfill", syncRunUnitStatusPlanned, now)
		}
		seedShareUnit(t, ctx, pool, guardUnitID("00000142d0", 0), guardTestRun, "incremental", syncRunUnitStatusPlanned, now)

		if got := sharePassAdmits(t, ctx, pool, shareQueuedRun, now); got != 0 {
			t.Fatalf("both slots held by backfill: waiting backfill admitted %d, want 0", got)
		}
		if got := sharePassAdmits(t, ctx, pool, guardTestRun, now); got != 0 {
			t.Fatalf("both slots held by backfill: incremental admitted %d, want 0 (a running unit keeps its slot)", got)
		}
		// The capped line of each pass names the run's stored mode and trigger.
		for runID, want := range map[string][2]string{shareQueuedRun: {"backfill", "backfill"}, guardTestRun: {"incremental", "schedule"}} {
			decision := sharePass(t, ctx, pool, runID, now)
			if decision.reasonCode != guardReasonCodeUnitConcurrencyCap || decision.runMode != want[0] || decision.runTriggeredBy != want[1] {
				t.Fatalf("run %s: reason=%q mode=%q triggered_by=%q, want %q %q %q", runID, decision.reasonCode,
					decision.runMode, decision.runTriggeredBy, guardReasonCodeUnitConcurrencyCap, want[0], want[1])
			}
		}

		if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET status = 'success', lease_owner = NULL, lease_expires_at = NULL WHERE id = $1::uuid`,
			guardUnitID("00000142b0", 0)); err != nil {
			t.Fatal(err)
		}
		if got := sharePassAdmits(t, ctx, pool, shareQueuedRun, now); got != 0 {
			t.Fatalf("one backfill unit ended: waiting backfill admitted %d, want 0 (the last slot stays for non-backfill work)", got)
		}
		if got := sharePassAdmits(t, ctx, pool, guardTestRun, now); got != 1 {
			t.Fatalf("one backfill unit ended: incremental admitted %d, want 1", got)
		}
	})
}

// The active count reads the backfill rule of readRunOrder: triggered_by 'backfill' OR mode 'backfill'. Each clause
// alone makes an active unit a backfill unit; an incremental active unit is not one.
func TestBackfillShareCountsEachBackfillClause(t *testing.T) {
	for _, tc := range []struct {
		name, triggeredBy, mode string
		want                    int
	}{
		{name: "triggered_by backfill only", triggeredBy: "backfill", mode: "incremental", want: 0},
		{name: "mode backfill only", triggeredBy: "manual", mode: "backfill", want: 0},
		{name: "incremental", triggeredBy: "schedule", mode: "incremental", want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				now := pgNow()
				seedShareRun(t, ctx, pool, shareActiveRunA, tc.triggeredBy, tc.mode, now.Add(-20*time.Hour))
				seedShareRun(t, ctx, pool, shareQueuedRun, "backfill", "backfill", now.Add(-18*time.Hour))
				seedShareUnit(t, ctx, pool, guardUnitID("00000142a0", 0), shareActiveRunA, tc.mode, syncRunUnitStatusRunning, now)
				for i := 0; i < 2; i++ {
					seedShareUnit(t, ctx, pool, guardUnitID("00000142c0", i), shareQueuedRun, "backfill", syncRunUnitStatusPlanned, now)
				}
				if got := sharePassAdmits(t, ctx, pool, shareQueuedRun, now); got != tc.want {
					t.Fatalf("one active unit (%s/%s): waiting backfill admitted %d, want %d", tc.triggeredBy, tc.mode, got, tc.want)
				}
			})
		})
	}
}
