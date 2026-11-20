//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

func mustExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Errorf("%s: %v", collapseStatement(sql), err)
	}
}

// r1 class 2: the deliveries of a cancelled unit are found under every dedupe form the reconciler
// mints (the original key and /reclaim/1../reclaim/3), delivered and not yet delivered.
func TestCancelSyncRunCountsDeliveriesOfEveryDedupeForm(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	now := cancelTestNow()
	fixture := seedCancelFixture(t, ctx, pool, cancelTestRun, now)
	pgseed.WorkerJobOutbox(ctx, t, pool, "sync.provider_unit:"+fixture.units["b-retrying"]+"/reclaim/2", "sync.provider_unit", "delivered")
	pgseed.WorkerJobOutbox(ctx, t, pool, "sync.provider_unit:"+fixture.units["e-running-expired"]+"/reclaim/1", "sync.provider_unit", "pending")
	pgseed.WorkerJobOutbox(ctx, t, pool, "sync.provider_unit:"+fixture.units["e-running-expired"]+"/reclaim/3", "sync.provider_unit", "delivered")

	outcome, err := CancelSyncRun(ctx, pool, cancelRequest(cancelTestRun, now))
	if err != nil || outcome.Outcome != CancelOutcomeCancelled {
		t.Fatalf("err=%v outcome=%+v", err, outcome)
	}
	delivered, pending := cancelDeliveries(outcome)
	if delivered != 3 || pending != 2 {
		t.Fatalf("deliveries delivered=%d pending=%d, want 3 (original, /reclaim/2, /reclaim/3) and 2 (original, /reclaim/1)", delivered, pending)
	}
}

// r1 class 4: a finalizer that commits between the cancel's first read and its run lock keeps its
// terminal status and result; the cancel rolls back and reports already_terminal.
func TestCancelSyncRunKeepsARunFinalizedBeforeTheRunLock(t *testing.T) {
	for _, final := range []string{"success", "partial_failed"} {
		t.Run(final, func(t *testing.T) {
			ctx, admin, hooked, hook := startCancelHookPools(t)
			now := cancelTestNow()
			seedCancelFixture(t, ctx, admin, cancelTestRun, now)
			before := cancelSnapshot(t, ctx, admin, cancelTestRun)
			hook.arm("FROM public.sync_runs WHERE id = $1 FOR UPDATE", func() {
				mustExec(t, ctx, admin, `UPDATE public.sync_runs SET status = $2, completed_at = $3, result = '{"finalized_by":"finalizer"}'
WHERE id = $1`, cancelTestRun, final, now)
			})
			outcome, err := CancelSyncRun(ctx, hooked, cancelRequest(cancelTestRun, now))
			if !hook.didFire() {
				t.Fatal("the run lock was never reached; the interleaving did not happen")
			}
			var status, result string
			if err := admin.QueryRow(ctx, `SELECT status, result::text FROM public.sync_runs WHERE id = $1`, cancelTestRun).Scan(&status, &result); err != nil {
				t.Fatal(err)
			}
			if err != nil || outcome.Outcome != CancelOutcomeAlreadyTerminal || status != final || result != `{"finalized_by":"finalizer"}` {
				t.Fatalf("err=%v outcome=%+v run status=%s result=%s, want already_terminal and the finalizer's %s kept", err, outcome, status, result, final)
			}
			after := cancelSnapshot(t, ctx, admin, cancelTestRun)
			if strings.Count(before, `"status":"planned"`) != strings.Count(after, `"status":"planned"`) {
				t.Fatalf("the cancel's staged unit writes were not rolled back:\nbefore %s\nafter  %s", before, after)
			}
		})
	}
}

// r1 class 5: units_failed counts only the units the cancel's own writes failed, not a unit a
// worker failed in between.
func TestCancelSyncRunCountsOnlyItsOwnUnitWrites(t *testing.T) {
	ctx, admin, hooked, hook := startCancelHookPools(t)
	now := cancelTestNow()
	fixture := seedCancelFixture(t, ctx, admin, cancelTestRun, now)
	hook.arm("SELECT DISTINCT org_id, provider, cost_class", func() {
		mustExec(t, ctx, admin, `UPDATE public.sync_run_units SET status = 'failed', result = '{"error_category":"worker_lost"}'::json
WHERE id = $1`, fixture.units["c-dispatching"])
	})
	outcome, err := CancelSyncRun(ctx, hooked, cancelRequest(cancelTestRun, now))
	if err != nil || outcome.Outcome != CancelOutcomeCancelled || !hook.didFire() {
		t.Fatalf("err=%v outcome=%+v fired=%v", err, outcome, hook.didFire())
	}
	if outcome.UnitsFailed != 4 {
		t.Fatalf("units_failed=%d, want 4: the worker-failed unit is not the operator's", outcome.UnitsFailed)
	}
	var category string
	if err := admin.QueryRow(ctx, `SELECT result::jsonb ->> 'error_category' FROM public.sync_run_units WHERE id = $1`,
		fixture.units["c-dispatching"]).Scan(&category); err != nil || category != "worker_lost" {
		t.Fatalf("worker-failed unit category=%q err=%v, want worker_lost kept", category, err)
	}
}

// r1 P2: a same-owner lease renewal that lands between the expired-lease read and the write keeps
// the unit running without --allow-running; nothing is written.
func TestCancelSyncRunLeavesASameOwnerRenewalAlone(t *testing.T) {
	ctx, admin, hooked, hook := startCancelHookPools(t)
	now := cancelTestNow()
	fixture := seedCancelFixture(t, ctx, admin, cancelTestRun, now)
	hook.arm("lease_owner IS NOT DISTINCT FROM", func() {
		mustExec(t, ctx, admin, `UPDATE public.sync_run_units SET lease_expires_at = $2 WHERE id = $1`,
			fixture.units["e-running-expired"], now.Add(time.Hour))
	})
	_, err := CancelSyncRun(ctx, hooked, cancelRequest(cancelTestRun, now))
	if !hook.didFire() {
		t.Fatal("the lease write was never reached; the interleaving did not happen")
	}
	var unitStatus, runStatus string
	if err := admin.QueryRow(ctx, `SELECT u.status, r.status FROM public.sync_run_units AS u JOIN public.sync_runs AS r ON r.id = u.sync_run_id
WHERE u.id = $1`, fixture.units["e-running-expired"]).Scan(&unitStatus, &runStatus); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(err, ErrSyncRunHasRunningUnit) || unitStatus != "running" || runStatus != "dispatching" {
		t.Fatalf("err=%v unit=%s run=%s, want ErrSyncRunHasRunningUnit, the renewed unit running and the run open", err, unitStatus, runStatus)
	}
}
