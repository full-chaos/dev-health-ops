//go:build integration

package syncdispatchruntime

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

const admissionOlderRun = "00000000-0000-4000-8000-0000000088e1"

// TestHeldBackPassesArmOneWakeRowForTheOlderRun (CHAOS-8880): an older live scheduled run waits with 2 medium units;
// the newer guard run has 3 planned medium units and the medium bucket (cap 2) is empty. Each pass of the newer run
// leaves both free slots to the older run and wakes it; two passes leave ONE dispatch wakeup row for the older run.
func TestHeldBackPassesArmOneWakeRowForTheOlderRun(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: admissionOlderRun, OrgID: guardTestOrg})
		for _, statement := range []struct {
			sql  string
			args []any
		}{
			{`UPDATE public.sync_runs SET triggered_by = 'schedule', mode = 'incremental', status = 'dispatching', created_at = $2 WHERE id = $1::uuid`,
				[]any{admissionOlderRun, now.Add(-time.Hour)}},
			{`UPDATE public.sync_runs SET triggered_by = 'schedule', mode = 'incremental', status = 'dispatching', created_at = $2 WHERE id = $1::uuid`,
				[]any{guardTestRun, now.Add(-time.Minute)}},
		} {
			if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
				t.Fatalf("seed runs: %v", err)
			}
		}
		recent := now.Add(-time.Minute)
		for i := 0; i < 2; i++ {
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
				ID: guardUnitID("00000088e0", i), RunID: admissionOlderRun, OrgID: guardTestOrg, Provider: "github", CostClass: "medium",
				Status: syncRunUnitStatusPlanned, UpdatedAt: &recent,
			})
		}
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
			ID: guardUnitID("00000088e1", 0), RunID: admissionOlderRun, OrgID: guardTestOrg, Provider: "github", CostClass: "medium",
			Status: "success", UpdatedAt: &recent,
		})
		for i := 0; i < 3; i++ {
			insertGuardUnit(t, ctx, pool, guardUnitID("00000088e2", i), "github", "medium", syncRunUnitStatusPlanned, now, nil, nil, nil)
		}

		for pass := 1; pass <= 2; pass++ {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			decision, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now)
			if err != nil {
				_ = tx.Rollback(ctx)
				t.Fatalf("pass %d: authorizeRun: %v", pass, err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if len(decision.cappedUnitIDs) != 3 {
				t.Fatalf("pass %d: capped=%d, want all 3 units of the newer run held back for the older live run", pass, len(decision.cappedUnitIDs))
			}
			if !reflect.DeepEqual(decision.heldBackRunIDs, []string{admissionOlderRun}) {
				t.Fatalf("pass %d: heldBackRunIDs=%v, want [%s]", pass, decision.heldBackRunIDs, admissionOlderRun)
			}
			wakeHeldBackRuns(ctx, pool, decision.heldBackRunIDs, now)
		}

		var rows int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_dispatch_outbox WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run' AND status = 'pending'`,
			admissionOlderRun).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("dispatch wakeup rows for the older run after two held-back passes = %d, want 1", rows)
		}
	})
}
