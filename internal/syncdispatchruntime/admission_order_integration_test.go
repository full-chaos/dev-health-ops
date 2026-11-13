//go:build integration

package syncdispatchruntime

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
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
		// The terminal write arms the run's finalize row in the same transaction; that row is the terminal sign the
		// liveness probe reads.
		armTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := syncrunrollup.ArmFinalize(ctx, armTx, admissionOlderRun, guardTestOrg, recent); err != nil {
			_ = armTx.Rollback(ctx)
			t.Fatalf("arm finalize: %v", err)
		}
		if err := armTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
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

// TestOlderRunsInBucketReadUsesTheActiveRunIndex (CHAOS-8880): the older-run read runs once per bucket per dispatch
// pass, under the bucket lock. It must not read the org's finished history: 3,000 finished runs with 10 units each,
// one open run deciding, and no Seq Scan on sync_runs or sync_run_units.
func TestOlderRunsInBucketReadUsesTheActiveRunIndex(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		insertGuardUnit(t, ctx, pool, guardUnitID("00000088f0", 0), "github", "medium", syncRunUnitStatusPlanned, now, nil, nil, nil)
		for _, statement := range []string{
			`INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
			 SELECT md5('history-8880-' || n)::uuid, $1, (SELECT integration_id FROM public.sync_runs WHERE id = $2::uuid),
				'schedule', 'incremental', 'success', 10, 10, 0, now() - make_interval(hours => n + 2)
			 FROM generate_series(1, 3000) AS n`,
			`INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, created_at, updated_at)
			 SELECT md5('history-unit-8880-' || run.id::text || '-' || k)::uuid, run.org_id, run.id, run.integration_id,
				(SELECT source_id FROM public.sync_run_units WHERE sync_run_id = $2::uuid LIMIT 1),
				'github', 'dataset-' || k, 'medium', 'incremental', 'success', 1, run.created_at, run.created_at
			 FROM public.sync_runs AS run CROSS JOIN generate_series(1, 10) AS k
			 WHERE run.org_id = $1 AND run.status = 'success'`,
		} {
			if _, err := pool.Exec(ctx, statement, guardTestOrg, guardTestRun); err != nil {
				t.Fatalf("seed history: %v", err)
			}
		}
		if _, err := pool.Exec(ctx, `ANALYZE`); err != nil {
			t.Fatal(err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		self, err := readRunOrder(ctx, tx, guardTestRun)
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		bucket := dispatchBucket{orgID: guardTestOrg, provider: "github", costClass: "medium"}
		older, err := readOlderRuns(ctx, tx, bucket, self, now)
		_ = tx.Rollback(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(older) != 0 {
			t.Fatalf("older runs on a finished history = %+v, want none", older)
		}

		rows, err := pool.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS) "+olderRunsInBucketSQL,
			bucket.orgID, bucket.provider, bucket.costClass, self.createdAt, self.id, self.backfill, now,
			concurrencyCapForCostClass(bucket.costClass), admissionLivenessBound().Seconds(), olderRunsReadLimit, olderRunWaitingScanLimit)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(line + "\n")
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		t.Logf("older runs in bucket read, 3000 finished runs x 10 units:\n%s", plan.String())
		for _, table := range []string{"sync_runs", "sync_run_units"} {
			if strings.Contains(plan.String(), "Seq Scan on "+table) {
				t.Fatalf("the older-run read scans %s sequentially:\n%s", table, plan.String())
			}
		}
	})
}
