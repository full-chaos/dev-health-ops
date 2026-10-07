//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The post-sync fan-out reads the raw rows its sync run wrote (the touched-day
// record). That read is complete only if no unit of the run can still write
// when the post_sync job exists. So: while one unit of the run is in any
// state other than success or failed, Finalize writes no post_sync dispatch
// and leaves the run open; when the last unit is terminal, it writes exactly
// one.
func TestNativeFinalizeSyncRunWritesNoPostSyncWhileAUnitIsNotTerminal(t *testing.T) {
	for _, last := range []string{syncRunUnitStatusSuccess, syncRunUnitStatusFailed} {
		t.Run("last unit ends "+last, func(t *testing.T) {
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
			createFinalizeTables(t, ctx, pool)
			seedFinalizeRoute(t, ctx, pool)
			insertFinalizeRun(t, ctx, pool, 2, "", "")
			pgseed.EnsureSyncIntegration(ctx, t, pool, finalizeTestOrg, finalizeTestIntegration, finalizeTestSource)
			day := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
			const openUnit = "00000000-0000-4000-8000-0000000000e6"
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
				ID: finalizeTestUnit, RunID: finalizeTestRun, OrgID: finalizeTestOrg, IntegrationID: finalizeTestIntegration,
				SourceID: finalizeTestSource, DatasetKey: "work-items", Status: syncRunUnitStatusSuccess, SinceAt: &day, BeforeAt: &day,
			})
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
				ID: openUnit, RunID: finalizeTestRun, OrgID: finalizeTestOrg, IntegrationID: finalizeTestIntegration,
				SourceID: finalizeTestSource, DatasetKey: "commits", Status: syncRunUnitStatusPlanned, SinceAt: &day, BeforeAt: &day,
			})
			service, err := NewNativeFinalizeSyncRunService(pool, nil)
			if err != nil {
				t.Fatal(err)
			}
			postSync := func() (ledger, outbox int, runStatus string) {
				t.Helper()
				if err := pool.QueryRow(ctx, `
SELECT (SELECT count(*) FROM sync_run_post_dispatches WHERE sync_run_id = $1),
       (SELECT count(*) FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = 'post_sync'),
       (SELECT status FROM sync_runs WHERE id = $1)`, finalizeTestRun).Scan(&ledger, &outbox, &runStatus); err != nil {
					t.Fatal(err)
				}
				return ledger, outbox, runStatus
			}
			setUnit := func(status string) {
				t.Helper()
				tag, err := pool.Exec(ctx, `UPDATE sync_run_units SET status = $1 WHERE id = $2`, status, openUnit)
				if err != nil || tag.RowsAffected() != 1 {
					t.Fatalf("set the unit to %s: rows=%d err=%v", status, tag.RowsAffected(), err)
				}
			}

			for _, status := range []string{
				syncRunUnitStatusPlanned, syncRunUnitStatusDispatching, syncRunUnitStatusRunning, syncRunUnitStatusRetrying,
			} {
				setUnit(status)
				if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
					t.Fatalf("Finalize with a %s unit: %v", status, err)
				}
				if ledger, outbox, runStatus := postSync(); ledger != 0 || outbox != 0 || runStatus != "dispatching" {
					t.Fatalf("a %s unit: post-sync ledger rows=%d outbox rows=%d run status=%q; want 0, 0 and the run still open",
						status, ledger, outbox, runStatus)
				}
			}

			setUnit(last)
			if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
				t.Fatalf("Finalize with every unit terminal: %v", err)
			}
			if ledger, outbox, runStatus := postSync(); ledger != 1 || outbox != 1 || runStatus == "dispatching" {
				t.Fatalf("every unit terminal: post-sync ledger rows=%d outbox rows=%d run status=%q; want 1, 1 and a finished run",
					ledger, outbox, runStatus)
			}
		})
	}
}
