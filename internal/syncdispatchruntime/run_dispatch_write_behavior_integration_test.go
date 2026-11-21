//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The finalize write (writeFinalizeRun) sets sync_runs.status from a Go value (aggregateRunStatus or
// zeroUnitTerminalFor), not from its SQL. TestRunFinalizeWriteKeepsATerminalRunTerminal pins that a finalize
// pass over a terminal run whose units are all closed leaves it terminal and leaves the units as they were.
//
// The dispatch pass sets sync_runs.status = 'dispatching' with no SQL check of the old run status
// (native_dispatch_sync_run_service.go, the riverQueued > 0 write). It is safe only through the unit
// rule: the write runs only when this pass claimed a unit, every claim reads a non-failed old unit
// status, and a terminal run has only closed units. These cells pin both halves on the real service:
// a pass over a terminal run leaves the run status and every unit row as they were, and a pass over
// an open run leaves its failed unit as it was while it still claims the planned sibling.

const (
	behaviorFailedUnit  = "00000000-0000-4000-8000-000000144441"
	behaviorClosedUnit  = "00000000-0000-4000-8000-000000144442"
	behaviorPlannedUnit = "00000000-0000-4000-8000-000000144443"
)

func insertBehaviorUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, status string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,error,result,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,$5::text,
        CASE WHEN $5::text = 'failed' THEN 'provider_unit_exhausted' END,
        CASE WHEN $5::text = 'failed' THEN '{"error_category":"provider_unit_exhausted"}'::json END,
        now() - interval '1 hour',(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'rest_core','incremental',
        CASE WHEN $5::text = 'planned' THEN 0 ELSE 1 END, now() - interval '1 hour')`,
		id, discoveryTestOrg, discoveryTestRun, dispatchTestSource, status); err != nil {
		t.Fatal(err)
	}
}

func behaviorUnitRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var row string
	if err := pool.QueryRow(ctx, `SELECT row_to_json(u)::text FROM sync_run_units AS u WHERE u.id = $1::uuid`, id).Scan(&row); err != nil {
		t.Fatal(err)
	}
	return row
}

func behaviorRunStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id = $1::uuid`, discoveryTestRun).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestRunDispatchWriteLeavesATerminalRunAndItsClosedUnitsUnchanged(t *testing.T) {
	for _, terminal := range []string{syncRunStatusSuccess, syncRunStatusPartialFailed, syncRunStatusFailed} {
		t.Run(terminal, func(t *testing.T) {
			withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				seedDispatchRoute(t, ctx, pool)
				markReferenceDiscoverySucceeded(t, ctx, pool)
				insertBehaviorUnit(t, ctx, pool, behaviorFailedUnit, syncRunUnitStatusFailed)
				insertBehaviorUnit(t, ctx, pool, behaviorClosedUnit, syncRunUnitStatusSuccess)
				if _, err := pool.Exec(ctx, `UPDATE sync_runs SET status = $2, total_units = 2, completed_units = 1, failed_units = 1, completed_at = now() WHERE id = $1::uuid`,
					discoveryTestRun, terminal); err != nil {
					t.Fatal(err)
				}
				failedBefore := behaviorUnitRow(t, ctx, pool, behaviorFailedUnit)
				closedBefore := behaviorUnitRow(t, ctx, pool, behaviorClosedUnit)

				if err := newTestDispatchService(t, pool).Dispatch(ctx, dispatchTestArgs()); err != nil {
					t.Fatalf("Dispatch: %v", err)
				}

				if got := behaviorRunStatus(t, ctx, pool); got != terminal {
					t.Errorf("run status after the dispatch pass = %q, want %q: a terminal run was moved back", got, terminal)
				}
				if got := behaviorUnitRow(t, ctx, pool, behaviorFailedUnit); got != failedBefore {
					t.Errorf("failed unit changed by the dispatch pass:\nbefore %s\nafter  %s", failedBefore, got)
				}
				if got := behaviorUnitRow(t, ctx, pool, behaviorClosedUnit); got != closedBefore {
					t.Errorf("success unit changed by the dispatch pass:\nbefore %s\nafter  %s", closedBefore, got)
				}
			})
		})
	}
}

func TestRunDispatchWriteLeavesAFailedUnitOfAnOpenRunUnchanged(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		insertBehaviorUnit(t, ctx, pool, behaviorFailedUnit, syncRunUnitStatusFailed)
		insertBehaviorUnit(t, ctx, pool, behaviorPlannedUnit, syncRunUnitStatusPlanned)
		if _, err := pool.Exec(ctx, `UPDATE sync_runs SET status = $2, total_units = 2, failed_units = 1 WHERE id = $1::uuid`,
			discoveryTestRun, syncRunStatusDispatching); err != nil {
			t.Fatal(err)
		}
		failedBefore := behaviorUnitRow(t, ctx, pool, behaviorFailedUnit)

		if err := newTestDispatchService(t, pool).Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}

		if got := behaviorUnitRow(t, ctx, pool, behaviorFailedUnit); got != failedBefore {
			t.Errorf("failed unit of an open run changed by the dispatch pass:\nbefore %s\nafter  %s", failedBefore, got)
		}
		// Control: the pass did claim work, so the unchanged failed unit is not an idle pass.
		var planned string
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id = $1::uuid`, behaviorPlannedUnit).Scan(&planned); err != nil {
			t.Fatal(err)
		}
		if planned != syncRunUnitStatusDispatching {
			t.Errorf("planned sibling = %q after the pass, want %q (the pass must claim it)", planned, syncRunUnitStatusDispatching)
		}
		if got := behaviorRunStatus(t, ctx, pool); got != syncRunStatusDispatching {
			t.Errorf("open run status = %q, want %q", got, syncRunStatusDispatching)
		}
	})
}

const finalizeBehaviorFailedUnit = "00000000-0000-4000-8000-000000144444"

func TestRunFinalizeWriteKeepsATerminalRunTerminal(t *testing.T) {
	terminalStatuses := map[string]bool{syncRunStatusSuccess: true, syncRunStatusPartialFailed: true, syncRunStatusFailed: true}
	for _, terminal := range []string{syncRunStatusSuccess, syncRunStatusPartialFailed, syncRunStatusFailed} {
		t.Run(terminal, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			instance := pgschema.Start(ctx, t)
			defer instance.Close(context.Background())
			pool, err := pgxpool.New(ctx, instance.URI)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			seedFinalizeRoute(t, ctx, pool)
			insertFinalizeRun(t, ctx, pool, 2, "", "")
			pgseed.EnsureSyncIntegration(ctx, t, pool, finalizeTestOrg, finalizeTestIntegration, finalizeTestSource)
			for id, status := range map[string]string{finalizeTestUnit: syncRunUnitStatusSuccess, finalizeBehaviorFailedUnit: syncRunUnitStatusFailed} {
				pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
					ID: id, RunID: finalizeTestRun, OrgID: finalizeTestOrg, IntegrationID: finalizeTestIntegration,
					SourceID: finalizeTestSource, Status: status, ResultJSON: `{"error_category":"provider_unit_exhausted"}`,
				})
			}
			if _, err := pool.Exec(ctx, `UPDATE sync_runs SET status = $2, completed_units = 1, failed_units = 1, completed_at = now() WHERE id = $1::uuid`,
				finalizeTestRun, terminal); err != nil {
				t.Fatal(err)
			}
			var unitsBefore string
			if err := pool.QueryRow(ctx, `SELECT json_agg(u ORDER BY u.id)::text FROM sync_run_units AS u WHERE u.sync_run_id = $1::uuid`, finalizeTestRun).Scan(&unitsBefore); err != nil {
				t.Fatal(err)
			}

			service, err := NewNativeFinalizeSyncRunService(pool, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
				t.Fatalf("Finalize: %v", err)
			}

			var status, unitsAfter string
			if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id = $1::uuid`, finalizeTestRun).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if !terminalStatuses[status] {
				t.Errorf("run status after the finalize pass = %q, was %q: a terminal run was moved back", status, terminal)
			}
			if err := pool.QueryRow(ctx, `SELECT json_agg(u ORDER BY u.id)::text FROM sync_run_units AS u WHERE u.sync_run_id = $1::uuid`, finalizeTestRun).Scan(&unitsAfter); err != nil {
				t.Fatal(err)
			}
			if unitsAfter != unitsBefore {
				t.Errorf("units changed by the finalize pass:\nbefore %s\nafter  %s", unitsBefore, unitsAfter)
			}
		})
	}
}
