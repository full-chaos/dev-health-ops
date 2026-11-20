//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncbucket"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestB4UnitCommitReservesTheFreedSlotForTheOldestWaiter (CHAOS-14250 plan 4, N1 on the real writer): a unit's
// terminal commit (Complete, and Fail) takes the bucket lock first and, in the same transaction, reserves the slot it
// frees for the bucket's oldest eligible waiter, so the reservation is visible to a pass that starts right after the
// commit, before any wake or sweep runs.
func TestB4UnitCommitReservesTheFreedSlotForTheOldestWaiter(t *testing.T) {
	for _, outcome := range []string{"complete", "fail"} {
		t.Run(outcome, func(t *testing.T) {
			withDispatchWakeFixture(t, func(ctx context.Context, pool *pgxpool.Pool, repository *PostgresRepository) {
				now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
				const waiterRun = "55555555-5555-4555-8555-555555555555"
				for _, statement := range []struct {
					sql  string
					args []any
				}{
					{`INSERT INTO public.sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
					  VALUES ($1, 'org-acme', $2, 'schedule', 'incremental', 'running', 1, 0, 0, $3)`, []any{waiterRun, firstIntegrationID, now.Add(-48 * time.Hour)}},
					{`INSERT INTO public.sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at)
					  VALUES ('66666666-6666-4666-8666-666666666666', $1, 'org-acme', 'success', 1, $2, $2, $2)`, []any{waiterRun, now}},
					{`INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode,
						since_at, before_at, status, processor_flags, updated_at, created_at, attempts)
					  SELECT '77777777-7777-4777-8777-777777777777', 'org-acme', $1, integration_id, source_id, provider, dataset_key, cost_class, mode,
						since_at, before_at, 'planned', processor_flags, $2, $2, 0
					  FROM public.sync_run_units WHERE id = $3`, []any{waiterRun, now, firstUnitID}},
				} {
					if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
						t.Fatalf("seed: %v", err)
					}
				}
				claim := claimFixtureUnit(t, ctx, repository, firstUnitID, now)
				before := syncbucket.LockWaitCount(syncbucket.LockWaitPathProviderUnit)
				var err error
				if outcome == "complete" {
					err = repository.Complete(ctx, claim, map[string]any{"records": 1}, nil, now, now.Add(time.Second))
				} else {
					err = repository.Fail(ctx, claim, "provider_error", now, now.Add(time.Second))
				}
				if err != nil {
					t.Fatal(err)
				}
				var reserved int
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.sync_dispatch_bucket_reservations WHERE sync_run_id = $1::uuid`, waiterRun).Scan(&reserved); err != nil {
					t.Fatal(err)
				}
				waits := syncbucket.LockWaitCount(syncbucket.LockWaitPathProviderUnit) - before
				t.Logf("%s: the waiting run holds %d reservation(s) after the commit; commit-path lock waits observed %d", outcome, reserved, waits)
				if reserved != 1 || waits != 1 {
					t.Fatalf("%s: the unit commit reserved %d slot(s) for the oldest waiter (want 1) and observed %d lock waits (want 1)", outcome, reserved, waits)
				}
			})
		})
	}
}
