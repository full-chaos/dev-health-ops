//go:build integration

package syncreconciler

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/jackc/pgx/v5/pgxpool"
)

// operatorCancelSnapshot is every row of the live run a repair could re-arm or reopen.
func operatorCancelSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(ctx, `
SELECT json_build_object(
  'run', (SELECT row_to_json(r) FROM public.sync_runs AS r WHERE r.id = $1::uuid),
  'units', (SELECT json_agg(u ORDER BY u.id) FROM public.sync_run_units AS u WHERE u.sync_run_id = $1::uuid),
  'dispatch_outbox', (SELECT json_agg(o ORDER BY o.kind) FROM public.sync_dispatch_outbox AS o WHERE o.sync_run_id = $1::uuid),
  'worker_outbox', (SELECT json_agg(w ORDER BY w.dedupe_key) FROM public.worker_job_outbox AS w),
  'river_jobs', (SELECT json_agg(json_build_object('id', j.id, 'state', j.state) ORDER BY j.id) FROM river.river_job AS j)
)::text`, liveRunID).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// After `dho workers sync-runs cancel` closes a run, no reconciler pass re-arms or reopens it:
// the orphaned-unit repair, the strand repair, the lease repair, the materializer, the
// unreclaimable sweep and the terminal-delivery repair each leave every row of the run as the
// cancel left it. The control first proves the same seed IS re-armed when the run is not cancelled.
func TestNoReconcilerPassRearmsAnOperatorCancelledRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	h := startOrphanHarness(t, ctx)
	now := time.Date(2026, time.September, 9, 3, 0, 0, 0, time.UTC)

	h.seed(t, ctx, now, liveShapeDefault(now))
	if result := h.step(t, ctx, now); result.ReArmed != 1 {
		t.Fatalf("control: the uncancelled seed re-armed %d, want 1; the passes below would prove nothing", result.ReArmed)
	}

	for _, riverState := range []string{"missing", "cancelled", "running"} {
		t.Run("river_"+riverState, func(t *testing.T) {
			// h.seed clears the run's own tables; the rows a previous cancel added reference the run.
			if _, err := h.admin.Exec(ctx, `
				DELETE FROM public.sync_dispatch_outbox;
				DELETE FROM public.sync_run_reference_discoveries;`); err != nil {
				t.Fatal(err)
			}
			shape := liveShapeDefault(now)
			shape.riverState = riverState
			h.seed(t, ctx, now, shape)
			outcome, err := syncdispatchruntime.CancelSyncRun(ctx, h.admin, syncdispatchruntime.CancelSyncRunRequest{
				OrgID: liveOrgID, SyncRunID: liveRunID, ReasonCode: "operator_test", Now: now,
			})
			if err != nil || outcome.Outcome != syncdispatchruntime.CancelOutcomeCancelled || outcome.UnitsFailed != 1 {
				t.Fatalf("CancelSyncRun: err=%v outcome=%+v", err, outcome)
			}
			before := operatorCancelSnapshot(t, ctx, h.admin)

			if result := h.step(t, ctx, now); result.ReArmed != 0 {
				t.Fatalf("orphaned-unit repair re-armed %d deliveries of a cancelled run", result.ReArmed)
			}
			strand, err := joboutbox.NewStrandRepair(h.admin, h.admin, "river")
			if err != nil {
				t.Fatal(err)
			}
			if result, err := strand.Step(ctx, now, 10); err != nil || result.Rearmed != 0 {
				t.Fatalf("strand repair: rearmed=%d err=%v", result.Rearmed, err)
			}
			leases, err := NewLeaseRepair(h.admin)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := leases.Step(ctx, now, 10); err != nil {
				t.Fatalf("lease repair: %v", err)
			}
			materializer, err := NewMaterializer(h.admin)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := materializer.Step(ctx, now, now.Add(-15*time.Minute), 10); err != nil {
				t.Fatalf("materializer: %v", err)
			}
			sweep, err := NewUnreclaimableSweep(h.admin, h.admin, h.admin, "river", UnreclaimableSweepConfig{
				Age: DefaultUnreclaimableAge, Idle: DefaultUnreclaimableIdle, Mode: SweepModeActive,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sweep.Step(ctx, now, 10); err != nil {
				t.Fatalf("unreclaimable sweep: %v", err)
			}
			delivery, err := NewTerminalDeliveryRepair(h.admin, h.admin, "river")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := delivery.Step(ctx, now, 10); err != nil {
				t.Fatalf("terminal delivery repair: %v", err)
			}

			if after := operatorCancelSnapshot(t, ctx, h.admin); after != before {
				t.Fatalf("a reconciler pass changed the cancelled run:\nbefore %s\nafter  %s", before, after)
			}
		})
	}
}
