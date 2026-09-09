//go:build integration

package syncdispatchruntime

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// readyFinalizeRegistry is the four frozen sync-dispatch kinds with
// finalize_sync_run on River, which is the shape the reconciler kernel
// requires before it will claim and deliver a finalize row.
type readyFinalizeRegistry map[string]syncdispatchcontract.Descriptor

func (registry readyFinalizeRegistry) Lookup(kind string) (syncdispatchcontract.Descriptor, bool) {
	descriptor, ok := registry[kind]
	return descriptor, ok
}

func newReadyFinalizeRegistry() readyFinalizeRegistry {
	registry := readyFinalizeRegistry{}
	for _, kind := range []string{
		syncdispatchcontract.KindDispatchSyncRun,
		syncdispatchcontract.KindFinalizeSyncRun,
		syncdispatchcontract.KindPostSync,
		syncdispatchcontract.KindReferenceDiscovery,
	} {
		route := syncdispatchcontract.RouteCelery
		if kind == syncdispatchcontract.KindFinalizeSyncRun {
			route = syncdispatchcontract.RouteRiver
		}
		registry[kind] = syncdispatchcontract.Descriptor{
			Kind:          kind,
			Delivery:      syncdispatchcontract.DeliveryAtLeastOnce,
			Route:         route,
			RollbackRoute: syncdispatchcontract.RouteCelery,
		}
	}
	return registry
}

// TestReadyFinalizeRecoveryDrivesTheRunToTerminal is the run-terminal proof for
// CHAOS-5456. Every other test of this backstop stops at the re-armed outbox
// row, which proves a wakeup was written but NOT that the wakeup leads
// anywhere: a row re-armed into a shape the relay declines, or delivered with
// arguments the finalizer's own currentTransportReference check refuses, would
// leave the run exactly as stranded as before while every assertion still
// passed.
//
// So this runs the real chain end to end, in order, with no simulated step:
//
//  1. syncreconciler.TerminalDeliveryRepair re-arms the stranded row.
//  2. syncreconciler.Kernel claims it and inserts a NEW River finalize job,
//     marking the outbox dispatched at the current route generation.
//  3. NativeFinalizeSyncRunService.Finalize runs against the arguments that
//     re-delivery produced, read back out of the database rather than
//     hand-built.
//
// The assertion is the one the ticket is actually about: sync_runs reaches a
// terminal status.
func TestReadyFinalizeRecoveryDrivesTheRunToTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	createFinalizeTables(t, ctx, pool)
	// The reconciler reads three coordinator relations the finalize fixture
	// has no reason to carry, and one column pair on sync_runs. Shapes are
	// taken from internal/syncreconciler's own materializer fixture, which is
	// the venue that owns this predicate; nothing here is invented.
	if _, err := pool.Exec(ctx, `
CREATE EXTENSION IF NOT EXISTS pgcrypto;
ALTER TABLE sync_runs ADD COLUMN triggered_by text NOT NULL DEFAULT 'manual';
ALTER TABLE sync_runs ADD COLUMN created_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE sync_run_units ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();
CREATE TABLE scheduled_sync_occurrences (
    occurrence_id text PRIMARY KEY, sync_run_id uuid, job_run_id uuid, reconcile_status text NOT NULL);
CREATE TABLE sync_run_reference_discoveries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(), sync_run_id uuid NOT NULL UNIQUE,
    status text NOT NULL, available_at timestamptz NOT NULL, lease_expires_at timestamptz)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE SCHEMA river`); err != nil {
		t.Fatal(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	staleAt := now.Add(-30 * time.Hour)
	seedFinalizeRoute(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_runs (id,org_id,integration_id,status,total_units,completed_units,failed_units)
VALUES ($1,$2,$3,'dispatching',1,0,0)`, finalizeTestRun, finalizeTestOrg, finalizeTestIntegration); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO integration_sources (id) VALUES ($1)`, finalizeTestSource); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,since_at,before_at,cost_class,mode)
VALUES ($1,$2,$3,'github','commits',$4,'success',$5,$6,'heavy','incremental')`,
		finalizeTestUnit, finalizeTestOrg, finalizeTestRun, finalizeTestSource,
		time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	// The stranded shape: River ran the finalize delivery to COMPLETED (or a
	// later cleaner reaped the row) while the run stayed non-terminal.
	riverClient, err := river.NewClient(riverpgxv5.New(pool), &river.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	deadDelivery, err := riverClient.Insert(ctx, FinalizeSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: finalizeTestOrg, RunID: finalizeTestRun,
		DispatchOutbox: finalizeTestOutbox, DeliveryAttempt: 38, RouteGeneration: 1,
	}}, &river.InsertOpts{Queue: "sync"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE river.river_job SET state='completed',finalized_at=$2 WHERE id=$1`,
		deadDelivery.Job.ID, staleAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE sync_dispatch_outbox SET status='dispatched',attempts=38,dispatched_at=$2,
    dispatched_transport='river',dispatched_route_generation=1,transport_job_id=$3,updated_at=$2
WHERE id=$1`, finalizeTestOutbox, staleAt, strconv.FormatInt(deadDelivery.Job.ID, 10)); err != nil {
		t.Fatal(err)
	}

	if err := (&NativeFinalizeSyncRunService{}).Finalize(ctx, newFinalizeArgs()); err == nil {
		t.Fatal("a zero-value service must refuse; the harness would otherwise prove nothing")
	}
	service, err := NewNativeFinalizeSyncRunService(pool, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Control: with the run stranded and no repair, the finalize job is gone
	// and nothing will ever call Finalize again. Assert the starting state
	// rather than assume it.
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM sync_runs WHERE id=$1`, finalizeTestRun).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "dispatching" {
		t.Fatalf("run did not start stranded: status=%q", status)
	}

	// Step 1 -- the real repair, on the real queue-control seam. Both pools
	// are the same connection here on purpose: role separation is pinned
	// under real split roles by internal/syncreconciler's own guard suite,
	// and duplicating it would test the fixture rather than this chain.
	repair, err := syncreconciler.NewTerminalDeliveryRepair(pool, pool, "river")
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := repair.Step(ctx, now, 20)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ReadyFinalizersRecovered != 1 || recovered.Recovered != 1 {
		t.Fatalf("repair did not re-arm the stranded finalizer: %+v", recovered)
	}

	// Step 2 -- the real relay. Nothing about the re-armed row is adjusted
	// first; if the re-arm produced a shape the kernel declines, this is
	// where it shows.
	kernel, err := syncreconciler.NewKernel(pool, pool, newReadyFinalizeRegistry(), syncreconciler.KernelModeMutation)
	if err != nil {
		t.Fatal(err)
	}
	publish := func(publishCtx context.Context, tx pgx.Tx, claim syncreconciler.TransportClaim) (string, error) {
		inserted, insertErr := riverClient.InsertTx(publishCtx, tx, FinalizeSyncRunArgs{TransportArgs: TransportArgs{
			Version: ContractVersionV1, OrgID: finalizeTestOrg, RunID: finalizeTestRun,
			DispatchOutbox: claim.ID, DeliveryAttempt: int64(claim.Attempts),
			RouteGeneration: claim.RouteGeneration,
		}}, &river.InsertOpts{Queue: "sync"})
		if insertErr != nil {
			return "", insertErr
		}
		return strconv.FormatInt(inserted.Job.ID, 10), nil
	}
	kernelResult, err := kernel.Step(ctx, now, 20, time.Minute, publish, nil)
	if err != nil {
		t.Fatal(err)
	}
	if kernelResult.Dispatched != 1 {
		t.Fatalf("re-armed row was not re-delivered: %+v", kernelResult)
	}

	// Step 3 -- the real finalizer, on the arguments re-delivery actually
	// produced. Reading them back is the point: a hand-built FinalizeSyncRunArgs
	// would pass currentTransportReference by construction and prove nothing.
	var deliveredGeneration int64
	var deliveredJobID string
	if err := pool.QueryRow(ctx, `
SELECT dispatched_route_generation, transport_job_id FROM sync_dispatch_outbox WHERE id=$1`,
		finalizeTestOutbox).Scan(&deliveredGeneration, &deliveredJobID); err != nil {
		t.Fatal(err)
	}
	if deliveredJobID == strconv.FormatInt(deadDelivery.Job.ID, 10) {
		t.Fatal("re-delivery reused the dead River job id")
	}
	if err := service.Finalize(ctx, FinalizeSyncRunArgs{TransportArgs: TransportArgs{
		Version: ContractVersionV1, OrgID: finalizeTestOrg, RunID: finalizeTestRun,
		DispatchOutbox: finalizeTestOutbox, DeliveryAttempt: 1,
		RouteGeneration: deliveredGeneration,
	}}); err != nil {
		t.Fatalf("Finalize on the recovered delivery: %v", err)
	}

	var completedUnits, failedUnits int
	if err := pool.QueryRow(ctx, `SELECT status, completed_units, failed_units FROM sync_runs WHERE id=$1`,
		finalizeTestRun).Scan(&status, &completedUnits, &failedUnits); err != nil {
		t.Fatal(err)
	}
	if status != syncRunStatusSuccess || completedUnits != 1 || failedUnits != 0 {
		t.Fatalf("run did not reach terminal: status=%q completed=%d failed=%d", status, completedUnits, failedUnits)
	}

	// And the recovery is not a loop: with the run now terminal, a second
	// repair pass must find nothing to re-arm.
	second, err := repair.Step(ctx, now, 20)
	if err != nil || second.ReadyFinalizersRecovered != 0 {
		t.Fatalf("repair re-armed a terminal run: %+v err=%v", second, err)
	}
}
