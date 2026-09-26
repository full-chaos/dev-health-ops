//go:build integration

package syncdispatchruntime

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The dispatch-pass half of CHAOS-6890, executed on real PostgreSQL.
//
// Prod readback 2026-09-26: run dbb92927's one non-terminal unit sat `dispatching`
// for 33 hours on a `delivered` (terminal) worker-outbox delivery -- every dispatch
// pass logged publish_hit_terminal_delivery, delivered nothing, and returned nil,
// and the run's dispatch wakeup was re-armed 7 609 times. (The cause that kept the
// unit unrecoverable is in the reconciler: see unreclaimable_sweep_handler_ran_
// integration_test.go and the strand-repair case; this file covers what the
// dispatch pass itself does with such a unit.)
//
// The chain, each link measured below:
//
//  1. A `dispatching` unit whose updated_at is still inside the stale window is a
//     bucket capacity consumer (countActiveBucketUnits).
//  2. Once it is stale the pass reclaims it (claimUnits stamps updated_at = now) and
//     republishes its delivery; the publish lands on the terminal row
//     (ErrDeliveryAlreadyTerminal), so NOTHING is delivered, the pass counts it as
//     terminalDeliveryPublishes, not as queued, and the unit is a fresh capacity
//     consumer again.
//  3. With the bucket cap filled by such units every `planned` unit is
//     concurrency_capped, so the tail re-arms the run at now + 60 s and returns nil.
//
// The invariant the system exists to reach: units make progress, or the run says
// loudly that they cannot. This test drives the real Dispatch service through several
// stale windows and requires progress OR a loud signal (the ERROR
// dispatch_sync_run.no_progress). Recovery itself belongs to the reconciler's repairs,
// which is why a passing run of this test proves the announcement, not the release.
func TestDeadDeliveryUnitsDoNotWedgeABucketForever(t *testing.T) {
	runDeadDeliveryScenario(t, false)
}

// The control: identical setup, except the occupants' continuations complete
// (their units reach `success`). The same progress-or-loud check must then pass,
// so it is the dead delivery -- not the fixture -- that the test above measures.
func TestFinishedOccupantsFreeTheBucketForPlannedUnits(t *testing.T) {
	runDeadDeliveryScenario(t, true)
}

func runDeadDeliveryScenario(t *testing.T, occupantsFinish bool) {
	t.Helper()
	const (
		bucketCap   = 2
		staleWindow = 16 * time.Minute // just over SYNC_UNIT_DISPATCH_STALE_SECONDS' 900 s default
		cycles      = 4
	)
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", fmt.Sprint(bucketCap))
		if got := pgseed.SyncTransportRoute(ctx, t, pool, "dispatch_sync_run", "river", dispatchRouteGeneration, false, "celery"); got != dispatchRouteGeneration {
			t.Fatalf("dispatch_sync_run route generation = %d, want %d", got, dispatchRouteGeneration)
		}

		clock := time.Now().UTC().Truncate(time.Microsecond)
		var captured bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelInfo}))
		service, err := NewNativeDispatchSyncRunService(pool, logger, &fakeBudgetEstimator{}, mustDispatchProducer(t, pool), &fakeJobRegistry{
			descriptors: map[string]jobruntime.Descriptor{jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river")},
		})
		if err != nil {
			t.Fatal(err)
		}
		service.now = func() time.Time { return clock }

		type run struct {
			n          int
			id, outbox string
			args       DispatchSyncRunArgs
		}
		newRun := func(n int) run {
			id := fmt.Sprintf("00000000-0000-4000-8000-00000000c%03d", n)
			outbox := fmt.Sprintf("00000000-0000-4000-8000-00000000d%03d", n)
			pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: id, OrgID: discoveryTestOrg, IntegrationID: discoveryTestIntegration})
			pgseed.EnsureSyncIntegration(ctx, t, pool, discoveryTestOrg, discoveryTestIntegration, dispatchTestSource)
			pgseed.SyncDispatchOutbox(ctx, t, pool, outbox, id, discoveryTestOrg, "dispatch_sync_run", "dispatched", "river", dispatchRouteGeneration)
			if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at,created_at,updated_at)
VALUES (gen_random_uuid(),$1,$2,$3,1,now(),now(),now())`, id, discoveryTestOrg, discoveryStatusSuccess); err != nil {
				t.Fatal(err)
			}
			return run{n: n, id: id, outbox: outbox, args: DispatchSyncRunArgs{TransportArgs: TransportArgs{
				Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: id,
				DispatchOutbox: outbox, RouteGeneration: dispatchRouteGeneration,
			}}}
		}
		unitID := func(runN, unitN int) string {
			return fmt.Sprintf("00000000-0000-4000-8000-00000000e%01d%02d", runN, unitN)
		}
		addUnit := func(r run, unitN int) string {
			id := unitID(r.n, unitN)
			now := clock
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
				ID: id, RunID: r.id, OrgID: discoveryTestOrg, SourceID: dispatchTestSource,
				Provider: "github", DatasetKey: "commits", CostClass: "heavy", Status: "planned", UpdatedAt: &now,
			})
			return id
		}
		// The relay has published the run's dispatch job: the row is `dispatched`,
		// which is the state Dispatch's own re-arm starts from.
		markPublished := func(r run) {
			if _, err := pool.Exec(ctx, `
UPDATE sync_dispatch_outbox SET status='dispatched', available_at=$2, dispatched_at=$2,
  dispatched_transport='river', dispatched_route_generation=$3,
  claim_token=NULL, claim_expires_at=NULL, claim_transport=NULL, claim_route_generation=NULL
WHERE id=$1::uuid`, r.outbox, clock, dispatchRouteGeneration); err != nil {
				t.Fatal(err)
			}
		}
		dispatch := func(r run) error {
			markPublished(r)
			return service.Dispatch(ctx, r.args)
		}
		unitStatus := func(id string) (status string, updatedAt time.Time) {
			if err := pool.QueryRow(ctx, `SELECT status, updated_at FROM sync_run_units WHERE id=$1::uuid`, id).Scan(&status, &updatedAt); err != nil {
				t.Fatal(err)
			}
			return
		}

		// Two occupant runs: one unit each, claimed by a first pass (so its delivery
		// row is the real, byte-identical envelope), whose delivery then goes
		// terminal -- the shape all eleven prod runs were in.
		occupants := []run{newRun(1), newRun(2)}
		var occupantUnits []string
		for i, r := range occupants {
			occupantUnits = append(occupantUnits, addUnit(r, i))
			if err := dispatch(r); err != nil {
				t.Fatalf("first pass of occupant run %d: %v", i, err)
			}
			if status, _ := unitStatus(occupantUnits[i]); status != "dispatching" {
				t.Fatalf("occupant unit %d is %q after its first pass, want dispatching: the test does not start from the prod shape", i, status)
			}
		}
		if _, err := pool.Exec(ctx, `
UPDATE worker_job_outbox AS o SET status='delivered', river_job_id=127713+numbered.rn, delivered_at=$1
FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn FROM worker_job_outbox) AS numbered
WHERE o.id = numbered.id`, clock); err != nil {
			t.Fatal(err)
		}
		var deliveries int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE status='delivered'`).Scan(&deliveries); err != nil || deliveries != len(occupants) {
			t.Fatalf("delivered outbox rows = %d (err %v), want %d: nothing below tests the prod shape", deliveries, err, len(occupants))
		}

		if occupantsFinish {
			if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status='success', updated_at=$1 WHERE id = ANY($2)`,
				clock, occupantUnits); err != nil {
				t.Fatal(err)
			}
		}

		// A third run in the same bucket, with planned units that only need a slot.
		waiting := newRun(3)
		var waitingUnits []string
		for i := 0; i < 3; i++ {
			waitingUnits = append(waitingUnits, addUnit(waiting, i))
		}

		var dispatchErrors, rearms int
		for cycle := 1; cycle <= cycles; cycle++ {
			clock = clock.Add(staleWindow)
			for _, r := range append(append([]run(nil), occupants...), waiting) {
				if err := dispatch(r); err != nil {
					dispatchErrors++
					t.Logf("cycle %d: Dispatch(%s) returned %v", cycle, r.id, err)
				}
				var status string
				var availableAt time.Time
				if err := pool.QueryRow(ctx, `SELECT status, available_at FROM sync_dispatch_outbox WHERE id=$1::uuid`, r.outbox).Scan(&status, &availableAt); err != nil {
					t.Fatal(err)
				}
				if status == "pending" && availableAt.After(clock) {
					rearms++
				}
			}
		}

		// What the system exists to reach: progress, or a loud signal.
		var claimed int
		for _, id := range waitingUnits {
			if status, _ := unitStatus(id); status != "planned" {
				claimed++
			}
		}
		var newDeliveries int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM worker_job_outbox WHERE status <> 'delivered'`).Scan(&newDeliveries); err != nil {
			t.Fatal(err)
		}
		var occupantsReleased int
		for _, id := range occupantUnits {
			if status, _ := unitStatus(id); status != "dispatching" {
				occupantsReleased++
			}
		}
		var runsFailed int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_runs WHERE id = ANY($1) AND status IN ('failed','partial_failed')`,
			[]string{occupants[0].id, occupants[1].id, waiting.id}).Scan(&runsFailed); err != nil {
			t.Fatal(err)
		}

		progress := claimed > 0 || newDeliveries > 0 || occupantsReleased > 0 || runsFailed > 0
		// The signal must be visible at the production log threshold (Info) AND at ERROR: the handler
		// here drops everything below Info, and the record must carry level ERROR (r1 P3).
		loud := strings.Contains(captured.String(), `"level":"ERROR","msg":"dispatch_sync_run.no_progress"`)
		if !progress && !loud {
			t.Fatalf("after %d stale windows (%v each) the bucket (cap %d) is wedged and nothing said so: "+
				"%d of %d planned units claimed, %d occupant units released, %d new deliveries, %d runs failed, "+
				"%d Dispatch errors and no dispatch_sync_run.no_progress record at level ERROR, yet the passes re-armed their run %d times -- a dead-delivery unit is reclaimed every "+
				"window, republished onto its terminal delivery (nothing delivered) and counted as a fresh capacity consumer, "+
				"so every planned unit stays concurrency_capped while each pass returns nil (CHAOS-6890)",
				cycles, staleWindow, bucketCap, claimed, len(waitingUnits), occupantsReleased, newDeliveries, runsFailed,
				dispatchErrors, rearms)
		}
	})
}

// The no-progress ERROR must not fire on a pass that DID deliver something: a run
// with one unit behind a dead delivery and one healthy unit is making progress, and
// an ERROR on every such pass would train operators to ignore the line.
func TestDispatchDoesNotReportNoProgressWhenAnotherUnitWasQueued(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedDispatchRoute(t, ctx, pool)
		markReferenceDiscoverySucceeded(t, ctx, pool)
		now := pgNow()
		deadUnit := "00000000-0000-4000-8000-0000000000fd"
		healthyUnit := "00000000-0000-4000-8000-0000000000fc"
		insert := func(id, source string) {
			if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github','commits',$4,'planned',$5,(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'rest_core','incremental',0,now())`,
				id, discoveryTestOrg, discoveryTestRun, source, now); err != nil {
				t.Fatal(err)
			}
		}
		insert(deadUnit, dispatchTestSource)
		if err := newTestDispatchService(t, pool).Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("first Dispatch: %v", err)
		}
		if _, err := pool.Exec(ctx, `UPDATE worker_job_outbox SET status='delivered', river_job_id=127714, delivered_at=$1
WHERE dedupe_key=$2`, now, "sync.provider_unit:"+deadUnit); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status='planned', updated_at=$2 WHERE id=$1::uuid`, deadUnit, now); err != nil {
			t.Fatal(err)
		}
		insert(healthyUnit, dispatchTestSourceB)

		var captured bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelInfo}))
		service, err := NewNativeDispatchSyncRunService(pool, logger, &fakeBudgetEstimator{}, mustDispatchProducer(t, pool), &fakeJobRegistry{
			descriptors: map[string]jobruntime.Descriptor{jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river")},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := service.Dispatch(ctx, dispatchTestArgs()); err != nil {
			t.Fatalf("second Dispatch: %v", err)
		}
		logged := captured.String()
		if !strings.Contains(logged, "dispatch_sync_run.terminal_delivery_publishes") || !strings.Contains(logged, `"queued_units":1`) {
			t.Fatalf("premise: the pass must hit the terminal delivery AND queue the healthy unit: %s", logged)
		}
		if strings.Contains(logged, "dispatch_sync_run.no_progress") {
			t.Fatalf("a pass that queued a unit reported no progress: %s", logged)
		}
	})
}
