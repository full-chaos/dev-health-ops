//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-14250 cells. A simulated clock drives the real Dispatch service and the real bucket sweep: a relay stand-in
// delivers the earliest due dispatch_sync_run row, a worker stand-in leases each claimed unit and ends it after
// unitTime with providersync Complete's statements (terminal write + ArmFinalize in one tx, then the release after
// commit), and a River stand-in (rqSweeps) keeps one pending sweep per bucket with sync.bucket_sweep's semantics:
// an enqueue of a bucket with no pending sweep makes one due now, an enqueue of a sweep waiting on its re-arm makes it
// due now, and a sweep that ends with an eligible waiter re-arms itself. The River uniqueness itself is pinned on a
// real River schema in bucket_sweep_river_integration_test.go.

const (
	rqOrg         = "00000000-0000-4000-8000-000000142501"
	rqIntegration = "00000000-0000-4000-8000-000000142502"
	rqSource      = "00000000-0000-4000-8000-000000142503"
)

type rqUnitEnd struct {
	unitID, runID, costClass string
	at                       time.Time
}

type rqPass struct {
	runID   string
	at      time.Time
	claimed int
}

type rqSweepRun struct {
	bucket string
	at     time.Time
	woken  []string
}

// rqSweeps stands in for River's one-pending-sweep-per-bucket; it implements syncrunrollup.BucketSweepEnqueuer.
type rqSweeps struct {
	now      func() time.Time
	due      map[string]time.Time
	enqueues int
}

func (sweeps *rqSweeps) EnqueueBucketSweep(_ context.Context, orgID, provider, costClass string) error {
	key := orgID + "|" + provider + "|" + costClass
	sweeps.enqueues++
	now := sweeps.now()
	if due, ok := sweeps.due[key]; !ok || due.After(now) {
		sweeps.due[key] = now
	}
	return nil
}

func (sweeps *rqSweeps) EnsureBucketSweep(_ context.Context, orgID, provider, costClass string) error {
	key := orgID + "|" + provider + "|" + costClass
	if _, ok := sweeps.due[key]; !ok {
		sweeps.due[key] = sweeps.now()
	}
	return nil
}

type rqSim struct {
	t          *testing.T
	ctx        context.Context
	pool       *pgxpool.Pool
	service    *NativeDispatchSyncRunService
	sweeps     *rqSweeps
	now        time.Time
	unitTime   time.Duration
	running    []rqUnitEnd
	passes     []rqPass
	sweepRuns  []rqSweepRun
	firstClaim map[string]time.Time
	// loseReleases drops the post-commit release of the next unit ends (a crash between commit and release).
	loseReleases int
}

func withRearmQueuePool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgseed.Org(ctx, t, pool, rqOrg, "community")
	if got := pgseed.SyncTransportRoute(ctx, t, pool, "dispatch_sync_run", "river", dispatchRouteGeneration, false, "celery"); got != dispatchRouteGeneration {
		t.Fatalf("dispatch_sync_run route generation = %d, want %d", got, dispatchRouteGeneration)
	}
	pgseed.EnsureSyncIntegration(ctx, t, pool, rqOrg, rqIntegration, rqSource)
	fn(ctx, pool)
}

func rqID(kind, run, n int) string {
	return fmt.Sprintf("00000000-0000-4000-%04x-%012x", 0x9000+kind, run*1000+n)
}

func newRQSim(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time, unitTime time.Duration) *rqSim {
	sim := &rqSim{t: t, ctx: ctx, pool: pool, now: now, unitTime: unitTime, firstClaim: map[string]time.Time{}}
	sim.sweeps = &rqSweeps{now: func() time.Time { return sim.now }, due: map[string]time.Time{}}
	sim.service = newTestDispatchService(t, pool)
	sim.service.now = func() time.Time { return sim.now }
	rqAttachSweeps(sim.service, sim.sweeps)
	return sim
}

// rqRunSpec is one seeded run: its class, age, units in the medium bucket (planned, running) and in the light bucket
// (planned, seeded BEFORE the medium ones), its first dispatch wakeup (nil = none) and when its running units end.
type rqRunSpec struct {
	index       int
	backfill    bool
	createdAt   time.Time
	medium      int
	running     int
	lightFirst  int
	wakeAt      *time.Time
	runningEnd  time.Time
	noDiscovery bool
}

func (sim *rqSim) seed(spec rqRunSpec) string {
	t, ctx, pool := sim.t, sim.ctx, sim.pool
	runID := rqID(1, spec.index, 0)
	triggeredBy, mode, unitMode := "schedule", "incremental", "incremental"
	if spec.backfill {
		triggeredBy, mode, unitMode = "backfill", "backfill", "backfill"
	}
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: rqOrg, IntegrationID: rqIntegration})
	if _, err := pool.Exec(ctx, `UPDATE sync_runs SET triggered_by = $2, mode = $3, status = 'running', created_at = $4 WHERE id = $1::uuid`,
		runID, triggeredBy, mode, spec.createdAt); err != nil {
		t.Fatal(err)
	}
	status := discoveryStatusSuccess
	if spec.noDiscovery {
		status = "planned"
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at,created_at,updated_at)
VALUES ($1,$2,$3,$4,1,$5,$5,$5)`, rqID(2, spec.index, 0), runID, rqOrg, status, spec.createdAt); err != nil {
		t.Fatal(err)
	}
	if spec.lightFirst > 0 {
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status,
	attempts, created_at, updated_at)
SELECT ('00000000-0000-4000-9005-' || lpad(to_hex($8::int * 1000 + k), 12, '0'))::uuid, $2, $1::uuid, $3::uuid, $4::uuid,
	'github', 'commits', 'light', $5, 'planned', 0, $6, $6
FROM generate_series(1, $7::int) AS k`, runID, rqOrg, rqIntegration, rqSource, unitMode, spec.createdAt, spec.lightFirst, spec.index); err != nil {
			t.Fatal(err)
		}
	}
	for n := 0; n < spec.medium+spec.running; n++ {
		unit := pgseed.SyncRunUnit{ID: rqID(3, spec.index, n), RunID: runID, OrgID: rqOrg, IntegrationID: rqIntegration, SourceID: rqSource,
			Provider: "github", DatasetKey: "commits", CostClass: "medium", Mode: unitMode, UpdatedAt: &spec.createdAt, Status: syncRunUnitStatusPlanned}
		if n < spec.running {
			lease, owner := spec.runningEnd.Add(10*time.Hour), "worker-14250"
			unit.LeaseExpiresAt, unit.LeaseOwner, unit.Status = &lease, &owner, syncRunUnitStatusRunning
			sim.running = append(sim.running, rqUnitEnd{unitID: unit.ID, runID: runID, costClass: "medium", at: spec.runningEnd})
		}
		pgseed.InsertSyncRunUnit(ctx, t, pool, unit)
	}
	if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET created_at = $2 WHERE sync_run_id = $1::uuid`, runID, spec.createdAt); err != nil {
		t.Fatal(err)
	}
	if spec.wakeAt != nil {
		pgseed.SyncDispatchOutbox(ctx, t, pool, rqID(4, spec.index, 0), runID, rqOrg, "dispatch_sync_run", "pending", "river", dispatchRouteGeneration)
		if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET available_at = $2, created_at = $3, updated_at = $3 WHERE id = $1::uuid`,
			rqID(4, spec.index, 0), *spec.wakeAt, spec.createdAt); err != nil {
			t.Fatal(err)
		}
	}
	return runID
}

// refuse makes the provider budget refuse every medium unit of the given runs (estimate 150 > limit 100).
func (sim *rqSim) refuse(indexes ...int) {
	estimates := map[string][]budgetEstimate{}
	if bridge, ok := sim.service.bridge.(*fakeBudgetEstimator); ok && bridge.estimates != nil {
		estimates = bridge.estimates
	}
	for _, index := range indexes {
		for n := 0; n < 8; n++ {
			estimates[rqID(3, index, n)] = estimateFor(150, "github", "rest_core", "work-items")
		}
	}
	sim.service.bridge = &fakeBudgetEstimator{estimates: estimates}
}

func (sim *rqSim) nextSweep() (string, time.Time, bool) {
	var key string
	var at time.Time
	found := false
	for candidate, due := range sim.sweeps.due {
		if !found || due.Before(at) || (due.Equal(at) && candidate < key) {
			key, at, found = candidate, due, true
		}
	}
	return key, at, found
}

// step runs the next event at or before horizon -- a unit end, then a due sweep, then a due dispatch delivery at
// equal instants -- and reports false when none is left.
func (sim *rqSim) step(horizon time.Time) bool {
	t, ctx, pool := sim.t, sim.ctx, sim.pool
	var outboxID, runID string
	var passAt time.Time
	hasPass := pool.QueryRow(ctx, `
SELECT id::text, sync_run_id::text, available_at FROM sync_dispatch_outbox
WHERE kind = 'dispatch_sync_run' AND status = 'pending' AND claim_token IS NULL
ORDER BY available_at, id LIMIT 1`).Scan(&outboxID, &runID, &passAt) == nil
	sort.Slice(sim.running, func(i, j int) bool { return sim.running[i].at.Before(sim.running[j].at) })
	sweepKey, sweepAt, hasSweep := sim.nextSweep()
	type event struct {
		kind int
		at   time.Time
	}
	var next *event
	consider := func(kind int, at time.Time, ok bool) {
		if ok && !at.After(horizon) && (next == nil || at.Before(next.at)) {
			next = &event{kind: kind, at: at}
		}
	}
	if len(sim.running) > 0 {
		consider(0, sim.running[0].at, true)
	}
	consider(1, sweepAt, hasSweep)
	consider(2, passAt, hasPass)
	if next == nil {
		return false
	}
	if next.at.After(sim.now) {
		sim.now = next.at
	}
	switch next.kind {
	case 0:
		end := sim.running[0]
		sim.running = sim.running[1:]
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE sync_run_units SET status = 'success', lease_owner = NULL, lease_expires_at = NULL, updated_at = $2
WHERE id = $1::uuid AND status = 'running'`, end.unitID, sim.now); err != nil {
			t.Fatal(err)
		}
		if err := syncrunrollup.ArmFinalize(ctx, tx, end.runID, rqOrg, sim.now); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if sim.loseReleases > 0 {
			sim.loseReleases--
			return true
		}
		rqRelease(ctx, pool, sim.sweeps, syncrunrollup.SlotFree{RunID: end.runID, OrgID: rqOrg, Provider: "github", CostClass: end.costClass}, sim.now)
	case 1:
		delete(sim.sweeps.due, sweepKey)
		parts := splitSweepKey(sweepKey)
		waiting, woken, err := rqSweep(ctx, pool, parts[0], parts[1], parts[2], sim.now)
		if err != nil {
			t.Fatalf("sweep of %s at %s: %v", sweepKey, sim.now.Format(time.RFC3339), err)
		}
		sim.sweepRuns = append(sim.sweepRuns, rqSweepRun{bucket: sweepKey, at: sim.now, woken: woken})
		if _, pending := sim.sweeps.due[sweepKey]; waiting && !pending {
			sim.sweeps.due[sweepKey] = sim.now.Add(rqSweepRearm())
		}
	case 2:
		if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET status = 'dispatched', dispatched_at = $2, dispatched_transport = 'river',
	dispatched_route_generation = $3, updated_at = $2 WHERE id = $1::uuid`, outboxID, sim.now, dispatchRouteGeneration); err != nil {
			t.Fatal(err)
		}
		args := DispatchSyncRunArgs{TransportArgs: TransportArgs{Version: ContractVersionV1, OrgID: rqOrg, RunID: runID,
			DispatchOutbox: outboxID, RouteGeneration: dispatchRouteGeneration}}
		if err := sim.service.Dispatch(ctx, args); err != nil {
			t.Fatalf("pass at %s of run %s: %v", sim.now.Format(time.RFC3339), runID, err)
		}
		rows, err := pool.Query(ctx, `UPDATE sync_run_units SET status = 'running', lease_owner = 'worker-14250', lease_expires_at = $2,
	last_heartbeat_at = $3, updated_at = $3 WHERE sync_run_id = $1::uuid AND status = 'dispatching' RETURNING id::text, cost_class`,
			runID, sim.now.Add(10*time.Hour), sim.now)
		if err != nil {
			t.Fatal(err)
		}
		claimed := 0
		for rows.Next() {
			var unitID, costClass string
			if err := rows.Scan(&unitID, &costClass); err != nil {
				t.Fatal(err)
			}
			sim.running = append(sim.running, rqUnitEnd{unitID: unitID, runID: runID, costClass: costClass, at: sim.now.Add(sim.unitTime)})
			claimed++
		}
		rows.Close()
		if _, ok := sim.firstClaim[runID]; claimed > 0 && !ok {
			sim.firstClaim[runID] = sim.now
		}
		sim.passes = append(sim.passes, rqPass{runID: runID, at: sim.now, claimed: claimed})
	}
	return true
}

func splitSweepKey(key string) [3]string {
	var parts [3]string
	index := 0
	start := 0
	for i := 0; i < len(key); i++ {
		if key[i] == '|' && index < 2 {
			parts[index] = key[start:i]
			index++
			start = i + 1
		}
	}
	parts[index] = key[start:]
	return parts
}

func (sim *rqSim) run(horizon time.Time) {
	for sim.step(horizon) {
	}
}

func (sim *rqSim) passesIn(from, to time.Time, runID string) (passes, claims int) {
	for _, pass := range sim.passes {
		if !pass.at.Before(from) && pass.at.Before(to) && (runID == "" || pass.runID == runID) {
			passes++
			if pass.claimed > 0 {
				claims++
			}
		}
	}
	return passes, claims
}

func (sim *rqSim) sweepsIn(from, to time.Time) int {
	count := 0
	for _, sweep := range sim.sweepRuns {
		if !sweep.at.Before(from) && sweep.at.Before(to) {
			count++
		}
	}
	return count
}

func rqSetEnv(t *testing.T) {
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", "300")
	t.Setenv("SYNC_DISPATCH_CAPPED_REARM_MAX", "3600")
	t.Setenv("SYNC_BUDGET_DEFAULT_LIMIT", "100")
	t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
	t.Setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "1800")
}

// cappedQueueHour: `waiting` backfill runs wait behind a full medium bucket (two units of a holder run run all hour).
// Returns the dispatch passes and the sweeps in the steady hour [t0+10 min, t0+70 min).
func cappedQueueHour(t *testing.T, waiting int) (int, int) {
	var passes, sweeps int
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		rqSetEnv(t)
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		long := t0.Add(-30 * time.Hour)
		sim.seed(rqRunSpec{index: 0, backfill: true, createdAt: long.Add(-time.Hour), running: 2, runningEnd: t0.Add(48 * time.Hour)})
		for i := 1; i <= waiting; i++ {
			wake := t0.Add(time.Duration(i) * 300 * time.Second / time.Duration(waiting))
			sim.seed(rqRunSpec{index: i, backfill: true, createdAt: long.Add(time.Duration(i) * time.Second), medium: 3, wakeAt: &wake})
		}
		from, to := t0.Add(10*time.Minute), t0.Add(70*time.Minute)
		sim.run(to)
		passes, _ = sim.passesIn(from, to, "")
		sweeps = sim.sweepsIn(from, to)
	})
	return passes, sweeps
}

// S1 (r1b P1-1): a queue behind one full bucket costs the same passes per hour at depth 70, 140 and 350, and the
// bucket's sweeps stay within 1 + releases (no release in this hour).
func TestCappedQueuePassesDoNotGrowWithQueueDepth(t *testing.T) {
	results := map[int][2]int{}
	for _, depth := range []int{70, 140, 350} {
		passes, sweeps := cappedQueueHour(t, depth)
		results[depth] = [2]int{passes, sweeps}
		t.Logf("capped queue, steady hour: depth %d -> %d dispatch passes, %d sweeps", depth, passes, sweeps)
	}
	for _, depth := range []int{140, 350} {
		if results[depth][0] > results[70][0]+5 {
			t.Fatalf("dispatch passes per hour grow with the queue: depth 70 -> %d/h, depth %d -> %d/h (want <= depth 70 + 5)",
				results[70][0], depth, results[depth][0])
		}
	}
	for depth, result := range results {
		if result[1] > 1 {
			t.Fatalf("depth %d: %d sweeps in an hour without a release, want <= 1", depth, result[1])
		}
	}
}

// S2 (r1b P1-2), both classes: the oldest waiter's two medium units lie behind 300 light units; a newer run of its
// class also waits. One medium slot frees at t0+5 min: the oldest is woken and claims it.
func TestOldestWaiterBehindOtherBucketUnitsIsFound(t *testing.T) {
	for _, backfill := range []bool{false, true} {
		t.Run(fmt.Sprintf("backfill=%v", backfill), func(t *testing.T) {
			withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				rqSetEnv(t)
				t0 := pgNow()
				sim := newRQSim(t, ctx, pool, t0, time.Hour)
				far := t0.Add(50 * time.Minute)
				sim.seed(rqRunSpec{index: 0, backfill: !backfill, createdAt: t0.Add(-40 * time.Hour), running: 1, runningEnd: t0.Add(5 * time.Minute)})
				sim.seed(rqRunSpec{index: 5, backfill: !backfill, createdAt: t0.Add(-40 * time.Hour), running: 1, runningEnd: t0.Add(10 * time.Hour)})
				oldest := sim.seed(rqRunSpec{index: 1, backfill: backfill, createdAt: t0.Add(-30 * time.Hour), medium: 2, lightFirst: 300, wakeAt: &far})
				newer := sim.seed(rqRunSpec{index: 2, backfill: backfill, createdAt: t0.Add(-1 * time.Hour), medium: 2, wakeAt: &far})
				sim.run(t0.Add(10 * time.Minute))
				first, ok := sim.firstClaim[oldest]
				_, newerClaimed := sim.firstClaim[newer]
				t.Logf("oldest behind 300 light rows: claimed=%v at +%s; newer claimed=%v", ok, first.Sub(t0).Round(time.Second), newerClaimed)
				if !ok || first.Sub(t0) > 6*time.Minute || newerClaimed {
					t.Fatalf("the freed slot did not go to the oldest waiter behind 300 other-bucket rows (oldest claimed=%v, newer claimed=%v)", ok, newerClaimed)
				}
			})
		})
	}
}

// S3 (r1b P1-3): an older run that is not eligible (reference discovery not done) waits ahead of a head the budget
// refuses and of an eligible run. One slot frees: the refusal gives the slot back, the next sweep passes over both
// ineligible runs and the eligible run claims within a minute.
func TestIneligiblePredecessorDoesNotStallTheNextEligibleRun(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		rqSetEnv(t)
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		far := t0.Add(50 * time.Minute)
		sim.seed(rqRunSpec{index: 0, backfill: true, createdAt: t0.Add(-40 * time.Hour), running: 1, runningEnd: t0.Add(10 * time.Hour)})
		sim.seed(rqRunSpec{index: 1, createdAt: t0.Add(-39 * time.Hour), running: 1, runningEnd: t0.Add(5 * time.Minute)})
		sim.seed(rqRunSpec{index: 2, createdAt: t0.Add(-30 * time.Hour), medium: 2, noDiscovery: true})
		sim.seed(rqRunSpec{index: 3, createdAt: t0.Add(-20 * time.Hour), medium: 2, wakeAt: &far})
		eligible := sim.seed(rqRunSpec{index: 4, createdAt: t0.Add(-10 * time.Hour), medium: 2, wakeAt: &far})
		sim.refuse(3)
		sim.run(t0.Add(6 * time.Minute))
		first, ok := sim.firstClaim[eligible]
		t.Logf("eligible run behind an undiscovered run and a refusing head: claimed=%v at +%s", ok, first.Sub(t0).Round(time.Second))
		if !ok || first.Sub(t0) > 6*time.Minute {
			t.Fatalf("eligible run did not get the freed slot within a minute (claimed=%v)", ok)
		}
	})
}

// refusingHeads: `refusing` scheduled runs whose units the budget refuses wait ahead of one eligible run; one slot
// frees at t0+5 min. Returns the eligible run's first claim, the passes of each refusing run before its retry is due,
// and the sweeps in the minute after the release.
func refusingHeads(t *testing.T, refusing int) (time.Duration, bool, map[string]int, int) {
	var wait time.Duration
	var claimed bool
	perRun := map[string]int{}
	var sweeps int
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		rqSetEnv(t)
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		far := t0.Add(50 * time.Minute)
		sim.seed(rqRunSpec{index: 0, backfill: true, createdAt: t0.Add(-40 * time.Hour), running: 1, runningEnd: t0.Add(10 * time.Hour)})
		sim.seed(rqRunSpec{index: 1, createdAt: t0.Add(-39 * time.Hour), running: 1, runningEnd: t0.Add(5 * time.Minute)})
		var refused []int
		for i := 0; i < refusing; i++ {
			sim.seed(rqRunSpec{index: 10 + i, createdAt: t0.Add(-30*time.Hour + time.Duration(i)*time.Minute), medium: 1, wakeAt: &far})
			refused = append(refused, 10+i)
		}
		eligible := sim.seed(rqRunSpec{index: 99, createdAt: t0.Add(-1 * time.Hour), medium: 1, wakeAt: &far})
		sim.refuse(refused...)
		release := t0.Add(5 * time.Minute)
		sim.run(release.Add(time.Minute))
		first, ok := sim.firstClaim[eligible]
		claimed, wait = ok, first.Sub(release)
		for _, index := range refused {
			passes, _ := sim.passesIn(release, release.Add(time.Minute), rqID(1, index, 0))
			perRun[rqID(1, index, 0)] = passes
		}
		sweeps = sim.sweepsIn(release, release.Add(time.Minute))
	})
	return wait, claimed, perRun, sweeps
}

// S4 + O3: heads that the budget refuses give the slot back one after another; each refusing head is woken once
// (not again before its retry is due), the eligible run behind them claims, and the sweeps of the release stay within
// 1 + releases + refusals.
func TestRefusingHeadsGiveTheSlotBackOneWakeEach(t *testing.T) {
	for _, refusing := range []int{2, 5, 20} {
		wait, claimed, perRun, sweeps := refusingHeads(t, refusing)
		t.Logf("%d refusing heads, one release: eligible run claimed=%v %s after; sweeps=%d; refusing passes=%v", refusing, claimed, wait.Round(time.Second), sweeps, perRun)
		if !claimed || wait > time.Minute {
			t.Fatalf("%d refusing heads: eligible run claimed=%v after %s, want within a minute", refusing, claimed, wait)
		}
		for runID, passes := range perRun {
			if passes != 1 {
				t.Fatalf("%d refusing heads: refusing run %s woke %d times before its retry is due, want 1", refusing, runID, passes)
			}
		}
		if sweeps > 1+1+refusing {
			t.Fatalf("%d refusing heads: %d sweeps for one release, want <= 1 + 1 + %d", refusing, sweeps, refusing)
		}
	}
}

// O3: A refuses -> B is woken by the next sweep; A is not woken again before its retry is due.
func TestRefusedHeadGivesTheSlotToTheNextRun(t *testing.T) {
	wait, claimed, perRun, _ := refusingHeads(t, 1)
	if !claimed || wait > time.Minute {
		t.Fatalf("B claimed=%v %s after the release, want within a minute", claimed, wait)
	}
	for runID, passes := range perRun {
		if passes != 1 {
			t.Fatalf("A (%s) woke %d times before its retry is due, want 1", runID, passes)
		}
	}
}

// S5 (r1b P1-3 cycle): run 1 is live by its recent creation and has two medium units the budget refuses; run 2 is
// newer and eligible. One release: run 1 refuses unit by unit, the slot goes to run 2, no 1->2->1 loop.
func TestRefusingLiveHeadDoesNotCycleWithTheNextRun(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		rqSetEnv(t)
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		far := t0.Add(50 * time.Minute)
		sim.seed(rqRunSpec{index: 0, backfill: true, createdAt: t0.Add(-40 * time.Hour), running: 1, runningEnd: t0.Add(10 * time.Hour)})
		sim.seed(rqRunSpec{index: 1, createdAt: t0.Add(-39 * time.Hour), running: 1, runningEnd: t0.Add(5 * time.Minute)})
		sim.seed(rqRunSpec{index: 2, createdAt: t0.Add(-2 * time.Minute), medium: 2, wakeAt: &far})
		second := sim.seed(rqRunSpec{index: 3, createdAt: t0.Add(-1 * time.Minute), medium: 1, wakeAt: &far})
		sim.refuse(2)
		release := t0.Add(5 * time.Minute)
		sim.run(release.Add(time.Minute))
		passes, _ := sim.passesIn(release, release.Add(time.Minute), "")
		first, ok := sim.firstClaim[second]
		t.Logf("refusing live head + next run: next claimed=%v %s after; passes in the minute=%d", ok, first.Sub(release).Round(time.Second), passes)
		if !ok || first.Sub(release) > time.Minute {
			t.Fatalf("next run did not claim within a minute (claimed=%v)", ok)
		}
		if passes > 6 {
			t.Fatalf("%d passes in the minute after one release, want <= 6 (no hand-off/hold-back cycle)", passes)
		}
	})
}

// S8 + O2 (r1b P2-2): the sweep's mark makes the run it picked live for the older-run hold-back however old its
// dispatch row is (601 s here), and a later sweep moves the mark.
func TestSweptHeadWithAStaleRowIsLiveAndTheMarkMoves(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		rqSetEnv(t)
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		stale := t0.Add(-601 * time.Second)
		older := sim.seed(rqRunSpec{index: 1, createdAt: t0.Add(-5 * time.Hour), medium: 2, wakeAt: &stale})
		if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET status = 'dispatched', dispatched_transport = 'river', dispatched_route_generation = $2,
	dispatched_at = available_at WHERE sync_run_id = $1::uuid`, older, dispatchRouteGeneration); err != nil {
			t.Fatal(err)
		}
		newer := sim.seed(rqRunSpec{index: 2, createdAt: t0.Add(-1 * time.Hour), medium: 2})
		mark := func() float64 {
			var value *float64
			if err := pool.QueryRow(ctx, `SELECT (result::jsonb ->> '`+rqSweepMarkKey+`')::double precision FROM sync_runs WHERE id = $1::uuid`, older).Scan(&value); err != nil {
				t.Fatal(err)
			}
			if value == nil {
				return 0
			}
			return *value
		}
		if _, _, err := rqSweep(ctx, pool, rqOrg, "github", "medium", t0); err != nil {
			t.Fatal(err)
		}
		first := mark()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		decision, err := authorizeRun(ctx, tx, nil, rqOrg, newer, t0.Add(time.Second))
		_ = tx.Rollback(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("swept head with a 601 s old row: mark=%.0f heldBack=%v", first, decision.heldBackRunIDs)
		if first == 0 || len(decision.heldBackRunIDs) != 1 || decision.heldBackRunIDs[0] != older {
			t.Fatalf("swept head with a 601 s old dispatch row: mark=%v heldBack=%v, want a mark and [%s]", first, decision.heldBackRunIDs, older)
		}
		if _, _, err := rqSweep(ctx, pool, rqOrg, "github", "medium", t0.Add(2*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if second := mark(); second <= first {
			t.Fatalf("a later sweep did not move the mark: %.3f -> %.3f", first, second)
		}
	})
}

// S9 (r1b P2-3): one pass claims a light unit, leaves a medium unit capped behind a full bucket and gets a 5400 s
// budget deferral for a third unit. The capped work's next opportunity is the medium bucket's sweep, due within the
// sweep re-arm (3600 s), not the run's 5400 s deferral wakeup.
func TestCappedWorkNextOpportunityIsWithinTheBound(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		rqSetEnv(t)
		t.Setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "5400")
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		sim.seed(rqRunSpec{index: 0, backfill: true, createdAt: t0.Add(-40 * time.Hour), running: 2, runningEnd: t0.Add(10 * time.Hour)})
		due := t0
		runID := sim.seed(rqRunSpec{index: 1, createdAt: t0.Add(-2 * time.Hour), medium: 1, lightFirst: 1, wakeAt: &due})
		refusedUnit := "00000000-0000-4000-9003-00000000ffff"
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: refusedUnit, RunID: runID, OrgID: rqOrg, IntegrationID: rqIntegration,
			SourceID: rqSource, Provider: "github", DatasetKey: "commits", CostClass: "heavy", Mode: "incremental", Status: syncRunUnitStatusPlanned})
		sim.service.bridge = &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{refusedUnit: estimateFor(150, "github", "rest_core", "work-items")}}
		if !sim.step(t0) {
			t.Fatal("the pass did not run")
		}
		var rowAt *time.Time
		_ = pool.QueryRow(ctx, `SELECT available_at FROM sync_dispatch_outbox WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run' AND status = 'pending'`, runID).Scan(&rowAt)
		next := time.Duration(1<<62 - 1)
		if rowAt != nil {
			next = rowAt.Sub(t0)
		}
		if sweepAt, ok := sim.sweeps.due[rqOrg+"|github|medium"]; ok && sweepAt.Sub(t0) < next {
			next = sweepAt.Sub(t0)
		}
		claims := len(sim.running) - 2
		t.Logf("partial pass: claimed=%d; capped medium work's next opportunity in %s", claims, next.Round(time.Second))
		if claims != 1 || next > time.Hour {
			t.Fatalf("capped medium work's next opportunity in %s (claimed %d), want <= 1h", next.Round(time.Second), claims)
		}
	})
}

// S10: a release lost between commit and enqueue. The bucket's sweep, re-armed by its last run, wakes the waiter within
// the re-arm (1 h); every waiter keeps a pending sweep or its own wakeup.
func TestLostReleaseIsBoundedByTheSweepRearm(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		rqSetEnv(t)
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, 10*time.Hour)
		sim.seed(rqRunSpec{index: 0, createdAt: t0.Add(-12 * time.Hour), running: 1, runningEnd: t0.Add(20 * time.Minute)})
		sim.seed(rqRunSpec{index: 1, createdAt: t0.Add(-12 * time.Hour), running: 1, runningEnd: t0.Add(10 * time.Hour)})
		first := t0.Add(30 * time.Second)
		waiter := sim.seed(rqRunSpec{index: 2, createdAt: t0.Add(-10 * time.Hour), medium: 3, wakeAt: &first})
		sim.loseReleases = 1
		sim.run(t0.Add(3 * time.Hour))
		claimAt, ok := sim.firstClaim[waiter]
		t.Logf("lost release at t0+20m: waiter claimed=%v at +%s", ok, claimAt.Sub(t0).Round(time.Second))
		if !ok || claimAt.Sub(t0.Add(20*time.Minute)) > time.Hour+time.Minute {
			t.Fatalf("waiter after a lost release claimed=%v at +%s, want within the 1 h sweep re-arm", ok, claimAt.Sub(t0))
		}
	})
}

// S11 (H-STARVE): an older run picked by a sweep whose pass never runs (paused route) holds newer runs back only while
// its mark is inside the liveness bound.
func TestPausedRouteSweptRunStopsHoldingBackWithinTheBound(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		bound := admissionLivenessBound()
		older := sim.seed(rqRunSpec{index: 1, createdAt: t0.Add(-5 * time.Hour), medium: 2})
		newer := sim.seed(rqRunSpec{index: 2, createdAt: t0.Add(-1 * time.Hour), medium: 2})
		setMark := func(at time.Time) {
			if _, err := pool.Exec(ctx, `UPDATE sync_runs SET result = jsonb_build_object('`+rqSweepMarkKey+`', $2::double precision)::json WHERE id = $1::uuid`,
				older, float64(at.UnixMicro())/1e6); err != nil {
				t.Fatal(err)
			}
		}
		admits := func() (int, []string) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			decision, err := authorizeRun(ctx, tx, nil, rqOrg, newer, t0)
			if err != nil {
				t.Fatal(err)
			}
			return len(decision.authorizedUnitIDs) - len(decision.cappedUnitIDs), decision.heldBackRunIDs
		}
		setMark(t0.Add(-bound + time.Minute))
		if got, held := admits(); got != 0 || len(held) != 1 || held[0] != older {
			t.Fatalf("mark inside the bound: newer admitted %d held for %v, want 0 held for [%s]", got, held, older)
		}
		setMark(t0.Add(-bound - time.Second))
		if got, held := admits(); got != 2 || len(held) != 0 {
			t.Fatalf("mark past the bound (paused route): newer admitted %d held for %v, want 2 and none", got, held)
		}
	})
}

// S12 (acp 14265 condition 5): the sweep commits its pick and mark before it wakes the picked run, each wake its own
// statement, so it never holds another run's outbox row across a commit. B's fence holds B's row while the sweep picks
// B: the mark is committed while the fence still holds the row, and the fence re-locks NOWAIT and commits.
func TestSweepWakeHoldsNoRowAcrossACommit(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		later := t0.Add(30 * time.Minute)
		runB := sim.seed(rqRunSpec{index: 2, createdAt: t0.Add(-2 * time.Hour), medium: 2, wakeAt: &later})
		lockB := func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `SELECT 1 FROM sync_dispatch_outbox WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run' FOR NO KEY UPDATE NOWAIT`, runB)
			return err
		}
		fenceB, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = fenceB.Rollback(ctx) }()
		if err := lockB(fenceB); err != nil {
			t.Fatalf("B's fence lock: %v", err)
		}
		started := time.Now()
		_, woken, err := rqSweep(ctx, pool, rqOrg, "github", "medium", t0)
		elapsed := time.Since(started)
		if err != nil {
			t.Fatal(err)
		}
		var marked bool
		if err := pool.QueryRow(ctx, `SELECT (result::jsonb ? '`+rqSweepMarkKey+`') FROM sync_runs WHERE id = $1::uuid`, runB).Scan(&marked); err != nil {
			t.Fatal(err)
		}
		t.Logf("cond-5: sweep returned in %s while B's fence held B's row; woken=%v; B's mark committed=%v", elapsed.Round(time.Millisecond), woken, marked)
		if !marked || len(woken) != 1 {
			t.Fatalf("the sweep did not commit its pick of B while B's fence held B's row (marked=%v woken=%v)", marked, woken)
		}
		if err := lockB(fenceB); err != nil {
			t.Fatalf("B's fence failed to re-lock NOWAIT after the sweep: %v", err)
		}
		if err := fenceB.Commit(ctx); err != nil {
			t.Fatalf("B's fence commit: %v", err)
		}
	})
}

// S13 (prod shape E4): the backfill share is held and 10 backfill runs wait; a newer scheduled run holds the
// non-backfill slot; an older scheduled run (26 h) waits. When the newer run's unit ends the older scheduled run gets
// the slot. The newer run's row sorts first among equal instants (the adversarial relay order).
func TestScheduledRunGetsTheNextFreeNonBackfillSlot(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		rqSetEnv(t)
		t0 := pgNow()
		const unitTime = 10 * time.Minute
		sim := newRQSim(t, ctx, pool, t0, unitTime)
		long := t0.Add(-30 * time.Hour)
		timer := func(i int) *time.Time { at := t0.Add(time.Duration(17*i%300) * time.Second); return &at }
		sim.seed(rqRunSpec{index: 0, backfill: true, createdAt: long, medium: 3, running: 1, wakeAt: timer(0), runningEnd: t0.Add(40 * time.Minute)})
		for i := 1; i <= 10; i++ {
			sim.seed(rqRunSpec{index: i, backfill: true, createdAt: long.Add(time.Duration(i) * time.Second), medium: 3, wakeAt: timer(i)})
		}
		oldRun := sim.seed(rqRunSpec{index: 22, createdAt: t0.Add(-26 * time.Hour), medium: 5, wakeAt: timer(22)})
		newRun := sim.seed(rqRunSpec{index: 21, createdAt: t0.Add(-1 * time.Hour), medium: 12, running: 1, wakeAt: timer(21), runningEnd: t0.Add(5 * time.Minute)})
		horizon := t0.Add(4 * time.Hour)
		sim.run(horizon)
		first, got := sim.firstClaim[oldRun]
		newClaims := 0
		for _, pass := range sim.passes {
			if pass.runID == newRun && pass.claimed > 0 && (!got || pass.at.Before(first)) {
				newClaims++
			}
		}
		wait := horizon.Sub(t0)
		if got {
			wait = first.Sub(t0)
		}
		t.Logf("older scheduled run: first unit %s after t0 (claimed=%v); newer run claimed %d times meanwhile; %d passes, %d sweeps in %s",
			wait.Round(time.Second), got, newClaims, len(sim.passes), len(sim.sweepRuns), horizon.Sub(t0))
		if !got || wait > 5*time.Minute+unitTime {
			t.Fatalf("older scheduled run held %s while a newer scheduled run took the non-backfill slot %d times", wait.Round(time.Second), newClaims)
		}
	})
}
