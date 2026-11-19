//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-14250 red cells. A simulated clock drives the real Dispatch service: the relay stand-in delivers the
// earliest due dispatch_sync_run row, a worker stand-in leases each claimed unit and ends it after unitTime with the
// statements providersync's Complete runs (terminal write + ArmFinalize in one tx, then WakeDispatch after commit).

const (
	rqOrg         = "00000000-0000-4000-8000-000000142501"
	rqIntegration = "00000000-0000-4000-8000-000000142502"
	rqSource      = "00000000-0000-4000-8000-000000142503"
)

type rqUnitEnd struct {
	unitID, runID string
	at            time.Time
}

type rqSim struct {
	t          *testing.T
	ctx        context.Context
	pool       *pgxpool.Pool
	service    *NativeDispatchSyncRunService
	now        time.Time
	unitTime   time.Duration
	running    []rqUnitEnd
	passes     []rqPass
	firstClaim map[string]time.Time
	// loseWakes drops the post-commit wakes of the next unit ends (a crash between commit and wake).
	loseWakes int
}

type rqPass struct {
	runID   string
	at      time.Time
	claimed int
}

func withRearmQueuePool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
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

// seedRun creates a non-terminal run with reference discovery done, a pending dispatch wakeup due at wakeAt (nil =
// no wakeup row), `planned` planned github/medium units and `running` running ones, all created at createdAt.
func (sim *rqSim) seedRun(runIndex int, triggeredBy, mode string, createdAt time.Time, planned, running int, wakeAt *time.Time, runningEnd time.Time) string {
	t, ctx, pool := sim.t, sim.ctx, sim.pool
	runID := rqID(1, runIndex, 0)
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: rqOrg, IntegrationID: rqIntegration})
	if _, err := pool.Exec(ctx, `UPDATE sync_runs SET triggered_by = $2, mode = $3, status = 'running', created_at = $4 WHERE id = $1::uuid`,
		runID, triggeredBy, mode, createdAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at,created_at,updated_at)
VALUES ($1,$2,$3,$4,1,$5,$5,$5)`, rqID(2, runIndex, 0), runID, rqOrg, discoveryStatusSuccess, createdAt); err != nil {
		t.Fatal(err)
	}
	unitMode := "incremental"
	if mode == "backfill" {
		unitMode = "backfill"
	}
	for n := 0; n < planned+running; n++ {
		status := syncRunUnitStatusPlanned
		unit := pgseed.SyncRunUnit{ID: rqID(3, runIndex, n), RunID: runID, OrgID: rqOrg, IntegrationID: rqIntegration, SourceID: rqSource,
			Provider: "github", DatasetKey: "commits", CostClass: "medium", Mode: unitMode, UpdatedAt: &createdAt}
		if n < running {
			status = syncRunUnitStatusRunning
			lease, owner := runningEnd.Add(10*time.Hour), "worker-14250"
			unit.LeaseExpiresAt, unit.LeaseOwner = &lease, &owner
			sim.running = append(sim.running, rqUnitEnd{unitID: unit.ID, runID: runID, at: runningEnd})
		}
		unit.Status = status
		pgseed.InsertSyncRunUnit(ctx, t, pool, unit)
	}
	if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET created_at = $2 WHERE sync_run_id = $1::uuid`, runID, createdAt); err != nil {
		t.Fatal(err)
	}
	if wakeAt != nil {
		pgseed.SyncDispatchOutbox(ctx, t, pool, rqID(4, runIndex, 0), runID, rqOrg, "dispatch_sync_run", "pending", "river", dispatchRouteGeneration)
		if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET available_at = $2, created_at = $3, updated_at = $3 WHERE id = $1::uuid`,
			rqID(4, runIndex, 0), *wakeAt, createdAt); err != nil {
			t.Fatal(err)
		}
	}
	return runID
}

// markEarlierProgress gives a run one unit that ended at `at` (and the finalize row that end armed), the shape of a
// scheduled run that ran some units long ago and now waits.
func (sim *rqSim) markEarlierProgress(runIndex int, runID string, at time.Time) {
	t, ctx, pool := sim.t, sim.ctx, sim.pool
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: rqID(3, runIndex, 999), RunID: runID, OrgID: rqOrg, IntegrationID: rqIntegration,
		SourceID: rqSource, Provider: "github", DatasetKey: "commits", CostClass: "medium", Mode: "incremental", Status: "success", UpdatedAt: &at})
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := syncrunrollup.ArmFinalize(ctx, tx, runID, rqOrg, at); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// step runs the next event (a unit end or a due dispatch delivery) at or before horizon; false when none is left.
func (sim *rqSim) step(horizon time.Time) bool {
	t, ctx, pool := sim.t, sim.ctx, sim.pool
	var outboxID, runID string
	var dueAt time.Time
	hasPass := pool.QueryRow(ctx, `
SELECT id::text, sync_run_id::text, available_at FROM sync_dispatch_outbox
WHERE kind = 'dispatch_sync_run' AND status = 'pending' AND claim_token IS NULL
ORDER BY available_at, id LIMIT 1`).Scan(&outboxID, &runID, &dueAt) == nil
	sort.Slice(sim.running, func(i, j int) bool { return sim.running[i].at.Before(sim.running[j].at) })
	hasEnd := len(sim.running) > 0
	if hasEnd && (!hasPass || !sim.running[0].at.After(dueAt)) {
		end := sim.running[0]
		if end.at.After(horizon) {
			return false
		}
		sim.running = sim.running[1:]
		if end.at.After(sim.now) {
			sim.now = end.at
		}
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
		if sim.loseWakes > 0 {
			sim.loseWakes--
			return true
		}
		sim.wakeAfterEnd(end)
		return true
	}
	if !hasPass || dueAt.After(horizon) {
		return false
	}
	if dueAt.After(sim.now) {
		sim.now = dueAt
	}
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
	last_heartbeat_at = $3, updated_at = $3 WHERE sync_run_id = $1::uuid AND status = 'dispatching' RETURNING id::text`,
		runID, sim.now.Add(10*time.Hour), sim.now)
	if err != nil {
		t.Fatal(err)
	}
	claimed := 0
	for rows.Next() {
		var unitID string
		if err := rows.Scan(&unitID); err != nil {
			t.Fatal(err)
		}
		sim.running = append(sim.running, rqUnitEnd{unitID: unitID, runID: runID, at: sim.now.Add(sim.unitTime)})
		claimed++
	}
	rows.Close()
	if claimed > 0 {
		if _, ok := sim.firstClaim[runID]; !ok {
			sim.firstClaim[runID] = sim.now
		}
	}
	sim.passes = append(sim.passes, rqPass{runID: runID, at: sim.now, claimed: claimed})
	return true
}

// wakeAfterEnd is providersync Complete's post-commit wake for the unit that ended.
func (sim *rqSim) wakeAfterEnd(end rqUnitEnd) {
	syncrunrollup.WakeAfterSlotFree(sim.ctx, sim.pool, syncrunrollup.SlotFree{RunID: end.runID, OrgID: rqOrg, Provider: "github", CostClass: "medium"}, sim.now, syncrunrollup.DispatchWakePathProviderUnit)
}

func newRQSim(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time, unitTime time.Duration) *rqSim {
	sim := &rqSim{t: t, ctx: ctx, pool: pool, now: now, unitTime: unitTime, firstClaim: map[string]time.Time{}}
	sim.service = newTestDispatchService(t, pool)
	sim.service.now = func() time.Time { return sim.now }
	return sim
}

// capped queue: `waiting` backfill runs wait behind a full medium bucket (a holder backfill run's 2 units run all
// hour). Returns the dispatch passes in the steady hour [t0+10 min, t0+70 min) and how many claimed a unit.
func cappedQueuePassesPerHour(t *testing.T, waiting int) (int, int) {
	var passes, claimed int
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", "300")
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		long := t0.Add(-30 * time.Hour)
		sim.seedRun(0, "backfill", "backfill", long.Add(-time.Hour), 3, 2, nil, t0.Add(48*time.Hour))
		for i := 1; i <= waiting; i++ {
			wake := t0.Add(time.Duration(i) * 300 * time.Second / time.Duration(waiting))
			sim.seedRun(i, "backfill", "backfill", long.Add(time.Duration(i)*time.Second), 3, 0, &wake, time.Time{})
		}
		from, to := t0.Add(10*time.Minute), t0.Add(70*time.Minute)
		for sim.step(to) {
		}
		for _, pass := range sim.passes {
			if !pass.at.Before(from) && pass.at.Before(to) {
				passes++
				if pass.claimed > 0 {
					claimed++
				}
			}
		}
	})
	return passes, claimed
}

// TestCappedQueueDispatchPassesDoNotGrowWithQueueDepth (Done when 2): a queue of N runs behind one full bucket must
// cost a bounded number of dispatch passes per hour. RED on main: each waiting run re-arms its own full pass every
// <= 300 s (rearmBackoff ceiling), so the hour costs about 12.5 x N passes.
func TestCappedQueueDispatchPassesDoNotGrowWithQueueDepth(t *testing.T) {
	smallPasses, smallClaimed := cappedQueuePassesPerHour(t, 5)
	bigPasses, bigClaimed := cappedQueuePassesPerHour(t, 50)
	t.Logf("capped queue, 1 simulated hour: N=5 -> %d dispatch passes (%d claimed a unit); N=50 -> %d dispatch passes (%d claimed a unit)",
		smallPasses, smallClaimed, bigPasses, bigClaimed)
	if smallClaimed+bigClaimed != 0 {
		t.Fatalf("a pass claimed a unit in a full bucket: the cell's premise is broken")
	}
	if bigPasses > smallPasses+60 {
		t.Fatalf("dispatch passes per hour grow with the queue: N=5 -> %d/h, N=50 -> %d/h (want N=50 <= N=5 + 60)", smallPasses, bigPasses)
	}
}

// TestScheduledRunGetsTheNextFreeNonBackfillSlot (Done when 3), prod shape E4: medium bucket cap 2; a backfill run
// holds the backfill share (1 running unit) with 10 more backfill runs queued; a newer scheduled run holds the
// non-backfill slot (1 running unit, 12 planned); an older scheduled run waits with 5 planned units. Every unit runs
// 10 min. When the newer run's unit ends at t0+5 min the non-backfill slot is free and the older scheduled run is the
// oldest non-backfill waiter. RED on main: the unit end wakes only its own run, which takes the slot again; the
// older run is not "live" for the CHAOS-8880 hold-back, so it waits until the newer run has drained. The relay
// stand-in delivers equal instants by row id; the newer run's row id sorts first, so its own wake runs before any
// wake of the older run (the adversarial order).
func TestScheduledRunGetsTheNextFreeNonBackfillSlot(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", "300")
		t0 := pgNow()
		const unitTime = 10 * time.Minute
		sim := newRQSim(t, ctx, pool, t0, unitTime)
		long := t0.Add(-30 * time.Hour)
		timer := func(i int) *time.Time { at := t0.Add(time.Duration(17*i%300) * time.Second); return &at }
		sim.seedRun(0, "backfill", "backfill", long, 3, 1, timer(0), t0.Add(40*time.Minute))
		for i := 1; i <= 10; i++ {
			sim.seedRun(i, "backfill", "backfill", long.Add(time.Duration(i)*time.Second), 3, 0, timer(i), time.Time{})
		}
		oldRun := sim.seedRun(22, "schedule", "incremental", t0.Add(-26*time.Hour), 5, 0, timer(22), time.Time{})
		sim.markEarlierProgress(22, oldRun, t0.Add(-25*time.Hour))
		newRun := sim.seedRun(21, "schedule", "incremental", t0.Add(-1*time.Hour), 12, 1, timer(21), t0.Add(5*time.Minute))
		horizon := t0.Add(4 * time.Hour)
		for sim.step(horizon) {
		}
		newPassesWhileOldWaited := 0
		first, got := sim.firstClaim[oldRun]
		for _, pass := range sim.passes {
			if pass.runID == newRun && pass.claimed > 0 && (!got || pass.at.Before(first)) {
				newPassesWhileOldWaited++
			}
		}
		wait := horizon.Sub(t0)
		if got {
			wait = first.Sub(t0)
		}
		t.Logf("older scheduled run: first unit claimed %s after t0 (claimed=%v); newer scheduled run claimed in %d passes meanwhile; %d dispatch passes in %s",
			wait.Round(time.Second), got, newPassesWhileOldWaited, len(sim.passes), horizon.Sub(t0))
		if !got || wait > 5*time.Minute+unitTime {
			t.Fatalf("older scheduled run held %s while a newer scheduled run took the non-backfill slot %d times (want its first unit by t0+%s)",
				wait.Round(time.Second), newPassesWhileOldWaited, 5*time.Minute+unitTime)
		}
	})
}

// TestLostSlotFreeWakeIsBoundedByTheHeadPoll: a crash between a unit's commit and its wakes loses them. Medium bucket
// cap 2, both slots held by two runs with nothing left to claim (one unit ends at t0+20 min, after every waiter's
// first pass, its wakes lost; the other runs 10 h); 10 scheduled runs wait. The oldest waiter (the head) still polls
// at the back-off (<= 300 s), so the freed slot is claimed within 300 s + one minute of the end, by the head; no
// waiting run goes longer than SYNC_DISPATCH_CAPPED_REARM_MAX between its own passes.
func TestLostSlotFreeWakeIsBoundedByTheHeadPoll(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", "300")
		t.Setenv("SYNC_DISPATCH_CAPPED_REARM_MAX", "3600")
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, 10*time.Hour)
		holder := sim.seedRun(0, "schedule", "incremental", t0.Add(-12*time.Hour), 0, 1, nil, t0.Add(20*time.Minute))
		sim.seedRun(1, "schedule", "incremental", t0.Add(-12*time.Hour), 0, 1, nil, t0.Add(10*time.Hour))
		var head string
		for i := 2; i <= 11; i++ {
			wake := t0.Add(time.Duration(i*29) * time.Second)
			runID := sim.seedRun(i, "schedule", "incremental", t0.Add(-10*time.Hour+time.Duration(i)*time.Minute), 3, 0, &wake, time.Time{})
			if head == "" {
				head = runID
			}
		}
		sim.loseWakes = 1
		horizon := t0.Add(3 * time.Hour)
		for sim.step(horizon) {
		}
		freedAt := t0.Add(20 * time.Minute)
		var firstAfter *rqPass
		lastPass := map[string]time.Time{}
		maxGap := map[string]time.Duration{}
		for index := range sim.passes {
			pass := sim.passes[index]
			if pass.claimed > 0 && !pass.at.Before(freedAt) && firstAfter == nil {
				firstAfter = &sim.passes[index]
			}
			if previous, ok := lastPass[pass.runID]; ok {
				maxGap[pass.runID] = max(maxGap[pass.runID], pass.at.Sub(previous))
			}
			lastPass[pass.runID] = pass.at
		}
		if firstAfter == nil {
			t.Fatalf("the slot freed at t0+20m (wakes lost) was never claimed in %s", horizon.Sub(t0))
		}
		t.Logf("slot freed by %s at t0+20m with its wakes lost: claimed %s later by %s (head %s); %d passes",
			holder, firstAfter.at.Sub(freedAt).Round(time.Second), firstAfter.runID, head, len(sim.passes))
		if wait := firstAfter.at.Sub(freedAt); wait > 6*time.Minute {
			t.Fatalf("lost wake: freed slot claimed %s after the end, want <= 6m (head poll <= 300 s)", wait.Round(time.Second))
		}
		if firstAfter.runID != head {
			t.Fatalf("lost wake: freed slot taken by %s, want the head %s", firstAfter.runID, head)
		}
		for runID, gap := range maxGap {
			if gap > time.Hour+time.Second {
				t.Fatalf("run %s went %s between its own passes, want <= SYNC_DISPATCH_CAPPED_REARM_MAX (1h)", runID, gap.Round(time.Second))
			}
		}
	})
}

// TestPausedRouteOlderRunStopsHoldingBackWithinTheBound (H-STARVE): an older scheduled run waits with planned medium
// units and a due dispatch wakeup that the relay never delivers (the route is paused). Its due wakeup is the woken
// liveness sign, so a newer run's pass leaves the free slots to it -- but only inside the liveness bound: past it the
// newer run claims.
func TestPausedRouteOlderRunStopsHoldingBackWithinTheBound(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		bound := admissionLivenessBound()
		inside := t0.Add(-bound + time.Minute)
		older := sim.seedRun(1, "schedule", "incremental", t0.Add(-5*time.Hour), 2, 0, &inside, time.Time{})
		newer := sim.seedRun(2, "schedule", "incremental", t0.Add(-1*time.Hour), 2, 0, nil, time.Time{})
		admits := func(at time.Time) (int, []string) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			decision, err := authorizeRun(ctx, tx, nil, rqOrg, newer, at)
			if err != nil {
				t.Fatal(err)
			}
			return len(decision.authorizedUnitIDs) - len(decision.cappedUnitIDs), decision.heldBackRunIDs
		}
		if got, held := admits(t0); got != 0 || len(held) != 1 || held[0] != older {
			t.Fatalf("older run's wakeup due %s ago (inside the bound): newer run admitted %d held for %v, want 0 held for [%s]",
				t0.Sub(inside), got, held, older)
		}
		past := t0.Add(-bound - time.Second)
		if _, err := pool.Exec(ctx, `UPDATE sync_dispatch_outbox SET available_at = $2 WHERE sync_run_id = $1::uuid`, older, past); err != nil {
			t.Fatal(err)
		}
		if got, held := admits(t0); got != 2 || len(held) != 0 {
			t.Fatalf("older run's wakeup due %s ago and never delivered (paused route): newer run admitted %d held for %v, want 2 held for none",
				t0.Sub(past), got, held)
		}
	})
}

// TestBudgetRefusedHeadHandsTheSlotOn (H-STARVE): the bucket head's units are refused by the provider budget. When a
// slot frees, the head is woken, cannot claim, and must hand the slot to the next waiting run of its class, which
// claims within the liveness bound instead of sleeping out its non-head re-arm.
func TestBudgetRefusedHeadHandsTheSlotOn(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", "300")
		t.Setenv("SYNC_DISPATCH_CAPPED_REARM_MAX", "3600")
		t.Setenv("SYNC_BUDGET_DEFAULT_LIMIT", "100")
		t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, 10*time.Hour)
		sim.seedRun(0, "schedule", "incremental", t0.Add(-6*time.Hour), 0, 1, nil, t0.Add(5*time.Minute))
		sim.seedRun(1, "backfill", "backfill", t0.Add(-30*time.Hour), 0, 1, nil, t0.Add(10*time.Hour))
		olderWake, newerWake := t0.Add(200*time.Second), t0.Add(250*time.Second)
		older := sim.seedRun(2, "schedule", "incremental", t0.Add(-3*time.Hour), 2, 0, &olderWake, time.Time{})
		newer := sim.seedRun(3, "schedule", "incremental", t0.Add(-2*time.Hour), 2, 0, &newerWake, time.Time{})
		estimates := map[string][]budgetEstimate{}
		for n := 0; n < 2; n++ {
			estimates[rqID(3, 2, n)] = estimateFor(150, "github", "rest_core", "work-items")
		}
		sim.service.bridge = &fakeBudgetEstimator{estimates: estimates}
		horizon := t0.Add(2 * time.Hour)
		for sim.step(horizon) {
		}
		freedAt := t0.Add(5 * time.Minute)
		first, claimed := sim.firstClaim[newer]
		_, olderClaimed := sim.firstClaim[older]
		t.Logf("slot freed at t0+5m; budget-refused head %s claimed=%v; next run %s first claim %s after the end (claimed=%v); %d passes",
			older, olderClaimed, newer, first.Sub(freedAt).Round(time.Second), claimed, len(sim.passes))
		if olderClaimed {
			t.Fatalf("the budget-refused head claimed a unit: the cell's premise is broken")
		}
		if !claimed || first.Sub(freedAt) > admissionLivenessBound() {
			t.Fatalf("next run behind a budget-refused head claimed %v, %s after the slot freed; want within the liveness bound %s",
				claimed, first.Sub(freedAt).Round(time.Second), admissionLivenessBound())
		}
	})
}

// refusedQueuePasses: `waiting` scheduled runs wait with 2 planned medium units each that the provider budget refuses
// (estimate 150 > limit 100; deferral 1800 s so the budget's own 60 s deferral cadence is held out of the hour). One
// slot frees at t0+20 min, after every waiter's first pass. Returns the passes in the hour [t0+10 min, t0+70 min) and
// the hand-off wakes armed by the freed slot.
func refusedQueuePasses(t *testing.T, waiting int) (int, uint64) {
	var passes int
	var handOffs uint64
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
		t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN_MAX", "300")
		t.Setenv("SYNC_DISPATCH_CAPPED_REARM_MAX", "3600")
		t.Setenv("SYNC_BUDGET_DEFAULT_LIMIT", "100")
		t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
		t.Setenv("SYNC_BUDGET_DEFERRAL_SECONDS", "1800")
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, 10*time.Hour)
		sim.seedRun(0, "schedule", "incremental", t0.Add(-40*time.Hour), 0, 1, nil, t0.Add(20*time.Minute))
		sim.seedRun(1, "backfill", "backfill", t0.Add(-40*time.Hour), 0, 1, nil, t0.Add(10*time.Hour))
		estimates := map[string][]budgetEstimate{}
		for i := 2; i < 2+waiting; i++ {
			wake := t0.Add(time.Duration(i) * 300 * time.Second / time.Duration(2+waiting))
			sim.seedRun(i, "schedule", "incremental", t0.Add(-30*time.Hour+time.Duration(i)*time.Second), 2, 0, &wake, time.Time{})
			for n := 0; n < 2; n++ {
				estimates[rqID(3, i, n)] = estimateFor(150, "github", "rest_core", "work-items")
			}
		}
		sim.service.bridge = &fakeBudgetEstimator{estimates: estimates}
		from, to := t0.Add(10*time.Minute), t0.Add(70*time.Minute)
		freedAt := t0.Add(20 * time.Minute)
		for sim.step(freedAt.Add(-time.Second)) {
		}
		before := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathHandOff, syncrunrollup.DispatchWakeOutcomeArmed)
		for sim.step(freedAt.Add(time.Minute)) {
		}
		handOffs = syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathHandOff, syncrunrollup.DispatchWakeOutcomeArmed) - before
		for sim.step(to) {
		}
		for _, pass := range sim.passes {
			if !pass.at.Before(from) && pass.at.Before(to) {
				passes++
				if pass.claimed > 0 {
					t.Fatalf("pass of %s at %s claimed a refused unit: the cell's premise is broken", pass.runID, pass.at)
				}
			}
		}
	})
	return passes, handOffs
}

// TestRefusedQueueHandOffChainIsBounded (cell F, acp chain bound): 50 waiting runs that all cannot claim (budget
// refused) and one freed slot. The freed slot buys at most one hand-off (the head hands to the next run, which does
// not hand on), and the hour's passes stay within cell A's bound (N=50 <= N=5 + 60).
func TestRefusedQueueHandOffChainIsBounded(t *testing.T) {
	smallPasses, smallHandOffs := refusedQueuePasses(t, 5)
	bigPasses, bigHandOffs := refusedQueuePasses(t, 50)
	t.Logf("refused queue, one slot freed: N=5 -> %d passes/h, %d hand-offs; N=50 -> %d passes/h, %d hand-offs",
		smallPasses, smallHandOffs, bigPasses, bigHandOffs)
	if smallHandOffs > 1 || bigHandOffs > 1 {
		t.Fatalf("one freed slot armed %d (N=5) and %d (N=50) hand-off wakes, want at most 1 (the chain bound)", smallHandOffs, bigHandOffs)
	}
	if bigPasses > smallPasses+60 {
		t.Fatalf("passes per hour behind refused heads grow with the queue: N=5 -> %d/h, N=50 -> %d/h (want N=50 <= N=5 + 60)", smallPasses, bigPasses)
	}
}

// TestHandOffWakeHoldsNoRowAcrossThePassCommit (cell G, acp 14265 condition 5): the wake a pass of run A issues for
// run B runs after A's commit, as its own statement, so it never holds B's outbox row across A's commit. B's
// dispatch fence holds B's row (FOR NO KEY UPDATE NOWAIT, as transport_fence.go's commit snapshot does) while A's
// pass runs and hands its free slot to B: A's pass must still commit (its units are deferred and visible before B's
// fence lets go), B's fence must re-lock NOWAIT and commit without error, and after A's pass no lock of it remains on
// B's row.
func TestHandOffWakeHoldsNoRowAcrossThePassCommit(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_BUDGET_DEFAULT_LIMIT", "100")
		t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
		t0 := pgNow()
		sim := newRQSim(t, ctx, pool, t0, time.Hour)
		sim.seedRun(0, "backfill", "backfill", t0.Add(-40*time.Hour), 0, 1, nil, t0.Add(10*time.Hour))
		due := t0
		runA := sim.seedRun(1, "schedule", "incremental", t0.Add(-3*time.Hour), 2, 0, &due, time.Time{})
		later := t0.Add(30 * time.Minute)
		runB := sim.seedRun(2, "schedule", "incremental", t0.Add(-2*time.Hour), 2, 0, &later, time.Time{})
		sim.service.bridge = &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			rqID(3, 1, 0): estimateFor(150, "github", "rest_core", "work-items"),
			rqID(3, 1, 1): estimateFor(150, "github", "rest_core", "work-items"),
		}}
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
		handOffsBefore := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathHandOff, syncrunrollup.DispatchWakeOutcomeFailed) +
			syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathHandOff, syncrunrollup.DispatchWakeOutcomeArmed)
		started := time.Now()
		if !sim.step(t0) {
			t.Fatal("A's pass did not run")
		}
		elapsed := time.Since(started)
		var retrying int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_run_units WHERE sync_run_id = $1::uuid AND status = 'retrying'`, runA).Scan(&retrying); err != nil {
			t.Fatal(err)
		}
		handOffs := syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathHandOff, syncrunrollup.DispatchWakeOutcomeFailed) +
			syncrunrollup.DispatchWakeCount(syncrunrollup.DispatchWakePathHandOff, syncrunrollup.DispatchWakeOutcomeArmed) - handOffsBefore
		t.Logf("cond-5: A's pass returned in %s while B's fence held B's row; A's units committed (retrying=%d) before B's fence let go; hand-off wakes tried=%d",
			elapsed.Round(time.Millisecond), retrying, handOffs)
		if retrying < 1 {
			t.Fatalf("A's pass did not commit while B's fence held B's row: retrying=%d, want >= 1 (its budget deferral)", retrying)
		}
		if handOffs != 1 {
			t.Fatalf("A's pass tried %d hand-off wakes, want 1 (the cell's premise)", handOffs)
		}
		if err := lockB(fenceB); err != nil {
			t.Fatalf("B's fence failed to re-lock its row NOWAIT after A's pass: %v", err)
		}
		if err := fenceB.Commit(ctx); err != nil {
			t.Fatalf("B's fence commit: %v", err)
		}
		check, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = check.Rollback(ctx) }()
		if err := lockB(check); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				t.Fatalf("B's row still locked after A's pass returned (%s): the wake held it past A's commit", pgErr.Code)
			}
			t.Fatal(err)
		}
	})
}
