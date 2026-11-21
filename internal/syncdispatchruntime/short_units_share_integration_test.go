//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const shortShareCredential = "00000000-0000-4000-8000-00000000c0c1"

type shortShareArm struct {
	name          string
	long          int // github/cicd medium units, ids lowest (first in the guard's id order)
	short         int // github prs/commits/deployments/security medium units, ids after the long ones
	continuations int // chunk-continuation hand-backs per long unit before it completes
}

var shortShareDatasets = []string{"prs", "commits", "deployments", "security"}

type shortShareUnit struct {
	id, label, dataset string
	long               bool
	left               int
	claim              providersync.Claim
	startedSeq         int
	startedBy          string
	endedSeq           int
	startedAt, endedAt time.Time
}

func shortShareInsert(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, dataset string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'github',$4,$5,'planned',now(),(SELECT integration_id FROM sync_runs WHERE id = $3::uuid),'medium','incremental',0,now())`,
		id, discoveryTestOrg, discoveryTestRun, dataset, dispatchTestSource); err != nil {
		t.Fatal(err)
	}
}

func shortShareStatuses(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT id::text, status FROM sync_run_units WHERE sync_run_id = $1::uuid`, discoveryTestRun)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, status string
		if err := rows.Scan(&id, &status); err != nil {
			t.Fatal(err)
		}
		out[id] = status
	}
	return out
}

func shortShareActive(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	now := time.Now().UTC()
	active, _, _, err := countActiveBucketUnits(ctx, tx, dispatchBucket{orgID: discoveryTestOrg, provider: "github", costClass: "medium"}, now.Add(-staleDispatchSeconds()), now)
	if err != nil {
		t.Fatal(err)
	}
	return active
}

// shortShareRelay delivers the run's pending dispatch row if due at `at` (nil = database clock).
func shortShareRelay(t *testing.T, ctx context.Context, pool *pgxpool.Pool, at *time.Time) (string, time.Time, bool) {
	t.Helper()
	var outboxID string
	var dueAt time.Time
	err := pool.QueryRow(ctx, `
UPDATE sync_dispatch_outbox
SET status = 'dispatched', dispatched_at = now(), dispatched_transport = 'river',
    dispatched_route_generation = $2, updated_at = now()
WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run'
  AND status = 'pending' AND claim_token IS NULL AND available_at <= COALESCE($3::timestamptz, now())
RETURNING id::text, available_at`, discoveryTestRun, dispatchRouteGeneration, at).Scan(&outboxID, &dueAt)
	if err != nil {
		return "", time.Time{}, false
	}
	return outboxID, dueAt.UTC(), true
}

// shortShareCheckProjection reads the stored rows through the guard's own projection: every long unit's stored
// dataset_key is chunked, every short unit's is not.
func shortShareCheckProjection(t *testing.T, ctx context.Context, pool *pgxpool.Pool, units []*shortShareUnit) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	chunked := map[string]bool{}
	keys := map[string]string{}
	if err := forEachDispatchGuardUnit(ctx, tx, discoveryTestRun, func(unit dispatchGuardUnit) error {
		chunked[unit.id], keys[unit.id] = unit.chunked, unit.datasetKey
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, unit := range units {
		if chunked[unit.id] != unit.long {
			t.Fatalf("%s: stored dataset_key %q chunked = %v, want %v", unit.label, keys[unit.id], chunked[unit.id], unit.long)
		}
	}
}

// shortShareSeed makes the one scheduled github run of the cells dispatchable, with its dispatch row due now.
func shortShareSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", "60")
	seedDispatchRoute(t, ctx, pool)
	markReferenceDiscoverySucceeded(t, ctx, pool)
	for _, stmt := range []string{
		`INSERT INTO integration_credentials (id, provider, name, created_at, updated_at) VALUES ('` + shortShareCredential + `', 'github', 'default', now(), now())`,
		`UPDATE integrations SET credential_id = '` + shortShareCredential + `' WHERE id = '` + discoveryTestIntegration + `'`,
		`UPDATE sync_runs SET triggered_by = 'schedule', mode = 'incremental', credential_id = '` + shortShareCredential + `',
		 credential_fingerprint = 'fp', auth_source = 'integration_credential' WHERE id = '` + discoveryTestRun + `'`,
		`UPDATE sync_dispatch_outbox SET status = 'pending', available_at = now(), dispatched_transport = NULL,
		 dispatched_route_generation = NULL WHERE id = '` + dispatchServiceTestOutbox + `'`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

func runShortShareArm(t *testing.T, arm shortShareArm) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		shortShareSeed(t, ctx, pool)
		var units []*shortShareUnit
		byID := map[string]*shortShareUnit{}
		for i := 0; i < arm.long; i++ {
			unit := &shortShareUnit{id: fmt.Sprintf("00000000-0000-4000-8000-00001a00%04d", i), label: fmt.Sprintf("L%d", i), dataset: "cicd", long: true, left: arm.continuations}
			units = append(units, unit)
		}
		for i := 0; i < arm.short; i++ {
			unit := &shortShareUnit{id: fmt.Sprintf("00000000-0000-4000-8000-00001b00%04d", i), label: fmt.Sprintf("S%d", i), dataset: shortShareDatasets[i%len(shortShareDatasets)]}
			units = append(units, unit)
		}
		for _, unit := range units {
			shortShareInsert(t, ctx, pool, unit.id, unit.dataset)
			byID[unit.id] = unit
		}
		shortShareCheckProjection(t, ctx, pool, units)
		service := newTestDispatchService(t, pool)
		repository, err := providersync.NewPostgresRepository(pool)
		if err != nil {
			t.Fatal(err)
		}
		defer repository.Close()

		t0 := time.Now().UTC()
		seq := 0
		off := func(at time.Time) string { return fmt.Sprintf("%+.3fs", at.Sub(t0).Seconds()) }
		holders := func() string {
			statuses := shortShareStatuses(t, ctx, pool)
			var parts []string
			for _, unit := range units {
				if s := statuses[unit.id]; s == "dispatching" || s == "running" {
					parts = append(parts, unit.label+"("+s+")")
				}
			}
			return strings.Join(parts, ",")
		}
		markStarted := func(kind string) []string {
			statuses := shortShareStatuses(t, ctx, pool)
			var got []string
			for _, unit := range units {
				if unit.startedSeq == 0 && (statuses[unit.id] == "dispatching" || statuses[unit.id] == "running") {
					unit.startedSeq, unit.startedAt, unit.startedBy = seq, time.Now().UTC(), kind
					got = append(got, unit.label)
				}
			}
			return got
		}
		// passes runs every due dispatch pass; timer=true also delivers a pending row at its own available_at
		// (the capped pass's 60 s re-arm firing while the long units sit in their continuation gap).
		passes := func(timer bool) string {
			var out []string
			for k := 0; k < 6; k++ {
				outboxID, dueAt, ok := shortShareRelay(t, ctx, pool, nil)
				kind := "wake"
				if !ok && timer {
					var at time.Time
					if err := pool.QueryRow(ctx, `SELECT available_at FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run' AND status = 'pending'`, discoveryTestRun).Scan(&at); err == nil {
						outboxID, dueAt, ok = shortShareRelay(t, ctx, pool, &at)
						kind = "timer"
					}
					timer = false
				}
				if !ok {
					break
				}
				activeBefore := shortShareActive(t, ctx, pool)
				if err := service.Dispatch(ctx, DispatchSyncRunArgs{TransportArgs: TransportArgs{
					Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: discoveryTestRun,
					DispatchOutbox: outboxID, RouteGeneration: dispatchRouteGeneration,
				}}); err != nil {
					t.Fatalf("Dispatch: %v", err)
				}
				started := markStarted(kind)
				out = append(out, fmt.Sprintf("%s(due %s, active_before=%d, admitted=[%s])", kind, off(dueAt), activeBefore, strings.Join(started, " ")))
			}
			return strings.Join(out, " ")
		}

		seq++
		t.Logf("ARM %s #%02d t=%s INITIAL %s | holders=%s", arm.name, seq, off(time.Now().UTC()), passes(false), holders())

		lastLongEnd := 0
		for round := 0; round < 400; round++ {
			statuses := shortShareStatuses(t, ctx, pool)
			remaining := 0
			for _, unit := range units {
				if statuses[unit.id] != "success" {
					remaining++
				}
			}
			if remaining == 0 {
				break
			}
			progressed := false
			for _, unit := range units {
				if statuses[unit.id] != "dispatching" {
					continue
				}
				now := time.Now().UTC()
				claim, err := repository.Claim(ctx, providersync.ClaimRequest{
					UnitID: unit.id, OrgID: discoveryTestOrg, Owner: uuid.NewString(), Now: now, LeaseDuration: 5 * time.Minute,
				})
				if err != nil {
					continue // its continuation not-before is still ahead (the snoozed River job is not due yet)
				}
				progressed = true
				seq++
				unit.claim = claim
				event := ""
				if unit.long && unit.left > 0 {
					unit.left--
					handback := time.Now().UTC()
					if err := repository.DeferChunkContinuation(ctx, claim, handback.Add(time.Second), handback); err != nil {
						t.Fatalf("DeferChunkContinuation %s: %v", unit.label, err)
					}
					event = fmt.Sprintf("%s claim(attempt %d) -> chunk continuation hand-back #%d (dispatching, not_before +1s); active=%d",
						unit.label, claim.Attempt, arm.continuations-unit.left, shortShareActive(t, ctx, pool))
					t.Logf("ARM %s #%02d t=%s %s | passes=[%s] | holders=%s", arm.name, seq, off(handback), event, passes(true), holders())
					continue
				}
				completedAt := time.Now().UTC()
				if err := repository.Complete(ctx, claim, map[string]any{"records": 1}, nil, completedAt.Add(-time.Millisecond), completedAt); err != nil {
					t.Fatalf("Complete %s: %v", unit.label, err)
				}
				unit.endedSeq, unit.endedAt = seq, completedAt
				if unit.long {
					lastLongEnd = seq
				}
				event = fmt.Sprintf("%s claim(attempt %d) -> COMPLETE", unit.label, claim.Attempt)
				t.Logf("ARM %s #%02d t=%s %s | passes=[%s] | holders=%s", arm.name, seq, off(completedAt), event, passes(false), holders())
			}
			if !progressed {
				time.Sleep(250 * time.Millisecond)
			}
		}

		var rows []string
		shortBefore, shortTotal, shortBeforeFirst, shortWhileLongPlanned := 0, 0, 0, 0
		firstLongEnd, lastLongStart := 0, 0
		for _, unit := range units {
			if unit.long && (firstLongEnd == 0 || unit.endedSeq < firstLongEnd) {
				firstLongEnd = unit.endedSeq
			}
			if unit.long && unit.startedSeq > lastLongStart {
				lastLongStart = unit.startedSeq
			}
		}
		var shortWait []float64
		var lastLongEndAt time.Time
		for _, unit := range units {
			if unit.long && unit.endedAt.After(lastLongEndAt) {
				lastLongEndAt = unit.endedAt
			}
		}
		for _, unit := range units {
			rows = append(rows, fmt.Sprintf("%s %s %s started=#%d@%s ended=#%d@%s", unit.label, unit.id[len(unit.id)-8:], unit.dataset,
				unit.startedSeq, off(unit.startedAt), unit.endedSeq, off(unit.endedAt)))
			if !unit.long {
				shortTotal++
				if unit.startedSeq > 0 && unit.startedSeq < lastLongEnd {
					shortBefore++
				}
				if unit.startedSeq > 0 && unit.startedSeq < firstLongEnd {
					shortBeforeFirst++
				}
				if unit.startedSeq > 0 && unit.startedSeq < lastLongStart {
					shortWhileLongPlanned++
				}
				shortWait = append(shortWait, unit.startedAt.Sub(t0).Seconds())
			}
		}
		sort.Float64s(shortWait)
		var finals []string
		for _, unit := range units {
			var status string
			var attempts int
			_ = pool.QueryRow(ctx, `SELECT status, attempts FROM sync_run_units WHERE id = $1::uuid`, unit.id).Scan(&status, &attempts)
			finals = append(finals, fmt.Sprintf("%s:%s:a%d", unit.label, status, attempts))
		}
		t.Logf("ARM %s UNITS\n  %s", arm.name, strings.Join(rows, "\n  "))
		t.Logf("ARM %s FINAL %s", arm.name, strings.Join(finals, " "))
		t.Logf("ARM %s RESULT short started before FIRST long end (#%d): %d of %d | before LAST long end (#%d @%s): %d of %d | before the last long unit STARTED (#%d): %d of %d | short start offsets s=%v",
			arm.name, firstLongEnd, shortBeforeFirst, shortTotal, lastLongEnd, off(lastLongEndAt), shortBefore, shortTotal, lastLongStart, shortWhileLongPlanned, shortTotal, shortWait)
		// C2: after the first pass, a long unit starts only in the wake pass of a unit end, never on a timer.
		for _, unit := range units {
			if unit.long && unit.startedSeq > 1 && unit.startedBy != "wake" {
				t.Errorf("ARM %s: %s started at #%d by a %q pass, want the wake pass of a unit end", arm.name, unit.label, unit.startedSeq, unit.startedBy)
			}
		}
		if shortBeforeFirst != shortTotal {
			t.Errorf("ARM %s: short units started before the first long end = %d of %d, want %d of %d", arm.name, shortBeforeFirst, shortTotal, shortTotal, shortTotal)
		}
	})
}

// Two long chunked cicd units and eight short units of one run in github:medium (cap 2): one slot stays with the short units.
func TestShortUnitsShareTwoLong(t *testing.T) {
	runShortShareArm(t, shortShareArm{name: "A-2long-8short", long: 2, short: 8, continuations: 4})
}

// Four long chunked units queued ahead of six short units by id: the short units do not wait for a long wave.
func TestShortUnitsShareFourLong(t *testing.T) {
	runShortShareArm(t, shortShareArm{name: "B-4long-6short", long: 4, short: 6, continuations: 3})
}

// The C1c cells: the budget step denies the run's short unit s0 in the pass that kept a slot for it (a real deferral by
// enforceRun: its estimate is over the limit), so that pass leaves the slot empty; l1 is the capped chunked unit. Every
// pass is delivered only when its dispatch row is due by the database clock. A budget deferral stands in for a cooldown
// deferral: both write retrying + a future available_at and the pass arms the same way.
const (
	denialL0 = "00000000-0000-4000-8000-00001c000000"
	denialL1 = "00000000-0000-4000-8000-00001c000001"
	denialS0 = "00000000-0000-4000-8000-00001d000000"
)

type shortShareDenial struct {
	t    *testing.T
	ctx  context.Context
	pool *pgxpool.Pool
	svc  *NativeDispatchSyncRunService
}

// newShortShareDenial seeds l0, l1 (cicd) and s0 (prs) with the given countdown, deferral and jitter (seconds).
func newShortShareDenial(t *testing.T, ctx context.Context, pool *pgxpool.Pool, countdown, deferral, jitter string) *shortShareDenial {
	t.Helper()
	shortShareSeed(t, ctx, pool)
	t.Setenv("SYNC_DISPATCH_REDISPATCH_COUNTDOWN", countdown)
	t.Setenv("SYNC_BUDGET_DEFAULT_LIMIT", "100")
	t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
	t.Setenv("SYNC_BUDGET_DEFERRAL_SECONDS", deferral)
	t.Setenv("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", jitter)
	shortShareInsert(t, ctx, pool, denialL0, "cicd")
	shortShareInsert(t, ctx, pool, denialL1, "cicd")
	shortShareInsert(t, ctx, pool, denialS0, "prs")
	registry := &fakeJobRegistry{descriptors: map[string]jobruntime.Descriptor{jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river")}}
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{denialS0: estimateFor(1000, "github", "rest_core", "prs")}}
	service, err := NewNativeDispatchSyncRunService(pool, nil, estimator, producer, registry)
	if err != nil {
		t.Fatal(err)
	}
	return &shortShareDenial{t: t, ctx: ctx, pool: pool, svc: service}
}

func (cell *shortShareDenial) exec(stmt string) {
	cell.t.Helper()
	if _, err := cell.pool.Exec(cell.ctx, stmt); err != nil {
		cell.t.Fatal(err)
	}
}

func (cell *shortShareDenial) state() string {
	var out []string
	for _, id := range []string{denialL0, denialL1, denialS0} {
		var status string
		var availableAt *time.Time
		if err := cell.pool.QueryRow(cell.ctx, `SELECT status, available_at FROM sync_run_units WHERE id = $1::uuid`, id).Scan(&status, &availableAt); err != nil {
			cell.t.Fatal(err)
		}
		at := "-"
		if availableAt != nil {
			at = availableAt.UTC().Format(time.RFC3339Nano)
		}
		out = append(out, fmt.Sprintf("%s:%s@%s", id[len(id)-8:], status, at))
	}
	var pending time.Time
	if err := cell.pool.QueryRow(cell.ctx, `SELECT available_at FROM sync_dispatch_outbox WHERE sync_run_id = $1 AND kind = 'dispatch_sync_run' AND status = 'pending'`, discoveryTestRun).Scan(&pending); err == nil {
		out = append(out, "armed@"+pending.UTC().Format(time.RFC3339Nano))
	}
	var dbNow time.Time
	_ = cell.pool.QueryRow(cell.ctx, `SELECT clock_timestamp()`).Scan(&dbNow)
	return strings.Join(out, " ") + " now@" + dbNow.UTC().Format(time.RFC3339Nano)
}

func (cell *shortShareDenial) status(id string) (string, bool) {
	var status string
	var future bool
	if err := cell.pool.QueryRow(cell.ctx, `SELECT status, COALESCE(available_at > clock_timestamp(), false) FROM sync_run_units WHERE id = $1::uuid`, id).Scan(&status, &future); err != nil {
		cell.t.Fatal(err)
	}
	return status, future
}

// pass waits until the run's dispatch row is due, delivers it and runs the pass.
func (cell *shortShareDenial) pass(name string) {
	cell.t.Helper()
	for {
		outboxID, _, ok := shortShareRelay(cell.t, cell.ctx, cell.pool, nil)
		if ok {
			if err := cell.svc.Dispatch(cell.ctx, DispatchSyncRunArgs{TransportArgs: TransportArgs{
				Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: discoveryTestRun,
				DispatchOutbox: outboxID, RouteGeneration: dispatchRouteGeneration,
			}}); err != nil {
				cell.t.Fatalf("Dispatch: %v", err)
			}
			cell.t.Logf("C1c %s: %s", name, cell.state())
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// want checks l0 holds its slot, l1's status, and that s0 is retrying with a future available_at.
func (cell *shortShareDenial) want(name, l1Status string) {
	cell.t.Helper()
	if got, _ := cell.status(denialL0); got != syncRunUnitStatusDispatching && got != syncRunUnitStatusRunning {
		cell.t.Errorf("%s: l0 %s, want dispatching or running", name, got)
	}
	if got, _ := cell.status(denialL1); got != l1Status {
		cell.t.Errorf("%s: l1 %s, want %s", name, got, l1Status)
	}
	if got, future := cell.status(denialS0); got != syncRunUnitStatusRetrying || !future {
		cell.t.Errorf("%s: s0 %s (available_at ahead %v), want retrying with a future available_at", name, got, future)
	}
}

// C1c (a), the re-arm order: a pass that queued l0, deferred s0 and capped l1 arms the EARLIER of the countdown (2 s)
// and the deferral (5 s), so the first pass inside the deferral window admits l1.
func TestShortUnitsShareCappedChunkedUnitAtTheCountdownInsideTheDeferral(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		cell := newShortShareDenial(t, ctx, pool, "2", "5", "0")
		cell.pass("pass 1")
		cell.want("pass 1 (s0 kept the slot, the budget step denied it)", syncRunUnitStatusPlanned)
		cell.pass("pass 2 (the run's own armed pass)")
		cell.want("pass 2 (first pass inside the deferral window)", syncRunUnitStatusDispatching)
	})
}

// C1c (b), the bound: a deferral (3 s, jitter 0) that ends before the countdown (60 s). The run's own next pass is
// armed at s0's available_at, so s0 is due, kept and denied again and the slot stays empty while the denial lasts. A
// pass inside the deferral window (here the wake of a provider unit end) gives the slot to l1.
func TestShortUnitsShareBudgetDeniedShortUnit(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		cell := newShortShareDenial(t, ctx, pool, "60", "3", "0")
		cell.pass("pass 1")
		cell.want("pass 1 (s0 kept the slot, the budget step denied it)", syncRunUnitStatusPlanned)
		cell.pass("pass 2 (the run's own armed pass, at s0's available_at)")
		cell.want("pass 2 (s0 due again and denied again)", syncRunUnitStatusPlanned)
		if !syncrunrollup.WakeDispatch(ctx, pool, discoveryTestRun, time.Now().UTC(), syncrunrollup.DispatchWakePathProviderUnit) {
			t.Fatal("wake not armed")
		}
		cell.pass("pass 3 (a wake inside the deferral window)")
		cell.want("pass 3", syncRunUnitStatusDispatching)
	})
}

// C1c (c), the bound in the idle back-off: l0 already runs, so the denying pass queues nothing and its tail arms the
// earlier of the deferral (3 s) and the backed-off countdown (base 1 s, the run idle 10 s: 16 s). That is the deferral,
// so the next pass lands on s0's available_at and the slot stays empty again.
func TestShortUnitsShareBudgetDeniedShortUnitInTheIdleBackoff(t *testing.T) {
	withDispatchServicePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		cell := newShortShareDenial(t, ctx, pool, "1", "3", "0")
		cell.exec(`UPDATE sync_run_units SET status = 'running', lease_owner = 'w', lease_expires_at = now() + interval '5 minutes', updated_at = now() WHERE id = '` + denialL0 + `'`)
		cell.exec(`UPDATE sync_run_units SET created_at = now() - interval '10 seconds' WHERE sync_run_id = '` + discoveryTestRun + `'`)
		cell.pass("pass 1")
		cell.want("pass 1 (s0 kept the slot, the budget step denied it, nothing queued)", syncRunUnitStatusPlanned)
		cell.pass("pass 2 (the run's own armed pass, at s0's available_at)")
		cell.want("pass 2 (s0 due again and denied again)", syncRunUnitStatusPlanned)
	})
}
