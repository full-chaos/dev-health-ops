//go:build integration

package syncdispatchruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PLAN-14250-3 build cells (acp ruling plan3-build-1): green-only cells the build adds beside the plan's red cells.

func b3Admitted(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string, now time.Time) int {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	decision, err := authorizeRun(ctx, tx, nil, rqOrg, runID, now)
	if err != nil {
		t.Fatal(err)
	}
	return len(decision.authorizedUnitIDs) - len(decision.cappedUnitIDs)
}

// Q1: the hold-back keeps only the sweep reservations; the in-flight slot is still counted by occupancy. A run whose
// unit is dispatching (claimed, not yet running) and holds no reservation row keeps its slot: a newer pass is not
// granted it, at cap 1 and at cap 2.
func TestB3DispatchingRunWithoutReservationIsNotDoubleGranted(t *testing.T) {
	for _, tc := range []struct {
		costClass string
		capValue  int
	}{{"heavy", 1}, {"medium", 2}} {
		t.Run(tc.costClass, func(t *testing.T) {
			withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
				rqSetEnv(t)
				now := pgNow()
				sim := newRQSim(t, ctx, pool, now, time.Hour)
				inFlight := sim.seed(rqRunSpec{index: 1, createdAt: now.Add(-2 * time.Hour), medium: 1})
				newer := sim.seed(rqRunSpec{index: 2, createdAt: now.Add(-time.Hour), medium: 2})
				if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET cost_class = $1`, tc.costClass); err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status = 'dispatching', updated_at = $2 WHERE sync_run_id = $1::uuid`,
					inFlight, now.Add(-time.Second)); err != nil {
					t.Fatal(err)
				}
				var reservations int
				if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_bucket_reservations`).Scan(&reservations); err != nil {
					t.Fatal(err)
				}
				admitted := b3Admitted(t, ctx, pool, newer, now)
				t.Logf("cap %d, one dispatching unit of another run, %d reservations: newer admitted %d", tc.capValue, reservations, admitted)
				if reservations != 0 || admitted != tc.capValue-1 {
					t.Fatalf("cap %d: newer admitted %d beside an in-flight unit with no reservation (rows %d), want %d",
						tc.capValue, admitted, reservations, tc.capValue-1)
				}
			})
		})
	}
}

// C1c: a parked head is delivered by its bucket's sweep: the sweep's wake moves the parked row to pending, due now,
// with last_error cleared.
func TestB3SweepWakeDeliversAParkedHead(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		sim, sweeps := newP3Sim(t, ctx, pool)
		holder := sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 2, runningEnd: sim.now.Add(10 * time.Hour)})
		due := sim.now
		waiter := sim.seed(rqRunSpec{index: 1, createdAt: sim.now.Add(-time.Hour), medium: 2, wakeAt: &due})
		if claimed := p3Pass(t, sim, waiter); claimed != 0 {
			t.Fatalf("premise: the capped pass claimed %d", claimed)
		}
		row := func() (string, *string, time.Time) {
			var status string
			var lastError *string
			var availableAt time.Time
			if err := pool.QueryRow(ctx, `SELECT status, last_error, available_at FROM sync_dispatch_outbox
WHERE sync_run_id = $1::uuid AND kind = 'dispatch_sync_run'`, waiter).Scan(&status, &lastError, &availableAt); err != nil {
				t.Fatal(err)
			}
			return status, lastError, availableAt
		}
		if status, lastError, _ := row(); status != "dispatched" || lastError == nil || *lastError != bucketParkedError {
			t.Fatalf("premise: the capped pass did not park its row (status %s, last_error %v)", status, lastError)
		}
		sim.now = sim.now.Add(5 * time.Minute)
		p3EndUnit(t, sim, sweeps, rqID(3, 0, 0), holder, syncrunrollup.DispatchWakePathProviderUnit, "success", nil)
		woken := sweeps.run(t, sim, p3Hooks{})
		status, lastError, availableAt := row()
		t.Logf("after a release: sweep woke %v; parked head row status=%s last_error=%v available_at=%s", woken, status, lastError, availableAt.Sub(sim.now))
		if len(woken) != 1 || woken[0] != waiter || status != "pending" || lastError != nil || availableAt.After(sim.now) {
			t.Fatalf("the sweep did not deliver the parked head: woken=%v status=%s last_error=%v available_at=%s",
				woken, status, lastError, availableAt)
		}
	})
}

// Class 2 class mix: 350 older backfill waiters and one non-backfill waiter. The non-backfill head read visits at
// most 8 rows: the class is an index column, so the read never walks the backfill queue.
func TestB3ClassMixHeadReadIsBounded(t *testing.T) {
	const headsK = 8
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		const depth = 350
		for _, statement := range []string{
			`INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
			 SELECT ('00000000-0000-4000-9006-' || lpad(to_hex(n), 12, '0'))::uuid, $1, $2::uuid,
				CASE WHEN n <= $3::int THEN 'backfill' ELSE 'schedule' END, CASE WHEN n <= $3::int THEN 'backfill' ELSE 'incremental' END,
				'running', 1, 0, 0, $4::timestamptz - make_interval(mins => $3::int + 1 - n)
			 FROM generate_series(1, $3::int + 1) AS n`,
			`INSERT INTO sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at)
			 SELECT ('00000000-0000-4000-9007-' || lpad(to_hex(n), 12, '0'))::uuid, ('00000000-0000-4000-9006-' || lpad(to_hex(n), 12, '0'))::uuid,
				$1, 'success', 1, $4::timestamptz, $4::timestamptz, $4::timestamptz
			 FROM generate_series(1, $3::int + 1) AS n WHERE $2::text IS NOT NULL`,
			`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, created_at, updated_at)
			 SELECT ('00000000-0000-4000-9008-' || lpad(to_hex(n), 12, '0'))::uuid, $1, ('00000000-0000-4000-9006-' || lpad(to_hex(n), 12, '0'))::uuid,
				$2::uuid, $5::uuid, 'github', 'commits', 'medium', 'incremental', 'planned', 0, $4::timestamptz, $4::timestamptz
			 FROM generate_series(1, $3::int + 1) AS n`,
		} {
			args := []any{rqOrg, rqIntegration, depth, now}
			if containsPlaceholder(statement, "$5") {
				args = append(args, rqSource)
			}
			if _, err := pool.Exec(ctx, statement, args...); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		if _, err := pool.Exec(ctx, `ANALYZE`); err != nil {
			t.Fatal(err)
		}
		visits, err := p3HeadsRowsRead(ctx, pool, rqOrg, "github", "medium", false, now)
		if err != nil {
			t.Fatal(err)
		}
		backfillVisits, err := p3HeadsRowsRead(ctx, pool, rqOrg, "github", "medium", true, now)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%d backfill waiters ahead of 1 non-backfill waiter: non-backfill head read visits %.0f rows, backfill head read %.0f (k %d)",
			depth, visits, backfillVisits, headsK)
		if visits > headsK || backfillVisits > headsK {
			t.Fatalf("class-mix head reads visit %.0f (non-backfill) and %.0f (backfill) rows, want <= %d", visits, backfillVisits, headsK)
		}
	})
}

// C6b: a reservation is retired by its run's pass, so the next sweep counts the claimed slot once (as active), not
// twice (active and reserved).
func TestB3RetiredReservationIsNotCountedTwice(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		sim, sweeps := newP3Sim(t, ctx, pool)
		holder := sim.seed(rqRunSpec{index: 0, createdAt: sim.now.Add(-40 * time.Hour), running: 1, runningEnd: sim.now.Add(10 * time.Hour)})
		first := sim.seed(rqRunSpec{index: 1, createdAt: sim.now.Add(-3 * time.Hour), medium: 1})
		second := sim.seed(rqRunSpec{index: 2, createdAt: sim.now.Add(-2 * time.Hour), medium: 1})
		firstWoken := sweeps.run(t, sim, p3Hooks{})
		claimed := p3Pass(t, sim, first)
		sim.now = sim.now.Add(time.Minute)
		p3EndUnit(t, sim, sweeps, rqID(3, 0, 0), holder, syncrunrollup.DispatchWakePathProviderUnit, "success", nil)
		secondWoken := sweeps.run(t, sim, p3Hooks{})
		var rows int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_dispatch_bucket_reservations WHERE sync_run_id = $1::uuid`, first).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		t.Logf("first sweep woke %v, its pass claimed %d (reservation rows left %d); after a release the next sweep woke %v",
			firstWoken, claimed, rows, secondWoken)
		if len(firstWoken) != 1 || firstWoken[0] != first || claimed != 1 || rows != 0 || len(secondWoken) != 1 || secondWoken[0] != second {
			t.Fatalf("a claimed slot was counted twice: first woke %v claimed %d rows %d; next woke %v, want [%s] 1 0 [%s]",
				firstWoken, claimed, rows, secondWoken, first, second)
		}
	})
}

// Class 4 bound with releases: a capped queue at depth 70, 140 and 350 where two medium slots turn over every 12
// minutes (10 releases in the measured hour). The bucket's sweeps stay within 1 + releases + refusals (no refusal
// here) and the dispatch passes within the sweeps' wakes plus 5, at every depth.
func TestB3SweepsPerHourStayWithinReleases(t *testing.T) {
	for _, depth := range []int{70, 140, 350} {
		withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
			rqSetEnv(t)
			t0 := pgNow()
			sim := newRQSim(t, ctx, pool, t0, 12*time.Minute)
			sim.seed(rqRunSpec{index: 0, createdAt: t0.Add(-40 * time.Hour), running: 1, runningEnd: t0.Add(6 * time.Minute)})
			sim.seed(rqRunSpec{index: 1, createdAt: t0.Add(-40 * time.Hour), running: 1, runningEnd: t0.Add(12 * time.Minute)})
			for i := 2; i < depth+2; i++ {
				wake := t0.Add(time.Duration(i) * 300 * time.Second / time.Duration(depth))
				sim.seed(rqRunSpec{index: i, createdAt: t0.Add(-30*time.Hour + time.Duration(i)*time.Second), medium: 1, wakeAt: &wake})
			}
			from, to := t0.Add(10*time.Minute), t0.Add(70*time.Minute)
			sim.run(to)
			var releases int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM sync_run_units WHERE status = 'success' AND updated_at >= $1 AND updated_at < $2`,
				from, to).Scan(&releases); err != nil {
				t.Fatal(err)
			}
			passes, _ := sim.passesIn(from, to, "")
			sweeps := sim.sweepsIn(from, to)
			t.Logf("depth %d: %d releases, %d sweeps, %d dispatch passes in the hour", depth, releases, sweeps, passes)
			if releases < 8 || sweeps > 1+releases || passes > releases+5 {
				t.Fatalf("depth %d: %d sweeps and %d passes for %d releases, want sweeps <= %d and passes <= %d (and >= 8 releases)",
					depth, sweeps, passes, releases, 1+releases, releases+5)
			}
		})
	}
}

// Q3: revision 0167 builds its two indexes inside the migration walk's transaction, so writes to sync_runs and
// sync_run_units wait for the build. A measurement, not a bound: seeded rows -> seconds and rows per second, which the
// live row counts (SIZING-14250) turn into the roll window's lock time (acp: STOP if > ~10 s at live size).
func TestB3IndexBuildTimeOnSeededRows(t *testing.T) {
	const runs, unitsPerRun = 100_000, 10
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
SELECT ('00000000-0000-4000-900a-' || lpad(to_hex(n), 12, '0'))::uuid, $1, $2::uuid,
	CASE WHEN n % 10 = 0 THEN 'backfill' ELSE 'schedule' END, CASE WHEN n % 10 = 0 THEN 'backfill' ELSE 'incremental' END,
	CASE WHEN n % 50 = 0 THEN 'running' ELSE 'success' END, $4::int, 0, 0, $5::timestamptz - make_interval(secs => n)
FROM generate_series(1, $3::int) AS n`, rqOrg, rqIntegration, runs, unitsPerRun, now); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, created_at, updated_at)
SELECT ('00000000-0000-4000-900b-' || lpad(to_hex(n * 16 + k), 12, '0'))::uuid, $1, ('00000000-0000-4000-900a-' || lpad(to_hex(n), 12, '0'))::uuid,
	$2::uuid, $3::uuid, 'github', 'commits', 'medium', 'incremental', CASE WHEN n % 50 = 0 THEN 'planned' ELSE 'success' END, 0, $6::timestamptz, $6::timestamptz
FROM generate_series(1, $4::int) AS n, generate_series(1, $5::int) AS k`, rqOrg, rqIntegration, rqSource, runs, unitsPerRun, now); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `ANALYZE sync_runs; ANALYZE sync_run_units`); err != nil {
			t.Fatal(err)
		}
		for _, index := range []struct{ name, table, create string }{
			{"ix_sync_runs_org_class_active_order", "sync_runs", `CREATE INDEX ix_sync_runs_org_class_active_order ON public.sync_runs USING btree
    (org_id, ((((triggered_by = 'backfill'::text) OR ((mode)::text = 'backfill'::text)))::integer), created_at, id)
    WHERE ((status)::text <> ALL ((ARRAY['success'::character varying, 'partial_failed'::character varying, 'failed'::character varying])::text[]))`},
			{"ix_sync_run_units_run_bucket_waiting", "sync_run_units", `CREATE INDEX ix_sync_run_units_run_bucket_waiting ON public.sync_run_units USING btree
    (sync_run_id, provider, cost_class, status, available_at)
    WHERE ((status)::text = ANY ((ARRAY['planned'::character varying, 'retrying'::character varying])::text[]))`},
		} {
			var definition string
			if err := pool.QueryRow(ctx, `SELECT pg_get_indexdef(to_regclass('public.' || $1))`, index.name).Scan(&definition); err != nil {
				t.Fatalf("%s: not created by the migration: %v", index.name, err)
			}
			if _, err := pool.Exec(ctx, `DROP INDEX public.`+index.name); err != nil {
				t.Fatal(err)
			}
			var rows int64
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.`+index.table).Scan(&rows); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			if _, err := pool.Exec(ctx, index.create); err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(started)
			var rebuilt string
			if err := pool.QueryRow(ctx, `SELECT pg_get_indexdef(to_regclass('public.' || $1))`, index.name).Scan(&rebuilt); err != nil || rebuilt != definition {
				t.Fatalf("%s: the timed build differs from the migration's index:\n%s\n%s", index.name, rebuilt, definition)
			}
			t.Logf("%s: %d %s rows -> build %s (%.0f rows/s)", index.name, rows, index.table, elapsed.Round(time.Millisecond),
				float64(rows)/elapsed.Seconds())
		}
	})
}

// planIndexConds lists the Index Cond of every plan node that scans the named index.
func planIndexConds(node planNode, index string) []string {
	var conds []string
	if node.IndexName == index {
		conds = append(conds, node.IndexCond)
	}
	for _, child := range node.Plans {
		conds = append(conds, planIndexConds(child, index)...)
	}
	return conds
}

// Class 2 (acp plan3-build ack b): the head read's class expression is the 0167 index's own expression, so the class
// is an index condition of ix_sync_runs_org_class_active_order for both classes. A query expression that differs from
// the index expression (even an equivalent one) is only a filter, and the read walks the other class's runs.
func TestB3HeadReadClassIsAnIndexConditionOfTheMigratedIndex(t *testing.T) {
	const index = "ix_sync_runs_org_class_active_order"
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		const perClass = 70
		for _, statement := range []string{
			`INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
			 SELECT ('00000000-0000-4000-900c-' || lpad(to_hex(n), 12, '0'))::uuid, $1, $2::uuid,
				CASE WHEN n % 2 = 0 THEN 'backfill' ELSE 'schedule' END, CASE WHEN n % 2 = 0 THEN 'backfill' ELSE 'incremental' END,
				'running', 1, 0, 0, $4::timestamptz - make_interval(mins => n)
			 FROM generate_series(1, 2 * $3::int) AS n`,
			`INSERT INTO sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at)
			 SELECT ('00000000-0000-4000-900d-' || lpad(to_hex(n), 12, '0'))::uuid, ('00000000-0000-4000-900c-' || lpad(to_hex(n), 12, '0'))::uuid,
				$1, 'success', 1, $4::timestamptz, $4::timestamptz, $4::timestamptz
			 FROM generate_series(1, 2 * $3::int) AS n WHERE $2::text IS NOT NULL`,
			`INSERT INTO sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, created_at, updated_at)
			 SELECT ('00000000-0000-4000-900e-' || lpad(to_hex(n), 12, '0'))::uuid, $1, ('00000000-0000-4000-900c-' || lpad(to_hex(n), 12, '0'))::uuid,
				$2::uuid, $5::uuid, 'github', 'commits', 'medium', 'incremental', 'planned', 0, $4::timestamptz, $4::timestamptz
			 FROM generate_series(1, 2 * $3::int) AS n`,
		} {
			args := []any{rqOrg, rqIntegration, perClass, now}
			if containsPlaceholder(statement, "$5") {
				args = append(args, rqSource)
			}
			if _, err := pool.Exec(ctx, statement, args...); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
		if _, err := pool.Exec(ctx, `ANALYZE`); err != nil {
			t.Fatal(err)
		}
		for _, class := range []int{0, 1} {
			plan, err := p3Explain(ctx, pool, bucketHeadsSQL, rqOrg, "github", "medium", class, now, 1)
			if err != nil {
				t.Fatal(err)
			}
			conds := planIndexConds(plan, index)
			t.Logf("class %d: %s conditions %q", class, index, conds)
			if len(conds) != 1 || !strings.Contains(conds[0], "::integer = ") {
				t.Fatalf("class %d: the head read's class is not an index condition of %s (conditions %q)", class, index, conds)
			}
		}
	})
}

// acp plan3-build ack 3: a reservation goes with its run (fk_sync_dispatch_bucket_reservations_run, ON DELETE
// CASCADE), proven on the migrated schema: the hand-built venues omit the foreign key so their TRUNCATE works.
func TestB3ReservationGoesWithItsRunOnTheMigratedSchema(t *testing.T) {
	withRearmQueuePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		gone, kept := rqID(1, 1, 0), rqID(1, 2, 0)
		for _, runID := range []string{gone, kept} {
			pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: rqOrg, IntegrationID: rqIntegration})
			if err := rqSetReservation(ctx, pool, rqOrg, "github", "medium", runID, false, now); err != nil {
				t.Fatal(err)
			}
		}
		var onDelete string
		if err := pool.QueryRow(ctx, `SELECT COALESCE((SELECT confdeltype::text FROM pg_constraint
WHERE conname = 'fk_sync_dispatch_bucket_reservations_run' AND conrelid = 'public.sync_dispatch_bucket_reservations'::regclass
	AND confrelid = 'public.sync_runs'::regclass), '')`).Scan(&onDelete); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM sync_runs WHERE id = $1::uuid`, gone); err != nil {
			t.Fatalf("delete the run: %v", err)
		}
		_, goneLeft, err := rqReservation(ctx, pool, rqOrg, "github", "medium", gone)
		if err != nil {
			t.Fatal(err)
		}
		_, keptLeft, err := rqReservation(ctx, pool, rqOrg, "github", "medium", kept)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("fk on delete=%q; after the run's delete its reservation left=%v, the other run's left=%v", onDelete, goneLeft, keptLeft)
		if onDelete != "c" || goneLeft || !keptLeft {
			t.Fatalf("reservation cascade: fk on delete=%q (want c), deleted run's reservation left=%v (want false), other left=%v (want true)",
				onDelete, goneLeft, keptLeft)
		}
	})
}
