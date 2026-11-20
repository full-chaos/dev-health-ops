//go:build integration

package syncdispatchruntime

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/domainroledb"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const waitGaugeOrg = "00000000-0000-4000-8000-0000000143a1"

func waitGaugeText(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	source, err := NewBackfillWaitSource(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := source.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func requireLine(t *testing.T, text, line string) {
	t.Helper()
	if !strings.Contains(text, line+"\n") {
		t.Fatalf("missing %q in:\n%s", line, text)
	}
}

func seedWaitUnits(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID, status, costClass string, n int, tag string, now time.Time) {
	t.Helper()
	for i := 0; i < n; i++ {
		seedShareUnitIn(t, ctx, pool, guardUnitID(tag, i), runID, "backfill", status, costClass, now)
	}
}

func seedShareUnitIn(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, runID, mode, status, costClass string, now time.Time) {
	t.Helper()
	unit := pgseed.SyncRunUnit{ID: id, RunID: runID, OrgID: waitGaugeOrg, Provider: "github", CostClass: costClass, Mode: mode, Status: status, UpdatedAt: &now}
	if status == syncRunUnitStatusRunning {
		lease, owner := now.Add(10*time.Minute), "worker-14304"
		unit.LeaseExpiresAt, unit.LeaseOwner = &lease, &owner
	}
	pgseed.InsertSyncRunUnit(ctx, t, pool, unit)
}

func seedWaitRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID, triggeredBy, mode, status string, createdAt time.Time) {
	t.Helper()
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: waitGaugeOrg})
	if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET triggered_by = $2, mode = $3, status = $4, created_at = $5 WHERE id = $1::uuid`,
		runID, triggeredBy, mode, status, createdAt); err != nil {
		t.Fatalf("seed run %s: %v", runID, err)
	}
}

// The ticket case on Postgres: medium cap 2, one backfill unit running, 60 planned units of open backfill runs.
func TestBackfillWaitGaugeTicketCaseOnPostgres(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		pgseed.Org(ctx, t, pool, waitGaugeOrg, "community")
		seedWaitRun(t, ctx, pool, "00000000-0000-4000-8000-0000000143b1", "backfill", "backfill", "running", now.Add(-3*time.Hour))
		seedWaitRun(t, ctx, pool, "00000000-0000-4000-8000-0000000143b2", "backfill", "backfill", "running", now.Add(-2*time.Hour))
		seedWaitRun(t, ctx, pool, "00000000-0000-4000-8000-0000000143b3", "manual", "backfill", "running", now.Add(-time.Hour))
		seedShareUnitIn(t, ctx, pool, guardUnitID("0000014300", 0), "00000000-0000-4000-8000-0000000143b1", "backfill", syncRunUnitStatusRunning, "medium", now)
		seedWaitUnits(t, ctx, pool, "00000000-0000-4000-8000-0000000143b2", syncRunUnitStatusPlanned, "medium", 30, "0000014301", now)
		seedWaitUnits(t, ctx, pool, "00000000-0000-4000-8000-0000000143b3", syncRunUnitStatusPlanned, "medium", 30, "0000014302", now)
		// not counted: an incremental run, a finished backfill run, a finished unit
		seedWaitRun(t, ctx, pool, "00000000-0000-4000-8000-0000000143c1", "schedule", "incremental", "running", now)
		seedWaitUnits(t, ctx, pool, "00000000-0000-4000-8000-0000000143c1", syncRunUnitStatusPlanned, "medium", 5, "0000014303", now)
		seedWaitRun(t, ctx, pool, "00000000-0000-4000-8000-0000000143c2", "backfill", "backfill", "success", now)
		seedWaitUnits(t, ctx, pool, "00000000-0000-4000-8000-0000000143c2", syncRunUnitStatusPlanned, "medium", 4, "0000014304", now)

		text := waitGaugeText(t, ctx, pool)
		requireLine(t, text, `sync_dispatch_backfill_wait_read_ok 1`)
		requireLine(t, text, `sync_dispatch_backfill_open_runs 3`)
		requireLine(t, text, `sync_dispatch_backfill_wait_units{provider="github",cost_class="medium",state="planned"} 60`)
		requireLine(t, text, `sync_dispatch_backfill_wait_units{provider="github",cost_class="medium",state="retrying"} 0`)
		requireLine(t, text, `sync_dispatch_backfill_wait_units{provider="github",cost_class="medium",state="active"} 1`)
		requireLine(t, text, `sync_dispatch_backfill_wait_held_back_units{provider="github",cost_class="medium"} 60`)
	})
}

// No open backfill run: no wait series, one open-runs zero.
func TestBackfillWaitGaugeEmptyOnPostgres(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		text := waitGaugeText(t, ctx, pool)
		requireLine(t, text, `sync_dispatch_backfill_wait_read_ok 1`)
		requireLine(t, text, `sync_dispatch_backfill_open_runs 0`)
		if strings.Contains(text, "sync_dispatch_backfill_wait_units{") {
			t.Fatalf("no wait series expected:\n%s", text)
		}
	})
}

// A free share: one backfill unit waits, nothing is live -> held back 0; a dead lease does not count active.
func TestBackfillWaitGaugeFreeShareAndDeadLeaseOnPostgres(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		pgseed.Org(ctx, t, pool, waitGaugeOrg, "community")
		run := "00000000-0000-4000-8000-0000000143d1"
		seedWaitRun(t, ctx, pool, run, "backfill", "backfill", "running", now.Add(-time.Hour))
		seedWaitUnits(t, ctx, pool, run, syncRunUnitStatusRetrying, "medium", 2, "0000014311", now)
		dead := now.Add(-time.Minute)
		owner := "worker-dead"
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: guardUnitID("0000014312", 0), RunID: run, OrgID: waitGaugeOrg,
			Provider: "github", CostClass: "medium", Mode: "backfill", Status: syncRunUnitStatusRunning, UpdatedAt: &now, LeaseExpiresAt: &dead, LeaseOwner: &owner})
		text := waitGaugeText(t, ctx, pool)
		requireLine(t, text, `sync_dispatch_backfill_wait_units{provider="github",cost_class="medium",state="retrying"} 2`)
		requireLine(t, text, `sync_dispatch_backfill_wait_units{provider="github",cost_class="medium",state="active"} 0`)
		requireLine(t, text, `sync_dispatch_backfill_wait_held_back_units{provider="github",cost_class="medium"} 0`)
	})
}

// The backfill rule of the gauge equals readRunOrder's, over every (triggered_by, mode) case.
func TestBackfillWaitGaugeBackfillRuleEqualsReadRunOrder(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		pgseed.Org(ctx, t, pool, waitGaugeOrg, "community")
		triggers := []string{"backfill", "manual", "schedule", "webhook"}
		modes := []string{"backfill", "incremental", "full"}
		want := 0
		for i, trigger := range triggers {
			for j, mode := range modes {
				run := fmt.Sprintf("00000000-0000-4000-8000-00000143e%d%02d", i, j)
				seedWaitRun(t, ctx, pool, run, trigger, mode, "running", now)
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				order, err := readRunOrder(ctx, tx, run)
				_ = tx.Rollback(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if order.backfill {
					want++
				}
			}
		}
		if want == 0 || want == len(triggers)*len(modes) {
			t.Fatalf("the case table must hold both kinds, backfill runs = %d", want)
		}
		requireLine(t, waitGaugeText(t, ctx, pool), fmt.Sprintf("sync_dispatch_backfill_open_runs %d", want))
	})
}

// A held bucket advisory lock (a guard pass in flight) does not block a gauge read.
func TestBackfillWaitGaugeDoesNotWaitForABucketLock(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		pgseed.Org(ctx, t, pool, waitGaugeOrg, "community")
		run := "00000000-0000-4000-8000-0000000143f1"
		seedWaitRun(t, ctx, pool, run, "backfill", "backfill", "running", now)
		seedWaitUnits(t, ctx, pool, run, syncRunUnitStatusPlanned, "medium", 2, "0000014321", now)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := acquireBucketAdvisoryLocks(ctx, tx, []dispatchBucket{{orgID: waitGaugeOrg, provider: "github", costClass: "medium"}}); err != nil {
			t.Fatal(err)
		}
		done := make(chan string, 1)
		go func() { done <- waitGaugeText(t, ctx, pool) }()
		select {
		case text := <-done:
			requireLine(t, text, `sync_dispatch_backfill_wait_read_ok 1`)
		case <-time.After(3 * time.Second):
			t.Fatal("the gauge read waited for a held bucket lock")
		}
	})
}

// The reconciler holds the domain role (NOSUPERUSER): the same reads run under the migrated grants without a denial.
func TestBackfillWaitGaugeUnderTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	venue := domainroledb.Start(t, ctx, "backfill_wait")
	venue.RequireReady(t, ctx)
	now := time.Now().UTC().Truncate(time.Microsecond)
	pgseed.Org(ctx, t, venue.Admin, waitGaugeOrg, "community")
	run := "00000000-0000-4000-8000-0000000143f2"
	seedWaitRun(t, ctx, venue.Admin, run, "backfill", "backfill", "running", now)
	seedWaitUnits(t, ctx, venue.Admin, run, syncRunUnitStatusPlanned, "heavy", 3, "0000014331", now)
	text := waitGaugeText(t, ctx, venue.Domain)
	requireLine(t, text, `sync_dispatch_backfill_wait_read_ok 1`)
	requireLine(t, text, `sync_dispatch_backfill_wait_units{provider="github",cost_class="heavy",state="planned"} 3`)
	if denials := venue.Denials.All(); len(denials) != 0 {
		t.Fatalf("permission denials under the domain role: %v", denials)
	}
}

// snapshotRaceTx runs a concurrent writer commit right before the active-count statement of a scrape (the claim of a
// waiting unit), the case of the r1 P1: with one snapshot per scrape the claim is invisible to the whole scrape.
type snapshotRaceDB struct {
	pool  *pgxpool.Pool
	claim func()
}

func (db snapshotRaceDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := db.pool.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return &snapshotRaceTxn{Tx: tx, claim: db.claim}, nil
}

type snapshotRaceTxn struct {
	pgx.Tx
	claim func()
	fired bool
}

func (tx *snapshotRaceTxn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if !tx.fired && strings.Contains(sql, "FILTER (WHERE run.triggered_by") {
		tx.fired = true
		tx.claim()
	}
	return tx.Tx.QueryRow(ctx, sql, args...)
}

func (tx *snapshotRaceTxn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if !tx.fired && strings.Contains(sql, "FILTER (WHERE run.triggered_by") {
		tx.fired = true
		tx.claim()
	}
	return tx.Tx.Query(ctx, sql, args...)
}

// One scrape never counts a unit as both waiting and active.
func TestBackfillWaitGaugeOneSnapshotPerScrapeOnPostgres(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		pgseed.Org(ctx, t, pool, waitGaugeOrg, "community")
		run := "00000000-0000-4000-8000-0000000143f3"
		seedWaitRun(t, ctx, pool, run, "backfill", "backfill", "running", now.Add(-time.Hour))
		seedWaitUnits(t, ctx, pool, run, syncRunUnitStatusPlanned, "medium", 2, "0000014341", now)
		source, err := NewBackfillWaitSource(snapshotRaceDB{pool: pool, claim: func() {
			if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET status = 'dispatching', updated_at = now() WHERE id = $1::uuid`, guardUnitID("0000014341", 0)); err != nil {
				t.Errorf("claim: %v", err)
			}
		}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		if err := source.WritePrometheus(&out); err != nil {
			t.Fatal(err)
		}
		text := out.String()
		requireLine(t, text, `sync_dispatch_backfill_wait_units{provider="github",cost_class="medium",state="planned"} 2`)
		requireLine(t, text, `sync_dispatch_backfill_wait_units{provider="github",cost_class="medium",state="active"} 0`)
		requireLine(t, text, `sync_dispatch_backfill_wait_held_back_units{provider="github",cost_class="medium"} 0`)
	})
}

// More waiting groups than the read limit: the groups past the limit fold into the ~other series; the sum of all series equals the true total and the truncation counter moves once.
func TestBackfillWaitGaugeOverTheGroupLimitFoldsIntoOtherOnPostgres(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		pgseed.Org(ctx, t, pool, waitGaugeOrg, "community")
		run := "00000000-0000-4000-8000-0000000143f4"
		seedWaitRun(t, ctx, pool, run, "backfill", "backfill", "running", now.Add(-time.Hour))
		for i := 0; i <= 1000; i++ {
			unit := pgseed.SyncRunUnit{ID: fmt.Sprintf("00000000-0000-4000-8000-%012x", 0x143510000+i), RunID: run, OrgID: waitGaugeOrg, Provider: fmt.Sprintf("provider-%04d", i),
				CostClass: "medium", Mode: "backfill", Status: syncRunUnitStatusPlanned, UpdatedAt: &now}
			pgseed.InsertSyncRunUnit(ctx, t, pool, unit)
		}
		// live units of a non-backfill run: two in the first bucket (kept) and two in the last one (folded); medium cap 2
		other := "00000000-0000-4000-8000-0000000143f5"
		seedWaitRun(t, ctx, pool, other, "schedule", "incremental", "running", now.Add(-time.Hour))
		lease, owner := now.Add(10*time.Minute), "worker-14304"
		for i, provider := range []string{"provider-0000", "provider-0000", "provider-1000", "provider-1000"} {
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: fmt.Sprintf("00000000-0000-4000-8000-%012x", 0x143520000+i), RunID: other, OrgID: waitGaugeOrg,
				Provider: provider, CostClass: "medium", Mode: "incremental", Status: syncRunUnitStatusRunning, UpdatedAt: &now, LeaseExpiresAt: &lease, LeaseOwner: &owner})
		}
		text := waitGaugeText(t, ctx, pool)
		requireLine(t, text, `sync_dispatch_backfill_wait_read_ok 1`)
		requireLine(t, text, `sync_dispatch_backfill_wait_units{provider="~other",cost_class="~other",state="planned"} 1`)
		requireLine(t, text, `sync_dispatch_backfill_wait_units{provider="~other",cost_class="~other",state="active"} 2`)
		requireLine(t, text, `sync_dispatch_backfill_wait_held_back_units{provider="~other",cost_class="~other"} 1`)
		sums := map[string]int{}
		for _, line := range strings.Split(text, "\n") {
			var key string
			switch {
			case strings.HasPrefix(line, "sync_dispatch_backfill_wait_units{") && strings.Contains(line, `state="planned"`):
				key = "planned"
			case strings.HasPrefix(line, "sync_dispatch_backfill_wait_units{") && strings.Contains(line, `state="retrying"`):
				key = "retrying"
			case strings.HasPrefix(line, "sync_dispatch_backfill_wait_units{") && strings.Contains(line, `state="active"`):
				key = "active"
			case strings.HasPrefix(line, "sync_dispatch_backfill_wait_held_back_units{"):
				key = "held_back"
			default:
				continue
			}
			var n int
			if _, err := fmt.Sscanf(line[strings.LastIndex(line, " ")+1:], "%d", &n); err != nil {
				t.Fatal(err)
			}
			sums[key] += n
		}
		for key, want := range map[string]int{"planned": 1001, "retrying": 0, "active": 4, "held_back": 2} {
			if sums[key] != want {
				t.Fatalf("%s over all series = %d, want %d (the true total):\n%s", key, sums[key], want, text)
			}
		}
		requireLine(t, text, `sync_dispatch_backfill_wait_truncated_total 1`)
	})
}

// The grouped statement of all buckets equals the guard's per-bucket calls, on a mixed set: a bucket with live units
// of a backfill run (held back), one with live units of another run, one with a dead lease, one with nothing live.
func TestBackfillWaitFoldedActiveEqualsTheGuardPerBucket(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		pgseed.Org(ctx, t, pool, waitGaugeOrg, "community")
		backfill, other := "00000000-0000-4000-8000-0000000143f6", "00000000-0000-4000-8000-0000000143f7"
		seedWaitRun(t, ctx, pool, backfill, "backfill", "backfill", "running", now.Add(-time.Hour))
		seedWaitRun(t, ctx, pool, other, "schedule", "incremental", "running", now.Add(-time.Hour))
		providers := []string{"mix-a", "mix-b", "mix-c", "mix-d"}
		for i, provider := range providers {
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: fmt.Sprintf("00000000-0000-4000-8000-%012x", 0x143530000+i), RunID: backfill, OrgID: waitGaugeOrg,
				Provider: provider, CostClass: "medium", Mode: "backfill", Status: syncRunUnitStatusPlanned, UpdatedAt: &now})
		}
		lease, owner, dead := now.Add(10*time.Minute), "worker-14304", now.Add(-time.Minute)
		live := func(n int, provider, runID, mode string, leaseAt *time.Time, status string) {
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: fmt.Sprintf("00000000-0000-4000-8000-%012x", 0x143540000+n), RunID: runID, OrgID: waitGaugeOrg,
				Provider: provider, CostClass: "medium", Mode: mode, Status: status, UpdatedAt: &now, LeaseExpiresAt: leaseAt, LeaseOwner: &owner})
		}
		live(0, "mix-a", backfill, "backfill", &lease, syncRunUnitStatusRunning)
		live(1, "mix-a", other, "incremental", &lease, syncRunUnitStatusRunning)
		live(2, "mix-b", other, "incremental", &lease, syncRunUnitStatusRunning)
		live(3, "mix-b", other, "incremental", &lease, syncRunUnitStatusDispatching)
		live(4, "mix-c", other, "incremental", &dead, syncRunUnitStatusRunning)
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		terminal := []string{syncRunStatusSuccess, syncRunStatusPartialFailed, syncRunStatusFailed}
		cutoff := now.Add(-staleDispatchSeconds())
		got, err := readBucketActive(ctx, tx, terminal, 1000, cutoff, now)
		if err != nil {
			t.Fatal(err)
		}
		byBucket := map[dispatchBucket]bucketActive{}
		for _, item := range got {
			byBucket[item.bucket] = item
		}
		sawLive := 0
		for _, provider := range providers {
			bucket := dispatchBucket{orgID: waitGaugeOrg, provider: provider, costClass: "medium"}
			active, activeBackfill, err := countActiveBucketUnits(ctx, tx, bucket, cutoff, now)
			if err != nil {
				t.Fatal(err)
			}
			item := byBucket[bucket]
			if item.active != active || item.activeBackfill != activeBackfill {
				t.Errorf("%s: grouped active=%d backfill=%d, guard active=%d backfill=%d", provider, item.active, item.activeBackfill, active, activeBackfill)
			}
			if active > 0 {
				sawLive++
			}
		}
		if sawLive != 2 || len(got) != 2 {
			t.Fatalf("the mixed set must hold two buckets with live units (got %d rows, %d live by the guard)", len(got), sawLive)
		}
	})
}

// 2,001 buckets (1,001 folded): one scrape stays well inside the read timeout.
func TestBackfillWaitGaugeManyBucketsScrapeUnderOneSecondOnPostgres(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "8")
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		pgseed.Org(ctx, t, pool, waitGaugeOrg, "community")
		run := "00000000-0000-4000-8000-0000000143f8"
		seedWaitRun(t, ctx, pool, run, "backfill", "backfill", "running", now.Add(-time.Hour))
		for i := 0; i <= 2000; i++ {
			pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: fmt.Sprintf("00000000-0000-4000-8000-%012x", 0x143550000+i), RunID: run, OrgID: waitGaugeOrg,
				Provider: fmt.Sprintf("many-%04d", i), CostClass: "medium", Mode: "backfill", Status: syncRunUnitStatusPlanned, UpdatedAt: &now})
		}
		started := time.Now()
		text := waitGaugeText(t, ctx, pool)
		elapsed := time.Since(started)
		requireLine(t, text, `sync_dispatch_backfill_wait_read_ok 1`)
		requireLine(t, text, `sync_dispatch_backfill_wait_truncated_total 1`)
		requireLine(t, text, `sync_dispatch_backfill_wait_units{provider="~other",cost_class="~other",state="planned"} 1001`)
		t.Logf("scrape over 2001 buckets: %s", elapsed)
		if elapsed > time.Second {
			t.Fatalf("one scrape over 2001 buckets took %s, want under 1s", elapsed)
		}
	})
}
