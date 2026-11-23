//go:build integration

package syncdispatchruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-14403 cells: the Info dispatch_guard.bucket_decision line names the older run a pass held the bucket back
// for (8-char id prefix) and the liveness signal that kept that run live. Bucket github/medium (cap 2). The older run
// (2 h old) waits with 2 planned medium units; the newer guard run has 3 planned medium units. Every signal row of the
// older run is present in every cell; only the chosen ones are inside the window. Signal units sit in the light bucket
// so they never use a medium slot.

const heldBackOlderRun = "14403a0e-0000-4000-8000-000000014403"

type heldBackSigns struct {
	recentRun, finalize, heartbeat, lease, dispatching, wake bool
}

func seedHeldBackCase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time, runID string, signs heldBackSigns) {
	t.Helper()
	recent, old := now.Add(-time.Minute), now.Add(-2*time.Hour)
	pick := func(live bool) time.Time {
		if live {
			return recent
		}
		return old
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed held-back case: %v", err)
		}
	}
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: guardTestOrg})
	exec(`UPDATE public.sync_runs SET triggered_by = 'schedule', mode = 'incremental', status = 'dispatching', created_at = $2 WHERE id = $1::uuid`,
		runID, pick(signs.recentRun))
	exec(`UPDATE public.sync_runs SET triggered_by = 'schedule', mode = 'incremental', status = 'dispatching', created_at = $2 WHERE id = $1::uuid`,
		guardTestRun, now.Add(-10*time.Second))
	unit := func(kind string, i int) string { return fmt.Sprintf("%s%s%010x", runID[:24], kind, i) }
	for i := 0; i < 2; i++ {
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
			ID: unit("a0", i), RunID: runID, OrgID: guardTestOrg, Provider: "github", CostClass: "medium",
			Status: syncRunUnitStatusPlanned, UpdatedAt: &old,
		})
	}
	// finalize_pending: a finished unit whose terminal write armed the finalize row (the real writers do both in one tx).
	terminalAt := pick(signs.finalize)
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: unit("c0", 0), RunID: runID, OrgID: guardTestOrg, Provider: "github", CostClass: "light", Status: "success", UpdatedAt: &terminalAt,
	})
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := syncrunrollup.ArmFinalize(ctx, tx, runID, guardTestOrg, terminalAt); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("arm finalize: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// unit_heartbeat: a running unit whose lease has expired; only its heartbeat can be recent.
	expired := now.Add(-time.Minute)
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: unit("c1", 0), RunID: runID, OrgID: guardTestOrg, Provider: "github", CostClass: "light",
		Status: syncRunUnitStatusRunning, UpdatedAt: &old, LeaseExpiresAt: &expired, LeaseOwner: ptrString("hb-owner"),
	})
	exec(`UPDATE public.sync_run_units SET last_heartbeat_at = $2 WHERE id = $1::uuid`, unit("c1", 0), pick(signs.heartbeat))
	// unit_lease: a running unit with an old heartbeat; only its lease can be live.
	lease := now.Add(-30 * time.Minute)
	if signs.lease {
		lease = now.Add(time.Hour)
	}
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: unit("c2", 0), RunID: runID, OrgID: guardTestOrg, Provider: "github", CostClass: "light",
		Status: syncRunUnitStatusRunning, UpdatedAt: &old, LeaseExpiresAt: &lease, LeaseOwner: ptrString("lease-owner"),
	})
	exec(`UPDATE public.sync_run_units SET last_heartbeat_at = $2 WHERE id = $1::uuid`, unit("c2", 0), old)
	// unit_dispatching: a claimed unit; only its claim time can be recent.
	dispatchingAt := pick(signs.dispatching)
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: unit("c3", 0), RunID: runID, OrgID: guardTestOrg, Provider: "github", CostClass: "light",
		Status: syncRunUnitStatusDispatching, UpdatedAt: &dispatchingAt,
	})
	// wake_armed: the run's dispatch_sync_run row, pending with no error; due one countdown ahead, or past the lower edge.
	wakeAt := now.Add(-admissionLivenessBound() - time.Second)
	if signs.wake {
		wakeAt = now.Add(150 * time.Second)
	}
	exec(`
INSERT INTO public.sync_dispatch_outbox (id, org_id, sync_run_id, kind, status, available_at, attempts, last_error, created_at, updated_at)
VALUES ($1::uuid, $2, $3::uuid, 'dispatch_sync_run', 'pending', $4, 3, NULL, $5, $5)`,
		unit("d0", 0), guardTestOrg, runID, wakeAt, old)
}

// seedHeldBackNewerRun gives the guard run its 3 planned medium units.
func seedHeldBackNewerRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time) {
	t.Helper()
	for i := 0; i < 3; i++ {
		insertGuardUnit(t, ctx, pool, guardUnitID("00014403b0", i), "github", "medium", syncRunUnitStatusPlanned, now, nil, nil, nil)
	}
}

// bucketDecisionLine runs the guard run's decision with a JSON logger and returns the github:medium bucket_decision
// line, parsed and raw.
func bucketDecisionLine(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time) (guardDecision, map[string]any, string) {
	t.Helper()
	var logs bytes.Buffer
	logger := synclog.New(slog.New(slog.NewJSONHandler(&logs, nil)))
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	decision, err := authorizeRun(ctx, tx, logger, guardTestOrg, guardTestRun, now)
	if err != nil {
		t.Fatalf("authorizeRun: %v", err)
	}
	var found map[string]any
	var raw string
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil || record["msg"] != "dispatch_guard.bucket_decision" || record["bucket"] != "github:medium" {
			continue
		}
		if found != nil {
			t.Fatalf("more than one github:medium bucket_decision line:\n%s", logs.String())
		}
		found, raw = record, line
	}
	if found == nil {
		t.Fatalf("no github:medium bucket_decision line in:\n%s", logs.String())
	}
	if found["level"] != "INFO" {
		t.Fatalf("bucket_decision level = %v, want INFO: %s", found["level"], raw)
	}
	return decision, found, raw
}

func assertHeldBackLine(t *testing.T, line map[string]any, raw string, heldBack float64, run, signal string) {
	t.Helper()
	if line["guard.held_back"] != heldBack || line["guard.oldest_held_run"] != run || line["guard.oldest_held_signal"] != signal {
		t.Fatalf("bucket_decision: guard.held_back=%v guard.oldest_held_run=%v guard.oldest_held_signal=%v, want %v %q %q:\n%s",
			line["guard.held_back"], line["guard.oldest_held_run"], line["guard.oldest_held_signal"], heldBack, run, signal, raw)
	}
}

// RED at base: an older run live ONLY by its armed wake holds the newer run back; the line names it and wake_armed.
func TestBucketDecisionNamesTheWakeArmedOlderRun(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedHeldBackCase(t, ctx, pool, now, heldBackOlderRun, heldBackSigns{wake: true})
		seedHeldBackNewerRun(t, ctx, pool, now)
		decision, line, raw := bucketDecisionLine(t, ctx, pool, now)
		if len(decision.heldBackRunIDs) != 1 || decision.heldBackRunIDs[0] != heldBackOlderRun {
			t.Fatalf("heldBackRunIDs = %v, want [%s]", decision.heldBackRunIDs, heldBackOlderRun)
		}
		assertHeldBackLine(t, line, raw, 2, "14403a0e", "wake_armed")
	})
}

// RED at base: an older run live by a running unit's heartbeat; the line names it and unit_heartbeat.
func TestBucketDecisionNamesTheHeartbeatOlderRun(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedHeldBackCase(t, ctx, pool, now, heldBackOlderRun, heldBackSigns{heartbeat: true})
		seedHeldBackNewerRun(t, ctx, pool, now)
		_, line, raw := bucketDecisionLine(t, ctx, pool, now)
		assertHeldBackLine(t, line, raw, 2, "14403a0e", "unit_heartbeat")
	})
}

// RED at base: no older run is live (every sign past the bound); nothing is held back and the line says none.
func TestBucketDecisionWithNoLiveOlderRunSaysNone(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedHeldBackCase(t, ctx, pool, now, heldBackOlderRun, heldBackSigns{})
		seedHeldBackNewerRun(t, ctx, pool, now)
		decision, line, raw := bucketDecisionLine(t, ctx, pool, now)
		if len(decision.heldBackRunIDs) != 0 {
			t.Fatalf("heldBackRunIDs = %v, want none", decision.heldBackRunIDs)
		}
		assertHeldBackLine(t, line, raw, 0, "none", "none")
	})
}

// assertHeldBackSignal seeds the older run with the given signs and checks the line names it with the signal.
func assertHeldBackSignal(t *testing.T, signs heldBackSigns, want heldBackSignal) {
	t.Helper()
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedHeldBackCase(t, ctx, pool, now, heldBackOlderRun, signs)
		seedHeldBackNewerRun(t, ctx, pool, now)
		decision, line, raw := bucketDecisionLine(t, ctx, pool, now)
		if len(decision.heldBackRunIDs) != 1 || decision.heldBackRunIDs[0] != heldBackOlderRun || len(decision.cappedUnitIDs) != 3 {
			t.Fatalf("heldBackRunIDs = %v capped = %d, want [%s] and 3", decision.heldBackRunIDs, len(decision.cappedUnitIDs), heldBackOlderRun)
		}
		assertHeldBackLine(t, line, raw, 2, "14403a0e", string(want))
	})
}

func TestBucketDecisionNamesTheRecentRunSignal(t *testing.T) {
	assertHeldBackSignal(t, heldBackSigns{recentRun: true}, heldBackSignalRecentRun)
}

func TestBucketDecisionNamesTheFinalizePendingSignal(t *testing.T) {
	assertHeldBackSignal(t, heldBackSigns{finalize: true}, heldBackSignalFinalizePending)
}

func TestBucketDecisionNamesTheUnitLeaseSignal(t *testing.T) {
	assertHeldBackSignal(t, heldBackSigns{lease: true}, heldBackSignalUnitLease)
}

func TestBucketDecisionNamesTheUnitDispatchingSignal(t *testing.T) {
	assertHeldBackSignal(t, heldBackSigns{dispatching: true}, heldBackSignalUnitDispatching)
}

// Overlap: with two signals true the first in heldBackSignalOrder wins; with all six, recent_run.
func TestBucketDecisionOverlappingSignalsTakeTheFirstInOrder(t *testing.T) {
	assertHeldBackSignal(t, heldBackSigns{heartbeat: true, wake: true}, heldBackSignalUnitHeartbeat)
}

func TestBucketDecisionEverySignalTakesRecentRun(t *testing.T) {
	assertHeldBackSignal(t, heldBackSigns{true, true, true, true, true, true}, heldBackSignalRecentRun)
}

// Mixed fixture: one older run with every signal live and one with every signal row present but past the bound.
// olderRunsInBucketSQL (unchanged, sha pinned by TestOlderRunsInBucketSQLIsTheBaseStatement) lists exactly the live one.
func TestOlderRunsRowSetWithEverySignalPresentIsTheLiveRunOnly(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		const deadRun = "14403b0e-0000-4000-8000-000000014404"
		seedHeldBackCase(t, ctx, pool, now, heldBackOlderRun, heldBackSigns{true, true, true, true, true, true})
		seedHeldBackCase(t, ctx, pool, now, deadRun, heldBackSigns{})
		seedHeldBackNewerRun(t, ctx, pool, now)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		self, err := readRunOrder(ctx, tx, guardTestRun)
		if err != nil {
			t.Fatal(err)
		}
		older, err := readOlderRuns(ctx, tx, dispatchBucket{orgID: guardTestOrg, provider: "github", costClass: "medium"}, self, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(older) != 1 || older[0].id != heldBackOlderRun || older[0].waiting != 2 || !older[0].progressAt.Equal(now) {
			t.Fatalf("older runs = %+v, want only %s waiting 2", older, heldBackOlderRun)
		}
		if got := readHeldBackSignal(ctx, tx, deadRun, now); got != heldBackSignalRecheckedNone {
			t.Fatalf("recheck of the dead run = %q, want rechecked_none", got)
		}
		if got := readHeldBackSignal(ctx, tx, "14403c0e-0000-4000-8000-000000014405", now); got != heldBackSignalRecheckedNone {
			t.Fatalf("recheck of a run that is gone = %q, want rechecked_none", got)
		}
	})
}

// signalStatementCounter counts heldBackSignalSQL executions on its pool.
type signalStatementCounter struct {
	mu    sync.Mutex
	count int
}

func (counter *signalStatementCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if data.SQL == heldBackSignalSQL {
		counter.mu.Lock()
		counter.count++
		counter.mu.Unlock()
	}
	return ctx
}

func (*signalStatementCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func countedSignalStatements(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time) int {
	t.Helper()
	config, err := pgxpool.ParseConfig(pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	counter := &signalStatementCounter{}
	config.ConnConfig.Tracer = counter
	traced, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer traced.Close()
	bucketDecisionLine(t, ctx, traced, now)
	return counter.count
}

// held == 0 -> the second statement is not run; held > 0 -> it runs once.
func TestHeldBackSignalStatementRunsOnlyWhenSlotsWereHeldBack(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedHeldBackCase(t, ctx, pool, now, heldBackOlderRun, heldBackSigns{})
		seedHeldBackNewerRun(t, ctx, pool, now)
		if got := countedSignalStatements(t, ctx, pool, now); got != 0 {
			t.Fatalf("nothing held back: heldBackSignalSQL ran %d times, want 0", got)
		}
		if _, err := pool.Exec(ctx, `UPDATE public.sync_runs SET created_at = $2 WHERE id = $1::uuid`, heldBackOlderRun, now.Add(-time.Minute)); err != nil {
			t.Fatal(err)
		}
		if got := countedSignalStatements(t, ctx, pool, now); got != 1 {
			t.Fatalf("held back: heldBackSignalSQL ran %d times, want 1", got)
		}
	})
}

// guardStatementTimeout is the guard transaction's statement_timeout as the next statement of the pass sees it.
func guardStatementTimeout(t *testing.T, ctx context.Context, tx pgx.Tx) string {
	t.Helper()
	var value string
	if err := tx.QueryRow(ctx, `SHOW statement_timeout`).Scan(&value); err != nil {
		t.Fatalf("read statement_timeout in the guard transaction: %v", err)
	}
	return value
}

// assertNoRecheckSavepointOpen fails if readHeldBackSignal's savepoint is still open in the guard transaction: with
// it released, ROLLBACK TO it fails with 3B001 (no such savepoint). The probe runs in its own savepoint, so the guard
// transaction stays usable either way.
func assertNoRecheckSavepointOpen(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	if _, err := tx.Exec(ctx, "SAVEPOINT held_back_probe"); err != nil {
		t.Fatal(err)
	}
	_, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+heldBackSignalSavepoint)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "3B001" {
		t.Fatalf("ROLLBACK TO SAVEPOINT %s after the pass: %v, want 3B001: the recheck left its savepoint open", heldBackSignalSavepoint, err)
	}
	if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT held_back_probe"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT held_back_probe"); err != nil {
		t.Fatal(err)
	}
}

// assertRecheckKeepsTheDecision runs the pass with heldBackSignalSQL set to planted (empty = the real statement): the
// decision and held count equal the control decision, the line carries wantSignal, the guard transaction keeps its
// statement_timeout and stays usable (the second unit fold, the claim UPDATE, COMMIT), and no error text is logged.
func assertRecheckKeepsTheDecision(t *testing.T, planted, wantSignal string) time.Duration {
	t.Helper()
	var elapsed time.Duration
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedHeldBackCase(t, ctx, pool, now, heldBackOlderRun, heldBackSigns{wake: true})
		seedHeldBackNewerRun(t, ctx, pool, now)
		control, line, raw := bucketDecisionLine(t, ctx, pool, now)
		assertHeldBackLine(t, line, raw, 2, "14403a0e", "wake_armed")

		if planted != "" {
			real := heldBackSignalSQL
			t.Cleanup(func() { heldBackSignalSQL = real })
			heldBackSignalSQL = planted
		}
		var logs bytes.Buffer
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		before := guardStatementTimeout(t, ctx, tx)
		started := time.Now()
		decision, err := authorizeRun(ctx, tx, synclog.New(slog.New(slog.NewJSONHandler(&logs, nil))), guardTestOrg, guardTestRun, now)
		elapsed = time.Since(started)
		if err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		if !reflect.DeepEqual(decision, control) {
			t.Fatalf("decision = %+v, want the control decision %+v", decision, control)
		}
		if want := `"guard.held_back":2,"guard.oldest_held_run":"14403a0e","guard.oldest_held_signal":"` + wantSignal + `"`; !strings.Contains(logs.String(), want) {
			t.Fatalf("no %s bucket_decision line:\n%s", wantSignal, logs.String())
		}
		for _, leaked := range []string{"division", "22012", "57014", "cancel", "timeout"} {
			if strings.Contains(logs.String(), leaked) {
				t.Fatalf("recheck error text %q reached the log:\n%s", leaked, logs.String())
			}
		}
		if after := guardStatementTimeout(t, ctx, tx); after != before {
			t.Fatalf("guard statement_timeout after the recheck = %q, want %q (the recheck's SET LOCAL leaked)", after, before)
		}
		assertNoRecheckSavepointOpen(t, ctx, tx)
		if _, err := tx.Exec(ctx, `UPDATE public.sync_run_units SET status = 'dispatching', updated_at = $2 WHERE sync_run_id = $1::uuid AND status = 'planned' AND id = ANY($3::uuid[])`,
			guardTestRun, now, []string{guardUnitID("00014403b0", 0)}); err != nil {
			t.Fatalf("claim after the recheck: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit after the recheck: %v", err)
		}
	})
	return elapsed
}

// A recheck that succeeds keeps the guard transaction's statement_timeout (its SET LOCAL is rolled back with the
// savepoint).
func TestHeldBackSignalRecheckLeavesTheGuardTransactionAsItWas(t *testing.T) {
	assertRecheckKeepsTheDecision(t, "", "wake_armed")
}

// A failed recheck (a statement error) logs read_failed and changes nothing else.
func TestHeldBackSignalReadFailureKeepsTheDecisionAndTheTransaction(t *testing.T) {
	assertRecheckKeepsTheDecision(t, `SELECT (1 / 0) = 1, false, false, false, false, false
WHERE $1::uuid IS NOT NULL AND $2::timestamptz IS NOT NULL AND $3::double precision IS NOT NULL`, "read_failed")
}

// A recheck that runs past heldBackSignalStatementTimeout is cancelled by it: read_failed, nothing else changes, and
// the pass does not wait for the slow statement.
func TestHeldBackSignalTimeoutKeepsTheDecisionAndTheTransaction(t *testing.T) {
	elapsed := assertRecheckKeepsTheDecision(t, `SELECT (SELECT true FROM pg_sleep(5)), false, false, false, false, false
WHERE $1::uuid IS NOT NULL AND $2::timestamptz IS NOT NULL AND $3::double precision IS NOT NULL`, "read_failed")
	if elapsed >= 2*time.Second {
		t.Fatalf("pass with a 5 s recheck took %v; heldBackSignalStatementTimeout did not cut it", elapsed)
	}
}

// Two held buckets in one pass (two rechecks): medium is held for an older run live by its armed wake, light for
// another live by a heartbeat. Each line names its own run and signal; afterwards the guard transaction has its
// statement_timeout back and no recheck savepoint open, and the claim and COMMIT succeed.
func TestTwoHeldBucketsInOnePassRecheckEachAndLeaveNoSavepoint(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		const lightRun = "14403d0e-0000-4000-8000-000000014406"
		seedHeldBackCase(t, ctx, pool, now, heldBackOlderRun, heldBackSigns{wake: true})
		seedHeldBackCase(t, ctx, pool, now, lightRun, heldBackSigns{heartbeat: true})
		if _, err := pool.Exec(ctx, `UPDATE public.sync_run_units SET cost_class = 'light' WHERE sync_run_id = $1::uuid AND status = 'planned'`, lightRun); err != nil {
			t.Fatal(err)
		}
		seedHeldBackNewerRun(t, ctx, pool, now)
		for i := 0; i < 3; i++ {
			insertGuardUnit(t, ctx, pool, guardUnitID("00014403b1", i), "github", "light", syncRunUnitStatusPlanned, now, nil, nil, nil)
		}
		var logs bytes.Buffer
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		before := guardStatementTimeout(t, ctx, tx)
		decision, err := authorizeRun(ctx, tx, synclog.New(slog.New(slog.NewJSONHandler(&logs, nil))), guardTestOrg, guardTestRun, now)
		if err != nil {
			t.Fatalf("authorizeRun: %v", err)
		}
		if !reflect.DeepEqual(decision.heldBackRunIDs, []string{lightRun, heldBackOlderRun}) {
			t.Fatalf("heldBackRunIDs = %v, want [%s %s] (buckets in light, medium order)", decision.heldBackRunIDs, lightRun, heldBackOlderRun)
		}
		lines := map[string]map[string]any{}
		for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			var record map[string]any
			if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "dispatch_guard.bucket_decision" {
				lines[record["bucket"].(string)] = record
			}
		}
		for bucket, want := range map[string][3]any{
			"github:medium": {float64(2), "14403a0e", "wake_armed"},
			"github:light":  {float64(2), "14403d0e", "unit_heartbeat"},
		} {
			line := lines[bucket]
			if line == nil || line["guard.held_back"] != want[0] || line["guard.oldest_held_run"] != want[1] || line["guard.oldest_held_signal"] != want[2] {
				t.Fatalf("%s line = %v, want held_back=%v run=%v signal=%v:\n%s", bucket, line, want[0], want[1], want[2], logs.String())
			}
		}
		if after := guardStatementTimeout(t, ctx, tx); after != before {
			t.Fatalf("guard statement_timeout after two rechecks = %q, want %q", after, before)
		}
		assertNoRecheckSavepointOpen(t, ctx, tx)
		if _, err := tx.Exec(ctx, `UPDATE public.sync_run_units SET status = 'dispatching', updated_at = $2 WHERE sync_run_id = $1::uuid AND status = 'planned' AND id = ANY($3::uuid[])`,
			guardTestRun, now, []string{guardUnitID("00014403b0", 0), guardUnitID("00014403b1", 0)}); err != nil {
			t.Fatalf("claim after two rechecks: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit after two rechecks: %v", err)
		}
	})
}
