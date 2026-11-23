//go:build integration

package syncdispatchruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
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
	if line["guard.held_back"] != heldBack || line["guard.held_back_run"] != run || line["guard.held_back_signal"] != signal {
		t.Fatalf("bucket_decision: guard.held_back=%v guard.held_back_run=%v guard.held_back_signal=%v, want %v %q %q:\n%s",
			line["guard.held_back"], line["guard.held_back_run"], line["guard.held_back_signal"], heldBack, run, signal, raw)
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
