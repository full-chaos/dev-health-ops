//go:build integration

package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncrunrollup"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-8880 liveness cells: an older run holds free slots back only while one progress sign of olderRunsInBucketSQL
// falls inside the bound. Each cell gives the older run exactly ONE recent sign (or none); its sign units sit in the
// light bucket so they never use a medium slot. The newer guard run decides the medium bucket (cap 2).

type livenessSign string

const (
	signCreated     livenessSign = "created"
	signTerminal    livenessSign = "terminal"
	signHeartbeat   livenessSign = "heartbeat"
	signLease       livenessSign = "lease"
	signDispatching livenessSign = "dispatching"
	signNone        livenessSign = "none"
	// signBackoff: the finalize row was created long ago and the relay keeps rewriting its updated_at (publish-failure
	// backoff); no sign is inside the bound.
	signBackoff livenessSign = "backoff"
)

func seedLivenessCase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time, sign livenessSign) {
	t.Helper()
	recent, old := now.Add(-time.Minute), now.Add(-2*time.Hour)
	created := old
	if sign == signCreated {
		created = recent
	}
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: admissionOlderRun, OrgID: guardTestOrg})
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed %s: %v", sign, err)
		}
	}
	exec(`UPDATE public.sync_runs SET triggered_by = 'schedule', mode = 'incremental', status = 'dispatching', created_at = $2 WHERE id = $1::uuid`,
		admissionOlderRun, created)
	exec(`UPDATE public.sync_runs SET triggered_by = 'schedule', mode = 'incremental', status = 'dispatching', created_at = $2 WHERE id = $1::uuid`,
		guardTestRun, now.Add(-10*time.Second))
	for i := 0; i < 2; i++ {
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
			ID: guardUnitID("00000088a0", i), RunID: admissionOlderRun, OrgID: guardTestOrg, Provider: "github", CostClass: "medium",
			Status: syncRunUnitStatusPlanned, UpdatedAt: &old,
		})
	}
	for i := 0; i < 3; i++ {
		insertGuardUnit(t, ctx, pool, guardUnitID("00000088b0", i), "github", "medium", syncRunUnitStatusPlanned, now, nil, nil, nil)
	}

	at := func(want livenessSign) time.Time {
		if sign == want {
			return recent
		}
		return old
	}
	// terminal: a finished unit whose terminal write armed the finalize row (the real writers do both in one tx).
	terminalAt := at(signTerminal)
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: guardUnitID("00000088c0", 0), RunID: admissionOlderRun, OrgID: guardTestOrg, Provider: "github", CostClass: "light",
		Status: "success", UpdatedAt: &terminalAt,
	})
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := syncrunrollup.ArmFinalize(ctx, tx, admissionOlderRun, guardTestOrg, terminalAt); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("arm finalize: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if sign == signBackoff {
		exec(`UPDATE public.sync_dispatch_outbox
SET status = 'pending', attempts = 7, last_error = 'publish_failed', available_at = $2, updated_at = $3
WHERE sync_run_id = $1::uuid AND kind = 'finalize_sync_run'`, admissionOlderRun, now.Add(5*time.Minute), recent)
	}
	// heartbeat: a running unit whose lease has expired; only its heartbeat can be recent.
	expired := now.Add(-time.Minute)
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: guardUnitID("00000088c1", 0), RunID: admissionOlderRun, OrgID: guardTestOrg, Provider: "github", CostClass: "light",
		Status: syncRunUnitStatusRunning, UpdatedAt: &old, LeaseExpiresAt: &expired, LeaseOwner: ptrString("hb-owner"),
	})
	exec(`UPDATE public.sync_run_units SET last_heartbeat_at = $2 WHERE id = $1::uuid`, guardUnitID("00000088c1", 0), at(signHeartbeat))
	// lease: a running unit with no recent heartbeat; only its lease can be live.
	lease := now.Add(-30 * time.Minute)
	if sign == signLease {
		lease = now.Add(time.Hour)
	}
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: guardUnitID("00000088c2", 0), RunID: admissionOlderRun, OrgID: guardTestOrg, Provider: "github", CostClass: "light",
		Status: syncRunUnitStatusRunning, UpdatedAt: &old, LeaseExpiresAt: &lease, LeaseOwner: ptrString("lease-owner"),
	})
	exec(`UPDATE public.sync_run_units SET last_heartbeat_at = $2 WHERE id = $1::uuid`, guardUnitID("00000088c2", 0), old)
	// dispatching: a claimed unit; only its claim time can be recent.
	dispatchingAt := at(signDispatching)
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: guardUnitID("00000088c3", 0), RunID: admissionOlderRun, OrgID: guardTestOrg, Provider: "github", CostClass: "light",
		Status: syncRunUnitStatusDispatching, UpdatedAt: &dispatchingAt,
	})
}

func authorizeLivenessCase(t *testing.T, ctx context.Context, pool *pgxpool.Pool, now time.Time) guardDecision {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	decision, err := authorizeRun(ctx, tx, nil, guardTestOrg, guardTestRun, now)
	if err != nil {
		t.Fatalf("authorizeRun: %v", err)
	}
	return decision
}

func assertLivenessHolds(t *testing.T, sign livenessSign) {
	t.Helper()
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedLivenessCase(t, ctx, pool, now, sign)
		decision := authorizeLivenessCase(t, ctx, pool, now)
		if len(decision.cappedUnitIDs) != 3 || !reflect.DeepEqual(decision.heldBackRunIDs, []string{admissionOlderRun}) {
			t.Fatalf("older run whose only recent sign is %q: capped=%d heldBack=%v, want 3 and [%s] (the sign keeps it live)",
				sign, len(decision.cappedUnitIDs), decision.heldBackRunIDs, admissionOlderRun)
		}
	})
}

func TestOlderRunLiveByCreationHoldsBack(t *testing.T)     { assertLivenessHolds(t, signCreated) }
func TestOlderRunLiveByTerminalUnitHoldsBack(t *testing.T) { assertLivenessHolds(t, signTerminal) }
func TestOlderRunLiveByHeartbeatHoldsBack(t *testing.T)    { assertLivenessHolds(t, signHeartbeat) }
func TestOlderRunLiveByLeaseHoldsBack(t *testing.T)        { assertLivenessHolds(t, signLease) }
func TestOlderRunLiveByDispatchingHoldsBack(t *testing.T)  { assertLivenessHolds(t, signDispatching) }

// TestOlderRunWhoseFinalizeRowOnlyBacksOffHoldsNothing (CHAOS-8880 Q1): the relay rewrites the finalize row's
// updated_at on every publish-failure backoff. That is not progress: with every real sign past the bound, the older
// run holds nothing back.
func TestOlderRunWhoseFinalizeRowOnlyBacksOffHoldsNothing(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedLivenessCase(t, ctx, pool, now, signBackoff)
		decision := authorizeLivenessCase(t, ctx, pool, now)
		if len(decision.cappedUnitIDs) != 1 || len(decision.heldBackRunIDs) != 0 {
			t.Fatalf("older run whose finalize row only backs off: capped=%d heldBack=%v, want 1 and none (cap 2, 3 planned)",
				len(decision.cappedUnitIDs), decision.heldBackRunIDs)
		}
	})
}

// TestOlderRunWaitingPastTheScanLimitHoldsNothing (CHAOS-8880 Q2, accepted fail-open): the waiting count reads at most
// olderRunWaitingScanLimit planned or retrying units of an older run, with no bucket filter in the index. A live older
// run whose first 300 planned units are in another bucket and whose 2 medium units lie past them holds nothing back
// in medium -- the behavior before CHAOS-8880, never more hold-back. This pins the limit until an index lands.
func TestOlderRunWaitingPastTheScanLimitHoldsNothing(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		for i := 0; i < 3; i++ {
			insertGuardUnit(t, ctx, pool, guardUnitID("00000088e3", i), "github", "medium", syncRunUnitStatusPlanned, now, nil, nil, nil)
		}
		pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: admissionOlderRun, OrgID: guardTestOrg})
		for _, statement := range []struct {
			sql  string
			args []any
		}{
			{`UPDATE public.sync_runs SET triggered_by = 'schedule', mode = 'incremental', status = 'dispatching', created_at = now() - interval '3 hours' WHERE id = $1::uuid`,
				[]any{admissionOlderRun}},
			{`UPDATE public.sync_runs SET triggered_by = 'schedule', mode = 'incremental', status = 'dispatching', created_at = now() - interval '10 seconds' WHERE id = $1::uuid`,
				[]any{guardTestRun}},
			{`INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, created_at, updated_at)
			 SELECT md5('past-scan-8880-' || k)::uuid, run.org_id, run.id, run.integration_id,
				(SELECT source_id FROM public.sync_run_units WHERE sync_run_id = $2::uuid LIMIT 1),
				'github', 'dataset-' || k, CASE WHEN k <= 300 THEN 'light' ELSE 'medium' END, 'incremental', 'planned', 0,
				now() - interval '3 hours', now() - interval '3 hours'
			 FROM public.sync_runs AS run CROSS JOIN generate_series(1, 302) AS k
			 WHERE run.id = $1::uuid
			 ORDER BY k`,
				[]any{admissionOlderRun, guardTestRun}},
		} {
			if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
				t.Fatalf("seed past-scan run: %v", err)
			}
		}
		liveLease := now.Add(time.Hour)
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
			ID: guardUnitID("00000088e4", 0), RunID: admissionOlderRun, OrgID: guardTestOrg, Provider: "github", CostClass: "light",
			Status: syncRunUnitStatusRunning, UpdatedAt: &now, LeaseExpiresAt: &liveLease, LeaseOwner: ptrString("past-scan-owner"),
		})
		decision := authorizeLivenessCase(t, ctx, pool, now)
		if len(decision.cappedUnitIDs) != 1 || len(decision.heldBackRunIDs) != 0 {
			t.Fatalf("older live run whose medium units lie past the scan limit: capped=%d heldBack=%v, want 1 and none",
				len(decision.cappedUnitIDs), decision.heldBackRunIDs)
		}
	})
}

func TestOlderRunWithEverySignPastTheBoundHoldsNothing(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		seedLivenessCase(t, ctx, pool, now, signNone)
		decision := authorizeLivenessCase(t, ctx, pool, now)
		if len(decision.cappedUnitIDs) != 1 || len(decision.heldBackRunIDs) != 0 {
			t.Fatalf("older run with every sign past the bound: capped=%d heldBack=%v, want 1 and none (cap 2, 3 planned)",
				len(decision.cappedUnitIDs), decision.heldBackRunIDs)
		}
	})
}

// planNode is the part of an EXPLAIN (FORMAT JSON) node the row-visit count needs.
type planNode struct {
	RelationName        string     `json:"Relation Name"`
	ActualRows          float64    `json:"Actual Rows"`
	ActualLoops         float64    `json:"Actual Loops"`
	RowsRemovedByFilter float64    `json:"Rows Removed by Filter"`
	Plans               []planNode `json:"Plans"`
}

// rowVisits sums, over every node that reads table, the rows it returned plus the rows its filter removed, times its
// loops.
func rowVisits(node planNode, table string) float64 {
	total := 0.0
	if node.RelationName == table {
		total += (node.ActualRows + node.RowsRemovedByFilter) * node.ActualLoops
	}
	for _, child := range node.Plans {
		total += rowVisits(child, table)
	}
	return total
}

// TestOlderRunsInBucketReadIsBoundedOnALargeOpenRun (CHAOS-8880 F1): one OPEN, live older run of 10,000 units (9,990
// planned in another bucket, 10 finished, 1 running) is read on every medium decision of a newer run. The read must
// visit a bounded number of its units, not all of them.
func TestOlderRunsInBucketReadIsBoundedOnALargeOpenRun(t *testing.T) {
	withGuardPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		insertGuardUnit(t, ctx, pool, guardUnitID("00000088d0", 0), "github", "medium", syncRunUnitStatusPlanned, now, nil, nil, nil)
		pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: admissionOlderRun, OrgID: guardTestOrg})
		for _, statement := range []struct {
			sql  string
			args []any
		}{
			{`UPDATE public.sync_runs SET triggered_by = 'backfill', mode = 'backfill', status = 'dispatching', created_at = now() - interval '3 hours' WHERE id = $1::uuid`,
				[]any{admissionOlderRun}},
			{`UPDATE public.sync_runs SET triggered_by = 'backfill', mode = 'backfill', status = 'dispatching', created_at = now() WHERE id = $1::uuid`,
				[]any{guardTestRun}},
			{`INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, attempts, created_at, updated_at)
			 SELECT md5('large-open-8880-' || k)::uuid, run.org_id, run.id, run.integration_id,
				(SELECT source_id FROM public.sync_run_units WHERE sync_run_id = $2::uuid LIMIT 1),
				'github', 'dataset-' || k, 'light', 'backfill', CASE WHEN k <= 10 THEN 'success' ELSE 'planned' END, 0,
				now() - interval '3 hours', now() - interval '3 hours'
			 FROM public.sync_runs AS run CROSS JOIN generate_series(1, 10000) AS k
			 WHERE run.id = $1::uuid`,
				[]any{admissionOlderRun, guardTestRun}},
		} {
			if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
				t.Fatalf("seed large open run: %v", err)
			}
		}
		// The large run is live (a running unit holds a live lease), so the read must look for its waiting units; a
		// dead run is dropped before its units are read at all.
		liveLease := now.Add(time.Hour)
		pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
			ID: guardUnitID("00000088d1", 0), RunID: admissionOlderRun, OrgID: guardTestOrg, Provider: "github", CostClass: "light",
			Status: syncRunUnitStatusRunning, UpdatedAt: &now, LeaseExpiresAt: &liveLease, LeaseOwner: ptrString("large-owner"),
		})
		if _, err := pool.Exec(ctx, `ANALYZE`); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		self, err := readRunOrder(ctx, tx, guardTestRun)
		if err != nil {
			t.Fatal(err)
		}
		bucket := dispatchBucket{orgID: guardTestOrg, provider: "github", costClass: "medium"}
		var raw string
		if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+olderRunsInBucketSQL,
			bucket.orgID, bucket.provider, bucket.costClass, self.createdAt, self.id, self.backfill, now,
			concurrencyCapForCostClass(bucket.costClass), admissionLivenessBound().Seconds(), olderRunsReadLimit, olderRunWaitingScanLimit,
		).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var plans []struct {
			Plan planNode `json:"Plan"`
		}
		if err := json.Unmarshal([]byte(raw), &plans); err != nil || len(plans) != 1 {
			t.Fatalf("parse plan: %v (%d plans)", err, len(plans))
		}
		visits := rowVisits(plans[0].Plan, "sync_run_units")
		const bound = 2 * olderRunWaitingScanLimit
		t.Logf("sync_run_units rows visited: %.0f (bound %d)", visits, bound)
		if visits > bound {
			t.Fatalf("the older-run read visits %.0f sync_run_units rows of an open 10,000-unit run, want <= %d:\n%s", visits, bound, raw)
		}
		older, err := readOlderRuns(ctx, tx, bucket, self, now)
		if err != nil {
			t.Fatal(err)
		}
		if len(older) != 0 {
			t.Fatalf("older runs = %s, want none: the large run waits only in the light bucket", fmt.Sprint(older))
		}
	})
}
