//go:build integration

package admin

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// An org delete removes the org's post_sync ledger rows and its outbox rows together, leaves the other org's rows,
// and leaves no ledger or outbox row without its run.
func TestOrgDeletionRemovesLedgerAndOutboxRowsTogether(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)

	target, control := uuid.New(), uuid.New()
	seed := func(org uuid.UUID, runs int) {
		integrationID, _ := pgseed.EnsureSyncIntegration(ctx, t, pool, org.String(), uuid.NewString(), uuid.NewString())
		for i := 0; i < runs; i++ {
			runID := uuid.NewString()
			pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: runID, OrgID: org.String(), IntegrationID: integrationID, Status: "success"})
			if _, err := pool.Exec(ctx, `INSERT INTO public.sync_run_post_dispatches (id, org_id, sync_run_id, kind, dispatched_at)
VALUES (gen_random_uuid(), $1, $2, 'post_sync', now())`, org.String(), runID); err != nil {
				t.Fatal(err)
			}
			pgseed.SyncDispatchOutbox(ctx, t, pool, uuid.NewString(), runID, org.String(), "post_sync", "pending", "", 0)
		}
	}
	seed(target, 3)
	seed(control, 2)

	count := func(sql string, args ...any) int {
		var n int
		if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	snapshot := func(org uuid.UUID) string {
		return fmt.Sprintf("ledger=%d outbox=%d runs=%d",
			count(`SELECT count(*) FROM sync_run_post_dispatches WHERE org_id = $1`, org.String()),
			count(`SELECT count(*) FROM sync_dispatch_outbox WHERE org_id = $1`, org.String()),
			count(`SELECT count(*) FROM sync_runs WHERE org_id = $1`, org.String()))
	}
	before, controlBefore := snapshot(target), snapshot(control)
	t.Logf("before: target %s | control %s", before, controlBefore)
	if before != "ledger=3 outbox=3 runs=3" || controlBefore != "ledger=2 outbox=2 runs=2" {
		t.Fatalf("seed counts wrong: %s | %s", before, controlBefore)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, tgt := range orgDeletionTargets {
		if _, err := tx.Exec(ctx, tgt.deleteSQL(), tgt.bindValue(target, target.String())); err != nil {
			t.Fatalf("delete %s: %v", tgt.Table, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	after, controlAfter := snapshot(target), snapshot(control)
	t.Logf("after:  target %s | control %s", after, controlAfter)
	if after != "ledger=0 outbox=0 runs=0" {
		t.Errorf("target after = %s", after)
	}
	if controlAfter != controlBefore {
		t.Errorf("control changed: %s -> %s", controlBefore, controlAfter)
	}
	orphans := count(`SELECT (SELECT count(*) FROM sync_run_post_dispatches l WHERE NOT EXISTS (SELECT 1 FROM sync_runs r WHERE r.id = l.sync_run_id))
 + (SELECT count(*) FROM sync_dispatch_outbox o WHERE NOT EXISTS (SELECT 1 FROM sync_runs r WHERE r.id = o.sync_run_id))`)
	t.Logf("orphan ledger+outbox rows: %d", orphans)
	if orphans != 0 {
		t.Errorf("orphans = %d", orphans)
	}
}
