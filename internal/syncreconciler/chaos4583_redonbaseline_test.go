//go:build integration

package syncreconciler

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestCHAOS4583RedOnBaselineOutboxReachesTerminalStatus is the GREEN half of
// the CHAOS-4583 behavioral red-on-baseline proof. The RED half is the
// byte-identical test (same seed, same 5 Materializer ticks, same
// assertion), run unmodified against origin/main in a detached worktree,
// where it fails with a genuine runtime assertion failure (row stays
// 'dispatched') -- not a compile error -- because nothing in that checkout's
// reconciler pipeline (Materializer, or any other stage) ever closes a
// 'dispatched' row whose owner has gone terminal. See
// terminal_outbox_close.go's package doc for the root cause.
//
// The only difference from the RED version is the one line this ticket
// exists to add: a TerminalOutboxClose.Step call after the same 5 ticks.
// Everything else -- fixture, seed, tick count, assertion -- is identical.
func TestCHAOS4583RedOnBaselineOutboxReachesTerminalStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := createMaterializerIntegrationFixture(ctx, pool); err != nil {
		t.Fatal(err)
	}
	resetMaterializerIntegrationTables(t, ctx, pool)

	materializer, err := NewMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	runID := "00000000-0000-4000-8000-0000000045f1"

	// The CHAOS-4583 root-cause shape: run 'success' (terminal), reference
	// discovery ledger 'success' (terminal, via stampSuccess), outbox row
	// 'dispatched' -- exactly what stampSuccess leaves behind.
	seedRun(t, ctx, pool, runID, "success", now.Add(-8*time.Hour))
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_run_reference_discoveries (
			sync_run_id, status, available_at
		) VALUES ($1, 'success', $2)`, runID, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-materializer', $1, 'reference_discovery',
			'dispatched', $2, 1, $2, 'river', 1, 'river-job-1', $2, $2
		)`, runID, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	// Five simulated reconciler ticks -- the real, unmodified Materializer,
	// run repeatedly, exactly as the production reconciler loop does every
	// second. Byte-identical to the RED version run against origin/main.
	for tick := 0; tick < 5; tick++ {
		if _, err := materializer.Step(ctx, now, now.Add(-time.Hour), 100); err != nil {
			t.Fatalf("tick %d: materializer step: %v", tick, err)
		}
	}

	// THE FIX (CHAOS-4583): the one call origin/main has no equivalent of.
	closer, err := NewTerminalOutboxClose(pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := closer.Step(ctx, now, 100); err != nil {
		t.Fatalf("terminal outbox close step: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, `
		SELECT status FROM public.sync_dispatch_outbox
		WHERE sync_run_id = $1 AND kind = 'reference_discovery'`, runID,
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "closed" {
		t.Fatalf(
			"CHAOS-4583 RED-ON-BASELINE (expected on this checkout): outbox row status = %q after 5 reconciler ticks, want %q -- "+
				"the row's owner (run AND reference-discovery ledger) reached a terminal status, but nothing in this checkout's "+
				"reconciler pipeline (Materializer, or any other stage) ever closes a 'dispatched' row. This is the exact "+
				"CHAOS-4583 defect: the row is stranded forever.",
			status, "closed",
		)
	}
}
