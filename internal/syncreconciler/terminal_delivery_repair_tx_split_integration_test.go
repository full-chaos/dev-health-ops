//go:build integration

package syncreconciler

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// CHAOS-6957. Before this ticket, terminal_delivery_repair.go's Step opened ONE transaction, ran the
// three River-terminal recovery branches (repairTerminalRiverDeliverySQL, UPDATE ... RETURNING) in
// it, then ran repairReadyFinalizers in the SAME transaction, then issued ONE tx.Commit covering
// both. CHAOS-6932's classification (Linear comment 90f7c28d, rev 191, executed) showed the
// ready-finalizer loop alone spending the whole 750ms StageTerminalDeliveryRepair budget
// (DefaultStageBudgets) in serial per-candidate statements, so Commit ran into the expired context
// and rolled back. CHAOS-6956 fixed the specific N+1 readiness read that consumed the budget in that
// sample, but the STRUCTURAL exposure named in the same comment survived that fix: whatever else
// makes the ready-finalizer loop slow (many candidates whose write path -- lockFinalizeJobSQL +
// rearmReadyFinalizeSQL, still one round trip each per READY candidate -- or per-statement latency
// under load, the same order of magnitude the classification measured, 2-100ms) could still exhaust
// the same fixed budget and roll back a River-terminal recovery that had already succeeded earlier in
// the SAME transaction, moments before the commit that discarded it.
//
// This file is the executed, red-first repro CHAOS-6957 required before any fix, and now pins the
// fix as a regression test: it reproduces the exact prod shape (a slow, serial, per-candidate
// ready-finalizer write path; the real 750ms StageTerminalDeliveryRepair budget) and asserts a real,
// otherwise-valid exhausted-delivery recovery survives an unrelated stage's slowness. RED on baseline
// origin/main d8001da14f655090839a25ffb23919b050b6e817 (executed: the outbox row stayed 'dispatched'
// after Step's error, proving the discard); GREEN once Step split into stepRiverTerminalBranches and
// stepReadyFinalizers, each with its own transaction and commit.
//
// slowFinalizeRearmTriggerSQL installs a per-statement delay on exactly the write repairReadyFinalizers
// issues for a READY candidate (the UPDATE that rearms a finalize_sync_run outbox row to 'pending').
// This is a test-only fixture, not a change to production code or its SQL text: it makes the loop's
// real, serial, per-candidate cost deterministic and host-latency-independent, at the same order of
// magnitude CHAOS-6932 measured (2-100ms per statement) instead of depending on this host's own
// network/DB latency (which is far below prod's, and would make a real-latency repro flaky).
const slowFinalizeRearmTriggerSQL = `
CREATE OR REPLACE FUNCTION test_slow_finalize_rearm() RETURNS trigger AS $$
BEGIN
	PERFORM pg_sleep(0.08);
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER test_slow_finalize_rearm
	BEFORE UPDATE ON public.sync_dispatch_outbox
	FOR EACH ROW
	WHEN (NEW.kind = 'finalize_sync_run' AND NEW.status = 'pending')
	EXECUTE FUNCTION test_slow_finalize_rearm();
`

// seedExhaustedPostSyncCandidate seeds a River-terminal recovery candidate for
// repairTerminalRiverDeliverySQL's exhausted-attempt-budget branch: a 'post_sync' outbox row
// (post_sync carries a seeded route in this harness, per backstopRoutes) whose River job spent its
// whole attempt budget. On its own, with an ordinary budget, this recovers under
// riverDeliveryExhaustedEvidence -- proven by the sibling non-shared-load run in this same test.
func seedExhaustedPostSyncCandidate(
	t *testing.T, ctx context.Context, h *finalizeBackstopHarness, now time.Time, runID, outboxID string, generation int64,
) {
	t.Helper()
	seedRun(t, ctx, h.admin, runID, "dispatching", now.Add(-2*time.Hour))
	args := syncdispatchruntime.PostSyncArgs{TransportArgs: syncdispatchruntime.TransportArgs{
		Version: 1, OrgID: "00000000-0000-4000-8000-000000000001", RunID: runID,
		DispatchOutbox: outboxID, DeliveryAttempt: 1, RouteGeneration: generation,
	}}
	inserted, err := h.river.Insert(ctx, args, &river.InsertOpts{Queue: "sync"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.admin.Exec(ctx, `
		UPDATE river.river_job
		SET state = 'discarded', finalized_at = $2, attempt = 5, max_attempts = 5,
			errors = ARRAY[jsonb_build_object('error', 'sync dispatch bridge request failed: status=503', 'attempt', 5)]
		WHERE id = $1`,
		inserted.Job.ID, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.admin.Exec(ctx, `INSERT INTO public.sync_dispatch_outbox
        (id,org_id,sync_run_id,kind,status,available_at,attempts,dispatched_at,dispatched_transport,dispatched_route_generation,transport_job_id,created_at,updated_at)
        VALUES ($1,'org-materializer',$2,'post_sync','dispatched',$3,1,$3,'river',$5,$4,$3,$3)`,
		outboxID, runID, now.Add(-time.Hour), strconv.FormatInt(inserted.Job.ID, 10), generation); err != nil {
		t.Fatal(err)
	}
}

func postSyncRouteGeneration(t *testing.T, ctx context.Context, h *finalizeBackstopHarness) int64 {
	t.Helper()
	var generation int64
	if err := h.admin.QueryRow(ctx,
		`SELECT generation FROM public.sync_dispatch_transport_routes WHERE kind = 'post_sync'`,
	).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	return generation
}

func TestTerminalDeliveryRepairRecoverySurvivesASlowReadyFinalizerLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	h := startFinalizeBackstopHarness(t, ctx)
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	resetMaterializerIntegrationTables(t, ctx, h.admin)
	if _, err := h.admin.Exec(ctx, `TRUNCATE river.river_job`); err != nil {
		t.Fatal(err)
	}
	finalizeGeneration := backstopRoutes(t, ctx, h.admin)
	postSyncGeneration := postSyncRouteGeneration(t, ctx, h)

	// Sibling control, run FIRST (before the shared-budget candidate exists, so this Step call sees
	// only its own candidate) against a fresh, unshared budget: proves the seeded candidate SHAPE is a
	// genuine, valid recovery on its own -- the failure below is caused by the shared transaction and
	// commit, not by an invalid fixture.
	t.Run("control: the same recovery shape succeeds on its own, ample budget", func(t *testing.T) {
		const controlRunID = "00000000-0000-4000-9100-000000000099"
		const controlOutboxID = "00000000-0000-4000-9101-000000000099"
		seedExhaustedPostSyncCandidate(t, ctx, h, now, controlRunID, controlOutboxID, postSyncGeneration)
		result, err := h.repair.Step(ctx, now, 10)
		if err != nil {
			t.Fatalf("Step() error = %v", err)
		}
		if result.Recovered != 1 || result.ExhaustedRecovered != 1 {
			t.Fatalf("result = %+v, want exactly the seeded exhausted delivery recovered", result)
		}
		var status string
		if err := h.admin.QueryRow(ctx, `SELECT status FROM public.sync_dispatch_outbox WHERE id = $1`,
			controlOutboxID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "pending" {
			t.Fatalf("control candidate status = %q, want pending (recovered)", status)
		}
	})

	// Now seed the shared-budget candidate, install the slow write, and 15 READY finalizer candidates:
	// production's own write path
	// (repairReadyFinalizers's per-READY-candidate lockFinalizeJobSQL + rearmReadyFinalizeSQL), at the
	// same order of magnitude CHAOS-6932 measured (2-100ms/statement), unmodified from what ships --
	// only the per-statement cost is injected, not the code path.
	const exhaustedRunID = "00000000-0000-4000-9100-000000000001"
	const exhaustedOutboxID = "00000000-0000-4000-9101-000000000001"
	seedExhaustedPostSyncCandidate(t, ctx, h, now, exhaustedRunID, exhaustedOutboxID, postSyncGeneration)

	if _, err := h.admin.Exec(ctx, slowFinalizeRearmTriggerSQL); err != nil {
		t.Fatal(err)
	}
	const readyCandidateCount = 15
	for i := 0; i < readyCandidateCount; i++ {
		seedReadyFinalizeCandidate(t, ctx, h, now, 9200+i, finalizeGeneration, true)
	}

	// The real production stage budget (DefaultStageBudgets, stage_budget.go) -- not an arbitrary
	// short deadline chosen to force a timeout. 15 candidates x 80ms >= 1.2s of serial per-candidate
	// write cost against a 750ms budget is exactly CHAOS-6932's shape: the loop's own statement time,
	// not pool wait, exhausts the stage before Commit runs.
	budget := DefaultStageBudgets()[StageTerminalDeliveryRepair]
	stepCtx, stepCancel := context.WithTimeout(ctx, budget)
	defer stepCancel()
	result, err := h.repair.Step(stepCtx, now, readyCandidateCount+5)
	t.Logf("Step() under a %s budget with %d ready finalizer candidates at 80ms/write: result=%+v err=%v",
		budget, readyCandidateCount, result, err)

	// r1 P3: this test claims to exercise a shared-budget failure, not merely check that a recovery
	// happens to survive an untimed pass. Without this assertion, a change that made the injected
	// delay a no-op (or the budget effectively unlimited) would still pass -- the status assertion
	// below would hold vacuously, for a reason unrelated to CHAOS-6957 at all. Pin the failure itself:
	// under this exact budget and load, the ready-finalizer half must actually fail.
	if err == nil {
		t.Fatalf("Step() succeeded with no error under a %s budget and %d x 80ms serial ready-finalizer "+
			"writes (result=%+v) -- this test's premise is a shared-budget failure; without one, the "+
			"status assertion below proves nothing about CHAOS-6957", budget, readyCandidateCount, result)
	}

	var status string
	if err := h.admin.QueryRow(ctx, `SELECT status FROM public.sync_dispatch_outbox WHERE id = $1`,
		exhaustedOutboxID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	// This is the invariant CHAOS-6957 guarantees: a River-terminal recovery that the candidates CTE
	// already matched and the UPDATE already applied must survive regardless of what an unrelated,
	// later stage of the same Step call does. Before the fix (baseline origin/main
	// d8001da14f655090839a25ffb23919b050b6e817, executed) it did not: the ready-finalizer loop's own
	// slowness (from candidates it selected AFTER the exhausted delivery's UPDATE already ran, and had
	// nothing to do with it) shared the same transaction and commit, so exhausting the budget there
	// discarded an already-successful recovery. stepRiverTerminalBranches now commits before
	// stepReadyFinalizers ever begins, so the two can no longer take each other down.
	if status != "pending" {
		t.Fatalf("exhausted-delivery outbox %s status = %q, want pending -- CHAOS-6957 regression: "+
			"the recovery this transaction's own UPDATE already applied was rolled back by the SAME "+
			"commit that the SLOW, UNRELATED ready-finalizer loop (result=%+v err=%v) also shares",
			exhaustedOutboxID, status, result, err)
	}
}

// A guard against the fixture itself silently seeding zero ready candidates (e.g. a schema change
// that makes seedReadyFinalizeCandidate's own inserts fail without seedReadyFinalizeCandidate's
// helper noticing -- it calls t.Fatal on error, but the WHEN-guarded trigger firing zero times would
// still let this test pass vacuously if the seeded candidates were never actually written).
func TestSlowFinalizeRearmTriggerFiresOncePerReadyCandidate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	h := startFinalizeBackstopHarness(t, ctx)
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	resetMaterializerIntegrationTables(t, ctx, h.admin)
	if _, err := h.admin.Exec(ctx, `TRUNCATE river.river_job`); err != nil {
		t.Fatal(err)
	}
	generation := backstopRoutes(t, ctx, h.admin)
	if _, err := h.admin.Exec(ctx, `
		CREATE TABLE test_slow_finalize_rearm_hits (id serial primary key);
		CREATE OR REPLACE FUNCTION test_slow_finalize_rearm_count() RETURNS trigger AS $$
		BEGIN
			INSERT INTO test_slow_finalize_rearm_hits DEFAULT VALUES;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public;
		CREATE TRIGGER test_slow_finalize_rearm_count
			BEFORE UPDATE ON public.sync_dispatch_outbox
			FOR EACH ROW
			WHEN (NEW.kind = 'finalize_sync_run' AND NEW.status = 'pending')
			EXECUTE FUNCTION test_slow_finalize_rearm_count();
	`); err != nil {
		t.Fatal(err)
	}
	const n = 5
	for i := 0; i < n; i++ {
		seedReadyFinalizeCandidate(t, ctx, h, now, 9300+i, generation, true)
	}
	result, err := h.repair.Step(ctx, now, n+5)
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	if result.ReadyFinalizersRecovered != n {
		t.Fatalf("ReadyFinalizersRecovered = %d, want %d", result.ReadyFinalizersRecovered, n)
	}
	var hits int
	if err := h.admin.QueryRow(ctx, `SELECT count(*) FROM test_slow_finalize_rearm_hits`).Scan(&hits); err != nil {
		t.Fatal(err)
	}
	if hits != n {
		t.Fatalf("trigger fired %d times for %d ready candidates, want exactly %d (one write each)", hits, n, n)
	}
}
