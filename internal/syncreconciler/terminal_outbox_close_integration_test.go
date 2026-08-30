//go:build integration

package syncreconciler

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	outboxCloseDispatchTerminal    = "00000000-0000-4000-8000-000000004501"
	outboxCloseFinalizeTerminal    = "00000000-0000-4000-8000-000000004502"
	outboxClosePostSyncTerminal    = "00000000-0000-4000-8000-000000004503"
	outboxCloseDiscoveryTerminal   = "00000000-0000-4000-8000-000000004504"
	outboxCloseDispatchNonTerminal = "00000000-0000-4000-8000-000000004505"
	outboxCloseDispatchLiveClaim   = "00000000-0000-4000-8000-000000004506"
	outboxCloseDiscoveryLiveLedger = "00000000-0000-4000-8000-000000004507"
	outboxClosePending             = "00000000-0000-4000-8000-000000004508"
)

// seedOutboxCloseDispatchedRow inserts a sync_dispatch_outbox row in the
// exact shape markRiverDispatchedSQL / a completed Celery delivery leaves
// behind: status 'dispatched', no live claim, unless claimExpiresAt is
// supplied (the CHAOS-4583 risk-note guard: a live claim must block the
// close even when the owner has gone terminal).
func seedOutboxCloseDispatchedRow(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	runID, kind string,
	dispatchedAt time.Time,
	claimExpiresAt *time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			dispatched_at, dispatched_transport, dispatched_route_generation,
			transport_job_id, claim_token, claim_expires_at, claim_transport,
			claim_route_generation, created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-outbox-close', $1, $2,
			'dispatched', $3, 1, $3, 'river', 1, 'river-job-1',
			CASE WHEN $4::timestamptz IS NULL THEN NULL ELSE 'live-claim-token' END,
			$4, CASE WHEN $4::timestamptz IS NULL THEN NULL ELSE 'river' END,
			CASE WHEN $4::timestamptz IS NULL THEN NULL ELSE 1 END,
			$3, $3
		)`,
		runID, kind, dispatchedAt, claimExpiresAt); err != nil {
		t.Fatal(err)
	}
}

func seedOutboxClosePendingRow(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID, kind string, availableAt time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.sync_dispatch_outbox (
			id, org_id, sync_run_id, kind, status, available_at, attempts,
			created_at, updated_at
		) VALUES (
			gen_random_uuid(), 'org-outbox-close', $1, $2, 'pending', $3, 0, $3, $3
		)`,
		runID, kind, availableAt); err != nil {
		t.Fatal(err)
	}
}

func outboxCloseRowState(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID, kind string,
) (status string, dispatchedTransport, transportJobID, claimToken *string) {
	t.Helper()
	if err := pool.QueryRow(ctx, `
		SELECT status, dispatched_transport, transport_job_id, claim_token
		FROM public.sync_dispatch_outbox
		WHERE sync_run_id = $1 AND kind = $2`,
		runID, kind,
	).Scan(&status, &dispatchedTransport, &transportJobID, &claimToken); err != nil {
		t.Fatal(err)
	}
	return status, dispatchedTransport, transportJobID, claimToken
}

// TestTerminalOutboxCloseClosesRowsWhoseOwnerIsTerminal is the CHAOS-4583
// red-on-baseline pin: on origin/main 2b3032b63, stampSuccess (and every
// other native completion path) arms its successor kind's wakeup but never
// closes the row it was itself dispatched for, so a 'dispatched' row whose
// owner has gone terminal is stranded forever -- this exact scenario (an
// executed local readback found 6568/6568 non-terminal rows). This test
// asserts the FIXED behavior (every kind closes once its owner is terminal),
// which fails to even compile against origin/main (TerminalOutboxClose does
// not exist there) and passes on this branch.
func TestTerminalOutboxCloseClosesRowsWhoseOwnerIsTerminal(t *testing.T) {
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

	closer, err := NewTerminalOutboxClose(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	dispatchedAt := now.Add(-time.Hour)

	// --- Positive cases: one per kind, each a distinct terminal outcome ---
	seedRun(t, ctx, pool, outboxCloseDispatchTerminal, "success", now.Add(-8*time.Hour))
	seedOutboxCloseDispatchedRow(t, ctx, pool, outboxCloseDispatchTerminal, "dispatch_sync_run", dispatchedAt, nil)

	seedRun(t, ctx, pool, outboxCloseFinalizeTerminal, "partial_failed", now.Add(-8*time.Hour))
	seedOutboxCloseDispatchedRow(t, ctx, pool, outboxCloseFinalizeTerminal, "finalize_sync_run", dispatchedAt, nil)

	// The exact CHAOS-4583 root-cause row: the RUN is still running (not
	// terminal), but the reference_discovery LEDGER itself already reached
	// 'success' via stampSuccess -- gated on the ledger, never on run.status.
	seedRun(t, ctx, pool, outboxCloseDiscoveryTerminal, "running", now.Add(-8*time.Hour))
	seedDiscoveryLedger(t, ctx, pool, outboxCloseDiscoveryTerminal, "success", now.Add(-2*time.Hour), nil, 0, nil)
	seedOutboxCloseDispatchedRow(t, ctx, pool, outboxCloseDiscoveryTerminal, "reference_discovery", dispatchedAt, nil)

	// --- Negative controls ---
	// Non-terminal owner: must never be closed.
	seedRun(t, ctx, pool, outboxCloseDispatchNonTerminal, "running", now.Add(-8*time.Hour))
	seedOutboxCloseDispatchedRow(t, ctx, pool, outboxCloseDispatchNonTerminal, "dispatch_sync_run", dispatchedAt, nil)

	// Terminal owner but a LIVE claim outstanding -- the CHAOS-4583 risk
	// note's own guard: an in-flight redelivery attempt must never be closed
	// out from under it.
	liveClaimExpiry := now.Add(time.Hour)
	seedRun(t, ctx, pool, outboxCloseDispatchLiveClaim, "success", now.Add(-8*time.Hour))
	seedOutboxCloseDispatchedRow(t, ctx, pool, outboxCloseDispatchLiveClaim, "dispatch_sync_run", dispatchedAt, &liveClaimExpiry)

	// reference_discovery is gated on ITS OWN ledger, not run.status: a
	// terminal run with a still-running ledger must not be closed.
	seedRun(t, ctx, pool, outboxCloseDiscoveryLiveLedger, "success", now.Add(-8*time.Hour))
	seedDiscoveryLedger(t, ctx, pool, outboxCloseDiscoveryLiveLedger, "running", now.Add(-2*time.Hour), nil, 0, nil)
	seedOutboxCloseDispatchedRow(t, ctx, pool, outboxCloseDiscoveryLiveLedger, "reference_discovery", dispatchedAt, nil)

	// A 'pending' row (never dispatched) on a terminal run: the predicate
	// requires status='dispatched', so this must be left untouched.
	seedRun(t, ctx, pool, outboxClosePending, "success", now.Add(-8*time.Hour))
	seedOutboxClosePendingRow(t, ctx, pool, outboxClosePending, "dispatch_sync_run", now.Add(-time.Hour))

	// post_sync is EXCLUDED even with a terminal owner (codex review round 1,
	// P1): its owning run is terminal from the instant its row is dispatched,
	// so "owner terminal" proves nothing about whether the post-sync fanout
	// (native_post_sync.go) has actually executed yet -- and that fanout
	// silently no-ops forever if it finds its own outbox row no longer
	// 'dispatched' when it finally runs. See TerminalOutboxCloseResult's doc
	// comment.
	seedRun(t, ctx, pool, outboxClosePostSyncTerminal, "failed", now.Add(-8*time.Hour))
	seedOutboxCloseDispatchedRow(t, ctx, pool, outboxClosePostSyncTerminal, "post_sync", dispatchedAt, nil)

	result, err := closer.Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("Step: %v", err)
	}
	if result.Dispatch != 1 || result.Finalize != 1 || result.Discovery != 1 {
		t.Fatalf("result = %#v, want exactly one close per kind (three kinds -- post_sync excluded)", result)
	}
	wantOutcomes := map[string]map[string]int64{
		"dispatch_sync_run":   {"success": 1},
		"finalize_sync_run":   {"partial_failed": 1},
		"reference_discovery": {"success": 1},
	}
	for kind, wantByOutcome := range wantOutcomes {
		gotByOutcome := result.ClosedByOutcome[kind]
		if len(gotByOutcome) != len(wantByOutcome) {
			t.Fatalf("ClosedByOutcome[%q] = %v, want %v", kind, gotByOutcome, wantByOutcome)
		}
		for outcome, count := range wantByOutcome {
			if gotByOutcome[outcome] != count {
				t.Fatalf("ClosedByOutcome[%q][%q] = %d, want %d", kind, outcome, gotByOutcome[outcome], count)
			}
		}
	}
	if len(result.ClosedByOutcome["post_sync"]) != 0 {
		t.Fatalf("ClosedByOutcome[post_sync] = %v, want none -- post_sync must never be closed",
			result.ClosedByOutcome["post_sync"])
	}

	// Every positive case actually reached 'closed', with the
	// dispatched_transport/route_generation/transport_job_id/claim_* columns
	// nulled (required by ck_sync_dispatch_outbox_dispatched_route_coherence
	// on the real production schema the moment status leaves 'dispatched').
	for _, seeded := range []struct{ runID, kind string }{
		{outboxCloseDispatchTerminal, "dispatch_sync_run"},
		{outboxCloseFinalizeTerminal, "finalize_sync_run"},
		{outboxCloseDiscoveryTerminal, "reference_discovery"},
	} {
		status, dispatchedTransport, transportJobID, claimToken := outboxCloseRowState(t, ctx, pool, seeded.runID, seeded.kind)
		if status != "closed" {
			t.Fatalf("%s/%s status = %q, want closed", seeded.runID, seeded.kind, status)
		}
		if dispatchedTransport != nil || transportJobID != nil || claimToken != nil {
			t.Fatalf("%s/%s left transport/claim columns set: transport=%v job=%v claim=%v",
				seeded.runID, seeded.kind, dispatchedTransport, transportJobID, claimToken)
		}
	}

	// Every negative control stayed untouched.
	for _, seeded := range []struct{ runID, kind, wantStatus string }{
		{outboxCloseDispatchNonTerminal, "dispatch_sync_run", "dispatched"},
		{outboxCloseDispatchLiveClaim, "dispatch_sync_run", "dispatched"},
		{outboxCloseDiscoveryLiveLedger, "reference_discovery", "dispatched"},
		{outboxClosePending, "dispatch_sync_run", "pending"},
		{outboxClosePostSyncTerminal, "post_sync", "dispatched"},
	} {
		status, _, _, _ := outboxCloseRowState(t, ctx, pool, seeded.runID, seeded.kind)
		if status != seeded.wantStatus {
			t.Fatalf("%s/%s status = %q, want %q (must not be closed)", seeded.runID, seeded.kind, status, seeded.wantStatus)
		}
	}
	// The live-claim control specifically must keep its claim intact -- a
	// closed row nulling the claim out from under an in-flight redelivery
	// would be the exact failure the risk note warns about.
	_, _, _, liveClaimToken := outboxCloseRowState(t, ctx, pool, outboxCloseDispatchLiveClaim, "dispatch_sync_run")
	if liveClaimToken == nil {
		t.Fatal("live claim was cleared on a row the risk note requires this step to leave alone")
	}

	// Idempotent: a second pass finds nothing left to close.
	second, err := closer.Step(ctx, now, 100)
	if err != nil {
		t.Fatalf("second Step: %v", err)
	}
	if second.Dispatch != 0 || second.Finalize != 0 || second.Discovery != 0 ||
		len(second.ClosedByOutcome) != 0 {
		t.Fatalf("second pass result = %#v, want an all-zero no-op", second)
	}
}

// TestReapTerminalOutboxBacklogDrainsAcrossPasses proves the CHAOS-4583
// backlog reaper's core promise: --dry-run reports the EXACT, unbounded
// backlog size (not capped at one batch), and a real run loops
// TerminalOutboxClose.Step batches until the backlog is drained, converging
// on a true no-op -- exactly the shape the local proof (dry-run -> real ->
// dry-run 0) exercises against the shared stack.
func TestReapTerminalOutboxBacklogDrainsAcrossPasses(t *testing.T) {
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

	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	dispatchedAt := now.Add(-time.Hour)
	const backlogSize = 5
	const batchSize = 2 // minimumStepLimit-respecting; forces multiple passes
	runIDs := make([]string, backlogSize)
	for index := range runIDs {
		runID := "00000000-0000-4000-8000-00000000460" + string(rune('0'+index))
		runIDs[index] = runID
		seedRun(t, ctx, pool, runID, "success", now.Add(-8*time.Hour))
		seedOutboxCloseDispatchedRow(t, ctx, pool, runID, "dispatch_sync_run", dispatchedAt, nil)
	}

	preview, err := ReapTerminalOutboxBacklog(ctx, pool, now, batchSize, true)
	if err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if !preview.DryRun || preview.CandidatesByKind["dispatch_sync_run"] != backlogSize || preview.Passes != 0 {
		t.Fatalf("dry-run preview = %#v, want %d dispatch_sync_run candidates and zero passes", preview, backlogSize)
	}
	// A dry-run must never write: re-reading the preview a second time must
	// report the identical count, not a shrinking one.
	previewAgain, err := ReapTerminalOutboxBacklog(ctx, pool, now, batchSize, true)
	if err != nil {
		t.Fatalf("second dry-run: %v", err)
	}
	if previewAgain.CandidatesByKind["dispatch_sync_run"] != backlogSize {
		t.Fatalf("second dry-run candidates = %d, want unchanged %d (a dry run must not write)",
			previewAgain.CandidatesByKind["dispatch_sync_run"], backlogSize)
	}

	real, err := ReapTerminalOutboxBacklog(ctx, pool, now, batchSize, false)
	if err != nil {
		t.Fatalf("real reap: %v", err)
	}
	if real.DryRun {
		t.Fatal("real reap reported DryRun=true")
	}
	if real.ClosedByKind["dispatch_sync_run"] != backlogSize {
		t.Fatalf("real reap closed %d dispatch_sync_run rows, want %d", real.ClosedByKind["dispatch_sync_run"], backlogSize)
	}
	if real.ClosedByOutcome["dispatch_sync_run"]["success"] != backlogSize {
		t.Fatalf("real reap ClosedByOutcome[dispatch_sync_run][success] = %d, want %d",
			real.ClosedByOutcome["dispatch_sync_run"]["success"], backlogSize)
	}
	// ceil(backlogSize/batchSize) closing passes plus one confirming
	// all-zero pass: the loop never assumes a partial batch means "done",
	// since a concurrent writer could still be arming new candidates.
	wantPasses := (backlogSize+batchSize-1)/batchSize + 1
	if real.Passes != wantPasses {
		t.Fatalf("real reap ran %d passes, want %d", real.Passes, wantPasses)
	}
	if real.PassLimitReached {
		t.Fatal("real reap reported PassLimitReached on a backlog well under the cap")
	}
	for _, runID := range runIDs {
		status, _, _, _ := outboxCloseRowState(t, ctx, pool, runID, "dispatch_sync_run")
		if status != "closed" {
			t.Fatalf("run %s status = %q, want closed", runID, status)
		}
	}

	// Converged: a dry-run after the real reap finds nothing left.
	after, err := ReapTerminalOutboxBacklog(ctx, pool, now, batchSize, true)
	if err != nil {
		t.Fatalf("post-reap dry-run: %v", err)
	}
	if after.CandidatesByKind["dispatch_sync_run"] != 0 {
		t.Fatalf("post-reap dry-run candidates = %d, want 0", after.CandidatesByKind["dispatch_sync_run"])
	}
}
