//go:build integration

package syncreconciler

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// CHAOS-6956, CHAOS-6932's root cause: repairReadyFinalizers issued one coordinator round trip
// PER CANDIDATE (readyFinalizeDomainSQL). Prod holds ~14-18 candidates that are legitimately not
// ready yet (a run still has a non-terminal unit) for many passes in a row -- churn, not a stuck
// run (executed evidence on the 6932 ticket) -- so that alone spent the 750ms
// terminal_delivery_repair stage budget in serial statements and the transaction's own Commit then
// ran into the same expired context and rolled back (syncreconciler.ready_finalize_uncommitted).
//
// countingTracer counts every query the coordinator pool executes matching readyFinalizeRunsSQL's
// (or readyFinalizeDomainSQL's) shape, so the round-trip count is asserted directly -- deterministic,
// and independent of this host's own network latency, which is far below prod's.
type countingTracer struct{ calls int }

func (t *countingTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	t.calls++
	return ctx
}
func (*countingTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// seedReadyFinalizeCandidate seeds one finalize_sync_run outbox row whose River job is completed
// (present=true evidence) or deleted (present=false), and whose run is or is not ready, by index so
// every id is distinct and every run's outbox row is independent.
func transportArgsFor(runID, outboxID string, generation int64) syncdispatchruntime.FinalizeSyncRunArgs {
	return syncdispatchruntime.FinalizeSyncRunArgs{TransportArgs: syncdispatchruntime.TransportArgs{
		Version: 1, OrgID: "00000000-0000-4000-8000-000000000001", RunID: runID,
		DispatchOutbox: outboxID, DeliveryAttempt: 38, RouteGeneration: generation,
	}}
}

func seedReadyFinalizeCandidate(
	t *testing.T, ctx context.Context, h *finalizeBackstopHarness, now time.Time, index int, generation int64, ready bool,
) {
	t.Helper()
	runID := fmt.Sprintf("00000000-0000-4000-9000-%012d", index)
	outboxID := fmt.Sprintf("00000000-0000-4000-9001-%012d", index)
	unitID := fmt.Sprintf("00000000-0000-4000-9002-%012d", index)
	unitStatus := "success"
	if !ready {
		unitStatus = "running"
	}
	seedRun(t, ctx, h.admin, runID, "dispatching", now.Add(-48*time.Hour))
	seedUnit(t, ctx, h.admin, unitID, runID, unitStatus, nil, now.Add(-24*time.Hour))
	args := transportArgsFor(runID, outboxID, generation)
	inserted, err := h.river.Insert(ctx, args, &river.InsertOpts{Queue: "sync"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.admin.Exec(ctx, `UPDATE river.river_job SET state='completed',finalized_at=$2 WHERE id=$1`,
		inserted.Job.ID, now.Add(-30*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.admin.Exec(ctx, `INSERT INTO public.sync_dispatch_outbox
        (id,org_id,sync_run_id,kind,status,available_at,attempts,dispatched_at,dispatched_transport,dispatched_route_generation,transport_job_id,created_at,updated_at)
        VALUES ($1,'org-materializer',$2,'finalize_sync_run','dispatched',$3,38,$3,'river',$5,$4,$3,$3)`,
		outboxID, runID, now.Add(-30*time.Hour), strconv.FormatInt(inserted.Job.ID, 10), generation); err != nil {
		t.Fatal(err)
	}
}

// TestReadyFinalizerCoordinatorReadIsOneRoundTripNotOnePerCandidate is the RED->GREEN proof: it
// counts coordinator statements, which needs no injected latency to be deterministic. Reverting
// ready_finalize_repair.go to main's per-candidate readyFinalizeDomainSQL call makes this fail with
// calls==candidateCount instead of a small constant.
func TestReadyFinalizerCoordinatorReadIsOneRoundTripNotOnePerCandidate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	h := startFinalizeBackstopHarness(t, ctx)
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	resetMaterializerIntegrationTables(t, ctx, h.admin)
	if _, err := h.admin.Exec(ctx, `TRUNCATE river.river_job`); err != nil {
		t.Fatal(err)
	}
	generation := backstopRoutes(t, ctx, h.admin)

	const candidateCount = 20
	for i := 0; i < candidateCount; i++ {
		seedReadyFinalizeCandidate(t, ctx, h, now, i, generation, false) // every candidate is NOT ready
	}

	tracer := &countingTracer{}
	tracedConfig := h.coordinator.Config()
	tracedConfig.ConnConfig.Tracer = tracer
	tracedPool, err := pgxpool.NewWithConfig(ctx, tracedConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer tracedPool.Close()
	repair, err := NewTerminalDeliveryRepair(h.queue, tracedPool, "river")
	if err != nil {
		t.Fatal(err)
	}

	result, err := repair.Step(ctx, now, candidateCount+5)
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	if result.Recovered != 0 {
		t.Fatalf("every seeded candidate is not ready, want 0 recovered, got %+v", result)
	}
	if tracer.calls == 0 {
		t.Fatal("no coordinator query observed at all -- the tracer or the seed is broken")
	}
	// r1 P3: "< candidateCount" alone would still pass at 19 calls for 20 candidates -- one query
	// short of one-per-candidate is still not O(1). readyFinalizeRuns issues exactly one
	// coordinator statement regardless of candidate count; pin that exact number, not merely
	// "fewer than every candidate".
	if tracer.calls != 1 {
		t.Fatalf("coordinator round trips = %d for %d candidates, want exactly 1 (one query, not one per candidate)",
			tracer.calls, candidateCount)
	}
}

// TestReadyFinalizerBatchedReadinessAgreesWithThePerRunAnswerForEveryMix proves the set-based
// readiness read is not merely fast but CORRECT: mixed ready/not-ready runs, each recovered or not
// exactly as the per-run predicate says. A run missing from the batched answer is asserted not
// ready, per readyFinalizeRunsSQL's own contract.
func TestReadyFinalizerBatchedReadinessAgreesWithThePerRunAnswerForEveryMix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	h := startFinalizeBackstopHarness(t, ctx)
	now := time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)
	resetMaterializerIntegrationTables(t, ctx, h.admin)
	if _, err := h.admin.Exec(ctx, `TRUNCATE river.river_job`); err != nil {
		t.Fatal(err)
	}
	generation := backstopRoutes(t, ctx, h.admin)

	readiness := []bool{true, false, true, true, false, false, false, true}
	for i, ready := range readiness {
		seedReadyFinalizeCandidate(t, ctx, h, now, i, generation, ready)
	}

	result, err := h.repair.Step(ctx, now, len(readiness)+5)
	if err != nil {
		t.Fatalf("Step() error = %v", err)
	}
	wantRecovered := 0
	for _, ready := range readiness {
		if ready {
			wantRecovered++
		}
	}
	if result.Recovered != wantRecovered || result.ReadyFinalizersRecovered != wantRecovered {
		t.Fatalf("recovered=%+v, want %d ready-finalizer recoveries out of %d candidates", result, wantRecovered, len(readiness))
	}
	for i, ready := range readiness {
		outboxID := fmt.Sprintf("00000000-0000-4000-9001-%012d", i)
		var status string
		if err := h.admin.QueryRow(ctx, `SELECT status FROM public.sync_dispatch_outbox WHERE id=$1`, outboxID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		wantStatus := "dispatched"
		if ready {
			wantStatus = "pending"
		}
		if status != wantStatus {
			t.Fatalf("candidate %d (ready=%v) status=%s, want %s", i, ready, status, wantStatus)
		}
	}
}
