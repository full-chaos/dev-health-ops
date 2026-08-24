//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// This file is the codex-round-2 (CHAOS-4175) red-first proof for the
// BudgetGuard estimate-bridge chunking fix: before it, a run whose
// candidate set exceeded the estimate bridge's own documented 500-id
// ceiling (DispatchBudgetEstimateReference.unit_ids: max_length=500) sent
// every candidate in ONE unchunked call, the real bridge would 422 it, and
// enforceRun's old whole-batch fail-open path admitted every one of those
// units with ZERO budget checked -- silently, no error, no counter. See
// fakeBudgetEstimator.DispatchBudgetEstimate's own >500 check
// (budget_consumption_integration_test.go), which mirrors the real
// endpoint's ceiling so this test observes the SAME failure a live bridge
// would produce, not a hand-picked stand-in error.

const budgetEnforceChunkingCandidateCount = 600

func seedManyCandidateUnits(t *testing.T, ctx context.Context, pool *pgxpool.Pool, count int) []string {
	t.Helper()
	ids := make([]string, count)
	// One statement via generate_series rather than `count` individual
	// round trips -- this fixture exists specifically to seed hundreds of
	// rows, and per-row INSERTs would make this test the slowest thing in
	// the package for no reason.
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units
 (id, sync_run_id, org_id, integration_id, source_id, provider, dataset_key, cost_class,
  status, available_at, updated_at, result)
SELECT
  ('00000000-0000-4000-9000-' || lpad(to_hex(n), 12, '0'))::uuid,
  $1::uuid, 'org-1', '00000000-0000-4000-8000-000000000010'::uuid,
  '00000000-0000-4000-8000-000000000011'::uuid, 'github', 'commits', 'rest_core',
  'planned', NULL, now(), '{}'::json
FROM generate_series(1, $2) AS n`,
		budgetCandidatesRunID, count); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT array_agg(id::text ORDER BY id) FROM public.sync_run_units WHERE sync_run_id = $1::uuid`,
		budgetCandidatesRunID).Scan(&ids); err != nil {
		t.Fatal(err)
	}
	return ids
}

// TestEnforceRunChunksOversizedCandidateBatches proves the fix's happy
// path: a candidate set well past the bridge's 500-id ceiling still gets
// every unit a real estimate, in multiple bounded calls, no call ever
// carrying more than the ceiling.
func TestEnforceRunChunksOversizedCandidateBatches(t *testing.T) {
	// The host shell may carry a real SYNC_BUDGET_BUCKET_LIMITS override
	// (a per-bucket limit wins before the default is ever consulted) --
	// cleared so this test's estimates land against the real default.
	t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		ids := seedManyCandidateUnits(t, ctx, pool, budgetEnforceChunkingCandidateCount)
		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{}}
		for _, id := range ids {
			estimator.estimates[id] = estimateFor(1, "github", "rest_core", "core")
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		result, err := enforceRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, nil, time.Now(), nil)
		if err != nil {
			t.Fatalf("enforceRun: %v, want nil -- chunking must make the oversized batch a non-event", err)
		}

		if len(estimator.calls) < 2 {
			t.Fatalf("bridge calls=%d, want at least 2 -- %d candidates must not fit in one call under the %d-id ceiling",
				len(estimator.calls), budgetEnforceChunkingCandidateCount, dispatchBudgetEstimateMaxUnitIDs)
		}
		seenUnits := 0
		for _, call := range estimator.calls {
			if len(call.unitIDs) > dispatchBudgetEstimateMaxUnitIDs {
				t.Fatalf("one bridge call carried %d unit ids, want at most %d", len(call.unitIDs), dispatchBudgetEstimateMaxUnitIDs)
			}
			seenUnits += len(call.unitIDs)
		}
		if seenUnits != budgetEnforceChunkingCandidateCount {
			t.Fatalf("bridge calls covered %d unit ids across all chunks, want %d (no unit dropped, none duplicated)",
				seenUnits, budgetEnforceChunkingCandidateCount)
		}
		if len(result.deferredUnitIDs) != 0 {
			t.Fatalf("deferredUnitIDs=%v, want none -- every unit's real (small) estimate must have been checked and admitted", result.deferredUnitIDs)
		}
	})
}

// TestEnforceRunFailsClosedOnAContractRejectedChunk is the disposition
// half: a chunk the bridge rejects as malformed (its own documented
// ceiling exceeded, or any other 4xx) must fail the WHOLE pass closed --
// not admit that chunk's units with zero budget checked. Forced by
// dropping the fake estimator's success threshold below the seeded
// candidate count, so the LAST chunk (not the first) is the one that
// trips the ceiling -- proving this isn't just a first-chunk special case.
func TestEnforceRunFailsClosedOnAContractRejectedChunk(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		seedManyCandidateUnits(t, ctx, pool, budgetEnforceChunkingCandidateCount)
		estimator := &contractRejectingBudgetEstimator{failOnCallIndex: 1} // second chunk (0-indexed)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		_, err = enforceRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, nil, time.Now(), nil)
		if !errors.Is(err, ErrBridgeContractRejected) {
			t.Fatalf("enforceRun error=%v, want ErrBridgeContractRejected -- a rejected chunk must fail the whole pass, not silently admit it", err)
		}
		if estimator.calls < 2 {
			t.Fatalf("bridge calls=%d, want at least 2 -- the rejection must come from a LATER chunk, not the batching itself", estimator.calls)
		}
	})
}

// contractRejectingBudgetEstimator fails one specific call (by 0-indexed
// call order) with ErrBridgeContractRejected and succeeds (with no
// estimates) otherwise -- a minimal fake purpose-built for this one
// disposition test, distinct from fakeBudgetEstimator's own >500 auto-fail
// (that one simulates the endpoint's real ceiling; this one simulates an
// arbitrary 4xx on a chunk that is itself correctly sized).
type contractRejectingBudgetEstimator struct {
	failOnCallIndex int
	calls           int
}

func (f *contractRejectingBudgetEstimator) DispatchBudgetEstimate(
	ctx context.Context, orgID, runID string, unitIDs []string,
) (map[string][]budgetEstimate, error) {
	index := f.calls
	f.calls++
	if index == f.failOnCallIndex {
		return nil, fmt.Errorf("%w: status=422 (fake: forced rejection)", ErrBridgeContractRejected)
	}
	return map[string][]budgetEstimate{}, nil
}
