//go:build integration

package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func createBudgetConsumptionTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE public.sync_run_units (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL, org_id text NOT NULL,
 integration_id uuid NOT NULL, source_id uuid NOT NULL, provider text NOT NULL,
 dataset_key text NOT NULL, cost_class text NOT NULL,
 since_at timestamptz NULL, before_at timestamptz NULL,
 status text NOT NULL, available_at timestamptz NULL,
 updated_at timestamptz NOT NULL DEFAULT now(), error text NULL, result json NULL,
 lease_owner text NULL, lease_expires_at timestamptz NULL, last_heartbeat_at timestamptz NULL,
 rate_limit_deferrals int NOT NULL DEFAULT 0, rate_limit_first_seen_at timestamptz NULL,
 budget_deferrals int NOT NULL DEFAULT 0, budget_first_deferred_at timestamptz NULL,
 first_blocked_at timestamptz NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}

func withBudgetConsumptionPool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createBudgetConsumptionTables(t, ctx, pool)
	fn(ctx, pool)
}

type consumptionUnitFixture struct {
	id            string
	syncRunID     string
	orgID         string
	status        string
	updatedAt     time.Time
	leaseExpires  *time.Time
	integrationID string
	sourceID      string
}

func insertConsumptionUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, f consumptionUnitFixture) {
	t.Helper()
	if f.integrationID == "" {
		f.integrationID = "00000000-0000-4000-8000-000000000010"
	}
	if f.sourceID == "" {
		f.sourceID = "00000000-0000-4000-8000-000000000011"
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units
 (id, sync_run_id, org_id, integration_id, source_id, provider, dataset_key, cost_class,
  status, updated_at, lease_expires_at, result)
VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5::uuid, 'github', 'commits', 'rest_core', $6, $7, $8, '{}'::json)`,
		f.id, f.syncRunID, f.orgID, f.integrationID, f.sourceID, f.status, f.updatedAt, f.leaseExpires); err != nil {
		t.Fatal(err)
	}
}

// fakeBudgetEstimator records every call it receives and returns canned
// estimates by unit id, so tests can assert BOTH the grouping (which
// (org, run, unit_ids) tuples were actually requested) and the aggregation
// (what activeBudgetConsumption does with the response).
type fakeBudgetEstimator struct {
	calls     []fakeEstimatorCall
	estimates map[string][]budgetEstimate // unit id -> estimates
	failFor   map[string]bool             // (orgID+":"+runID) -> return an error for this group
}

type fakeEstimatorCall struct {
	orgID, runID string
	unitIDs      []string
}

func (f *fakeBudgetEstimator) DispatchBudgetEstimate(ctx context.Context, orgID, runID string, unitIDs []string) (map[string][]budgetEstimate, error) {
	sorted := append([]string(nil), unitIDs...)
	sort.Strings(sorted)
	f.calls = append(f.calls, fakeEstimatorCall{orgID: orgID, runID: runID, unitIDs: sorted})
	// Mirrors the REAL bridge's own documented ceiling
	// (DispatchBudgetEstimateReference.unit_ids: max_length=500) exactly --
	// a fake that never enforces this could pass every test while the real
	// endpoint 422s on the first oversized batch it ever sees (codex round
	// 2, CHAOS-4175).
	if len(unitIDs) > dispatchBudgetEstimateMaxUnitIDs {
		return nil, fmt.Errorf("%w: status=422 (fake: batch of %d exceeds the %d-id cap)",
			ErrBridgeContractRejected, len(unitIDs), dispatchBudgetEstimateMaxUnitIDs)
	}
	if f.failFor[orgID+":"+runID] {
		return nil, errors.New("simulated bridge failure")
	}
	result := map[string][]budgetEstimate{}
	for _, id := range unitIDs {
		if est, ok := f.estimates[id]; ok {
			result[id] = est
		}
	}
	return result, nil
}

func estimateFor(units int, provider, dimension, routeFamily string) []budgetEstimate {
	return []budgetEstimate{{
		Bucket:         budgetEstimateBucket{Provider: provider, Dimension: dimension},
		EstimatedUnits: units,
		RouteFamily:    routeFamily,
		Confidence:     "high",
	}}
}

// TestActiveBudgetConsumptionReturnsEmptyWithoutQueryingWhenNoBudgetKeys
// pins the fail-fast short circuit: no budget keys means nothing could
// possibly be measured against, so the function must not even touch the
// database or the bridge.
func TestActiveBudgetConsumptionReturnsEmptyWithoutQueryingWhenNoBudgetKeys(t *testing.T) {
	withBudgetConsumptionPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		estimator := &fakeBudgetEstimator{}
		got, err := activeBudgetConsumption(ctx, tx, estimator, nil, time.Now(), map[string]bool{})
		if err != nil {
			t.Fatalf("activeBudgetConsumption: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("got=%v want empty", got)
		}
		if len(estimator.calls) != 0 {
			t.Fatalf("estimator called %d times, want 0", len(estimator.calls))
		}
	})
}

// TestActiveBudgetConsumptionIsNotScopedToOneSyncRun is the core property
// this function exists for: units from TWO DIFFERENT sync runs (and
// different orgs) both count toward the same provider's consumption, and
// each run's units are grouped into their OWN bridge call (the estimate
// bridge is tenant-fenced to one (org, run) per call).
func TestActiveBudgetConsumptionIsNotScopedToOneSyncRun(t *testing.T) {
	withBudgetConsumptionPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		future := now.Add(time.Hour)

		unitA := "00000000-0000-4000-8000-000000000101" // run-1 / org-1
		unitB := "00000000-0000-4000-8000-000000000102" // run-2 / org-2

		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: unitA, syncRunID: "00000000-0000-4000-8000-000000000201", orgID: "org-1", status: syncRunUnitStatusDispatching, updatedAt: now})
		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: unitB, syncRunID: "00000000-0000-4000-8000-000000000202", orgID: "org-2", status: syncRunUnitStatusRunning, updatedAt: now, leaseExpires: &future})

		bucket := budgetEstimateBucket{Provider: "github", Dimension: "rest_core"}
		budgetKey := budgetKeyFor(bucket, "work-items")
		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			unitA: estimateFor(30, "github", "rest_core", "work-items"),
			unitB: estimateFor(70, "github", "rest_core", "work-items"),
		}}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		got, err := activeBudgetConsumption(ctx, tx, estimator, nil, now, map[string]bool{budgetKey: true})
		if err != nil {
			t.Fatalf("activeBudgetConsumption: %v", err)
		}
		if got[budgetKey] != 100 {
			t.Fatalf("consumed[%s]=%d want=100 (30 from run-1's org-1 unit + 70 from run-2's org-2 unit)", budgetKey, got[budgetKey])
		}
		if len(estimator.calls) != 2 {
			t.Fatalf("estimator called %d times, want 2 (one per (org, run) group)", len(estimator.calls))
		}
		for _, call := range estimator.calls {
			if len(call.unitIDs) != 1 {
				t.Fatalf("call %+v has %d unit ids, want 1 (each run's own unit only)", call, len(call.unitIDs))
			}
		}
	})
}

// TestActiveBudgetConsumptionExcludesStaleDispatchingAndExpiredLeases pins
// the eligibility predicate's OWN boundary: a stale DISPATCHING unit (its
// claim never completed) and a RUNNING unit whose lease already expired are
// both NOT live consumers -- their capacity is effectively already back on
// the table, whether or not a reconciler has gotten to them yet.
func TestActiveBudgetConsumptionExcludesStaleDispatchingAndExpiredLeases(t *testing.T) {
	withBudgetConsumptionPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		past := now.Add(-time.Hour)
		staleCutoff := staleDispatchCutoff(now)

		freshDispatching := "00000000-0000-4000-8000-000000000111"
		staleDispatching := "00000000-0000-4000-8000-000000000112"
		liveRunning := "00000000-0000-4000-8000-000000000113"
		expiredRunning := "00000000-0000-4000-8000-000000000114"
		leaselessRunning := "00000000-0000-4000-8000-000000000115"
		// Exact-boundary fixtures: Python's predicates are both strict (>),
		// so a unit sitting EXACTLY on the boundary must be excluded. A
		// fixture using only clearly-past/clearly-future values passes
		// vacuously against a > -> >= mutation on either branch.
		boundaryStaleDispatching := "00000000-0000-4000-8000-000000000116"
		boundaryExpiredRunning := "00000000-0000-4000-8000-000000000117"
		runID := "00000000-0000-4000-8000-000000000210"

		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: freshDispatching, syncRunID: runID, orgID: "org-1", status: syncRunUnitStatusDispatching, updatedAt: now})
		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: staleDispatching, syncRunID: runID, orgID: "org-1", status: syncRunUnitStatusDispatching, updatedAt: staleCutoff.Add(-time.Minute)})
		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: liveRunning, syncRunID: runID, orgID: "org-1", status: syncRunUnitStatusRunning, updatedAt: now, leaseExpires: nil})
		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: expiredRunning, syncRunID: runID, orgID: "org-1", status: syncRunUnitStatusRunning, updatedAt: now, leaseExpires: &past})
		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: leaselessRunning, syncRunID: runID, orgID: "org-1", status: syncRunUnitStatusRunning, updatedAt: now, leaseExpires: nil})
		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: boundaryStaleDispatching, syncRunID: runID, orgID: "org-1", status: syncRunUnitStatusDispatching, updatedAt: staleCutoff})
		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: boundaryExpiredRunning, syncRunID: runID, orgID: "org-1", status: syncRunUnitStatusRunning, updatedAt: now, leaseExpires: &now})

		bucket := budgetEstimateBucket{Provider: "github", Dimension: "rest_core"}
		budgetKey := budgetKeyFor(bucket, "work-items")
		estimates := map[string][]budgetEstimate{}
		for _, id := range []string{freshDispatching, staleDispatching, liveRunning, expiredRunning, leaselessRunning, boundaryStaleDispatching, boundaryExpiredRunning} {
			estimates[id] = estimateFor(1, "github", "rest_core", "work-items")
		}
		estimator := &fakeBudgetEstimator{estimates: estimates}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		got, err := activeBudgetConsumption(ctx, tx, estimator, nil, now, map[string]bool{budgetKey: true})
		if err != nil {
			t.Fatalf("activeBudgetConsumption: %v", err)
		}
		// freshDispatching (1) + liveRunning (1, no lease) + leaselessRunning (1) = 3.
		// staleDispatching, expiredRunning, and BOTH exact-boundary units must NOT count.
		if got[budgetKey] != 3 {
			t.Fatalf("consumed[%s]=%d want=3 (stale-dispatching, expired-lease, and exact-boundary units must all be excluded)", budgetKey, got[budgetKey])
		}
	})
}

// TestActiveBudgetConsumptionIgnoresEstimatesOutsideTheTargetBudgetKeys
// pins the final membership filter: an estimate whose bucket is NOT one of
// the caller's own budget_keys must not be counted, even though it came
// back from a real active unit.
func TestActiveBudgetConsumptionIgnoresEstimatesOutsideTheTargetBudgetKeys(t *testing.T) {
	withBudgetConsumptionPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		unitID := "00000000-0000-4000-8000-000000000121"
		runID := "00000000-0000-4000-8000-000000000211"
		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: unitID, syncRunID: runID, orgID: "org-1", status: syncRunUnitStatusDispatching, updatedAt: now})

		targetBucket := budgetEstimateBucket{Provider: "github", Dimension: "rest_core"}
		targetKey := budgetKeyFor(targetBucket, "work-items")
		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			unitID: estimateFor(99, "gitlab", "graphql", "commits"), // a DIFFERENT bucket entirely
		}}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		got, err := activeBudgetConsumption(ctx, tx, estimator, nil, now, map[string]bool{targetKey: true})
		if err != nil {
			t.Fatalf("activeBudgetConsumption: %v", err)
		}
		if got[targetKey] != 0 {
			t.Fatalf("consumed[%s]=%d want=0 (only the returned estimate is a DIFFERENT bucket)", targetKey, got[targetKey])
		}
		if len(got) != 0 {
			t.Fatalf("got=%v want empty -- no key should be created for a bucket outside the target set", got)
		}
	})
}

// TestActiveBudgetConsumptionDegradesAFailedGroupWithoutFailingTheWhole
// pins the fail-open discipline: one group's bridge call failing (network
// error, decode error -- not a per-unit estimator failure, which is already
// degraded server-side) must not take down another group's contribution.
func TestActiveBudgetConsumptionDegradesAFailedGroupWithoutFailingTheWhole(t *testing.T) {
	withBudgetConsumptionPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		okUnit := "00000000-0000-4000-8000-000000000131"
		failUnit := "00000000-0000-4000-8000-000000000132"
		okRun := "00000000-0000-4000-8000-000000000221"
		failRun := "00000000-0000-4000-8000-000000000222"

		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: okUnit, syncRunID: okRun, orgID: "org-ok", status: syncRunUnitStatusDispatching, updatedAt: now})
		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: failUnit, syncRunID: failRun, orgID: "org-fail", status: syncRunUnitStatusDispatching, updatedAt: now})

		bucket := budgetEstimateBucket{Provider: "github", Dimension: "rest_core"}
		budgetKey := budgetKeyFor(bucket, "work-items")
		estimator := &fakeBudgetEstimator{
			estimates: map[string][]budgetEstimate{
				okUnit:   estimateFor(42, "github", "rest_core", "work-items"),
				failUnit: estimateFor(999, "github", "rest_core", "work-items"),
			},
			failFor: map[string]bool{"org-fail:" + failRun: true},
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		got, err := activeBudgetConsumption(ctx, tx, estimator, nil, now, map[string]bool{budgetKey: true})
		if err != nil {
			t.Fatalf("activeBudgetConsumption: %v", err)
		}
		if got[budgetKey] != 42 {
			t.Fatalf("consumed[%s]=%d want=42 -- the failing group's 999 must not count, and the ok group's 42 must still land", budgetKey, got[budgetKey])
		}
	})
}

// TestActiveBudgetConsumptionLogsTheGroupFanout pins the observability
// addition: when active units span more than one (org, run) group, the
// per-pass fanout (group count + total unit count) must be visible in the
// logs, not just discoverable as latency after the fact.
func TestActiveBudgetConsumptionLogsTheGroupFanout(t *testing.T) {
	withBudgetConsumptionPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := time.Now().UTC()
		unitA := "00000000-0000-4000-8000-000000000141"
		unitB := "00000000-0000-4000-8000-000000000142"
		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: unitA, syncRunID: "00000000-0000-4000-8000-000000000241", orgID: "org-a", status: syncRunUnitStatusDispatching, updatedAt: now})
		insertConsumptionUnit(t, ctx, pool, consumptionUnitFixture{id: unitB, syncRunID: "00000000-0000-4000-8000-000000000242", orgID: "org-b", status: syncRunUnitStatusDispatching, updatedAt: now})

		budgetKey := budgetKeyFor(budgetEstimateBucket{Provider: "github", Dimension: "rest_core"}, "work-items")
		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			unitA: estimateFor(1, "github", "rest_core", "work-items"),
			unitB: estimateFor(1, "github", "rest_core", "work-items"),
		}}

		var logged strings.Builder
		logger := slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: slog.LevelInfo}))

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := activeBudgetConsumption(ctx, tx, estimator, logger, now, map[string]bool{budgetKey: true}); err != nil {
			t.Fatalf("activeBudgetConsumption: %v", err)
		}

		output := logged.String()
		if !strings.Contains(output, `"dispatch_sync_run.budget_guard_active_consumption_fanout"`) {
			t.Fatalf("log output missing the fanout line: %s", output)
		}
		if !strings.Contains(output, `"group_count":2`) {
			t.Fatalf("log output missing group_count=2: %s", output)
		}
		if !strings.Contains(output, `"total_units":2`) {
			t.Fatalf("log output missing total_units=2: %s", output)
		}
	})
}
