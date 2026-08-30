//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func createBudgetCandidatesTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	// sync_runs backs bumpSyncRunRollup's seam (CHAOS-4586): every mechanism
	// that terminalizes a sync_run_units row in this package recomputes this
	// row's completed_units/failed_units in the same transaction.
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.sync_runs (
 id uuid PRIMARY KEY, completed_units int NOT NULL DEFAULT 0,
 failed_units int NOT NULL DEFAULT 0, total_units int NOT NULL DEFAULT 0
);
INSERT INTO public.sync_runs (id, total_units) VALUES ('`+budgetCandidatesRunID+`', 0);`); err != nil {
		t.Fatal(err)
	}
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
 first_blocked_at timestamptz NULL, last_retry_reason text NULL, processor_flags json NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const budgetCandidatesRunID = "00000000-0000-4000-8000-0000000001a0"

func withBudgetCandidatesPool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
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
	createBudgetCandidatesTables(t, ctx, pool)
	fn(ctx, pool)
}

type candidateUnitFixture struct {
	id               string
	status           string
	availableAt      *time.Time
	updatedAt        time.Time
	resultJSON       string
	budgetFirstDefAt *time.Time
	firstBlockedAt   *time.Time
	// integrationID overrides the default fixed integration id when set --
	// needed to put two units in the SAME budget bucket (which does not key
	// on integration_id) but DIFFERENT cooldown scopes (which do).
	integrationID string
	// datasetKey/processorFlagsJSON override the fixed defaults ('commits'
	// / none) -- needed to exercise validate_provider_family_claim's own
	// atomic-family bitset via a claimed unit's real processor_flags column.
	datasetKey         string
	processorFlagsJSON string
}

func insertCandidateUnit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, f candidateUnitFixture) {
	t.Helper()
	orgID := "org-1"
	integrationID := "00000000-0000-4000-8000-000000000010"
	if f.integrationID != "" {
		integrationID = f.integrationID
	}
	sourceID := "00000000-0000-4000-8000-000000000011"
	if f.resultJSON == "" {
		f.resultJSON = "{}"
	}
	datasetKey := "commits"
	if f.datasetKey != "" {
		datasetKey = f.datasetKey
	}
	var processorFlagsJSON *string
	if f.processorFlagsJSON != "" {
		processorFlagsJSON = &f.processorFlagsJSON
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units
 (id, sync_run_id, org_id, integration_id, source_id, provider, dataset_key, cost_class,
  status, available_at, updated_at, result, budget_first_deferred_at, first_blocked_at, processor_flags)
VALUES ($1::uuid, $2::uuid, $3, $4::uuid, $5::uuid, 'github', $12, 'rest_core',
        $6, $7, $8, $9::json, $10, $11, $13::json)`,
		f.id, budgetCandidatesRunID, orgID, integrationID, sourceID,
		f.status, f.availableAt, f.updatedAt, f.resultJSON, f.budgetFirstDefAt, f.firstBlockedAt,
		datasetKey, processorFlagsJSON); err != nil {
		t.Fatal(err)
	}
}

func idsOf(units []budgetUnit) map[string]bool {
	set := make(map[string]bool, len(units))
	for _, u := range units {
		set[u.id] = true
	}
	return set
}

// TestDispatchCandidateUnitsSelectsEligibleStatuses pins the three-way OR
// eligibility test (_dispatch_candidate_units): PLANNED always in; RETRYING
// only when due; DISPATCHING only when stale; RUNNING/SUCCESS/FAILED never
// candidates regardless of timing.
func TestDispatchCandidateUnitsSelectsEligibleStatuses(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		past := now.Add(-time.Hour)
		future := now.Add(time.Hour)
		staleCutoff := staleDispatchCutoff(now)

		planned := "00000000-0000-4000-8000-000000000001"
		dueRetrying := "00000000-0000-4000-8000-000000000002"
		notDueRetrying := "00000000-0000-4000-8000-000000000003"
		staleDispatching := "00000000-0000-4000-8000-000000000004"
		freshDispatching := "00000000-0000-4000-8000-000000000005"
		running := "00000000-0000-4000-8000-000000000006"
		success := "00000000-0000-4000-8000-000000000007"
		// Exact-boundary fixtures: Python's comparisons are both <= (never
		// <), so a unit exactly AT the boundary must still be a candidate.
		// Without these, a <= -> < mutation on either branch passes
		// vacuously against a fixture that only tests strictly-past values.
		boundaryDueRetrying := "00000000-0000-4000-8000-000000000008"
		boundaryStaleDispatching := "00000000-0000-4000-8000-000000000009"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: planned, status: syncRunUnitStatusPlanned, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: dueRetrying, status: syncRunUnitStatusRetrying, availableAt: &past, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: notDueRetrying, status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: staleDispatching, status: syncRunUnitStatusDispatching, updatedAt: staleCutoff.Add(-time.Minute)})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: freshDispatching, status: syncRunUnitStatusDispatching, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: running, status: syncRunUnitStatusRunning, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: success, status: syncRunUnitStatusSuccess, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: boundaryDueRetrying, status: syncRunUnitStatusRetrying, availableAt: &now, updatedAt: now})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: boundaryStaleDispatching, status: syncRunUnitStatusDispatching, updatedAt: staleCutoff})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, err := dispatchCandidateUnits(ctx, tx, budgetCandidatesRunID, nil, now)
		if err != nil {
			t.Fatalf("dispatchCandidateUnits: %v", err)
		}
		got := idsOf(units)
		want := map[string]bool{
			planned: true, dueRetrying: true, staleDispatching: true,
			boundaryDueRetrying: true, boundaryStaleDispatching: true,
		}
		if len(got) != len(want) {
			t.Fatalf("got %d candidates %v, want %d %v", len(got), got, len(want), want)
		}
		for id := range want {
			if !got[id] {
				t.Fatalf("missing expected candidate %s; got=%v", id, got)
			}
		}
		for id, excluded := range map[string]string{notDueRetrying: "not due", freshDispatching: "not stale", running: "never a candidate", success: "terminal"} {
			if got[id] {
				t.Fatalf("unit %s (%s) must NOT be a candidate", id, excluded)
			}
		}
	})
}

// TestDispatchCandidateUnitsExcludesIgnoredIDs pins the caller-supplied
// exclusion set -- a candidate the caller has already decided about this
// pass must not resurface.
func TestDispatchCandidateUnitsExcludesIgnoredIDs(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		id := "00000000-0000-4000-8000-000000000021"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: id, status: syncRunUnitStatusPlanned, updatedAt: now})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		units, err := dispatchCandidateUnits(ctx, tx, budgetCandidatesRunID, map[string]bool{id: true}, now)
		if err != nil {
			t.Fatalf("dispatchCandidateUnits: %v", err)
		}
		if len(units) != 0 {
			t.Fatalf("got %d units, want 0 -- the sole candidate was ignored", len(units))
		}
	})
}

// TestSurplusRetryCandidatesFiltersToNotYetDueBudgetDeferrals pins the
// three deliberate exclusions: empty slotHeadroom -> no candidates at all;
// a due deferral is NOT a surplus candidate (it is an ordinary candidate);
// a unit whose own last cause is not budget_deferred is skipped.
func TestSurplusRetryCandidatesFiltersToNotYetDueBudgetDeferrals(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		future := now.Add(time.Hour)
		past := now.Add(-time.Hour)

		notYetDueBudget := "00000000-0000-4000-8000-000000000031"
		dueBudget := "00000000-0000-4000-8000-000000000032"
		notYetDueCooldown := "00000000-0000-4000-8000-000000000033"

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{
			id: notYetDueBudget, status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now,
			resultJSON: `{"error_category":"budget_deferred"}`,
		})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{
			id: dueBudget, status: syncRunUnitStatusRetrying, availableAt: &past, updatedAt: now,
			resultJSON: `{"error_category":"budget_deferred"}`,
		})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{
			id: notYetDueCooldown, status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now,
			resultJSON: `{"error_category":"rate_limit_cooldown_deferred"}`,
		})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// Empty slotHeadroom -> fails closed regardless of what would
		// otherwise match.
		none, err := surplusRetryCandidates(ctx, tx, nil, budgetCandidatesRunID, nil, map[dispatchBucket]int{}, now)
		if err != nil {
			t.Fatalf("surplusRetryCandidates (empty headroom): %v", err)
		}
		if len(none) != 0 {
			t.Fatalf("got %d candidates with empty slotHeadroom, want 0", len(none))
		}

		headroom := map[dispatchBucket]int{{orgID: "org-1", provider: "github", costClass: "rest_core"}: 5}
		units, err := surplusRetryCandidates(ctx, tx, nil, budgetCandidatesRunID, nil, headroom, now)
		if err != nil {
			t.Fatalf("surplusRetryCandidates: %v", err)
		}
		if len(units) != 1 || units[0].id != notYetDueBudget {
			t.Fatalf("got %v, want exactly [%s] (not-yet-due AND budget_deferred)", idsOf(units), notYetDueBudget)
		}
	})
}

// TestSurplusRetryCandidatesOrdersLongestDeferredFirst pins the ordering
// contract itself -- the whole point of the feature, per the Python
// docstring: serving the newest deferral first would let a steady trickle
// of fresh deferrals starve the oldest indefinitely.
func TestSurplusRetryCandidatesOrdersLongestDeferredFirst(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		future := now.Add(time.Hour)

		oldest := "00000000-0000-4000-8000-000000000041"
		middle := "00000000-0000-4000-8000-000000000042"
		newest := "00000000-0000-4000-8000-000000000043"
		noDeferredAt := "00000000-0000-4000-8000-000000000044" // tiebreak: sorts LAST (far-future stand-in)

		oldestDeferredAt := now.Add(-3 * time.Hour)
		middleDeferredAt := now.Add(-2 * time.Hour)
		newestDeferredAt := now.Add(-1 * time.Hour)

		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: newest, status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now, resultJSON: `{"error_category":"budget_deferred"}`, budgetFirstDefAt: &newestDeferredAt})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: oldest, status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now, resultJSON: `{"error_category":"budget_deferred"}`, budgetFirstDefAt: &oldestDeferredAt})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: noDeferredAt, status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now, resultJSON: `{"error_category":"budget_deferred"}`})
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: middle, status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now, resultJSON: `{"error_category":"budget_deferred"}`, budgetFirstDefAt: &middleDeferredAt})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		headroom := map[dispatchBucket]int{{orgID: "org-1", provider: "github", costClass: "rest_core"}: 5}
		units, err := surplusRetryCandidates(ctx, tx, nil, budgetCandidatesRunID, nil, headroom, now)
		if err != nil {
			t.Fatalf("surplusRetryCandidates: %v", err)
		}
		gotOrder := make([]string, len(units))
		for i, u := range units {
			gotOrder[i] = u.id
		}
		wantOrder := []string{oldest, middle, newest, noDeferredAt}
		for i := range wantOrder {
			if i >= len(gotOrder) || gotOrder[i] != wantOrder[i] {
				t.Fatalf("order=%v, want %v (longest-deferred first, nil-deferred-at last)", gotOrder, wantOrder)
			}
		}
	})
}

// TestSurplusRetryCandidatesTruncatesAtTheConfiguredMax pins the
// silent-cap-must-be-logged discipline: truncation happens, but the test
// asserts on the OBSERVABLE contract (count), not the log line itself,
// since the count is what a caller actually depends on.
func TestSurplusRetryCandidatesTruncatesAtTheConfiguredMax(t *testing.T) {
	withBudgetCandidatesPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_BUDGET_SURPLUS_MAX_CANDIDATES", "2")
		now := pgNow()
		future := now.Add(time.Hour)
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("00000000-0000-4000-8000-00000000005%d", i)
			deferredAt := now.Add(-time.Duration(i+1) * time.Hour)
			insertCandidateUnit(t, ctx, pool, candidateUnitFixture{
				id: id, status: syncRunUnitStatusRetrying, availableAt: &future, updatedAt: now,
				resultJSON: `{"error_category":"budget_deferred"}`, budgetFirstDefAt: &deferredAt,
			})
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		headroom := map[dispatchBucket]int{{orgID: "org-1", provider: "github", costClass: "rest_core"}: 5}
		units, err := surplusRetryCandidates(ctx, tx, nil, budgetCandidatesRunID, nil, headroom, now)
		if err != nil {
			t.Fatalf("surplusRetryCandidates: %v", err)
		}
		if len(units) != 2 {
			t.Fatalf("got %d units, want 2 (truncated from 5)", len(units))
		}
	})
}
