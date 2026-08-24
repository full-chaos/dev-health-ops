//go:build integration

package syncdispatchruntime

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func withObserveRunPool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
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

// TestObserveRunReturnsEmptyWithoutCallingTheBridgeWhenNoCandidates pins
// the short-circuit, matching enforceRun's own.
func TestObserveRunReturnsEmptyWithoutCallingTheBridgeWhenNoCandidates(t *testing.T) {
	withObserveRunPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		estimator := &fakeBudgetEstimator{}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		observations, err := observeRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, pgNow())
		if err != nil {
			t.Fatalf("observeRun: %v", err)
		}
		if len(observations) != 0 {
			t.Fatalf("got=%v want empty", observations)
		}
		if len(estimator.calls) != 0 {
			t.Fatalf("estimator called %d times, want 0", len(estimator.calls))
		}
	})
}

// TestObserveRunEmitsTheParityPinnedLogLine pins the exact message name
// and field set operator dashboards may already parse:
// dispatch_sync_run.budget_guard_dry_run, carrying decision/bucket/
// budget_key/estimated_units/projected_units/budget_limit/confidence/
// route_family plus the identifying unit_id/sync_run_id/source_id/
// dataset_key/provider/cost_class fields.
func TestObserveRunEmitsTheParityPinnedLogLine(t *testing.T) {
	withObserveRunPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		unitID := "00000000-0000-4000-8000-000000000401"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusPlanned, updatedAt: now})

		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			unitID: estimateFor(10, "github", "rest_core", "work-items"),
		}}
		var logged strings.Builder
		logger := slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: slog.LevelInfo}))

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		observations, err := observeRun(ctx, tx, estimator, logger, "org-1", budgetCandidatesRunID, nil, now)
		if err != nil {
			t.Fatalf("observeRun: %v", err)
		}
		if len(observations) != 1 {
			t.Fatalf("got %d observations, want 1", len(observations))
		}

		output := logged.String()
		if !strings.Contains(output, `"dispatch_sync_run.budget_guard_dry_run"`) {
			t.Fatalf("log output missing the parity-pinned message name: %s", output)
		}
		for _, field := range []string{
			`"decision"`, `"bucket"`, `"budget_key"`, `"estimated_units"`, `"projected_units"`,
			`"budget_limit"`, `"confidence"`, `"route_family"`, `"unit_id"`, `"sync_run_id"`,
			`"source_id"`, `"dataset_key"`, `"provider"`, `"cost_class"`,
		} {
			if !strings.Contains(output, field) {
				t.Fatalf("log output missing field %s: %s", field, output)
			}
		}
	})
}

// TestObserveRunUsesTheDryRunLimitSourceNotTheRealOne pins the "keep the
// separate dry-run-limit source exactly as Python resolves it" condition:
// SYNC_BUDGET_DRY_RUN_DEFAULT_LIMIT must govern this pass, and
// SYNC_BUDGET_DEFAULT_LIMIT (the REAL enforceRun limit) must have zero
// effect on it.
func TestObserveRunUsesTheDryRunLimitSourceNotTheRealOne(t *testing.T) {
	withObserveRunPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		t.Setenv("SYNC_BUDGET_DRY_RUN_BUCKET_LIMITS", "")
		t.Setenv("SYNC_BUDGET_BUCKET_LIMITS", "")
		t.Setenv("SYNC_BUDGET_DRY_RUN_DEFAULT_LIMIT", "50")
		t.Setenv("SYNC_BUDGET_DEFAULT_LIMIT", "1000") // must be IGNORED by observeRun
		now := pgNow()
		unitID := "00000000-0000-4000-8000-000000000402"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusPlanned, updatedAt: now})

		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			unitID: estimateFor(60, "github", "rest_core", "work-items"), // 60 > 50 (dry-run limit), 60 < 1000 (real limit)
		}}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		observations, err := observeRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, now)
		if err != nil {
			t.Fatalf("observeRun: %v", err)
		}
		if len(observations) != 1 || observations[0]["decision"] != "would_defer" {
			t.Fatalf("got=%v want a single would_defer observation -- if the REAL 1000 limit leaked in, this would be would_allow instead", observations)
		}
	})
}

// TestObserveRunNeverWritesToTheDatabase pins the read-only / zero-effect
// property empirically, not just by inspecting the signature: after a full
// observeRun pass over a candidate, its row must be byte-identical to
// before.
func TestObserveRunNeverWritesToTheDatabase(t *testing.T) {
	withObserveRunPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		unitID := "00000000-0000-4000-8000-000000000403"
		insertCandidateUnit(t, ctx, pool, candidateUnitFixture{id: unitID, status: syncRunUnitStatusPlanned, updatedAt: now})

		var beforeStatus string
		var beforeUpdatedAt time.Time
		if err := pool.QueryRow(ctx, `SELECT status, updated_at FROM sync_run_units WHERE id=$1`, unitID).
			Scan(&beforeStatus, &beforeUpdatedAt); err != nil {
			t.Fatal(err)
		}

		// A tiny bucket limit so this unit would_defer -- if observeRun
		// mistakenly routed a would-defer decision into a real write path,
		// this is the fixture that would expose it.
		t.Setenv("SYNC_BUDGET_DRY_RUN_DEFAULT_LIMIT", "1")
		estimator := &fakeBudgetEstimator{estimates: map[string][]budgetEstimate{
			unitID: estimateFor(999, "github", "rest_core", "work-items"),
		}}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		observations, err := observeRun(ctx, tx, estimator, nil, "org-1", budgetCandidatesRunID, nil, now)
		if err != nil {
			t.Fatalf("observeRun: %v", err)
		}
		if len(observations) != 1 || observations[0]["decision"] != "would_defer" {
			t.Fatalf("got=%v want a single would_defer observation", observations)
		}

		var afterStatus string
		var afterUpdatedAt time.Time
		if err := tx.QueryRow(ctx, `SELECT status, updated_at FROM sync_run_units WHERE id=$1`, unitID).
			Scan(&afterStatus, &afterUpdatedAt); err != nil {
			t.Fatal(err)
		}
		if afterStatus != beforeStatus || !afterUpdatedAt.Equal(beforeUpdatedAt) {
			t.Fatalf("row changed: before=(%s,%s) after=(%s,%s) -- observeRun must never write", beforeStatus, beforeUpdatedAt, afterStatus, afterUpdatedAt)
		}
	})
}
