//go:build integration

package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

const keepSkippedRunID = "00000000-0000-4000-8000-0000000000e1"

func syncRunResultOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) map[string]any {
	t.Helper()
	var raw *string
	if err := pool.QueryRow(ctx, `SELECT result::text FROM public.sync_runs WHERE id=$1::uuid`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw == nil {
		t.Fatal("sync run result is NULL")
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(*raw), &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

// The plan writes "skipped_datasets" into sync_runs.result; the run's final
// write replaces the whole result and must keep that entry.
func TestFinalizeKeepsTheSkippedDatasetsTheRunWasPlannedWith(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{
			ID: keepSkippedRunID, OrgID: "org-1", Status: "planned",
			ResultJSON: `{"skipped_datasets":[{"dataset_key":"incidents","reason":"feature_disabled","message":"skipped: feature off"}]}`,
		})
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := writeFinalizeRun(ctx, tx, keepSkippedRunID, 2, 0, time.Now(), "success",
			map[string]any{"completed_units": 2, "failed_units": 0}, nil); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		got := syncRunResultOf(t, ctx, pool, keepSkippedRunID)
		skipped, ok := got["skipped_datasets"].([]any)
		if !ok || len(skipped) != 1 {
			t.Fatalf("result = %v, want skipped_datasets with one entry kept", got)
		}
		if entry, _ := skipped[0].(map[string]any); entry["dataset_key"] != "incidents" || entry["reason"] != "feature_disabled" {
			t.Fatalf("skipped entry = %v", skipped[0])
		}
		if got["completed_units"] != float64(2) {
			t.Fatalf("result = %v, want the final payload written too", got)
		}
	})
}

// A run that was planned with no skip keeps the exact result it always got.
func TestFinalizeWithoutSkippedDatasetsWritesOnlyThePayload(t *testing.T) {
	withBudgetEnforcePool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: keepSkippedRunID, OrgID: "org-1", Status: "planned"})
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := writeFinalizeRun(ctx, tx, keepSkippedRunID, 1, 0, time.Now(), "success",
			map[string]any{"completed_units": 1, "failed_units": 0}, nil); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		got := syncRunResultOf(t, ctx, pool, keepSkippedRunID)
		if _, has := got["skipped_datasets"]; has || len(got) != 2 {
			t.Fatalf("result = %v, want only completed_units and failed_units", got)
		}
	})
}
