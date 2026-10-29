//go:build integration

package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-11888 (D6467): a run that planned zero units because the provider has no such dataset (GitHub + incidents)
// finalizes as a no-op success. The persisted rows are the contract: the run row reads success with no error and a
// start time, the reason is recorded, and the configuration's last-sync fields keep describing the last REAL sync.
func TestNativeFinalizeSyncRunNoDatasetForProviderPersistsANoOpSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance := pgschema.Start(ctx, t)
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	seedFinalizeRoute(t, ctx, pool)

	lastRealSync := time.Date(2026, time.October, 1, 6, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `UPDATE sync_configurations
SET last_sync_at=$2, last_sync_success=TRUE, last_sync_error=NULL, last_sync_stats='{"units":7}'::json WHERE id=$1`,
		finalizeTestSyncConfig, lastRealSync); err != nil {
		t.Fatal(err)
	}
	insertZeroUnitRun(t, ctx, pool, `{"reason":"no_dataset_for_provider"}`, "")

	metrics, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewNativeFinalizeSyncRunService(pool, nil, metrics)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	var status string
	var runError *string
	var startedAt, completedAt *time.Time
	var resultJSON []byte
	if err := pool.QueryRow(ctx, `SELECT status, error, started_at, completed_at, result FROM sync_runs WHERE id=$1`, finalizeTestRun).
		Scan(&status, &runError, &startedAt, &completedAt, &resultJSON); err != nil {
		t.Fatal(err)
	}
	if status != syncRunStatusSuccess {
		t.Errorf("status=%q want=%q", status, syncRunStatusSuccess)
	}
	if runError != nil {
		t.Errorf("error=%q want NULL", *runError)
	}
	if startedAt == nil {
		t.Error("started_at is NULL, want it set")
	}
	if completedAt == nil {
		t.Error("completed_at is NULL, want it set")
	}
	var result map[string]any
	if err := json.Unmarshal(resultJSON, &result); err != nil {
		t.Fatal(err)
	}
	if result["reason"] != "no_dataset_for_provider" {
		t.Errorf("result=%s want the reason no_dataset_for_provider recorded", resultJSON)
	}

	var lastAt *time.Time
	var lastSuccess *bool
	var lastError *string
	var lastStats []byte
	if err := pool.QueryRow(ctx, `SELECT last_sync_at, last_sync_success, last_sync_error, last_sync_stats FROM sync_configurations WHERE id=$1`,
		finalizeTestSyncConfig).Scan(&lastAt, &lastSuccess, &lastError, &lastStats); err != nil {
		t.Fatal(err)
	}
	if lastAt == nil || !lastAt.Equal(lastRealSync) {
		t.Errorf("last_sync_at=%v want unchanged %v", lastAt, lastRealSync)
	}
	if lastSuccess == nil || !*lastSuccess {
		t.Errorf("last_sync_success=%v want unchanged true", lastSuccess)
	}
	if lastError != nil {
		t.Errorf("last_sync_error=%q want unchanged NULL", *lastError)
	}
	var stats map[string]any
	if err := json.Unmarshal(lastStats, &stats); err != nil || stats["units"] != float64(7) {
		t.Errorf("last_sync_stats=%s (err %v) want unchanged {\"units\":7}", lastStats, err)
	}
}
