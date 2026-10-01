//go:build integration

package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// CHAOS-7132: a discovery whose additive leg failed (the jira Atlassian Teams leg) leaves a degraded
// entry in the ledger; the run still succeeds (its units did) but its result must not read as clean.
func TestNativeFinalizeSyncRunCopiesTheDiscoveryLedgersDegradedLegsIntoTheRunResult(t *testing.T) {
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
	createFinalizeTables(t, ctx, pool)
	seedFinalizeRoute(t, ctx, pool)
	insertFinalizeRun(t, ctx, pool, 1, "", "")
	pgseed.EnsureSyncIntegration(ctx, t, pool, finalizeTestOrg, finalizeTestIntegration, finalizeTestSource)
	since, before := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
		ID: finalizeTestUnit, RunID: finalizeTestRun, OrgID: finalizeTestOrg, IntegrationID: finalizeTestIntegration,
		SourceID: finalizeTestSource, Status: "success", SinceAt: &since, BeforeAt: &before, CostClass: "heavy",
	})
	ledger := `{"provider":"jira","outcome":"native_degraded","degraded":[{"dataset":"teams","leg":"jira_atlassian_teams","outcome":"failed","reason":"unclassified","detail":"Invalid Organization Ari"}]}`
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_reference_discoveries (id, sync_run_id, org_id, status, attempts, available_at, created_at, updated_at, result)
VALUES (gen_random_uuid(), $1::uuid, $2, 'success', 1, now(), now(), now(), $3::json)`, finalizeTestRun, finalizeTestOrg, ledger); err != nil {
		t.Fatal(err)
	}

	service, err := NewNativeFinalizeSyncRunService(pool, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	var status string
	var resultRaw []byte
	if err := pool.QueryRow(ctx, `SELECT status, result::text FROM sync_runs WHERE id=$1`, finalizeTestRun).Scan(&status, &resultRaw); err != nil {
		t.Fatal(err)
	}
	if status != syncRunStatusSuccess {
		t.Fatalf("status = %q: a failed ADDITIVE leg must not fail a run whose units all succeeded", status)
	}
	var result struct {
		Degraded []map[string]string `json:"degraded"`
	}
	if err := json.Unmarshal(resultRaw, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Degraded) != 1 || result.Degraded[0]["leg"] != "jira_atlassian_teams" || result.Degraded[0]["outcome"] != "failed" || result.Degraded[0]["dataset"] != "teams" {
		t.Fatalf("run result %s does not carry the degraded leg", resultRaw)
	}
}
