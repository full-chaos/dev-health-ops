//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-13827: TestOpsBudgetBlockedUnitsSQL reads the budget block off the unit result the route writes. Each result is
// built by the production producer (githubTestsFinalMetadataBatch, then the completion metadata Complete adds before it
// persists), never hand-written JSON. The RUN and the ARTIFACT inventory page budgets count (CHAOS-13801), the artifact walk bound included.
func TestTestOpsBudgetBlockedUnitsSQLReadsTheRouteResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeContext)
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	since := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	before := since.Add(72 * time.Hour)
	claim := nativeTestClaim("github", "cicd")
	claim.SinceAt, claim.BeforeAt = &since, &before
	produce := func(t *testing.T, cursor githubTestsChunkCursor) string {
		t.Helper()
		batch, err := githubTestsFinalMetadataBatch(claim, cursor)
		if err != nil {
			t.Fatal(err)
		}
		_, audited, err := workItemAliasCompletionMetadata(claim.Provider, claim.Dataset,
			map[string]bool{workItemFamilyFlagForDataset("cicd"): true}, batch.Result)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(audited)
		if err != nil {
			t.Fatal(err)
		}
		return string(encoded)
	}
	base := githubTestsChunkCursor{Phase: "done", Repo: "acme/api"}
	runBudget := recordGitHubTestsInventoryTruncation(base, nil, claim, githubTestsRunInventoryComponent, 100)
	artifactBudget := recordGitHubTestsInventoryTruncation(base, nil, claim, githubTestsArtifactInventoryComponent, 100)
	walkBound := recordGitHubTestsArtifactsWalkBound(base, nil, claim, githubTestsMaxWalkRecords)
	perRun := recordGitHubTestsPerRunTruncation(base, nil, claim, githubTestsRunJobsComponent, githubTestsPerRunPageBudgetCause, "77", 1000)
	saturated := recordGitHubTestsSaturated(base, nil, claim, githubTestsRunInventorySaturatedComponent, githubTestsRunsPhase, "2026-11-12T00:00:00Z", 1000)

	type unit struct {
		dataset, status, result string
		blocked                 bool
	}
	at := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	// Newest first in the read; inserted oldest first.
	units := []unit{
		{"cicd", "success", produce(t, base), false},
		{"cicd", "success", produce(t, runBudget), true},
		{"cicd", "success", produce(t, artifactBudget), true},
		{"cicd", "success", produce(t, walkBound), true},
		{"cicd", "success", produce(t, perRun), false},
		{"cicd", "success", produce(t, saturated), false},
		{"cicd", "failed", `{"cause_class":"rate_limited"}`, false},
		{"tests", "success", produce(t, runBudget), false},
		{"cicd", "running", produce(t, runBudget), false},
	}
	var want []bool
	for index, item := range units {
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, result, updated_at, created_at, attempts)
VALUES ($1, 'org-acme', $2, $3, $4, 'github', $5, 'medium', 'incremental', $6, $7::json, $8, now(), 0)`,
			uuid.NewString(), firstRunID, firstIntegrationID, firstSourceID, item.dataset, item.status, item.result,
			at.Add(time.Duration(index)*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if item.dataset == "cicd" && (item.status == "success" || item.status == "failed") {
			want = append([]bool{item.blocked}, want...)
		}
	}
	rows, err := pool.Query(ctx, TestOpsBudgetBlockedUnitsSQL, "org-acme", firstIntegrationID, "cicd", TestOpsNarrowingLookback)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []bool
	for rows.Next() {
		var source string
		var blocked bool
		var updated time.Time
		if err := rows.Scan(&source, &blocked, &updated); err != nil {
			t.Fatal(err)
		}
		if source != firstSourceID {
			t.Fatalf("row of source %s, want only %s", source, firstSourceID)
		}
		got = append(got, blocked)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("budget_blocked newest first = %v, want %v", got, want)
	}
	if count := TestOpsBudgetBlockedCount(got); count != 3 {
		t.Fatalf("blocked count = %d, want 3 (the run and artifact inventory budgets, the artifact walk bound)", count)
	}
}
