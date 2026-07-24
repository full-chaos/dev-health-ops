//go:build integration

package externalrecompute

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresCompatibilityBridgeIsDeterministicAndDoesNotDuplicateBatchStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createCompatibilityTables(t, ctx, pool)

	firstID := uuid.MustParse("11111111-2222-4333-8444-555555555555")
	secondID := uuid.MustParse("22222222-3333-4444-8555-666666666666")
	initialScope := map[string]any{
		"repoIds": []string{"repo-a"}, "teamIds": []string{"team-a"},
		"recordKinds":     []string{"commit.v1"},
		"windowStartedAt": "2026-07-01T00:00:00Z",
		"windowEndedAt":   "2026-07-23T00:00:00Z",
	}
	for _, ingestionID := range []uuid.UUID{firstID, secondID} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO external_ingest_batches (
				ingestion_id, org_id, source_system, source_instance,
				recompute_status, recompute_scope, updated_at
			) VALUES ($1,'org-1','github','Acme/API','pending',$2,now())
		`, ingestionID, initialScope); err != nil {
			t.Fatal(err)
		}
	}
	dispatcher, err := NewPostgresCompatibilityDispatcher(pool)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := dispatcher.PendingScopes(ctx, 10)
	if err != nil || len(pending) != 2 {
		t.Fatalf("recoverable pending scopes=%v err=%v", pending, err)
	}

	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
	claim := Claim{
		ID: "external-ingest:recompute:pending|7",
		Scope: streamhandlers.ExternalRecomputeScope{
			OrgID: "org-1", SourceSystem: "github", SourceInstance: "Acme/API",
			IngestionID: firstID, RepoIDs: []string{"repo-a"},
			TeamIDs: []string{"team-a"}, RecordKinds: []string{"commit.v1"},
			WindowStart: &start, WindowEnd: &end,
		},
		ingestionIDs: []string{firstID.String(), secondID.String()},
	}
	if err := dispatcher.Dispatch(ctx, claim); err != nil {
		t.Fatal(err)
	}
	// Crash-after-bridge-before-Valkey-complete retries the same claim.
	if err := dispatcher.Dispatch(ctx, claim); err != nil {
		t.Fatal(err)
	}
	var jobCount, batchCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM external_ingest_recompute_jobs`).Scan(&jobCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM external_ingest_batches`).Scan(&batchCount); err != nil {
		t.Fatal(err)
	}
	if jobCount != 1 || batchCount != 2 {
		t.Fatalf("duplicate bridge state: jobs=%d batches=%d", jobCount, batchCount)
	}
	rows, err := pool.Query(ctx, `SELECT recompute_status, recompute_scope FROM external_ingest_batches ORDER BY ingestion_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var raw []byte
		if err := rows.Scan(&status, &raw); err != nil {
			t.Fatal(err)
		}
		var scope bridgeScope
		if err := json.Unmarshal(raw, &scope); err != nil {
			t.Fatal(err)
		}
		if status != "pending" || scope.BridgeKind != CompatibilityBridgeKind ||
			scope.BridgeID == "" || scope.BridgeVersion != 1 {
			t.Fatalf("persisted bridge status=%s scope=%#v", status, scope)
		}
	}
	pending, err = dispatcher.PendingScopes(ctx, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("bridged scopes were re-coalesced: scopes=%v err=%v", pending, err)
	}

	// Bridged rows must be excluded before LIMIT. Otherwise an old, still
	// pending bridge can occupy every recovery slot and starve a later
	// unbridged batch for this or another source.
	thirdID := uuid.MustParse("33333333-4444-4555-8666-777777777777")
	if _, err := pool.Exec(ctx, `
		INSERT INTO external_ingest_batches (
			ingestion_id, org_id, source_system, source_instance,
			recompute_status, recompute_scope, updated_at
		) VALUES ($1,'org-2','gitlab','Acme/Web','pending',$2,now())
	`, thirdID, initialScope); err != nil {
		t.Fatal(err)
	}
	pending, err = dispatcher.PendingScopes(ctx, 1)
	if err != nil || len(pending) != 1 || pending[0].IngestionID != thirdID {
		t.Fatalf("unbridged scope behind old bridges = %v err=%v", pending, err)
	}
}

func createCompatibilityTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		CREATE TABLE external_ingest_batches (
			ingestion_id uuid PRIMARY KEY,
			org_id text NOT NULL,
			source_system text NOT NULL,
			source_instance text NOT NULL,
			recompute_status text NOT NULL,
			recompute_scope jsonb NULL,
			updated_at timestamptz NOT NULL
		);
		CREATE TABLE external_ingest_recompute_jobs (
			id uuid PRIMARY KEY,
			org_id text NOT NULL,
			source_system text NOT NULL,
			source_instance text NOT NULL,
			celery_task_name text NOT NULL,
			celery_task_id text NULL,
			queue text NOT NULL,
			repo_id text NULL,
			status text NOT NULL,
			dispatched_at timestamptz NOT NULL
		)
	`)
	if err != nil {
		t.Fatal(err)
	}
}
