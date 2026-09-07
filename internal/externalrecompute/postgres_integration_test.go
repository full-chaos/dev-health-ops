//go:build integration

package externalrecompute

import (
	"context"
	"encoding/json"
	"strings"
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
	dispatcher, err := NewPostgresNativeDispatcher(pool)
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

// TestDispatchAlwaysWritesUTCWindowsWithAZuluOffset is what keeps the
// deliberate UTC divergence in plan.go UNREACHABLE rather than merely unlikely
// (r1 P1-b).
//
// The Go planner takes UTC calendar dates; the Python planner takes the
// calendar date in the PRODUCER's offset. Those disagree for any payload
// carrying a non-zero offset. They cannot disagree today because this
// dispatcher is the only writer of a bridge payload anywhere in the tree and it
// formats .UTC(), so every windowStartedAt/windowEndedAt ends in "Z".
//
// That is a property of one line of code, and a future producer emitting a
// local offset would silently change which day gets recomputed. This test makes
// that a build failure instead.
func TestDispatchAlwaysWritesUTCWindowsWithAZuluOffset(t *testing.T) {
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

	ingestionID := uuid.MustParse("44444444-5555-4666-8777-888888888888")
	if _, err := pool.Exec(ctx, `
		INSERT INTO external_ingest_batches (
			ingestion_id, org_id, source_system, source_instance,
			recompute_status, recompute_scope, updated_at
		) VALUES ($1,'org-1','github','Acme/API','pending','{}'::jsonb,now())
	`, ingestionID); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := NewPostgresNativeDispatcher(pool)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately offset-bearing inputs: +05:00 is the exact case that made
	// the two planners disagree in review.
	offset := time.FixedZone("plus5", 5*60*60)
	start := time.Date(2026, 6, 25, 23, 30, 0, 0, offset)
	end := time.Date(2026, 6, 26, 0, 30, 0, 0, offset)
	if err := dispatcher.Dispatch(ctx, Claim{
		ID: "external-ingest:recompute:pending|offset",
		Scope: streamhandlers.ExternalRecomputeScope{
			OrgID: "org-1", SourceSystem: "github", SourceInstance: "Acme/API",
			IngestionID: ingestionID, RepoIDs: []string{"repo-a"},
			RecordKinds: []string{"commit.v1"},
			WindowStart: &start, WindowEnd: &end,
		},
		ingestionIDs: []string{ingestionID.String()},
	}); err != nil {
		t.Fatal(err)
	}

	var raw []byte
	if err := pool.QueryRow(ctx,
		`SELECT recompute_scope FROM external_ingest_batches WHERE ingestion_id = $1`,
		ingestionID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var scope bridgeScope
	if err := json.Unmarshal(raw, &scope); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]*string{
		"windowStartedAt": scope.WindowStartedAt,
		"windowEndedAt":   scope.WindowEndedAt,
	} {
		if value == nil {
			t.Fatalf("%s was not persisted", name)
		}
		if !strings.HasSuffix(*value, "Z") {
			t.Fatalf("%s = %q: a bridge payload must carry a UTC instant. A "+
				"non-Zulu offset here makes plan.go's UTC calendar-date "+
				"arithmetic disagree with the Python planner about which day "+
				"to recompute (r1 P1-b).", name, *value)
		}
		parsed, err := time.Parse(time.RFC3339Nano, *value)
		if err != nil {
			t.Fatal(err)
		}
		if _, offsetSeconds := parsed.Zone(); offsetSeconds != 0 {
			t.Fatalf("%s = %q has a non-zero zone offset", name, *value)
		}
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
			recompute_dispatched_at timestamptz NULL,
			recompute_completed_at timestamptz NULL,
			recompute_error text NULL,
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
