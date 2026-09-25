//go:build integration

package schedulerservice

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestProductionOccurrenceStepperMintsWebhookSyncRequests proves the
// production composition reaches the CHAOS-6695 minter: the stepper
// productionSchedulerRuntimeSources.newOccurrences builds -- the one the
// scheduler binary runs -- settles a pending webhook_sync_requests row in its
// Reconcile window, leaving the occurrence and its manual trigger and no
// request. A composition that dropped WithWebhookRequests would leave the
// request pending here forever.
func TestProductionOccurrenceStepperMintsWebhookSyncRequests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
	conn, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("apply the schema: %v", err)
	}
	_ = conn.Close(ctx)

	const org = "00000000-0000-4000-8000-000000006695"
	now := time.Now().UTC()
	var integrationID, configID, sourceID string
	for _, step := range []struct {
		sql  string
		args []any
		out  *string
	}{
		{`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at)
VALUES ($1, 'wsr', 'wsr', '{}', 'enterprise', true, $2, $2) RETURNING id::text`, []any{org, now}, new(string)},
		{`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'github', 'wsr', '{}', true, $2, $2) RETURNING id::text`, []any{org, now}, &integrationID},
	} {
		if err := pool.QueryRow(ctx, step.sql, step.args...).Scan(step.out); err != nil {
			t.Fatal(err)
		}
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata,
	is_enabled, discovered_at, last_seen_at)
VALUES (gen_random_uuid(), $1, $2, 'github', 'repository', '42', 'r', 'o/r', '{}', true, $3, $3) RETURNING id::text`,
		org, integrationID, now).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
INSERT INTO sync_configurations (id, org_id, name, provider, integration_id, source_id, sync_targets, sync_options, is_active,
	planner_managed, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'wsr', 'github', $2, $3, '[]', '{}', true, false, $4, $4) RETURNING id::text`,
		org, integrationID, sourceID, now).Scan(&configID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO webhook_sync_requests (delivery_id, org_id, sync_config_id, mode, source_ids, scheduled_for)
VALUES (gen_random_uuid(), $1, $2, 'incremental', ARRAY[$3], $4)`, org, configID, sourceID, now); err != nil {
		t.Fatal(err)
	}

	stepper, err := productionSchedulerRuntimeSources.newOccurrences(pool, pool, config.Config{RiverDatabaseSchema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stepper.Reconcile(ctx, time.Now().UTC(), 10); err != nil {
		t.Fatal(err)
	}
	var requests, occurrences, triggers int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM webhook_sync_requests),
  (SELECT count(*) FROM scheduled_sync_occurrences WHERE sync_config_id = $1::uuid),
  (SELECT count(*) FROM sync_manual_triggers)`, configID).Scan(&requests, &occurrences, &triggers); err != nil {
		t.Fatal(err)
	}
	if requests != 0 || occurrences != 1 || triggers != 1 {
		t.Fatalf("after one production Reconcile window: requests=%d occurrences=%d triggers=%d, want 0/1/1",
			requests, occurrences, triggers)
	}
}
