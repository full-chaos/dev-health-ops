//go:build integration

package backfillrun

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestBackfillPlansTheScheduledSourceSet (RED-only form for main, CHAOS-10064):
// a backfill that names no sources must plan exactly the sources a scheduled
// run plans: the enabled sources tagged for the configuration (the
// loadPlanSources predicate, written out here because main has no exported
// form of it).
func TestBackfillPlansTheScheduledSourceSet(t *testing.T) {
	instance, admin := startDatabase(t)
	uri := freshDatabase(t, instance, admin)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	integration, config := uuidN(0x1a, 900), uuidN(0xc0, 900)
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, 'github', 'parity', '{}'::json, true, now(), now())`, integration, testOrg)
	exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1::uuid, $2, 'parity', 'github', '["prs"]'::json, '{}'::json, true, true, $3::uuid, now(), now())`, config, testOrg, integration)
	exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES (gen_random_uuid(), $1, $2::uuid, 'prs', true, '{}'::json)`, testOrg, integration)
	tag := func(configID string) string { return fmt.Sprintf(`{"planner_managed_sync_config_id": %q}`, configID) }
	sources := []struct {
		name, metadata string
		enabled        bool
	}{
		{"acme/a-selected", tag(config), true},
		{"acme/b-selected", tag(config), true},
		{"acme/c-untagged", `{}`, true},
		{"acme/d-selected-disabled", tag(config), false},
		{"acme/e-other-config", tag(uuidN(0xc0, 901)), true},
	}
	for index, source := range sources {
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, 'github', 'repository', $4, $4, $4, $5::json, $6, now(), now())`, uuidN(0x5a, 9000+index), testOrg, integration, source.name, source.metadata, source.enabled)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT src.id::text FROM public.integration_sources AS src
WHERE src.org_id = $1 AND src.integration_id = $2::uuid AND src.is_enabled
  AND src.metadata->>'planner_managed_sync_config_id' = $3
ORDER BY src.full_name, src.id`, testOrg, integration, config)
	if err != nil {
		t.Fatal(err)
	}
	scheduled := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		scheduled = append(scheduled, id)
	}
	rows.Close()
	cfg, err := LoadConfig(ctx, tx, config)
	if err != nil || cfg == nil {
		t.Fatalf("load config: %v", err)
	}
	validated, err := Validate(ctx, tx, cfg, Params{ConfigID: config})
	if err != nil {
		t.Fatalf("Validate with no --source-ids: %v", err)
	}
	if !reflect.DeepEqual(validated.SourceIDs, scheduled) {
		t.Fatalf("backfill source set (%d) = %v, scheduled source set (%d) = %v: they must be equal", len(validated.SourceIDs), validated.SourceIDs, len(scheduled), scheduled)
	}
}
