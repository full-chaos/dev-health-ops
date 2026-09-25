//go:build integration

package pgseed_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// TestEveryHelperInsertsIntoTheMigratedSchema is the drift guard for the seed helpers themselves:
// each helper's INSERT must satisfy every NOT NULL column, check constraint and foreign key of the
// real table. A migration that adds a NOT NULL column to one of these tables fails HERE, once,
// instead of in every consumer test (CHAOS-6769, Trap #412), and each row is read back so a helper
// that inserts nothing cannot pass.
func TestEveryHelperInsertsIntoTheMigratedSchema(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)

	org, flag, integration, source, credential := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	pgseed.Org(ctx, t, pool, org, "team")
	pgseed.FeatureFlag(ctx, t, pool, flag, "pgseed_probe_flag", "team", true)
	pgseed.OrgOverride(ctx, t, pool, org, flag, true)
	pgseed.OrgLicense(ctx, t, pool, org, "team", `{}`)
	pgseed.Setting(ctx, t, pool, org, "llm", "provider", "openai", false)
	pgseed.Integration(ctx, t, pool, integration, org, "github")
	pgseed.IntegrationSource(ctx, t, pool, source, org, integration, "github", "acme/api")
	pgseed.Credential(ctx, t, pool, credential, org, "github")
	pgseed.RoutingState(ctx, t, pool, "schema", "document", "operation", "canary")
	pgseed.RoutingState(ctx, t, pool, "schema", "document", "operation", "primary") // the upsert path
	unit, run := uuid.NewString(), uuid.NewString()
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: run, TotalUnits: 1})
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: run}) // idempotent
	pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{ID: unit, RunID: run, Status: "planned", ResultJSON: `{}`})
	pgseed.TierLimit(ctx, t, pool, "community", "max_sync_units", "3")
	pgseed.TierLimit(ctx, t, pool, "community", "max_sync_units", "4") // the upsert path

	for _, table := range []struct {
		name  string
		query string
		want  int
	}{
		{"organizations", `SELECT count(*) FROM organizations WHERE id = '` + org + `'`, 1},
		{"feature_flags", `SELECT count(*) FROM feature_flags WHERE key = 'pgseed_probe_flag'`, 1},
		{"org_feature_overrides", `SELECT count(*) FROM org_feature_overrides WHERE org_id = '` + org + `'`, 1},
		{"org_licenses", `SELECT count(*) FROM org_licenses WHERE org_id = '` + org + `'`, 1},
		{"settings", `SELECT count(*) FROM settings WHERE org_id = '` + org + `' AND key = 'provider'`, 1},
		{"integrations", `SELECT count(*) FROM integrations WHERE id = '` + integration + `'`, 1},
		{"integration_sources", `SELECT count(*) FROM integration_sources WHERE id = '` + source + `'`, 1},
		{"integration_credentials", `SELECT count(*) FROM integration_credentials WHERE id = '` + credential + `'`, 1},
		{"sync_runs", `SELECT count(*) FROM sync_runs WHERE id = '` + run + `' AND total_units = 1`, 1},
		{"sync_run_units", `SELECT count(*) FROM sync_run_units WHERE id = '` + unit + `' AND sync_run_id = '` + run + `'`, 1},
		{"tier_limits", `SELECT count(*) FROM tier_limits WHERE tier = 'community' AND limit_key = 'max_sync_units' AND limit_value = '4'`, 1},
		{"go_api_routing_state", `SELECT count(*) FROM go_api_routing_state WHERE mode = 'primary'`, 1},
	} {
		var got int
		if err := pool.QueryRow(ctx, table.query).Scan(&got); err != nil {
			t.Fatalf("%s: %v", table.name, err)
		}
		if got != table.want {
			t.Errorf("%s: %d rows read back, want %d", table.name, got, table.want)
		}
	}
}
