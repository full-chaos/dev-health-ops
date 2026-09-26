//go:build integration

package sync

import (
	"context"
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// plannerFixture is one sync configuration and its schedule marker (scheduled_jobs row), seeded
// into the MIGRATED schema. Empty fields take defaults; the columns the tests do not care about
// (name, provider, timestamps) are filled so the real NOT NULL columns and foreign keys hold.
type plannerFixture struct {
	configID, jobID, orgID string
	// plannerManaged is passed through: the column defaults FALSE in production, and the refusal
	// gate under test refuses FALSE, so a fixture that wants to mint says TRUE.
	plannerManaged bool
	// targets is sync_targets JSON text ("[]" when empty); options is sync_options JSON text.
	targets, options string
	// lastSyncAt and createdAt are timestamp literals; lastSyncAt "" means NULL.
	lastSyncAt, createdAt string
	// jobUpdatedAt is the marker's updated_at (createdAt when empty).
	jobUpdatedAt string
	// cron and timezone are the marker's schedule ("0 * * * *" / "UTC" when empty).
	cron, timezone string
}

func insertPlannerFixture(ctx context.Context, pool *pgxpool.Pool, f plannerFixture) error {
	if f.targets == "" {
		f.targets = "[]"
	}
	if f.options == "" {
		f.options = `{"schedule_cron":"0 * * * *","timezone":"UTC"}`
	}
	if f.cron == "" {
		f.cron = "0 * * * *"
	}
	if f.timezone == "" {
		f.timezone = "UTC"
	}
	if f.jobUpdatedAt == "" {
		f.jobUpdatedAt = f.createdAt
	}
	var lastSyncAt any
	if f.lastSyncAt != "" {
		lastSyncAt = f.lastSyncAt
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_configurations
	(id, org_id, name, provider, is_active, planner_managed, sync_targets, sync_options, last_sync_at, created_at, updated_at)
VALUES ($1::uuid, $2, 'config-' || $1::text, 'github', TRUE, $3, $4::json, $5::json, $6::timestamptz, $7::timestamptz, $7::timestamptz)`,
		f.configID, f.orgID, f.plannerManaged, f.targets, f.options, lastSyncAt, f.createdAt); err != nil {
		return fmt.Errorf("seed sync configuration: %w", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO public.scheduled_jobs
	(id, org_id, name, sync_config_id, job_type, schedule_cron, timezone, status, is_running, created_at, updated_at)
VALUES ($1::uuid, $2, 'job-' || $1::text, $3::uuid, 'sync', $4, $5, 0, FALSE, $6::timestamptz, $6::timestamptz)`,
		f.jobID, f.orgID, f.configID, f.cron, f.timezone, f.jobUpdatedAt); err != nil {
		return fmt.Errorf("seed scheduled job: %w", err)
	}
	return nil
}

// plannerGraph is the org / integration / dataset / sync configuration / schedule marker chain the
// materializer fixtures start from, seeded into the migrated schema. datasetKey "" seeds no dataset;
// sourceID "" leaves sync_configurations.source_id NULL.
type plannerGraph struct {
	orgID, integrationID, provider, configID, jobID, featureID string
	datasetID, datasetKey                                      string
	targets                                                    string
	plannerManaged                                             bool
	jobStatus                                                  int
}

func seedPlannerGraph(ctx context.Context, t *testing.T, pool *pgxpool.Pool, g plannerGraph) {
	t.Helper()
	pgseed.Org(ctx, t, pool, g.orgID, "community")
	pgseed.Integration(ctx, t, pool, g.integrationID, g.orgID, g.provider)
	// The migrations pre-register canonical_incident_ingestion; these fixtures want it community-tier, on.
	pgseed.SetFeatureFlag(ctx, t, pool, g.featureID, "canonical_incident_ingestion", "community", true)
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, integration_id, is_active, planner_managed, created_at, updated_at)
VALUES ($1::uuid, $2, 'graph-config-' || $1::text, $3, $4::json, '{"schedule_cron":"0 * * * *"}'::json, $5::uuid, TRUE, $6, now(), now())`,
		g.configID, g.orgID, g.provider, g.targets, g.integrationID, g.plannerManaged); err != nil {
		t.Fatalf("seed sync configuration: %v", err)
	}
	if g.datasetKey != "" {
		if _, err := pool.Exec(ctx, `
INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES ($1::uuid, $2, $3::uuid, $4, TRUE, '{}'::json)`, g.datasetID, g.orgID, g.integrationID, g.datasetKey); err != nil {
			t.Fatalf("seed integration dataset: %v", err)
		}
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO scheduled_jobs (id, org_id, name, sync_config_id, job_type, schedule_cron, timezone, status, is_running, created_at, updated_at)
VALUES ($1::uuid, $2, 'graph-job-' || $1::text, $3::uuid, 'sync', '0 * * * *', 'UTC', $4, FALSE, now(), now())`,
		g.jobID, g.orgID, g.configID, g.jobStatus); err != nil {
		t.Fatalf("seed scheduled job: %v", err)
	}
}
