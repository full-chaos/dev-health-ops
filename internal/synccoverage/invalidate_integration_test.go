//go:build integration

package synccoverage

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestInvalidateForIntegrationMarksOnlyTheIntegrationsProjections pins the
// shared invalidation (the worker's finalize_sync_run and the api's sync
// config writes both call it): every projection of the org's configs on
// the integration gets invalidated_at and updated_at set; projections of
// another integration, of the same integration id in another org, and of a
// config with no integration keep their values; an integration with no
// config is a no-op.
func TestInvalidateForIntegrationMarksOnlyTheIntegrationsProjections(t *testing.T) {
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
	if _, err := pool.Exec(ctx, `
CREATE TABLE public.sync_configurations (id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid);
CREATE TABLE public.sync_coverage_projections (
 id uuid PRIMARY KEY, org_id text NOT NULL, sync_config_id uuid NOT NULL, invalidated_at timestamptz,
 updated_at timestamptz NOT NULL DEFAULT '2000-01-01 00:00:00+00')`); err != nil {
		t.Fatal(err)
	}
	integration, otherIntegration := uuid.New(), uuid.New()
	type seed struct {
		org         string
		integration any
		want        string
	}
	seeds := []seed{
		{"org", integration, "invalidated moved"},
		{"org", integration, "invalidated moved"},
		{"org", otherIntegration, "valid kept"},
		{"other-org", integration, "valid kept"},
		{"org", nil, "valid kept"},
	}
	// Rows whose projection org differs from its config's org, so each org
	// filter is the only one that decides them: an org config's projection
	// stored under another org (only the UPDATE's org filter keeps it), and
	// another org's config on the integration whose projection is stored
	// under this org (only the config lookup's org filter keeps it).
	type crossed struct{ configOrg, projectionOrg string }
	crossedSeeds := []crossed{{"org", "other-org"}, {"other-org", "org"}}
	var projections []uuid.UUID
	for _, s := range seeds {
		config, projection := uuid.New(), uuid.New()
		projections = append(projections, projection)
		if _, err := pool.Exec(ctx, `INSERT INTO sync_configurations (id, org_id, integration_id) VALUES ($1, $2, $3)`, config, s.org, s.integration); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO sync_coverage_projections (id, org_id, sync_config_id) VALUES ($1, $2, $3)`, projection, s.org, config); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range crossedSeeds {
		config, projection := uuid.New(), uuid.New()
		projections = append(projections, projection)
		seeds = append(seeds, seed{org: c.configOrg + "/" + c.projectionOrg, integration: integration, want: "valid kept"})
		if _, err := pool.Exec(ctx, `INSERT INTO sync_configurations (id, org_id, integration_id) VALUES ($1, $2, $3)`, config, c.configOrg, integration); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO sync_coverage_projections (id, org_id, sync_config_id) VALUES ($1, $2, $3)`, projection, c.projectionOrg, config); err != nil {
			t.Fatal(err)
		}
	}
	run := func(org, integrationID string) {
		t.Helper()
		if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			return InvalidateForIntegration(ctx, tx, org, integrationID)
		}); err != nil {
			t.Fatal(err)
		}
	}
	run("org", uuid.NewString()) // no config on this integration: no-op
	run("org", integration.String())
	for index, projection := range projections {
		var got string
		if err := pool.QueryRow(ctx, `SELECT
 CASE WHEN invalidated_at IS NULL THEN 'valid' ELSE 'invalidated' END || ' ' ||
 CASE WHEN updated_at > '2000-01-01 00:00:00+00' THEN 'moved' ELSE 'kept' END
FROM sync_coverage_projections WHERE id = $1`, projection).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != seeds[index].want {
			t.Errorf("projection %d (%s, %v): %s, want %s", index, seeds[index].org, seeds[index].integration, got, seeds[index].want)
		}
	}
}
