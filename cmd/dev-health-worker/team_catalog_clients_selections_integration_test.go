//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createTeamCatalogSelectionsTables is a minimal, local Postgres fixture for
// teamCatalogSelectionsResolver.ResolveSelections -- includes sync_
// configurations (absent from createTeamCatalogCredentialStampTables, which
// resolveTeamCatalogIntegration never touches) and integrations.config
// (needed for the no-canonical-row owner/group_path fallback below).
func createTeamCatalogSelectionsTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
CREATE TABLE integrations (
  id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL, credential_id uuid NULL,
  config jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE TABLE sync_runs (
  id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
  credential_id uuid NULL, credential_fingerprint text NULL, auth_source text NULL
);
CREATE TABLE sync_configurations (
  id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
  parent_id uuid NULL, sync_options json NOT NULL DEFAULT '{}'::json, created_at timestamptz NOT NULL DEFAULT now()
);
`); err != nil {
		t.Fatal(err)
	}
}

// TestResolveSelectionsFallsBackToIntegrationConfigOwnerWithNoCanonicalRow
// pins the fix routed from 4432's codex round (base-owner item, 2026-08-28):
// reference_discovery.py:329-333 falls back to `dict(integration.config or
// {})` when no canonical sync_configurations row exists, specifically so a
// provider's own org/group_path scope fallback (GitHub's "owner", GitLab's
// group_path) still resolves for a legacy/no-config integration -- it is
// NEVER trusted for CHAOS-4323 auto_import_* selections, which is why this
// test asserts the selections stay at the strict-mode unrestricted default
// (CHAOS-4431 round 2) while the returned map still carries "owner". Before
// this fix, ResolveSelections returned nil for syncOptions on ErrNoRows,
// so "owner"/"group_path" could only ever come from credential.Config,
// leaving a legacy integration's native route unable to resolve it at all.
func TestResolveSelectionsFallsBackToIntegrationConfigOwnerWithNoCanonicalRow(t *testing.T) {
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
	createTeamCatalogSelectionsTables(t, ctx, pool)

	const (
		orgID         = "org-selections-legacy-config"
		integrationID = "00000000-0000-4000-8000-000000000031"
		runID         = "00000000-0000-4000-8000-000000000032"
	)
	for _, statement := range []string{
		`INSERT INTO integrations (id,org_id,provider,config) VALUES ('` + integrationID + `','` + orgID + `','github','{"owner":"acme-org"}'::jsonb)`,
		// No sync_configurations row at all for this integration.
		`INSERT INTO sync_runs (id,org_id,integration_id) VALUES ('` + runID + `','` + orgID + `','` + integrationID + `')`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	resolver := teamCatalogSelectionsResolver{pool: pool}
	selections, syncOptions, err := resolver.ResolveSelections(ctx, orgID, runID, "github", true)
	if err != nil {
		t.Fatalf("ResolveSelections: %v", err)
	}
	if !selections.Teams || !selections.Projects || !selections.Members {
		t.Fatalf("selections=%+v want unrestricted (strict, no canonical row)", selections)
	}
	if owner, _ := syncOptions["owner"].(string); owner != "acme-org" {
		t.Fatalf("syncOptions=%#v want owner=acme-org from integrations.config fallback", syncOptions)
	}
}

// TestResolveSelectionsNonStrictFallbackStaysAllOffDespiteConfigFallback
// proves the selections decision itself never reads the fallback config --
// non-strict (post-sync) dispatch stays all-off on no canonical row exactly
// like before, even though syncOptions is now populated.
func TestResolveSelectionsNonStrictFallbackStaysAllOffDespiteConfigFallback(t *testing.T) {
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
	createTeamCatalogSelectionsTables(t, ctx, pool)

	const (
		orgID         = "org-selections-legacy-config-nonstrict"
		integrationID = "00000000-0000-4000-8000-000000000041"
		runID         = "00000000-0000-4000-8000-000000000042"
	)
	for _, statement := range []string{
		`INSERT INTO integrations (id,org_id,provider,config) VALUES ('` + integrationID + `','` + orgID + `','gitlab','{"group_path":"acme/group"}'::jsonb)`,
		`INSERT INTO sync_runs (id,org_id,integration_id) VALUES ('` + runID + `','` + orgID + `','` + integrationID + `')`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	resolver := teamCatalogSelectionsResolver{pool: pool}
	selections, syncOptions, err := resolver.ResolveSelections(ctx, orgID, runID, "gitlab", false)
	if err != nil {
		t.Fatalf("ResolveSelections: %v", err)
	}
	if selections.Any() {
		t.Fatalf("selections=%+v want all-off (non-strict, no canonical row)", selections)
	}
	if groupPath, _ := syncOptions["group_path"].(string); groupPath != "acme/group" {
		t.Fatalf("syncOptions=%#v want group_path=acme/group from integrations.config fallback", syncOptions)
	}
}
