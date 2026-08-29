//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createTeamCatalogCredentialStampTables is a minimal, local Postgres
// fixture -- NOT providersyncschema.Create's full fixture, which this
// package's other integration tests use for end-to-end provider-unit runs --
// because resolveTeamCatalogIntegration only ever touches sync_runs and
// integrations. Mirrors internal/syncdispatchruntime/
// native_reference_discovery_integration_test.go's equally minimal
// createReferenceDiscoveryTables for the same reason.
func createTeamCatalogCredentialStampTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
CREATE TABLE integrations (
  id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL, credential_id uuid NULL
);
CREATE TABLE sync_runs (
  id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
  credential_id uuid NULL, credential_fingerprint text NULL, auth_source text NULL
);
`); err != nil {
		t.Fatal(err)
	}
}

// TestResolveTeamCatalogIntegrationUsesTheRunFrozenCredentialWhenStamped
// pins the CHAOS-4431 codex review P1 fix: resolve_run_auth's exact branch
// (sync_bootstrap.py:163-216) for a STAMPED run (auth_source NOT NULL) is
// the run's OWN credential_id, never the live, mutable
// integrations.credential_id -- even when the two have since diverged
// (simulating a credential rotated mid-run). Before this fix,
// resolveTeamCatalogIntegration always returned the mutable one.
func TestResolveTeamCatalogIntegrationUsesTheRunFrozenCredentialWhenStamped(t *testing.T) {
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
	createTeamCatalogCredentialStampTables(t, ctx, pool)

	const (
		orgID           = "org-credential-stamp"
		integrationID   = "00000000-0000-4000-8000-000000000001"
		runID           = "00000000-0000-4000-8000-000000000002"
		stampedCredID   = "00000000-0000-4000-8000-0000000000aa"
		mutatedLiveCred = "00000000-0000-4000-8000-0000000000bb"
	)
	const stampedFingerprintValue = "deadbeef-fingerprint"
	for _, statement := range []string{
		`INSERT INTO integrations (id,org_id,provider,credential_id) VALUES ('` + integrationID + `','` + orgID + `','linear','` + mutatedLiveCred + `')`,
		// Run was planned/stamped against stampedCredID (CHAOS-2755); the
		// integration's OWN credential_id was rotated to a DIFFERENT value
		// after the run started -- resolveTeamCatalogIntegration must still
		// report the run's own stamp, not the now-mutated live value.
		`INSERT INTO sync_runs (id,org_id,integration_id,credential_id,credential_fingerprint,auth_source) VALUES ('` + runID + `','` + orgID + `','` + integrationID + `','` + stampedCredID + `','` + stampedFingerprintValue + `','integration_credential')`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	gotIntegrationID, gotCredentialID, gotFingerprint, err := resolveTeamCatalogIntegration(ctx, pool, orgID, runID, "linear")
	if err != nil {
		t.Fatalf("resolveTeamCatalogIntegration: %v", err)
	}
	if gotIntegrationID != integrationID {
		t.Fatalf("integrationID=%q want=%q", gotIntegrationID, integrationID)
	}
	if gotCredentialID != stampedCredID {
		t.Fatalf("credentialID=%q want the run-frozen stamp=%q (not the mutated live value=%q)", gotCredentialID, stampedCredID, mutatedLiveCred)
	}
	if gotFingerprint != stampedFingerprintValue {
		t.Fatalf("stampedFingerprint=%q want=%q", gotFingerprint, stampedFingerprintValue)
	}
}

// TestResolveTeamCatalogIntegrationFallsBackToLiveCredentialWhenUnstamped
// pins resolve_run_auth's complementary branch: a run with auth_source NULL
// (legacy/in-flight-at-deploy, CHAOS-2755's migration note) falls back to
// the mutable integrations.credential_id exactly like before this fix --
// this path must NOT regress.
func TestResolveTeamCatalogIntegrationFallsBackToLiveCredentialWhenUnstamped(t *testing.T) {
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
	createTeamCatalogCredentialStampTables(t, ctx, pool)

	const (
		orgID         = "org-credential-unstamped"
		integrationID = "00000000-0000-4000-8000-000000000011"
		runID         = "00000000-0000-4000-8000-000000000012"
		liveCredID    = "00000000-0000-4000-8000-0000000000cc"
	)
	for _, statement := range []string{
		`INSERT INTO integrations (id,org_id,provider,credential_id) VALUES ('` + integrationID + `','` + orgID + `','linear','` + liveCredID + `')`,
		// auth_source and credential_id left NULL: a legacy/in-flight run.
		`INSERT INTO sync_runs (id,org_id,integration_id) VALUES ('` + runID + `','` + orgID + `','` + integrationID + `')`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	gotIntegrationID, gotCredentialID, gotFingerprint, err := resolveTeamCatalogIntegration(ctx, pool, orgID, runID, "linear")
	if err != nil {
		t.Fatalf("resolveTeamCatalogIntegration: %v", err)
	}
	if gotIntegrationID != integrationID || gotCredentialID != liveCredID {
		t.Fatalf("integrationID=%q credentialID=%q want integrationID=%q credentialID=%q (live fallback)", gotIntegrationID, gotCredentialID, integrationID, liveCredID)
	}
	// Unstamped run: _verify_stamped_fingerprint is a no-op (no stamped
	// fingerprint to check against), so ResolveClient must not attempt the
	// comparison at all.
	if gotFingerprint != "" {
		t.Fatalf("stampedFingerprint=%q want empty for an unstamped run", gotFingerprint)
	}
}
