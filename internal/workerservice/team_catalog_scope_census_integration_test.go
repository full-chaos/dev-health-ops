//go:build integration

package workerservice

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestTeamCatalogScopeCensusListsTheOrgsOtherActiveIntegrationsOfOneProvider
// pins the read the ownership close gate trusts: every other ACTIVE
// integration of the same provider in the same org, with its credential's
// plain config and its root sync_options (or the integration's own config
// when it has no root row). An inactive integration, another provider, another
// org and the run's own integration are not listed; a row that does not
// decode fails the read.
func TestTeamCatalogScopeCensusListsTheOrgsOtherActiveIntegrationsOfOneProvider(t *testing.T) {
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
	pgschema.Apply(ctx, t, pool)

	const (
		org          = "org-scope-census"
		self         = "00000000-0000-4000-8000-0000000000a1"
		withOptions  = "00000000-0000-4000-8000-0000000000a2"
		withFallback = "00000000-0000-4000-8000-0000000000a3"
		inactive     = "00000000-0000-4000-8000-0000000000a4"
		otherKind    = "00000000-0000-4000-8000-0000000000a5"
		otherOrg     = "00000000-0000-4000-8000-0000000000a6"
		credentialB  = "00000000-0000-4000-8000-0000000000b2"
	)
	at := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	exec := func(statement string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, statement, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'gitlab', 'cred-b', true, 'ciphertext', '{"gitlab_url":"https://gitlab.internal","group_path":"org","port":443}'::json, $3, $3)`,
		credentialB, org, at)
	integration := func(id, orgID, provider, credentialID, config string, active bool) {
		t.Helper()
		var credential any
		if credentialID != "" {
			credential = credentialID
		}
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6::json, $7, $8, $8)`, id, orgID, provider, credential, "int-"+id[len(id)-2:], config, active, at)
	}
	integration(self, org, "gitlab", "", `{}`, true)
	integration(withOptions, org, "GitLab", credentialB, `{"group_path":"ignored-when-a-root-row-exists"}`, true)
	integration(withFallback, org, "gitlab", "", `{"group_path":"legacy/group"}`, true)
	integration(inactive, org, "gitlab", "", `{"group_path":"org"}`, false)
	integration(otherKind, org, "github", "", `{"owner":"org"}`, true)
	integration(otherOrg, "org-elsewhere", "gitlab", "", `{"group_path":"org"}`, true)
	exec(`INSERT INTO sync_configurations (id, org_id, name, provider, integration_id, sync_targets, sync_options, is_active,
	planner_managed, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'root', 'gitlab', $2, '[]', '{"group_path":"org","auto_import_projects":true}'::json, true, true, $3, $3)`,
		org, withOptions, at)

	census := teamCatalogScopeCensus{pool: pool}
	siblings, err := census.ActiveSiblingIntegrations(ctx, org, "gitlab", self)
	if err != nil {
		t.Fatalf("ActiveSiblingIntegrations: %v", err)
	}
	want := []providersync.OwnershipSiblingIntegration{
		{IntegrationID: withOptions,
			CredentialConfig: map[string]string{"gitlab_url": "https://gitlab.internal", "group_path": "org"},
			SyncOptions:      map[string]any{"group_path": "org", "auto_import_projects": true}},
		{IntegrationID: withFallback, CredentialConfig: map[string]string{},
			SyncOptions: map[string]any{"group_path": "legacy/group"}},
	}
	if !reflect.DeepEqual(siblings, want) {
		t.Fatalf("siblings =\n%#v\nwant\n%#v", siblings, want)
	}

	// The org's only other integration of a provider: none for github.
	if siblings, err := census.ActiveSiblingIntegrations(ctx, org, "github", otherKind); err != nil || len(siblings) != 0 {
		t.Fatalf("github siblings = %#v, err = %v; want none", siblings, err)
	}

	// A config that is valid JSON but not an object fails the whole read.
	exec(`UPDATE integrations SET config = '[]'::json WHERE id = $1`, withFallback)
	if siblings, err := census.ActiveSiblingIntegrations(ctx, org, "gitlab", self); err == nil {
		t.Fatalf("a sibling config that does not decode read as %#v, want an error", siblings)
	}
}
