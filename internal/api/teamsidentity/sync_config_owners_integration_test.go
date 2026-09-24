//go:build integration

package teamsidentity

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// syncConfigurationsDDL is alembic 0001_initial_schema.py's own
// sync_configurations table, transcribed column for column -- the real
// table deriveOwnersFromSyncConfigs' SELECT runs against, not a hand-typed
// stub.
const syncConfigurationsDDL = `
CREATE TABLE public.sync_configurations (
    id uuid PRIMARY KEY,
    org_id text NOT NULL DEFAULT 'default',
    name text NOT NULL,
    provider text NOT NULL,
    credential_id uuid,
    sync_targets json NOT NULL DEFAULT '[]',
    sync_options json NOT NULL DEFAULT '{}',
    is_active boolean NOT NULL DEFAULT true,
    last_sync_at timestamptz,
    last_sync_success boolean,
    last_sync_error text,
    last_sync_stats json,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (org_id, name)
)`

func startSyncConfigPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if closeErr := instance.Close(closeCtx); closeErr != nil {
			t.Errorf("terminate PostgreSQL test dependency: %v", closeErr)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, syncConfigurationsDDL); err != nil {
		t.Fatal(err)
	}
	return pool
}

func insertSyncConfig(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, orgID, name, provider, syncOptionsJSON string, active bool) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO public.sync_configurations (id, org_id, name, provider, sync_options, is_active)
		 VALUES ($1::uuid, $2, $3, $4, $5::json, $6)`,
		id, orgID, name, provider, syncOptionsJSON, active); err != nil {
		t.Fatal(err)
	}
}

// TestDeriveOwnersFromSyncConfigsCollectsAcrossActiveConfigs proves the
// exact shape of _derive_owners_from_sync_configs (teams.py:55-79): two
// active github configs, one carrying "owner" and one carrying "org" (both
// option keys checked), a THIRD inactive config whose value must never
// appear, and a duplicate value on a later row that must not be
// double-counted.
func TestDeriveOwnersFromSyncConfigsCollectsAcrossActiveConfigs(t *testing.T) {
	ctx := context.Background()
	pool := startSyncConfigPool(t, ctx)

	insertSyncConfig(t, ctx, pool, "11111111-1111-1111-1111-111111111111", "org-1", "cfg-a", "github", `{"owner":"acme"}`, true)
	insertSyncConfig(t, ctx, pool, "22222222-2222-2222-2222-222222222222", "org-1", "cfg-b", "github", `{"org":"beta-corp"}`, true)
	insertSyncConfig(t, ctx, pool, "33333333-3333-3333-3333-333333333333", "org-1", "cfg-c-inactive", "github", `{"owner":"never-seen"}`, false)
	insertSyncConfig(t, ctx, pool, "44444444-4444-4444-4444-444444444444", "org-1", "cfg-d-dup", "github", `{"owner":"acme"}`, true)
	insertSyncConfig(t, ctx, pool, "55555555-5555-5555-5555-555555555555", "org-2", "cfg-other-org", "github", `{"owner":"wrong-org"}`, true)
	insertSyncConfig(t, ctx, pool, "66666666-6666-6666-6666-666666666666", "org-1", "cfg-gitlab", "gitlab", `{"owner":"wrong-provider"}`, true)

	owners, err := deriveOwnersFromSyncConfigs(ctx, pool, "org-1", "github", []string{"owner", "org"})
	if err != nil {
		t.Fatal(err)
	}
	if len(owners) != 2 || owners[0] != "acme" || owners[1] != "beta-corp" {
		t.Fatalf("owners = %v, want [acme beta-corp]", owners)
	}
}

func TestDeriveOwnersFromSyncConfigsNoMatchReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	pool := startSyncConfigPool(t, ctx)

	owners, err := deriveOwnersFromSyncConfigs(ctx, pool, "org-1", "github", []string{"owner", "org"})
	if err != nil {
		t.Fatal(err)
	}
	if len(owners) != 0 {
		t.Fatalf("owners = %v, want empty", owners)
	}
}

// TestDeriveOwnersFromSyncConfigsGitlabKeyOrder proves gitlab's own key
// order ("group", "owner" -- teams.py:271-273): a single config carrying
// BOTH keys contributes group before owner.
func TestDeriveOwnersFromSyncConfigsGitlabKeyOrder(t *testing.T) {
	ctx := context.Background()
	pool := startSyncConfigPool(t, ctx)

	insertSyncConfig(t, ctx, pool, "77777777-7777-7777-7777-777777777777", "org-1", "cfg-gl", "gitlab", `{"group":"platform-team","owner":"legacy-owner"}`, true)

	owners, err := deriveOwnersFromSyncConfigs(ctx, pool, "org-1", "gitlab", []string{"group", "owner"})
	if err != nil {
		t.Fatal(err)
	}
	if len(owners) != 2 || owners[0] != "platform-team" || owners[1] != "legacy-owner" {
		t.Fatalf("owners = %v, want [platform-team legacy-owner]", owners)
	}
}
