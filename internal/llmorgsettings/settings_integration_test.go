//go:build integration

package llmorgsettings

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// newTestStore boots a real Postgres (containers.StartPostgres -- same
// harness cmd/dev-health-worker's entitlement integration tests use) with
// the settings/feature_flags/org_feature_overrides/org_licenses/
// organizations tables this package reads. One instance per test keeps
// each precedence-matrix case's fixtures independent.
func newTestStore(t *testing.T) (Store, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	for _, statement := range []string{
		`CREATE TABLE settings (
		   org_id text NOT NULL, category text NOT NULL, key text NOT NULL,
		   value text, is_encrypted boolean NOT NULL DEFAULT false)`,
		`CREATE TABLE organizations (id uuid PRIMARY KEY, tier text NOT NULL)`,
		`CREATE TABLE feature_flags (
		   id uuid PRIMARY KEY, key text UNIQUE NOT NULL, min_tier text NOT NULL,
		   is_enabled boolean NOT NULL)`,
		`CREATE TABLE org_feature_overrides (
		   org_id uuid NOT NULL, feature_id uuid NOT NULL, is_enabled boolean NOT NULL,
		   expires_at timestamptz, PRIMARY KEY (org_id, feature_id))`,
		`CREATE TABLE org_licenses (
		   org_id uuid PRIMARY KEY, tier text NOT NULL, features_override json)`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	fixedNow := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	return Store{Pool: pool, Decryptor: decryptor, Now: func() time.Time { return fixedNow }}, pool
}

func seedByoLLMFeature(ctx context.Context, t *testing.T, pool *pgxpool.Pool, minTier string, globallyEnabled bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := pool.Exec(ctx, `
INSERT INTO feature_flags (id, key, min_tier, is_enabled) VALUES ($1, 'byo_llm', $2, $3)`,
		id, minTier, globallyEnabled); err != nil {
		t.Fatal(err)
	}
	return id
}

func seedOrg(ctx context.Context, t *testing.T, pool *pgxpool.Pool, tier string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, tier) VALUES ($1, $2)`, id, tier); err != nil {
		t.Fatal(err)
	}
	return id
}

func insertSetting(ctx context.Context, t *testing.T, pool *pgxpool.Pool, orgID uuid.UUID, key, value string) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO settings (org_id, category, key, value, is_encrypted) VALUES ($1, 'llm', $2, $3, false)`,
		orgID.String(), key, value); err != nil {
		t.Fatal(err)
	}
}

func insertEncryptedSetting(ctx context.Context, t *testing.T, store Store, pool *pgxpool.Pool, orgID uuid.UUID, key, plaintext string) {
	t.Helper()
	ciphertext, err := store.Decryptor.Encrypt([]byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO settings (org_id, category, key, value, is_encrypted) VALUES ($1, 'llm', $2, $3, true)`,
		orgID.String(), key, ciphertext.Reveal()); err != nil {
		t.Fatal(err)
	}
}

// TestResolveUsableProvider_PrecedenceMatrix is the bigboy proof team-lead
// asked for: seeds the same rows an equivalent Python fixture would and
// checks ResolveUsableProvider's answer at every point in the precedence
// matrix (flag off / override on / override off / tier below floor / tier
// at floor / no license), plus the encrypted-column and unknown-org paths
// this package's unit tests cannot reach without a real Postgres.
func TestResolveUsableProvider_PrecedenceMatrix(t *testing.T) {
	ctx := context.Background()

	t.Run("no settings at all -> not usable, no flag lookup needed", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\"", got)
		}
	})

	t.Run("enterprise org, flag globally enabled, no override -> usable (enabled by tier)", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		insertSetting(ctx, t, pool, orgID, "base_url", "https://my-gateway.example.com/v1")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "openai" {
			t.Fatalf("got %q, want %q", got, "openai")
		}
	})

	t.Run("flag globally disabled -> not usable regardless of tier", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", false)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" (flag globally disabled)", got)
		}
	})

	t.Run("community tier below the TEAM floor -> not usable even with a positive org override", func(t *testing.T) {
		store, pool := newTestStore(t)
		featureID := seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "community")
		if _, err := pool.Exec(ctx, `
INSERT INTO org_feature_overrides (org_id, feature_id, is_enabled) VALUES ($1, $2, true)`,
			orgID, featureID); err != nil {
			t.Fatal(err)
		}
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" -- the TEAM floor must not be bypassed by an org override", got)
		}
	})

	t.Run("team tier at the floor, org override disabled -> not usable", func(t *testing.T) {
		store, pool := newTestStore(t)
		featureID := seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "team")
		if _, err := pool.Exec(ctx, `
INSERT INTO org_feature_overrides (org_id, feature_id, is_enabled) VALUES ($1, $2, false)`,
			orgID, featureID); err != nil {
			t.Fatal(err)
		}
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" (org override disabled)", got)
		}
	})

	t.Run("no org_licenses row -> falls back to organizations.tier", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "team")
		insertSetting(ctx, t, pool, orgID, "provider", "ollama")
		insertSetting(ctx, t, pool, orgID, "base_url", "https://my-gateway.example.com/v1")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "ollama" {
			t.Fatalf("got %q, want %q", got, "ollama")
		}
	})

	t.Run("org_licenses.tier wins over organizations.tier", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		if _, err := pool.Exec(ctx,
			`INSERT INTO org_licenses (org_id, tier) VALUES ($1, 'community')`, orgID); err != nil {
			t.Fatal(err)
		}
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" -- org_licenses.tier (community) must win over organizations.tier (enterprise)", got)
		}
	})

	t.Run("no feature_flags row at all -> unregistered, backward-compatible ungated", func(t *testing.T) {
		store, pool := newTestStore(t)
		// Deliberately no seedByoLLMFeature call.
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "openai" {
			t.Fatalf("got %q, want %q (unregistered feature treated as ungated)", got, "openai")
		}
	})

	t.Run("encrypted api_key column decrypts and is usable", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertEncryptedSetting(ctx, t, store, pool, orgID, "api_key", "sk-org-secret")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "openai" {
			t.Fatalf("got %q, want %q", got, "openai")
		}
	})

	t.Run("SSRF-unsafe base_url falls back, not usable", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "ollama")
		insertSetting(ctx, t, pool, orgID, "base_url", "http://169.254.169.254/latest")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" (SSRF-unsafe base_url)", got)
		}
	})

	t.Run("incomplete credentials (api-key-required provider, no key) not usable", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "base_url", "https://my-gateway.example.com/v1")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" (openai requires an api_key)", got)
		}
	})

	t.Run("non-UUID org_id with settings fails closed (error, not silent fallback)", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		insertSetting(ctx, t, pool, uuid.Nil, "provider", "openai")
		if _, err := pool.Exec(ctx,
			`UPDATE settings SET org_id = 'not-a-uuid' WHERE org_id = $1`, uuid.Nil.String()); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ResolveUsableProvider(ctx, "not-a-uuid"); err == nil {
			t.Fatal("expected an error for a BYO-configured org with a non-UUID org_id, got nil")
		}
	})
}

// TestCredentials_SourceBound proves Credentials()/Model() stay source-
// bound (CHAOS-2550): a request for a DIFFERENT provider than the org
// configured gets nothing, even though the org has usable BYO settings.
func TestCredentials_SourceBound(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", true)
	orgID := seedOrg(ctx, t, pool, "enterprise")
	insertSetting(ctx, t, pool, orgID, "provider", "openai")
	insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
	insertSetting(ctx, t, pool, orgID, "model", "gpt-5-mini")

	creds, ok, err := store.Credentials(ctx, orgID.String(), "anthropic")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected not-ok for a mismatched provider, got %+v", creds)
	}

	creds, ok, err = store.Credentials(ctx, orgID.String(), "openai")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || creds.APIKey != "sk-org" {
		t.Fatalf("expected the org's own api_key for a matching provider, got ok=%v creds=%+v", ok, creds)
	}

	model, err := store.Model(ctx, orgID.String(), "openai")
	if err != nil {
		t.Fatal(err)
	}
	if model != "gpt-5-mini" {
		t.Fatalf("got model %q, want %q", model, "gpt-5-mini")
	}
}

// TestResolveUsableProvider_NoFeatureFlagsTable is the codex round 1 P2
// regression: a pre-migration/minimal DB where the feature_flags TABLE
// itself does not exist (not just an absent row for this org) must
// resolve as "unregistered" -- backward-compatible, ungated -- matching
// feature_flag_state's own `if not has_table("feature_flags"): return
// "unregistered"` early return, which skips the TEAM-tier floor check
// entirely. Proven with a COMMUNITY-tier org specifically: if the table-
// missing case were (incorrectly) treated as a fatal error, or as a
// floor-check failure instead of an early "unregistered" return, this
// case would come back "" (not usable) instead of the org's own BYO
// setting.
func TestResolveUsableProvider_NoFeatureFlagsTable(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	if _, err := pool.Exec(ctx, `DROP TABLE feature_flags`); err != nil {
		t.Fatal(err)
	}
	orgID := seedOrg(ctx, t, pool, "community")
	insertSetting(ctx, t, pool, orgID, "provider", "openai")
	insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")

	got, err := store.ResolveUsableProvider(ctx, orgID.String())
	if err != nil {
		t.Fatal(err)
	}
	if got != "openai" {
		t.Fatalf("got %q, want %q (missing feature_flags table -> unregistered -> ungated, "+
			"even for a community-tier org)", got, "openai")
	}
}
