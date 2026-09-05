//go:build integration

package categorize_test

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestResolveProviderKindForOrg_UsesRealOrgSettings is the end-to-end
// proof CHAOS-5006 needs, in an external test package (categorize_test) so
// it can depend on internal/llmorgsettings without categorize itself ever
// importing it (categorize.OrgProviderResolver stays a plain function
// type -- see providerkind.go's doc comment): a real BYO org's Postgres
// settings rows, read through llmorgsettings.Store.ResolveUsableProvider
// (a method value, no adapter needed), drive
// ResolveProviderKindForOrg's ACTUAL decision -- not a fake resolver.
func TestResolveProviderKindForOrg_UsesRealOrgSettings(t *testing.T) {
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
	store := llmorgsettings.Store{Pool: pool, Decryptor: decryptor}

	featureID := uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO feature_flags (id, key, min_tier, is_enabled) VALUES ($1, 'byo_llm', 'team', true)`,
		featureID); err != nil {
		t.Fatal(err)
	}
	orgID := uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO organizations (id, tier) VALUES ($1, 'enterprise')`, orgID); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ key, value string }{
		{"provider", "ollama"},
		{"base_url", "https://my-gateway.example.com/v1"},
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO settings (org_id, category, key, value, is_encrypted) VALUES ($1, 'llm', $2, $3, false)`,
			orgID.String(), row.key, row.value); err != nil {
			t.Fatal(err)
		}
	}

	// A platform LLM_PROVIDER env pointing elsewhere must still lose to
	// the org's own BYO setting -- the exact CHAOS-5006 divergence.
	t.Setenv("LLM_PROVIDER", "openai")
	t.Setenv("OPENAI_API_KEY", "sk-platform-should-not-be-selected")

	kind, err := categorize.ResolveProviderKindForOrg(
		ctx, "auto", orgID.String(), store.ResolveUsableProvider)
	if err != nil {
		t.Fatal(err)
	}
	if kind != categorize.ProviderKindOllama {
		t.Fatalf("kind = %q, want ollama (the org's own BYO setting, not the platform LLM_PROVIDER)", kind)
	}
}
