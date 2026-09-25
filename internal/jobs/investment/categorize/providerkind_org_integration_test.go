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
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
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

	// The migrated schema (CHAOS-6769 ledger): the hand-written tables lacked the real tables' NOT NULL
	// columns, and the migrations already register the shipped feature flags.
	pgschema.Apply(ctx, t, pool)

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	store := llmorgsettings.Store{Pool: pool, Decryptor: decryptor}

	featureID := uuid.New()
	pgseed.SetFeatureFlag(ctx, t, pool, featureID.String(), "byo_llm", "team", true)
	orgID := uuid.New()
	pgseed.Org(ctx, t, pool, orgID.String(), "enterprise")
	for _, row := range []struct{ key, value string }{
		{"provider", "ollama"},
		{"base_url", "https://my-gateway.example.com/v1"},
	} {
		pgseed.Setting(ctx, t, pool, orgID.String(), "llm", row.key, row.value, false)
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
