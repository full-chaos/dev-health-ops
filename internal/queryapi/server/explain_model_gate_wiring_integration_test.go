//go:build integration

package server

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// The production constructor, with a real registry pool and the explain-purpose settings, decides from the
// rows it reads: a wrong pool or config wiring fails here. No fake is injected.
func TestNewModelGateReadsTheRegistryThroughTheProductionWiring(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
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
	pgschema.Apply(ctx, t, pool)
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = 'byo_llm'`); err != nil {
		t.Fatal(err)
	}
	pgseed.FeatureFlag(ctx, t, pool, uuid.NewString(), "byo_llm", "team", true)
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	gate := newModelGate(pool, llmorgsettings.Store{Pool: pool, Decryptor: decryptor}.ForPurpose(llmorgsettings.PurposeExplain))

	t.Run("saved complete config runs on the own model", func(t *testing.T) {
		orgID := uuid.NewString()
		pgseed.Org(ctx, t, pool, orgID, "team")
		pgseed.Setting(ctx, t, pool, orgID, "llm", "provider", "anthropic", false)
		pgseed.Setting(ctx, t, pool, orgID, "llm", "api_key", "k", false)
		if d := gate.Decide(ctx, orgID); d.Kind != llmorgsettings.Own {
			t.Fatalf("decision = %+v, want Own", d)
		}
	})
	t.Run("saved config without a key is refused with its reason", func(t *testing.T) {
		orgID := uuid.NewString()
		pgseed.Org(ctx, t, pool, orgID, "team")
		pgseed.Setting(ctx, t, pool, orgID, "llm", "provider", "openai", false)
		if d := gate.Decide(ctx, orgID); d.Status != 403 || d.Code != "missing_credentials" {
			t.Fatalf("decision = %+v, want 403 missing_credentials", d)
		}
	})
	t.Run("no config and no grants is refused", func(t *testing.T) {
		orgID := uuid.NewString()
		pgseed.Org(ctx, t, pool, orgID, "team")
		if d := gate.Decide(ctx, orgID); d.Kind != llmorgsettings.Refuse {
			t.Fatalf("decision = %+v, want a refusal", d)
		}
	})
}
