//go:build integration

package licensing

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func schemaFor(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
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
}

// TestPostgresStoreDecideEndToEnd proves the real query against a real
// Postgres for an explicit-purchase feature (agent_context_runtime): org
// registered/enabled with no override must be CLOSED, and an org override
// enables it -- the property CHAOS-6244 exists to prove, now against the
// shared engine every future route reuses.
func TestPostgresStoreDecideEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schemaFor(ctx, t, pool)

	store := PostgresStore{Pool: pool, Now: func() time.Time { return evaluatedAt }}
	orgID := uuid.NewString()

	t.Run("no organization row at all: falls back to community, still closed", func(t *testing.T) {
		decision, err := store.Decide(ctx, orgID, "agent_context_runtime")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if decision.Allowed {
			t.Fatal("Allowed = true, want false: feature is not even registered yet")
		}
	})

	if _, err := pool.Exec(ctx,
		`INSERT INTO organizations (id, tier) VALUES ($1, 'community')`, orgID,
	); err != nil {
		t.Fatal(err)
	}
	featureID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
INSERT INTO feature_flags (id, key, min_tier, is_enabled)
VALUES ($1, 'agent_context_runtime', 'community', true)`, featureID); err != nil {
		t.Fatal(err)
	}

	t.Run("registered, enabled, no override: explicit purchase required", func(t *testing.T) {
		decision, err := store.Decide(ctx, orgID, "agent_context_runtime")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if decision.Allowed {
			t.Fatal("Allowed = true, want false: no override exists and agent_context_runtime is explicit-purchase")
		}
		if decision.Reason != ReasonExplicitPurchaseRequired {
			t.Fatalf("Reason = %q, want %q", decision.Reason, ReasonExplicitPurchaseRequired)
		}
	})

	if _, err := pool.Exec(ctx, `
INSERT INTO org_feature_overrides (org_id, feature_id, is_enabled)
VALUES ($1, $2, true)`, orgID, featureID); err != nil {
		t.Fatal(err)
	}

	t.Run("org override enables it", func(t *testing.T) {
		decision, err := store.Decide(ctx, orgID, "agent_context_runtime")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !decision.Allowed || decision.Reason != ReasonEnabledByOrgOverride {
			t.Fatalf("decision = %+v, want allowed by org override", decision)
		}
	})
}
