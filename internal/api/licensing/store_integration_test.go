//go:build integration

package licensing

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaFor applies the migrated schema: the hand-written organizations, feature_flags,
// org_feature_overrides and org_licenses this file used to carry lacked the real tables'
// NOT NULL columns and keys (CHAOS-6769 ledger).
func schemaFor(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	pgschema.Apply(ctx, t, pool)
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
	// The migrations already register agent_context_runtime as a feature flag. This test walks
	// the unregistered -> registered -> overridden transitions, so it starts from the state
	// before the flag existed; the hand-written schema hid that the flag is pre-seeded.
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = 'agent_context_runtime'`); err != nil {
		t.Fatal(err)
	}

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

	pgseed.Org(ctx, t, pool, orgID, "community")
	featureID := uuid.NewString()
	pgseed.FeatureFlag(ctx, t, pool, featureID, "agent_context_runtime", "community", true)

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

	pgseed.OrgOverride(ctx, t, pool, orgID, featureID, true)

	t.Run("org override enables it", func(t *testing.T) {
		decision, err := store.Decide(ctx, orgID, "agent_context_runtime")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !decision.Allowed || decision.Reason != ReasonEnabledByOrgOverride {
			t.Fatalf("decision = %+v, want allowed by org override", decision)
		}
	})

	if _, err := pool.Exec(ctx,
		`UPDATE org_feature_overrides SET config = $2 WHERE org_id = $1 AND feature_id = $3`,
		orgID, `{"customer_limit": 17}`, featureID,
	); err != nil {
		t.Fatal(err)
	}

	// The confirmed P2: org_feature_overrides.config must reach
	// Decision.Config against a real Postgres row, not just the fake
	// queryer in store_test.go.
	t.Run("org override config propagates to the decision", func(t *testing.T) {
		decision, err := store.Decide(ctx, orgID, "agent_context_runtime")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if decision.Config == nil {
			t.Fatal("Config = nil, want the row's config propagated")
		}
	})

	otherOrgID := uuid.NewString()
	pgseed.Org(ctx, t, pool, otherOrgID, "enterprise")
	pgseed.OrgLicense(ctx, t, pool, otherOrgID, "enterprise", `{"agent_context_runtime": 1e10000}`)

	// The confirmed P1 that reached live Postgres: a features_override
	// value containing a JSON number that overflows float64 must not error
	// the whole column and deny an otherwise-valid entitlement.
	t.Run("overflowing license-override JSON number still decides, not a 503", func(t *testing.T) {
		decision, err := store.Decide(ctx, otherOrgID, "agent_context_runtime")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !decision.Allowed || decision.Reason != ReasonEnabledByLicenseOverride {
			t.Fatalf("decision = %+v, want allowed by license override (1e10000 overflows to +Inf, truthy)", decision)
		}
	})
}
