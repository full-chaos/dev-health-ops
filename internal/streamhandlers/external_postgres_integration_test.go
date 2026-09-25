//go:build integration

package streamhandlers

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

func newOperationalAllowedFixture(ctx context.Context, t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
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
	// The migrations register both operational features; these tests exercise the "not registered"
	// branches of the entitlement query, so they start from the state before either existed.
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key IN ('customer_push_ingest', 'canonical_incident_ingestion')`); err != nil {
		t.Fatal(err)
	}
	orgID := uuid.NewString()
	pgseed.Org(ctx, t, pool, orgID, "enterprise")
	return pool, orgID
}

func insertOperationalFeature(ctx context.Context, t *testing.T, pool *pgxpool.Pool, key, minTier string) {
	t.Helper()
	pgseed.SetFeatureFlag(ctx, t, pool, uuid.NewString(), key, minTier, true)
}

// TestPostgresExternalBatchRepositoryOperationalAllowedRequiresBothFeatures
// is the first real test coverage OperationalAllowed has ever had (it had
// none before CHAOS-6286 replaced its hand-rolled query with
// internal/api/licensing): both externalOperationalFeatureKeys must be
// Allowed, and either one alone being unregistered denies the whole batch.
func TestPostgresExternalBatchRepositoryOperationalAllowedRequiresBothFeatures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool, orgID := newOperationalAllowedFixture(ctx, t)

	repo, err := NewPostgresExternalBatchRepository(pool)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("neither feature registered: denied", func(t *testing.T) {
		allowed, err := repo.OperationalAllowed(ctx, orgID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if allowed {
			t.Fatal("allowed = true, want false: neither feature is registered")
		}
	})

	insertOperationalFeature(ctx, t, pool, "customer_push_ingest", "team")

	t.Run("only one of two features registered: still denied", func(t *testing.T) {
		allowed, err := repo.OperationalAllowed(ctx, orgID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if allowed {
			t.Fatal("allowed = true, want false: canonical_incident_ingestion is not registered")
		}
	})

	insertOperationalFeature(ctx, t, pool, "canonical_incident_ingestion", "community")

	t.Run("both features registered and enabled, enterprise tier: allowed", func(t *testing.T) {
		allowed, err := repo.OperationalAllowed(ctx, orgID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !allowed {
			t.Fatal("allowed = false, want true: both features registered, enabled, tier satisfied")
		}
	})
}

// TestPostgresExternalBatchRepositoryOperationalAllowedNonObjectLicenseOverride
// is the regression proof for CHAOS-6286: on main, a features_override value
// that is valid JSON but not an object (json.Unmarshal into map[string]bool
// fails) made featureAllowed's `if err != nil { return false, nil }` guard
// silently deny the org -- a tier-qualified org was refused because of a
// license-column shape neither plane's real decision engine treats as an
// error. On this tip (via internal/api/licensing) that same value must not
// change the outcome from the tier-only case above.
func TestPostgresExternalBatchRepositoryOperationalAllowedNonObjectLicenseOverride(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool, orgID := newOperationalAllowedFixture(ctx, t)
	insertOperationalFeature(ctx, t, pool, "customer_push_ingest", "team")
	insertOperationalFeature(ctx, t, pool, "canonical_incident_ingestion", "community")
	pgseed.OrgLicense(ctx, t, pool, orgID, "enterprise", `[]`)

	repo, err := NewPostgresExternalBatchRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := repo.OperationalAllowed(ctx, orgID)
	if err != nil {
		t.Fatalf("non-object features_override must not error: %v", err)
	}
	if !allowed {
		t.Fatal("allowed = false, want true: a non-object features_override must not change a tier-qualified org's outcome")
	}
}
