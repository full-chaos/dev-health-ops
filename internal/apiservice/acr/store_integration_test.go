//go:build integration

package acr

import (
	"context"
	"errors"
	"fmt"
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
  expires_at timestamptz, config json, PRIMARY KEY (org_id, feature_id))`,
		`CREATE TABLE org_licenses (
  org_id uuid PRIMARY KEY, tier text NOT NULL, features_override json)`,
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

// TestPostgresEntitlementStoreLookupEndToEnd proves the ROUTE-LEVEL contract
// this package owns on top of licensing.PostgresStore (already proven on its
// own terms in internal/api/licensing/store_integration_test.go): org
// existence is a 404, independent of the entitlement decision itself.
func TestPostgresEntitlementStoreLookupEndToEnd(t *testing.T) {
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

	store := PostgresEntitlementStore{Pool: pool, Now: func() time.Time { return time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC) }}

	t.Run("org does not exist", func(t *testing.T) {
		_, err := store.Lookup(ctx, uuid.NewString())
		if !errors.Is(err, ErrOrgNotFound) {
			t.Fatalf("error = %v, want ErrOrgNotFound", err)
		}
	})

	orgID := uuid.NewString()
	if _, err := pool.Exec(ctx,
		`INSERT INTO organizations (id, tier) VALUES ($1, 'community')`, orgID,
	); err != nil {
		t.Fatal(err)
	}

	t.Run("org exists, feature not registered: closed, no error, echoes org_id verbatim", func(t *testing.T) {
		entitlement, err := store.Lookup(ctx, orgID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if entitlement.AgentContextRuntime {
			t.Fatal("AgentContextRuntime = true, want false: feature is not registered")
		}
		if entitlement.OrgID != orgID {
			t.Fatalf("OrgID = %q, want the exact org_id argument %q", entitlement.OrgID, orgID)
		}
	})

	featureID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
INSERT INTO feature_flags (id, key, min_tier, is_enabled)
VALUES ($1, 'agent_context_runtime', 'community', true)`, featureID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO org_feature_overrides (org_id, feature_id, is_enabled)
VALUES ($1, $2, true)`, orgID, featureID); err != nil {
		t.Fatal(err)
	}

	t.Run("org override enables it", func(t *testing.T) {
		entitlement, err := store.Lookup(ctx, orgID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !entitlement.AgentContextRuntime {
			t.Fatal("AgentContextRuntime = false, want true: an active org override exists")
		}
	})
}

// TestPostgresEntitlementStoreReadyFailsWhenAnyEntitlementTableIsUnreadable:
// the health route answers 503 when Ready fails, so Ready must fail when any
// table the entitlement route reads is unreadable, not only organizations.
func TestPostgresEntitlementStoreReadyFailsWhenAnyEntitlementTableIsUnreadable(t *testing.T) {
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

	store := PostgresEntitlementStore{Pool: pool}
	if err := store.Ready(ctx); err != nil {
		t.Fatalf("Ready with every table readable = %v, want nil", err)
	}
	for _, table := range []string{"feature_flags", "org_feature_overrides", "org_licenses", "organizations"} {
		if _, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q RENAME TO %q`, table, table+"_gone")); err != nil {
			t.Fatal(err)
		}
		if err := store.Ready(ctx); !errors.Is(err, ErrUnavailable) {
			t.Errorf("Ready with %s unreadable = %v, want ErrUnavailable", table, err)
		}
		if _, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %q RENAME TO %q`, table+"_gone", table)); err != nil {
			t.Fatal(err)
		}
	}
}
