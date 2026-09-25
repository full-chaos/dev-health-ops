//go:build integration

package acr

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaFor applies the migrated schema (CHAOS-6769 ledger): the hand-written organizations,
// feature_flags, org_feature_overrides and org_licenses lacked the real tables' NOT NULL
// columns and keys, and did not have the agent_context_runtime flag the migrations register.
func schemaFor(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	pgschema.Apply(ctx, t, pool)
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
	pgseed.Org(ctx, t, pool, orgID, "community")
	// The migrations register agent_context_runtime; the "not registered" case starts without it.
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = 'agent_context_runtime'`); err != nil {
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
	pgseed.FeatureFlag(ctx, t, pool, featureID, "agent_context_runtime", "community", true)
	pgseed.OrgOverride(ctx, t, pool, orgID, featureID, true)

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
