//go:build integration

package syncroute

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// startFenceSchemaPostgres starts a real Postgres testcontainer and creates
// the sync_dispatch_transport_routes table Fence.Check() queries. Schema
// shape matches src/dev_health_ops/alembic/versions/0049_add_sync_dispatch_transport_fence.py,
// minus the columns/constraints Check() never reads -- consistent with every
// other Postgres integration test in this repo, none of which run the real
// migration chain against a testcontainer; they all hand-build the shape
// their target query needs.
func startFenceSchemaPostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL: %v", err)
		}
	})

	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	// The migrated schema: the routes table with its real constraints and trigger, seeded by the
	// migrations (every kind on celery, generation 2, rollback none).
	pgschema.Apply(ctx, t, pool)
	return pool
}

// insertFenceRoute puts a kind's route into the given state. The migrations already seed every kind
// and a trigger demands a generation increase for a state change, so generation is a minimum.
func insertFenceRoute(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind, transport, rollbackTransport string, generation int64) {
	t.Helper()
	pgseed.SyncTransportRoute(ctx, t, pool, kind, transport, generation, false, rollbackTransport)
}

// TestFenceAgainstMigratedPostgres proves the fence's real SQL executes
// correctly against real Postgres when persisted state matches the current
// contract. Every other Check() test in this package (fence_test.go) drives
// a fake routeRows, which cannot catch a query/scan mismatch the way a live
// driver round-trip can. It seeds exactly the four frozen routes the
// checked-in contract declares (via the same validStates helper the
// fake-backed tests use), so a passing Check() here is evidence about the
// query, not only the in-memory validation logic.
//
// This alone is non-vacuous (a broken query/scan still fails it) but cannot
// by itself prove Check() rejects a REAL stale state, because it seeds from
// the same registry Check() reads -- see the paired drift case below.
//
// CHAOS-3948: this used to live in fence_test.go gated on
// DEV_HEALTH_POSTGRES_TEST_URI, which nothing in ci/check_go.sh or go.yml
// ever set -- it had never executed anywhere in the automated pipeline.
// Moved behind the integration build tag and wired to the same
// containers.StartPostgres pattern control_integration_test.go already uses
// in this package, now that `ci/check_go.sh all` genuinely runs integration.
func TestFenceAgainstMigratedPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := startFenceSchemaPostgres(t, ctx)

	registry := loadRegistry(t)
	for _, state := range validStates(registry) {
		insertFenceRoute(t, ctx, pool, state.kind, state.transport, state.rollbackTransport, state.generation)
	}

	fence, err := New(pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.Check(ctx); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
}

// TestFenceDetectsDriftAgainstTheOriginalMigrationBaseline is the paired
// negative case codex adversarial review asked for: the healthy case above
// seeds from the same registry Check() validates against, so on its own it
// cannot prove Check() rejects a genuinely stale persisted state -- it would
// pass even if Check()'s comparison were a no-op. This seeds the table with
// migration 0049's real original bulk_insert values (every kind on
// 'celery', generation 1, unpaused -- see
// src/dev_health_ops/alembic/versions/0049_add_sync_dispatch_transport_fence.py:63-78),
// which the current contract (contracts/sync-dispatch/v1/transport-routes.json)
// has since moved to 'river' for. A real database that was migrated but
// never cut over must fail Check() with ErrDrift.
func TestFenceDetectsDriftAgainstTheOriginalMigrationBaseline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool := startFenceSchemaPostgres(t, ctx)

	registry := loadRegistry(t)
	for _, kind := range frozenKinds {
		insertFenceRoute(t, ctx, pool, kind, "celery", "celery", 1)
	}

	fence, err := New(pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.Check(ctx); !errors.Is(err, ErrDrift) {
		t.Fatalf("Check() error = %v, want ErrDrift", err)
	}
}
