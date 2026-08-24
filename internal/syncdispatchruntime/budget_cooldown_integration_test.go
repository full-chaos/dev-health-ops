//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

func createCooldownTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE public.provider_rate_limit_observations (
 id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL, host text NULL,
 integration_id uuid NOT NULL, sync_run_id uuid NOT NULL, sync_run_unit_id uuid NOT NULL,
 route_family text NULL, route_family_attribution text NULL, dimension text NULL,
 retry_after_seconds double precision NULL, reset_at timestamptz NULL, reason text NULL,
 request_id text NULL, observed_at timestamptz NOT NULL
)`)
	if err != nil {
		t.Fatal(err)
	}
}

const (
	cooldownTestOrg  = "00000000-0000-4000-8000-0000000000c1"
	cooldownTestInt  = "00000000-0000-4000-8000-0000000000c2"
	cooldownTestRun  = "00000000-0000-4000-8000-0000000000c3"
	cooldownTestUnit = "00000000-0000-4000-8000-0000000000c4"
)

func insertObservation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string, opts func(*insertObservationOpts)) {
	t.Helper()
	o := insertObservationOpts{
		orgID: cooldownTestOrg, provider: "github", integrationID: cooldownTestInt,
		observedAt: pgNow(),
	}
	opts(&o)
	if _, err := pool.Exec(ctx, `
INSERT INTO public.provider_rate_limit_observations
 (id, org_id, provider, integration_id, sync_run_id, sync_run_unit_id, route_family,
  route_family_attribution, dimension, retry_after_seconds, reset_at, observed_at)
VALUES ($1::uuid, $2, $3, $4::uuid, $5::uuid, $6::uuid, $7, $8, $9, $10, $11, $12)`,
		id, o.orgID, o.provider, o.integrationID, cooldownTestRun, cooldownTestUnit,
		o.routeFamily, o.routeFamilyAttribution, o.dimension, o.retryAfterSeconds, o.resetAt, o.observedAt); err != nil {
		t.Fatal(err)
	}
}

type insertObservationOpts struct {
	orgID, provider, integrationID string
	routeFamily                    *string
	routeFamilyAttribution         *string
	dimension                      *string
	retryAfterSeconds              *float64
	resetAt                        *time.Time
	observedAt                     time.Time
}

func withCooldownPool(t *testing.T, fn func(ctx context.Context, pool *pgxpool.Pool)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createCooldownTables(t, ctx, pool)
	fn(ctx, pool)
}

func candidateUnit(orgID, provider, integrationID string) budgetUnit {
	return budgetUnit{orgID: orgID, provider: provider, integrationID: integrationID}
}

// TestActiveCooldownsMatchesByFamily pins the common case: an observation
// with a resolved route_family gates the family map, not the dimension map.
func TestActiveCooldownsMatchesByFamily(t *testing.T) {
	withCooldownPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		future := "work-items"
		insertObservation(t, ctx, pool, "00000000-0000-4000-8000-0000000000d1", func(o *insertObservationOpts) {
			o.routeFamily = &future
			o.retryAfterSeconds = floatPtr(3600)
			o.observedAt = now
		})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		family, dimension := activeCooldowns(ctx, tx, nil, cooldownTestRun, []budgetUnit{candidateUnit(cooldownTestOrg, "github", cooldownTestInt)}, now)
		key := cooldownKey{orgID: cooldownTestOrg, provider: "github", integrationID: cooldownTestInt, familyOrDimension: "work-items"}
		if _, ok := family[key]; !ok {
			t.Fatalf("family map missing key, got %+v", family)
		}
		if len(dimension) != 0 {
			t.Fatalf("dimension map should be empty for a family-attributed row, got %+v", dimension)
		}
	})
}

// TestActiveCooldownsFallsBackToDimensionWhenAmbiguous pins the
// ambiguous-attribution fallback: a NULL-family, dimension-tagged
// observation gates on dimension, never the family map.
func TestActiveCooldownsFallsBackToDimensionWhenAmbiguous(t *testing.T) {
	withCooldownPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		attribution := ambiguousRouteFamilyAttribution
		dim := "rest_core"
		insertObservation(t, ctx, pool, "00000000-0000-4000-8000-0000000000d2", func(o *insertObservationOpts) {
			o.routeFamilyAttribution = &attribution
			o.dimension = &dim
			o.retryAfterSeconds = floatPtr(3600)
			o.observedAt = now
		})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		family, dimensionMap := activeCooldowns(ctx, tx, nil, cooldownTestRun, []budgetUnit{candidateUnit(cooldownTestOrg, "github", cooldownTestInt)}, now)
		if len(family) != 0 {
			t.Fatalf("family map should be empty for an ambiguous-attribution row, got %+v", family)
		}
		key := cooldownKey{orgID: cooldownTestOrg, provider: "github", integrationID: cooldownTestInt, familyOrDimension: "rest_core"}
		if _, ok := dimensionMap[key]; !ok {
			t.Fatalf("dimension map missing key, got %+v", dimensionMap)
		}
	})
}

// TestActiveCooldownsIsOrgIsolated pins that a cooldown recorded under a
// different org_id never gates, even when provider/integration/family
// coincide.
func TestActiveCooldownsIsOrgIsolated(t *testing.T) {
	withCooldownPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		family := "work-items"
		otherOrg := "00000000-0000-4000-8000-0000000000cc"
		insertObservation(t, ctx, pool, "00000000-0000-4000-8000-0000000000d3", func(o *insertObservationOpts) {
			o.orgID = otherOrg
			o.routeFamily = &family
			o.retryAfterSeconds = floatPtr(3600)
			o.observedAt = now
		})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		familyMap, _ := activeCooldowns(ctx, tx, nil, cooldownTestRun, []budgetUnit{candidateUnit(cooldownTestOrg, "github", cooldownTestInt)}, now)
		if len(familyMap) != 0 {
			t.Fatalf("a different org's cooldown must never gate this org's units, got %+v", familyMap)
		}
	})
}

// TestActiveCooldownsExcludesAnExpiredObservation pins that a cooldown
// whose expiry has already passed does not gate.
func TestActiveCooldownsExcludesAnExpiredObservation(t *testing.T) {
	withCooldownPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		now := pgNow()
		family := "work-items"
		insertObservation(t, ctx, pool, "00000000-0000-4000-8000-0000000000d4", func(o *insertObservationOpts) {
			o.routeFamily = &family
			o.retryAfterSeconds = floatPtr(1) // expired long before `now`
			o.observedAt = now.Add(-time.Hour)
		})

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		familyMap, _ := activeCooldowns(ctx, tx, nil, cooldownTestRun, []budgetUnit{candidateUnit(cooldownTestOrg, "github", cooldownTestInt)}, now)
		if len(familyMap) != 0 {
			t.Fatalf("an expired cooldown must not gate, got %+v", familyMap)
		}
	})
}

// TestActiveCooldownsIsFailOpenOnAQueryError pins the fail-open discipline:
// a broken observation-store read must never block dispatch -- it returns
// two empty maps (or, mid-stream, whatever it already collected) instead of
// propagating the error.
func TestActiveCooldownsIsFailOpenOnAQueryError(t *testing.T) {
	withCooldownPool(t, func(ctx context.Context, pool *pgxpool.Pool) {
		// Break the query itself: drop a column activeCooldowns' SELECT
		// depends on, forcing a real Postgres query-execution error rather
		// than a Go-level mock.
		if _, err := pool.Exec(ctx, `ALTER TABLE public.provider_rate_limit_observations DROP COLUMN route_family_attribution`); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		familyMap, dimensionMap := activeCooldowns(ctx, tx, nil, cooldownTestRun, []budgetUnit{candidateUnit(cooldownTestOrg, "github", cooldownTestInt)}, pgNow())
		if len(familyMap) != 0 || len(dimensionMap) != 0 {
			t.Fatalf("want two empty maps on a query error, got family=%+v dimension=%+v", familyMap, dimensionMap)
		}
	})
}
