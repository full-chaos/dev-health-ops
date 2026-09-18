//go:build integration

package riverstore_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	jobsv1 "github.com/full-chaos/dev-health-ops/contracts/jobs/v1"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// workerJobRoutesDDL mirrors the application schema's worker_job_routes
// (src/dev_health_ops/models/worker_job_route.py): primary key, transport
// vocabulary and generation floor are what an insert has to satisfy.
const workerJobRoutesDDL = `
CREATE TABLE public.worker_job_routes (
  job_kind varchar(96) PRIMARY KEY,
  transport varchar(16) NOT NULL
    CONSTRAINT ck_worker_job_route_transport
    CHECK (transport IN ('celery', 'shadow', 'river_canary', 'river')),
  paused boolean NOT NULL,
  generation bigint NOT NULL CONSTRAINT ck_worker_job_route_generation CHECK (generation >= 1),
  updated_at timestamptz NOT NULL
)`

type routeRow struct {
	transport  string
	paused     bool
	generation int64
	updatedAt  time.Time
}

func readRoutes(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]routeRow {
	t.Helper()
	rows, err := pool.Query(ctx,
		"SELECT job_kind, transport, paused, generation, updated_at FROM public.worker_job_routes")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	routes := map[string]routeRow{}
	for rows.Next() {
		var kind string
		var row routeRow
		if err := rows.Scan(&kind, &row.transport, &row.paused, &row.generation, &row.updatedAt); err != nil {
			t.Fatal(err)
		}
		routes[kind] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return routes
}

type noQuiescer struct{}

func (noQuiescer) Quiesce(context.Context, string) error { return nil }

// TestMigrateSeedsEveryRiverOnlyRoute pins the route contract a migrate run
// leaves behind: every River-only kind in the checked-in policy has a route
// row, so the route controller resolves every registered kind instead of
// failing the whole relay step on the first kind without one. A row that
// already exists keeps every value it had, a table that does not exist yet
// is reported rather than failed, and a second run changes nothing.
func TestMigrateSeedsEveryRiverOnlyRoute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, instance)
	domainRole, err := containers.RoleName("route_seed_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("route_seed_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	pool := openPool(t, ctx, instance.URI)
	defer pool.Close()
	defer containers.DropRole(pool, domainRole, t.Logf)
	defer containers.DropRole(pool, queueRole, t.Logf)
	createRuntimeRoles(t, ctx, pool, domainRole, queueRole)

	kinds, err := jobcontract.NativeRiverRouteKinds(jobsv1.MigrationState)
	if err != nil {
		t.Fatal(err)
	}
	if len(kinds) < 2 {
		t.Fatalf("checked-in policy has %d River-only kinds; the test needs two", len(kinds))
	}
	options := riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
		NativeRiverRoutes: kinds,
	}

	// The application schema has not created the table yet.
	absent, err := riverstore.ApplyPinnedMigrations(ctx, pool, options)
	if err != nil {
		t.Fatal(err)
	}
	if !absent.RouteTableAbsent || len(absent.SeededRoutes) != 0 || len(absent.PresentRoutes) != 0 {
		t.Fatalf("run without the route table = %+v", absent)
	}

	if _, err := pool.Exec(ctx, workerJobRoutesDDL); err != nil {
		t.Fatal(err)
	}
	registry, err := jobruntime.Load("../../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	controller, err := jobroute.NewController(pool, registry, noQuiescer{})
	if err != nil {
		t.Fatal(err)
	}
	// The failure a missing row causes: one unknown route fails the step for
	// every kind.
	if _, err := controller.DeferredKinds(ctx); !errors.Is(err, jobroute.ErrUnknownRoute) {
		t.Fatalf("DeferredKinds on an empty route table err=%v, want ErrUnknownRoute", err)
	}

	// An operator already paused one kind at a later generation.
	held := kinds[0]
	heldAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO public.worker_job_routes (job_kind, transport, paused, generation, updated_at)
VALUES ($1, 'river', TRUE, 7, $2)`, held, heldAt); err != nil {
		t.Fatal(err)
	}

	first, err := riverstore.ApplyPinnedMigrations(ctx, pool, options)
	if err != nil {
		t.Fatal(err)
	}
	if first.RouteTableAbsent || !reflect.DeepEqual(first.PresentRoutes, []string{held}) ||
		!reflect.DeepEqual(first.SeededRoutes, kinds[1:]) {
		t.Fatalf("first run seeded=%v present=%v absent=%v", first.SeededRoutes, first.PresentRoutes, first.RouteTableAbsent)
	}
	routes := readRoutes(t, ctx, pool)
	if len(routes) != len(kinds) {
		t.Fatalf("route rows=%d want %d", len(routes), len(kinds))
	}
	if got := routes[held]; got.transport != "river" || !got.paused || got.generation != 7 || !got.updatedAt.Equal(heldAt) {
		t.Fatalf("existing row changed: %+v", got)
	}
	for _, kind := range kinds[1:] {
		if got := routes[kind]; got.transport != "river" || got.paused || got.generation != 1 {
			t.Fatalf("seeded %s = %+v", kind, got)
		}
	}
	if _, err := pool.Exec(ctx,
		"UPDATE public.worker_job_routes SET paused = FALSE WHERE job_kind = $1", held); err != nil {
		t.Fatal(err)
	}
	deferred, err := controller.DeferredKinds(ctx)
	if err != nil || len(deferred) != 0 {
		t.Fatalf("DeferredKinds after migrate = %v, %v", deferred, err)
	}

	before := readRoutes(t, ctx, pool)
	second, err := riverstore.ApplyPinnedMigrations(ctx, pool, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.SeededRoutes) != 0 || !reflect.DeepEqual(second.PresentRoutes, kinds) {
		t.Fatalf("second run seeded=%v present=%v", second.SeededRoutes, second.PresentRoutes)
	}
	if after := readRoutes(t, ctx, pool); !reflect.DeepEqual(after, before) {
		t.Fatalf("second run changed rows:\nbefore %+v\nafter  %+v", before, after)
	}
}
