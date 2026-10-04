//go:build integration

package riverstore_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	syncdispatchv1 "github.com/full-chaos/dev-health-ops/contracts/sync-dispatch/v1"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// syncRouteRecord is every column of one sync_dispatch_transport_routes row.
type syncRouteRecord struct {
	Transport         string
	Generation        int64
	Paused            bool
	PausedAt          *time.Time
	RollbackTransport string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func readSyncRoutes(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]syncRouteRecord {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT kind, transport, generation, paused, paused_at, rollback_transport, created_at, updated_at
FROM public.sync_dispatch_transport_routes`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	routes := map[string]syncRouteRecord{}
	for rows.Next() {
		var kind string
		var row syncRouteRecord
		if err := rows.Scan(&kind, &row.Transport, &row.Generation, &row.Paused, &row.PausedAt,
			&row.RollbackTransport, &row.CreatedAt, &row.UpdatedAt); err != nil {
			t.Fatal(err)
		}
		routes[kind] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return routes
}

// outcomesOf reduces a run's outcomes to kind -> outcome.
func outcomesOf(result riverstore.MigrationResult) map[string]string {
	outcomes := map[string]string{}
	for _, route := range result.SyncRoutes {
		outcomes[route.Kind] = route.Outcome
	}
	return outcomes
}

// TestMigrateMovesOnlyTheRetiredCelerySyncRoutes pins what a migrate run does
// to the sync-dispatch route rows (CHAOS-8600), on the REAL migrated schema
// (the baseline and the chain) with the checked-in route policy:
//
//   - a row that holds the seed of a fresh database (transport celery, no
//     rollback route, not paused, no live claim) is moved to river at
//     generation + 1;
//   - a row already on river is not changed, in any column;
//   - a paused row, a row that still names a rollback route, a river row that
//     is paused or names a rollback route, and a seed row with a live outbox
//     claim are not changed, in any column;
//   - a kind with no row gets none;
//   - a second run changes nothing;
//   - a database without the route table is reported, not failed.
//
// The live claim is released at the end, and the next run then moves that row:
// the claim was the only thing that held it.
func TestMigrateMovesOnlyTheRetiredCelerySyncRoutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, instance)
	domainRole, err := containers.RoleName("sync_route_domain", instance)
	if err != nil {
		t.Fatal(err)
	}
	queueRole, err := containers.RoleName("sync_route_queue", instance)
	if err != nil {
		t.Fatal(err)
	}
	pool := openPool(t, ctx, instance.URI)
	defer pool.Close()
	defer containers.DropRole(pool, domainRole, t.Logf)
	defer containers.DropRole(pool, queueRole, t.Logf)
	createRuntimeRoles(t, ctx, pool, domainRole, queueRole)

	kinds, err := syncdispatchcontract.NativeRiverRouteKinds(syncdispatchv1.TransportRoutes)
	if err != nil {
		t.Fatal(err)
	}
	if len(kinds) != 4 {
		t.Fatalf("the checked-in policy has %d river kinds with no rollback route (%v); this test places four states", len(kinds), kinds)
	}
	seed, onRiver, paused, withRollback := kinds[0], kinds[1], kinds[2], kinds[3]
	options := riverstore.MigrationOptions{
		Schema: "river", DomainRole: domainRole, QueueRole: queueRole,
		RiverSyncDispatchRoutes: kinds,
	}
	migrate := func() riverstore.MigrationResult {
		t.Helper()
		result, err := riverstore.ApplyPinnedMigrations(ctx, pool, options)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}

	// A database the application schema has not reached yet has no route
	// table. It is a second, empty database of the same server: the schema
	// below must go into a database that nothing has touched.
	emptyName, err := containers.RoleName("sync_route_empty", instance)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "CREATE DATABASE "+emptyName); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP DATABASE IF EXISTS "+emptyName+" WITH (FORCE)")
	}()
	emptyConfig, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	emptyConfig.ConnConfig.Database = emptyName
	emptyPool, err := pgxpool.NewWithConfig(ctx, emptyConfig)
	if err != nil {
		t.Fatal(err)
	}
	absent, err := riverstore.ApplyPinnedMigrations(ctx, emptyPool, options)
	emptyPool.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !absent.SyncRouteTableAbsent || len(absent.SyncRoutes) != 0 {
		t.Fatalf("run without the route table = %+v", absent)
	}

	pgschema.Apply(ctx, t, pool)
	fresh := readSyncRoutes(t, ctx, pool)
	for _, kind := range kinds {
		row, ok := fresh[kind]
		if !ok || row.Transport != "celery" || row.RollbackTransport != "none" || row.Paused {
			t.Fatalf("the migrated schema seeds %s as %+v (present=%t); this test is about the seed (celery, rollback none, not paused): "+
				"if the seed changed, the move this test pins has nothing left to do", kind, row, ok)
		}
	}

	// Round one: the seed, a row on river, a paused seed, a row that still names a rollback route.
	pgseed.SyncTransportRoute(ctx, t, pool, onRiver, "river", 1, false, "none")
	pgseed.SyncTransportRoute(ctx, t, pool, paused, "celery", 1, true, "none")
	pgseed.SyncTransportRoute(ctx, t, pool, withRollback, "celery", 1, false, "celery")
	before := readSyncRoutes(t, ctx, pool)
	first := migrate()
	if first.SyncRouteTableAbsent || !reflect.DeepEqual(outcomesOf(first), map[string]string{
		seed: riverstore.SyncRouteMoved, onRiver: riverstore.SyncRoutePresent,
		paused: riverstore.SyncRouteHeld, withRollback: riverstore.SyncRouteHeld,
	}) {
		t.Fatalf("round one outcomes = %+v", first.SyncRoutes)
	}
	after := readSyncRoutes(t, ctx, pool)
	if got, was := after[seed], before[seed]; got.Transport != "river" || got.Generation != was.Generation+1 || got.Paused ||
		got.PausedAt != nil || got.RollbackTransport != "none" || !got.CreatedAt.Equal(was.CreatedAt) || !got.UpdatedAt.After(was.UpdatedAt) {
		t.Fatalf("the seed row was %+v and is %+v after the run; want river at generation + 1, nothing else changed", was, got)
	}
	for _, kind := range []string{onRiver, paused, withRollback} {
		if !reflect.DeepEqual(after[kind], before[kind]) {
			t.Fatalf("the %s row changed:\nbefore %+v\nafter  %+v", kind, before[kind], after[kind])
		}
	}
	for _, route := range first.SyncRoutes {
		if was := before[route.Kind]; route.Transport != was.Transport || route.RollbackTransport != was.RollbackTransport ||
			route.Paused != was.Paused || route.Generation != was.Generation {
			t.Fatalf("the outcome of %s reports %+v, the row the run found was %+v", route.Kind, route, was)
		}
	}

	// A second run finds nothing to move and changes nothing.
	second := migrate()
	if !reflect.DeepEqual(outcomesOf(second), map[string]string{
		seed: riverstore.SyncRoutePresent, onRiver: riverstore.SyncRoutePresent,
		paused: riverstore.SyncRouteHeld, withRollback: riverstore.SyncRouteHeld,
	}) {
		t.Fatalf("second run outcomes = %+v", second.SyncRoutes)
	}
	if again := readSyncRoutes(t, ctx, pool); !reflect.DeepEqual(again, after) {
		t.Fatalf("the second run changed rows:\nbefore %+v\nafter  %+v", after, again)
	}

	// Round two: a seed row with a live claim, a river row that names a rollback route, a paused river row, no row.
	pgseed.SyncTransportRoute(ctx, t, pool, seed, "celery", 1, false, "none")
	pgseed.SyncTransportRoute(ctx, t, pool, onRiver, "river", 1, false, "celery")
	pgseed.SyncTransportRoute(ctx, t, pool, paused, "river", 1, true, "none")
	if _, err := pool.Exec(ctx, "DELETE FROM public.sync_dispatch_transport_routes WHERE kind = $1", withRollback); err != nil {
		t.Fatal(err)
	}
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{})
	const outboxID = "00000000-0000-4000-8000-00000000c860"
	pgseed.SyncDispatchOutbox(ctx, t, pool, outboxID, pgseed.DefaultSyncRunID, pgseed.DefaultSyncOrgID, seed, "pending", "", 0)
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_dispatch_outbox
SET claim_token = 'live-claim', claim_expires_at = now() + interval '10 minutes'
WHERE id = $1::uuid`, outboxID); err != nil {
		t.Fatalf("claim the outbox row: %v", err)
	}
	before = readSyncRoutes(t, ctx, pool)
	third := migrate()
	if !reflect.DeepEqual(outcomesOf(third), map[string]string{
		seed: riverstore.SyncRouteHeld, onRiver: riverstore.SyncRouteHeld,
		paused: riverstore.SyncRouteHeld, withRollback: riverstore.SyncRouteMissing,
	}) {
		t.Fatalf("round two outcomes = %+v", third.SyncRoutes)
	}
	for _, route := range third.SyncRoutes {
		if route.Kind == seed && route.LiveClaims != 1 {
			t.Fatalf("the seed row with a live claim reports live_claims=%d, want 1", route.LiveClaims)
		}
	}
	if after = readSyncRoutes(t, ctx, pool); !reflect.DeepEqual(after, before) {
		t.Fatalf("round two changed rows:\nbefore %+v\nafter  %+v", before, after)
	}
	if _, present := after[withRollback]; present {
		t.Fatalf("the run created a route row for %s", withRollback)
	}

	// The claim ends: the same row is now only the seed, and the next run moves it.
	if _, err := pool.Exec(ctx, `
UPDATE public.sync_dispatch_outbox
SET claim_token = NULL, claim_expires_at = NULL
WHERE id = $1::uuid`, outboxID); err != nil {
		t.Fatalf("release the outbox row: %v", err)
	}
	fourth := migrate()
	if got := outcomesOf(fourth)[seed]; got != riverstore.SyncRouteMoved {
		t.Fatalf("after the claim ended the seed row is %q, want moved (outcomes %+v)", got, fourth.SyncRoutes)
	}
	final := readSyncRoutes(t, ctx, pool)
	if got, was := final[seed], before[seed]; got.Transport != "river" || got.Generation != was.Generation+1 || got.Paused || got.RollbackTransport != "none" {
		t.Fatalf("the released seed row was %+v and is %+v; want river at generation + 1", was, got)
	}
	for _, kind := range []string{onRiver, paused} {
		if !reflect.DeepEqual(final[kind], before[kind]) {
			t.Fatalf("the %s row changed:\nbefore %+v\nafter  %+v", kind, before[kind], final[kind])
		}
	}
}
