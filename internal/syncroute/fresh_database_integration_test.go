//go:build integration

package syncroute_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// freshRandom is a throwaway name suffix or password, made at test time.
func freshRandom(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(raw)
}

// migrateVerb finds a verb of `dho migrate` by its path.
func migrateVerb(t *testing.T, path ...string) cli.Command {
	t.Helper()
	node := rivermigrate.Command()
	for _, name := range path {
		found := false
		for _, child := range node.Children {
			if child.Name == name {
				node, found = child, true
				break
			}
		}
		if !found {
			t.Fatalf("`dho migrate` has no %q under %q", name, node.Name)
		}
	}
	if node.Run == nil {
		t.Fatalf("`dho migrate %v` is not a verb", path)
	}
	return node
}

// TestFreshDatabaseHoldsTheCheckedInSyncRoutes is the state a fresh install must reach
// (CHAOS-8600): an EMPTY database, migrated by the commands a deployment runs and by
// nothing else, holds every sync-dispatch route on the route the CHECKED-IN contract
// names. Then the route activation step (`dho workers routes apply`, which calls
// ApplyCheckedIn) succeeds and changes nothing, and the reconciler's readiness fence
// passes.
//
// The steps are the Compose services, in their order: `migrate` (dho migrate postgres
// upgrade), `go-river-provision` (dho migrate roles), `go-river-migrate` (dho migrate
// river --apply-and-check). No statement of this test writes a route row.
//
// The registry is the checked-in one (contracts/sync-dispatch/v1) and the capabilities
// are the ones the operator registers. A test registry that names `celery` as the
// rollback route accepts a row the checked-in contract refuses, so it cannot show this.
func TestFreshDatabaseHoldsTheCheckedInSyncRoutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })

	suffix := freshRandom(t)
	roles := map[string]string{
		"domain": "fresh_domain_" + suffix, "queue": "fresh_queue_" + suffix, "coordinator": "fresh_coord_" + suffix,
		"api": "fresh_api_" + suffix, "query_api": "fresh_qapi_" + suffix,
	}
	passwords := map[string]string{}
	for label := range roles {
		passwords[label] = freshRandom(t) + freshRandom(t)
	}
	settings := map[string]string{
		"MIGRATION_DATABASE_URI":              instance.URI,
		pgmigrate.CutoverEnv:                  "1",
		pgmigrate.RiverSchemaEnv:              "river",
		"RIVER_DOMAIN_DATABASE_ROLE":          roles["domain"],
		"RIVER_QUEUE_DATABASE_ROLE":           roles["queue"],
		"RIVER_COORDINATOR_DATABASE_ROLE":     roles["coordinator"],
		"RIVER_DOMAIN_DATABASE_PASSWORD":      passwords["domain"],
		"RIVER_QUEUE_DATABASE_PASSWORD":       passwords["queue"],
		"RIVER_COORDINATOR_DATABASE_PASSWORD": passwords["coordinator"],
		"API_DATABASE_ROLE":                   roles["api"],
		"API_DATABASE_PASSWORD":               passwords["api"],
		"QUERY_API_DATABASE_ROLE":             roles["query_api"],
		"QUERY_API_DATABASE_PASSWORD":         passwords["query_api"],
	}
	run := func(args []string, path ...string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		code := migrateVerb(t, path...).Run(ctx, cli.Env{
			Args:   args,
			Lookup: func(key string) (string, bool) { value, ok := settings[key]; return value, ok },
			Stdout: &stdout, Stderr: &stderr,
		})
		for label, password := range passwords {
			if bytes.Contains(stdout.Bytes(), []byte(password)) || bytes.Contains(stderr.Bytes(), []byte(password)) {
				t.Fatalf("`dho migrate %v` printed the password of the %s role", path, label)
			}
		}
		if code != cli.ExitOK {
			t.Fatalf("`dho migrate %v %v` exited %d on a fresh database\nstdout:\n%s\nstderr:\n%s", path, args, code, &stdout, &stderr)
		}
	}
	run(nil, "postgres", "upgrade")
	run(nil, "roles")
	run([]string{"--apply-and-check"}, "river")

	login := func(label string) *pgxpool.Pool {
		t.Helper()
		config, err := pgxpool.ParseConfig(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		config.ConnConfig.User, config.ConnConfig.Password = roles[label], passwords[label]
		config.MaxConns = 2
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pool
	}

	registry, err := syncdispatchcontract.Load(filepath.Join("..", "..", "contracts", "sync-dispatch", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := syncroute.NewCapabilities(syncdispatchruntime.RouteCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	// The operator mutates routes on its coordinator login (internal/workersctl).
	controller, err := syncroute.NewController(login("coordinator"), registry, capabilities)
	if err != nil {
		t.Fatal(err)
	}
	kinds := syncdispatchcontract.Kinds()
	if len(kinds) == 0 {
		t.Fatal("the contract names no sync-dispatch kind: the measurement did not happen")
	}
	for _, kind := range kinds {
		descriptor, known := registry.Lookup(kind)
		if !known {
			t.Fatalf("the checked-in contract has no route for %s", kind)
		}
		before, err := controller.Inspect(ctx, kind)
		if err != nil {
			t.Fatalf("Inspect(%s) on a fresh database: %v", kind, err)
		}
		if before.Transport != descriptor.Route || before.Paused || before.RollbackTransport != descriptor.RollbackRoute {
			t.Errorf("a fresh database holds the %s route as transport=%s paused=%t rollback=%s; the checked-in contract is route=%s rollback=%s",
				kind, before.Transport, before.Paused, before.RollbackTransport, descriptor.Route, descriptor.RollbackRoute)
		}
		applied, err := controller.ApplyCheckedIn(ctx, kind)
		if err != nil {
			t.Fatalf("ApplyCheckedIn(%s) on a fresh database: %v (the route activation step of a fresh install fails)", kind, err)
		}
		if applied.Transport != descriptor.Route || applied.Paused || applied.RollbackTransport != descriptor.RollbackRoute {
			t.Errorf("ApplyCheckedIn(%s) left transport=%s paused=%t rollback=%s", kind, applied.Transport, applied.Paused, applied.RollbackTransport)
		}
		if applied.Generation != before.Generation {
			t.Errorf("ApplyCheckedIn(%s) moved the route (generation %d -> %d); on a fresh database it must find nothing to do",
				kind, before.Generation, applied.Generation)
		}
	}

	// The reconciler's readiness check `sync_dispatch_route_fence` is this fence, built
	// by this constructor, on the domain login (internal/reconcilerservice).
	fence, err := syncroute.New(login("domain"), registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := fence.Check(ctx); err != nil {
		t.Fatalf("the route fence on a fresh database: %v (the reconciler is never ready)", err)
	}
}
