//go:build integration

package main

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobroute"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// newJobRouteController's three pools are not interchangeable in production
// (see its doc comment in main.go): coordinatorPool is coordinator-exclusive
// for worker_job_routes/worker_job_outbox/worker_job_runs, domainPool is the
// least-privilege role that can still read sync_run_units, and queuePool only
// ever touches the River schema. A test that reused one superuser pool for
// all three arguments would compile and even pass, but it would prove nothing
// about the CHAOS-3113 split this constructor exists to encode -- the domain
// role genuinely cannot run the coordinator's statements in production (that
// was the bug), so a test pool that CAN run them is asserting something
// production does not do. This harness therefore provisions three real,
// least-privilege logins the same way
// internal/storage/postgres/domain_grant_reconciliation_integration_test.go
// and coordinator_statement_privileges_integration_test.go do, and derives
// the coordinator grants from postgres.CoordinatorPosture() -- the single
// authority CheckCoordinatorAuthorization itself asserts against -- rather
// than a hand-copied list that could drift from it.
const (
	workerctlDomainRole      = "workerctl_domain_runtime"
	workerctlQueueRole       = "workerctl_queue_runtime"
	workerctlCoordinatorRole = "workerctl_coordinator_runtime"
	workerctlDomainPass      = "workerctl_domain_password"
	workerctlQueuePass       = "workerctl_queue_password"
	workerctlCoordinatorPass = "workerctl_coordinator_password"
	workerctlSchema          = "river"
	workerctlDatabase        = "worker_test"
)

// startJobRouteHarness starts a real PostgreSQL container, creates the three
// least-privilege runtime roles the production binary authenticates as, and
// applies the real pinned River migration (including the real coordinator
// grants derived from postgres.CoordinatorPosture()) against it. It returns
// the admin pool (for seeding fixtures) and the instance URI (for connecting
// as each restricted role).
func startJobRouteHarness(t *testing.T, ctx context.Context) (*pgxpool.Pool, string) {
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
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	setup := []string{
		"CREATE ROLE " + workerctlDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + workerctlDomainPass + "'",
		"CREATE ROLE " + workerctlQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + workerctlQueuePass + "'",
		"CREATE ROLE " + workerctlCoordinatorRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + workerctlCoordinatorPass + "'",
		"GRANT CONNECT ON DATABASE " + workerctlDatabase + " TO " + workerctlDomainRole + ", " + workerctlQueueRole + ", " + workerctlCoordinatorRole,
		"REVOKE TEMPORARY ON DATABASE " + workerctlDatabase + " FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		// Real production shapes: the three worker_job_* tables newJobRouteController's
		// coordinator pool touches (control.go), and sync_run_units, which the
		// domain pool's Celery quiescer reads (quiescer.go).
		`CREATE TABLE public.worker_job_routes (
			job_kind text PRIMARY KEY, transport text NOT NULL, paused boolean NOT NULL,
			generation bigint NOT NULL, updated_at timestamptz NOT NULL
		)`,
		`CREATE TABLE public.worker_job_outbox (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		)`,
		`CREATE TABLE public.worker_job_runs (
			id uuid PRIMARY KEY, job_kind text NOT NULL, status text NOT NULL
		)`,
		`CREATE TABLE public.sync_run_units (
			id uuid PRIMARY KEY, provider text NOT NULL, dataset_key text NOT NULL,
			status text NOT NULL
		)`,
	}
	for _, statement := range setup {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	// Derived from CoordinatorPosture(), not restated: this is the same
	// authority coordinatorGrantStatements uses in the real one-shot
	// migration command and CheckCoordinatorAuthorization asserts readiness
	// against. Every table in the posture that this harness never created
	// (internal_service_credentials, scheduled_jobs, ...) is skipped by
	// migrate.go's to_regclass guard rather than failing.
	posture := postgresstore.CoordinatorPosture()
	coordinatorGrants := make([]riverstore.TableGrant, 0, len(posture.RequiredTables))
	for _, table := range posture.RequiredTables {
		coordinatorGrants = append(coordinatorGrants, riverstore.TableGrant{
			TableName:   table.TableName,
			AllowInsert: table.AllowInsert,
			AllowUpdate: table.AllowUpdate,
			AllowDelete: table.AllowDelete,
		})
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, admin, riverstore.MigrationOptions{
		Schema:            workerctlSchema,
		DomainRole:        workerctlDomainRole,
		QueueRole:         workerctlQueueRole,
		CoordinatorRole:   workerctlCoordinatorRole,
		CoordinatorGrants: coordinatorGrants,
	}); err != nil {
		t.Fatal(err)
	}
	return admin, instance.URI
}

// connectAsRole opens a pool authenticated as one of the harness's restricted
// logins, mirroring internal/storage/postgres's connectAs test helper.
func connectAsRole(t *testing.T, ctx context.Context, rawURI, role, password string) *pgxpool.Pool {
	t.Helper()
	parsed, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	pool, err := pgxpool.New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestNewJobRouteControllerWiresCelerySyncProviderQuiescence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	admin, uri := startJobRouteHarness(t, ctx)

	if _, err := admin.Exec(ctx, `
		INSERT INTO public.worker_job_routes
			(job_kind, transport, paused, generation, updated_at)
		VALUES ('sync.provider_unit', 'celery', FALSE, 1, statement_timestamp())`); err != nil {
		t.Fatal(err)
	}

	registry, err := jobruntime.Load("../../contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}

	// The load-bearing part of this test: coordinatorPool is a REAL
	// coordinator-role connection, not the admin pool and not the domain
	// pool. If newJobRouteController were wired back onto the domain role (the
	// CHAOS-3113 regression), every coordinator statement below would fail
	// with 42501 rather than the assertions below failing on their own terms.
	coordinatorPool := connectAsRole(t, ctx, uri, workerctlCoordinatorRole, workerctlCoordinatorPass)
	domainPool := connectAsRole(t, ctx, uri, workerctlDomainRole, workerctlDomainPass)
	queuePool := connectAsRole(t, ctx, uri, workerctlQueueRole, workerctlQueuePass)

	controller, err := newJobRouteController(coordinatorPool, domainPool, queuePool, workerctlSchema, registry)
	if err != nil {
		t.Fatal(err)
	}
	state, err := controller.ApplyCheckedIn(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatalf("empty Celery unit ledger activation: %v", err)
	}
	if state.Transport != "river_canary" || state.Generation != 2 {
		t.Fatalf("activated state = %+v", state)
	}
	if _, err := controller.Rollback(ctx, "sync.provider_unit"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if _, err := admin.Exec(ctx, `
		INSERT INTO public.sync_run_units (id, provider, dataset_key, status)
		VALUES ('00000000-0000-4000-8000-000000000002', 'launchdarkly', 'feature-flags', 'running')`); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ApplyCheckedIn(ctx, "sync.provider_unit"); !errors.Is(err, jobroute.ErrLiveClaims) {
		t.Fatalf("nonterminal Celery unit activation error = %v, want %v", err, jobroute.ErrLiveClaims)
	}
	state, err = controller.Inspect(ctx, "sync.provider_unit")
	if err != nil {
		t.Fatal(err)
	}
	if state.Transport != "celery" || state.Generation != 3 {
		t.Fatalf("failed activation changed state = %+v", state)
	}
}
