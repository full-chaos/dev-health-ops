//go:build integration

package riverstore_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

const (
	domainRole     = "worker_domain_runtime"
	queueRole      = "worker_queue_runtime"
	domainPassword = "domain_test_password"
	queuePassword  = "queue_test_password"
)

type integrationArgs struct {
	Marker string `json:"marker"`
}

func (integrationArgs) Kind() string { return "test.integration" }

type integrationWorker struct {
	river.WorkerDefaults[integrationArgs]
}

func (*integrationWorker) Work(context.Context, *river.Job[integrationArgs]) error { return nil }

func TestRiverMigrationRolesRetentionGrowthAndRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeInstance(t, instance)

	adminPool := openPool(t, ctx, instance.URI)
	defer adminPool.Close()
	assertRuntimeRolePreflightHasNoSideEffects(t, ctx, adminPool)
	createRuntimeRoles(t, ctx, adminPool)
	assertRuntimeRolePosture(t, ctx, adminPool, domainRole)
	assertRuntimeRolePosture(t, ctx, adminPool, queueRole)
	for _, statement := range []string{
		"CREATE TABLE public.domain_runtime_probe (id bigserial PRIMARY KEY, value text NOT NULL)",
		"CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY)",
		"CREATE TABLE public.integrations (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_sources (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_datasets (id uuid PRIMARY KEY)",
		"CREATE TABLE public.integration_credentials (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_runs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.worker_job_routes (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_run_units (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_watermarks (key text PRIMARY KEY, value text NOT NULL)",
		"CREATE TABLE public.worker_job_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.worker_job_completion_fences (completion_key text PRIMARY KEY)",
		"CREATE TABLE public.sync_dispatch_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_dispatch_transport_routes (kind text PRIMARY KEY, generation bigint NOT NULL)",
		"CREATE FUNCTION public.domain_runtime_forbidden() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
	} {
		if _, err := adminPool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	domainURI := roleURI(t, instance.URI, domainRole, domainPassword, "worker_test")
	queueURI := roleURI(t, instance.URI, queueRole, queuePassword, "worker_test")
	assertPrefixUpgrade(t, ctx, adminPool)

	// Pool construction alone is deliberately DDL-free.
	runtimePools, err := postgresstore.OpenRuntimePools(ctx, postgresstore.RuntimeConfig{
		DomainURI:        domainURI,
		QueueControlURI:  queueURI,
		DomainRole:       domainRole,
		QueueRole:        queueRole,
		RiverSchema:      "river",
		QueueControlMode: config.QueueControlDirect,
		DomainMaxConns:   1,
		QueueMaxConns:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimePools.Close()
	var beforeMigration *string
	if err := adminPool.QueryRow(ctx, "SELECT to_regclass('river.river_migration')::text").Scan(&beforeMigration); err != nil {
		t.Fatal(err)
	}
	if beforeMigration != nil {
		t.Fatalf("runtime pool construction auto-migrated River: %q", *beforeMigration)
	}

	result, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema:     "river",
		DomainRole: domainRole,
		QueueRole:  queueRole,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CurrentVersion != riverstore.PinnedSchemaVersion || len(result.AppliedVersions) != riverstore.PinnedSchemaVersion {
		t.Fatalf("unexpected migration result: %#v", result)
	}
	second, err := riverstore.ApplyPinnedMigrations(ctx, adminPool, riverstore.MigrationOptions{
		Schema:     "river",
		DomainRole: domainRole,
		QueueRole:  queueRole,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.CurrentVersion != riverstore.PinnedSchemaVersion || len(second.AppliedVersions) != 0 {
		t.Fatalf("migration was not idempotent: %#v", second)
	}

	assertRequiredIndexes(t, ctx, adminPool)
	assertRuntimePrivileges(t, ctx, domainURI, queueURI)
	assertRetention(t, ctx, adminPool, queueURI)
	assertGrowthAndVacuum(t, ctx, adminPool, queueURI)
	assertBackupRestore(t, ctx, instance, adminPool, queueURI)
	assertSuffixMismatchFailsClosed(t, ctx, adminPool)
}

func assertRuntimeRolePreflightHasNoSideEffects(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	const validDomainRole = "preflight_domain_valid"
	const validQueueRole = "preflight_queue_valid"
	roles := []struct {
		name       string
		attributes string
	}{
		{name: validDomainRole, attributes: "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"},
		{name: validQueueRole, attributes: "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"},
		{name: "preflight_no_login", attributes: "NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"},
		{name: "preflight_superuser", attributes: "LOGIN SUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"},
		{name: "preflight_createdb", attributes: "LOGIN NOSUPERUSER CREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"},
		{name: "preflight_createrole", attributes: "LOGIN NOSUPERUSER NOCREATEDB CREATEROLE NOREPLICATION NOBYPASSRLS"},
		{name: "preflight_replication", attributes: "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE REPLICATION NOBYPASSRLS"},
		{name: "preflight_bypassrls", attributes: "LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION BYPASSRLS"},
	}
	for _, role := range roles {
		if _, err := pool.Exec(ctx, "CREATE ROLE "+role.name+" "+role.attributes); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name       string
		domainRole string
		queueRole  string
	}{
		{name: "missing_queue", domainRole: validDomainRole, queueRole: "preflight_queue_missing"},
		{name: "missing_domain", domainRole: "preflight_domain_missing", queueRole: validQueueRole},
		{name: "domain_no_login", domainRole: "preflight_no_login", queueRole: validQueueRole},
		{name: "domain_superuser", domainRole: "preflight_superuser", queueRole: validQueueRole},
		{name: "domain_createdb", domainRole: "preflight_createdb", queueRole: validQueueRole},
		{name: "domain_createrole", domainRole: "preflight_createrole", queueRole: validQueueRole},
		{name: "domain_replication", domainRole: "preflight_replication", queueRole: validQueueRole},
		{name: "domain_bypassrls", domainRole: "preflight_bypassrls", queueRole: validQueueRole},
		{name: "queue_no_login", domainRole: validDomainRole, queueRole: "preflight_no_login"},
		{name: "queue_superuser", domainRole: validDomainRole, queueRole: "preflight_superuser"},
		{name: "queue_createdb", domainRole: validDomainRole, queueRole: "preflight_createdb"},
		{name: "queue_createrole", domainRole: validDomainRole, queueRole: "preflight_createrole"},
		{name: "queue_replication", domainRole: validDomainRole, queueRole: "preflight_replication"},
		{name: "queue_bypassrls", domainRole: validDomainRole, queueRole: "preflight_bypassrls"},
	}
	for index, test := range tests {
		schema := fmt.Sprintf("river_preflight_%02d", index)
		result, err := riverstore.ApplyPinnedMigrations(ctx, pool, riverstore.MigrationOptions{
			Schema:     schema,
			DomainRole: test.domainRole,
			QueueRole:  test.queueRole,
		})
		if !errors.Is(err, riverstore.ErrMigrationConfiguration) {
			t.Fatalf("ApplyPinnedMigrations() %s role posture error = %v", test.name, err)
		}
		if result.CurrentVersion != 0 || len(result.AppliedVersions) != 0 {
			t.Fatalf("ineligible-role migration returned side effects: %#v", result)
		}
		var schemaExists bool
		if err := pool.QueryRow(
			ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1)",
			schema,
		).Scan(&schemaExists); err != nil {
			t.Fatal(err)
		}
		if schemaExists {
			t.Fatalf("ineligible-role preflight created schema %s", schema)
		}
		var migrationTable *string
		if err := pool.QueryRow(
			ctx,
			"SELECT to_regclass($1)::text",
			schema+".river_migration",
		).Scan(&migrationTable); err != nil {
			t.Fatal(err)
		}
		if migrationTable != nil {
			t.Fatalf("ineligible-role preflight created migration table %s", *migrationTable)
		}
	}
	for _, missingRole := range []string{"preflight_queue_missing", "preflight_domain_missing"} {
		var roleExists bool
		if err := pool.QueryRow(
			ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1)",
			missingRole,
		).Scan(&roleExists); err != nil {
			t.Fatal(err)
		}
		if roleExists {
			t.Fatalf("migration auto-created runtime role %s", missingRole)
		}
	}
	for _, role := range roles {
		if _, err := pool.Exec(ctx, "DROP ROLE "+role.name); err != nil {
			t.Fatalf("drop preflight role %s: %v", role.name, err)
		}
	}
}

func assertRuntimeRolePosture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, role string) {
	t.Helper()
	var canLogin, superuser, createDB, createRole, replication, bypassRLS bool
	if err := pool.QueryRow(
		ctx,
		`SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls
		FROM pg_catalog.pg_roles WHERE rolname = $1`,
		role,
	).Scan(&canLogin, &superuser, &createDB, &createRole, &replication, &bypassRLS); err != nil {
		t.Fatal(err)
	}
	if !canLogin || superuser || createDB || createRole || replication || bypassRLS {
		t.Fatalf("runtime role %s does not have least-privilege LOGIN posture", role)
	}
}

func assertPrefixUpgrade(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA river_prefix"); err != nil {
		t.Fatal(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Schema: "river_prefix"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{TargetVersion: 6}); err != nil {
		t.Fatal(err)
	}
	result, err := riverstore.ApplyPinnedMigrations(ctx, pool, riverstore.MigrationOptions{
		Schema:     "river_prefix",
		DomainRole: domainRole,
		QueueRole:  queueRole,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CurrentVersion != 7 || len(result.AppliedVersions) != 1 || result.AppliedVersions[0] != 7 {
		t.Fatalf("schema-6 prefix upgrade = %#v", result)
	}
}

func assertSuffixMismatchFailsClosed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, "INSERT INTO river.river_migration(line, version) VALUES ('main', 8)"); err != nil {
		t.Fatal(err)
	}
	if _, err := riverstore.CheckSchema(ctx, pool, "river", nil); !errors.Is(err, riverstore.ErrSchemaNotCurrent) {
		t.Fatalf("CheckSchema() with unknown suffix error = %v", err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM river.river_migration WHERE line='main' AND version=8"); err != nil {
		t.Fatal(err)
	}
}

func createRuntimeRoles(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{
		"CREATE ROLE " + domainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + domainPassword + "'",
		"CREATE ROLE " + queueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + queuePassword + "'",
	} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
}

func assertRequiredIndexes(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	rows, err := pool.Query(ctx, "SELECT indexname FROM pg_indexes WHERE schemaname = 'river'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	found := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		found[name] = true
	}
	for _, required := range []string{
		"river_job_prioritized_fetching_index",
		"river_job_state_and_finalized_at_index",
		"river_job_unique_idx",
		"river_notification_created_at_idx",
	} {
		if !found[required] {
			t.Errorf("pinned River schema missing index %s", required)
		}
	}
}

func assertRuntimePrivileges(t *testing.T, ctx context.Context, domainURI, queueURI string) {
	t.Helper()
	domainPool := openPool(t, ctx, domainURI)
	defer domainPool.Close()
	for _, table := range []string{
		"integrations",
		"integration_sources",
		"integration_datasets",
		"integration_credentials",
		"sync_runs",
		"worker_job_routes",
		"sync_dispatch_transport_routes",
		"sync_run_units",
		"sync_watermarks",
		"sync_dispatch_outbox",
		"worker_job_outbox",
	} {
		if _, err := domainPool.Exec(ctx, "SELECT count(*) FROM public."+table); err != nil {
			t.Fatalf("domain role cannot SELECT %s: %v", table, err)
		}
	}
	if _, err := domainPool.Exec(ctx, "UPDATE public.sync_run_units SET state='updated'"); err != nil {
		t.Fatalf("domain role cannot UPDATE sync run units: %v", err)
	}
	if _, err := domainPool.Exec(ctx, "INSERT INTO public.sync_watermarks (key, value) VALUES ('privilege', 'ready')"); err != nil {
		t.Fatalf("domain role cannot INSERT sync watermarks: %v", err)
	}
	if _, err := domainPool.Exec(ctx, "UPDATE public.sync_watermarks SET value='updated' WHERE key='privilege'"); err != nil {
		t.Fatalf("domain role cannot UPDATE sync watermarks: %v", err)
	}
	if _, err := domainPool.Exec(
		ctx,
		"INSERT INTO public.sync_dispatch_outbox (id, state) VALUES ('00000000-0000-0000-0000-000000000003', 'pending')",
	); err != nil {
		t.Fatalf("domain role cannot INSERT sync-dispatch outbox state: %v", err)
	}
	if _, err := domainPool.Exec(
		ctx,
		"UPDATE public.sync_dispatch_outbox SET state='ready' WHERE id='00000000-0000-0000-0000-000000000003'",
	); err != nil {
		t.Fatalf("domain role cannot UPDATE sync-dispatch outbox state: %v", err)
	}
	for _, statement := range []string{
		"SELECT count(*) FROM public.domain_runtime_probe",
		"INSERT INTO public.domain_runtime_probe (value) VALUES ('forbidden')",
		"UPDATE public.domain_runtime_probe SET value='forbidden'",
		"DELETE FROM public.domain_runtime_probe",
		"SELECT nextval('public.domain_runtime_probe_id_seq')",
		"INSERT INTO public.integrations (id) VALUES ('00000000-0000-0000-0000-000000000010')",
		"INSERT INTO public.sync_run_units (id, state) VALUES ('00000000-0000-0000-0000-000000000011', 'forbidden')",
		"DELETE FROM public.sync_run_units",
		"DELETE FROM public.sync_watermarks",
		"DELETE FROM public.sync_dispatch_outbox",
	} {
		if _, err := domainPool.Exec(ctx, statement); err == nil {
			t.Fatalf("domain role unexpectedly has broad semantic access for %q", statement)
		}
	}
	if _, err := domainPool.Exec(
		ctx,
		"INSERT INTO public.worker_job_outbox (id, state) VALUES ('00000000-0000-0000-0000-000000000001', 'pending')",
	); err != nil {
		t.Fatalf("domain producer cannot insert outbox state: %v", err)
	}
	if _, err := domainPool.Exec(
		ctx,
		"INSERT INTO public.worker_job_completion_fences (completion_key) VALUES ('daily_metrics_run:00000000-0000-4000-8000-000000000001')",
	); err == nil {
		t.Fatal("domain runtime unexpectedly inserts completion fences")
	}
	if _, err := domainPool.Exec(
		ctx,
		"UPDATE public.worker_job_outbox SET state='forged' WHERE id='00000000-0000-0000-0000-000000000001'",
	); err == nil {
		t.Fatal("domain producer unexpectedly updates relay-owned outbox state")
	}
	if _, err := domainPool.Exec(
		ctx,
		"DELETE FROM public.worker_job_outbox WHERE id='00000000-0000-0000-0000-000000000001'",
	); err == nil {
		t.Fatal("domain producer unexpectedly deletes relay-owned outbox state")
	}
	if _, err := domainPool.Exec(ctx, "TRUNCATE public.worker_job_outbox"); err == nil {
		t.Fatal("domain producer unexpectedly truncates relay-owned outbox state")
	}
	if _, err := domainPool.Exec(ctx, "SELECT * FROM public.alembic_version"); err == nil {
		t.Fatal("domain role unexpectedly reads Alembic migration metadata")
	}
	if _, err := domainPool.Exec(ctx, "CREATE TABLE public.domain_ddl_forbidden (id bigint)"); err == nil {
		t.Fatal("domain role unexpectedly has CREATE on public schema")
	}
	if _, err := domainPool.Exec(ctx, "SELECT count(*) FROM river.river_job"); err == nil {
		t.Fatal("domain role unexpectedly reads River tables")
	}
	if _, err := domainPool.Exec(ctx, "SELECT public.domain_runtime_forbidden()"); err == nil {
		t.Fatal("domain role unexpectedly executes public functions")
	}

	queuePool := openPool(t, ctx, queueURI)
	defer queuePool.Close()
	client, err := river.NewClient(riverpgxv5.New(queuePool), &river.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Insert(ctx, integrationArgs{Marker: "privilege"}, &river.InsertOpts{Queue: "maintenance"}); err != nil {
		t.Fatalf("queue role cannot insert through River API: %v", err)
	}
	if _, err := queuePool.Exec(ctx, "SELECT count(*) FROM public.domain_runtime_probe"); err == nil {
		t.Fatal("queue role unexpectedly reads semantic domain tables")
	}
	if _, err := queuePool.Exec(ctx, "SELECT nextval('public.domain_runtime_probe_id_seq')"); err == nil {
		t.Fatal("queue role unexpectedly uses semantic domain sequences")
	}
	var outboxState string
	if err := queuePool.QueryRow(
		ctx,
		"SELECT state FROM public.worker_job_outbox WHERE id='00000000-0000-0000-0000-000000000001'",
	).Scan(&outboxState); err != nil || outboxState != "pending" {
		t.Fatalf("queue role outbox SELECT = %q, %v", outboxState, err)
	}
	if _, err := queuePool.Exec(
		ctx,
		"UPDATE public.worker_job_outbox SET state='claimed' WHERE id='00000000-0000-0000-0000-000000000001'",
	); err != nil {
		t.Fatalf("queue role cannot claim outbox state: %v", err)
	}
	if _, err := queuePool.Exec(
		ctx,
		"DELETE FROM public.worker_job_outbox WHERE id='00000000-0000-0000-0000-000000000001'",
	); err != nil {
		t.Fatalf("queue role cannot retire outbox state: %v", err)
	}
	if _, err := queuePool.Exec(
		ctx,
		"SELECT completion_key FROM public.worker_job_completion_fences",
	); err != nil {
		t.Fatalf("queue role cannot read completion fences: %v", err)
	}
	if _, err := queuePool.Exec(
		ctx,
		"UPDATE public.worker_job_completion_fences SET completion_key=completion_key",
	); err != nil {
		t.Fatalf("queue role cannot lock completion fences for bounded retention: %v", err)
	}
	if _, err := queuePool.Exec(
		ctx,
		"DELETE FROM public.worker_job_completion_fences",
	); err != nil {
		t.Fatalf("queue role cannot retire completion fences: %v", err)
	}
	if _, err := queuePool.Exec(
		ctx,
		"INSERT INTO public.worker_job_completion_fences (completion_key) VALUES ('forbidden')",
	); err == nil {
		t.Fatal("queue role unexpectedly inserts completion fences")
	}
	if _, err := queuePool.Exec(
		ctx,
		"INSERT INTO public.worker_job_outbox (id, state) VALUES ('00000000-0000-0000-0000-000000000002', 'forbidden')",
	); err == nil {
		t.Fatal("queue role unexpectedly inserts producer-owned outbox state")
	}
	if _, err := queuePool.Exec(
		ctx,
		"UPDATE public.sync_dispatch_outbox SET state='claimed' WHERE id='00000000-0000-0000-0000-000000000003'",
	); err != nil {
		t.Fatalf("queue role cannot transition sync-dispatch outbox state: %v", err)
	}
	if _, err := queuePool.Exec(
		ctx,
		"INSERT INTO public.sync_dispatch_outbox (id, state) VALUES ('00000000-0000-0000-0000-000000000004', 'forbidden')",
	); err == nil {
		t.Fatal("queue role unexpectedly inserts sync-dispatch outbox state")
	}
	if _, err := queuePool.Exec(ctx, "DELETE FROM public.sync_dispatch_outbox"); err == nil {
		t.Fatal("queue role unexpectedly deletes sync-dispatch outbox state")
	}
	if _, err := queuePool.Exec(ctx, "SELECT generation FROM public.sync_dispatch_transport_routes"); err != nil {
		t.Fatalf("queue role cannot read sync-dispatch route state: %v", err)
	}
	if _, err := queuePool.Exec(ctx, "UPDATE public.sync_dispatch_transport_routes SET generation = generation + 1"); err == nil {
		t.Fatal("queue role unexpectedly updates sync-dispatch route state")
	}
	if _, err := queuePool.Exec(ctx, "CREATE TABLE river.forbidden(id bigint)"); err == nil {
		t.Fatal("queue role unexpectedly has CREATE on the River schema")
	}
	if _, err := queuePool.Exec(ctx, "DROP TABLE river.river_job"); err == nil {
		t.Fatal("queue role unexpectedly owns River tables")
	}
}

func assertRetention(t *testing.T, ctx context.Context, adminPool *pgxpool.Pool, queueURI string) {
	t.Helper()
	var oldID, recentID int64
	insertTerminal := `
		INSERT INTO river.river_job (state, max_attempts, finalized_at, args, kind, queue, scheduled_at)
		VALUES ('completed', 3, $1, '{}', 'test.integration', 'maintenance', $1)
		RETURNING id`
	if err := adminPool.QueryRow(ctx, insertTerminal, time.Now().Add(-48*time.Hour)).Scan(&oldID); err != nil {
		t.Fatal(err)
	}
	if err := adminPool.QueryRow(ctx, insertTerminal, time.Now().Add(-time.Hour)).Scan(&recentID); err != nil {
		t.Fatal(err)
	}

	queuePool := openPool(t, ctx, queueURI)
	defer queuePool.Close()
	workers := river.NewWorkers()
	river.AddWorker(workers, &integrationWorker{})
	clientConfig := &river.Config{
		Queues:  map[string]river.QueueConfig{"maintenance": {MaxWorkers: 1}},
		Schema:  "river",
		Workers: workers,
	}
	maintenance := riverstore.DefaultMaintenanceConfig()
	maintenance.CompletedJobRetention = 24 * time.Hour
	if err := riverstore.ApplyMaintenance(clientConfig, maintenance); err != nil {
		t.Fatal(err)
	}
	client, err := river.NewClient(riverpgxv5.New(queuePool), clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	clientCtx, stopClient := context.WithCancel(ctx)
	defer stopClient()
	if err := client.Start(clientCtx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Stop(context.Background()) }()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var oldExists, recentExists bool
		if err := adminPool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM river.river_job WHERE id=$1)", oldID).Scan(&oldExists); err != nil {
			t.Fatal(err)
		}
		if err := adminPool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM river.river_job WHERE id=$1)", recentID).Scan(&recentExists); err != nil {
			t.Fatal(err)
		}
		if !oldExists {
			if !recentExists {
				t.Fatal("retention deleted a recent terminal job")
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("River cleaner did not delete an expired completed job")
}

func assertGrowthAndVacuum(t *testing.T, ctx context.Context, adminPool *pgxpool.Pool, queueURI string) {
	t.Helper()
	queuePool := openPool(t, ctx, queueURI)
	defer queuePool.Close()
	client, err := river.NewClient(riverpgxv5.New(queuePool), &river.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 200; index++ {
		if _, err := client.Insert(ctx, integrationArgs{Marker: fmt.Sprintf("growth-%03d", index)}, &river.InsertOpts{Queue: "growth"}); err != nil {
			t.Fatal(err)
		}
	}
	var bytes int64
	if err := adminPool.QueryRow(ctx, "SELECT pg_total_relation_size('river.river_job')").Scan(&bytes); err != nil {
		t.Fatal(err)
	}
	if bytes <= 0 || bytes > 16*1024*1024 {
		t.Fatalf("unexpected River growth for 200 bounded jobs: %d bytes", bytes)
	}
	if _, err := adminPool.Exec(ctx, "VACUUM (ANALYZE) river.river_job"); err != nil {
		t.Fatal(err)
	}
	var analyzed bool
	if err := adminPool.QueryRow(ctx, "SELECT last_analyze IS NOT NULL FROM pg_stat_user_tables WHERE schemaname='river' AND relname='river_job'").Scan(&analyzed); err != nil {
		t.Fatal(err)
	}
	if !analyzed {
		t.Fatal("VACUUM ANALYZE did not update River table statistics")
	}
}

func assertBackupRestore(
	t *testing.T,
	ctx context.Context,
	instance *containers.Instance,
	adminPool *pgxpool.Pool,
	queueURI string,
) {
	t.Helper()
	queuePool := openPool(t, ctx, queueURI)
	client, err := river.NewClient(riverpgxv5.New(queuePool), &river.Config{Schema: "river"})
	if err != nil {
		t.Fatal(err)
	}
	available, err := client.Insert(ctx, integrationArgs{Marker: "restore-available"}, &river.InsertOpts{Queue: "restore"})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := client.Insert(ctx, integrationArgs{Marker: "restore-completed"}, &river.InsertOpts{Queue: "restore"})
	if err != nil {
		t.Fatal(err)
	}
	queuePool.Close()
	if _, err := adminPool.Exec(ctx, "UPDATE river.river_job SET state='completed', finalized_at=now() WHERE id=$1", completed.Job.ID); err != nil {
		t.Fatal(err)
	}

	runContainerCommand(t, ctx, instance, "pg_dump", "--username=worker_test", "--dbname=worker_test", "--schema=river", "--format=custom", "--file=/tmp/river.dump")
	runContainerCommand(t, ctx, instance, "createdb", "--username=worker_test", "restored_worker_test")
	runContainerCommand(t, ctx, instance, "pg_restore", "--username=worker_test", "--dbname=restored_worker_test", "--exit-on-error", "--no-owner", "--no-privileges", "/tmp/river.dump")

	restoredURI := roleURI(t, instance.URI, "worker_test", "worker_test_password", "restored_worker_test")
	restoredPool := openPool(t, ctx, restoredURI)
	defer restoredPool.Close()
	for id, wantState := range map[int64]string{available.Job.ID: "available", completed.Job.ID: "completed"} {
		var state string
		if err := restoredPool.QueryRow(ctx, "SELECT state::text FROM river.river_job WHERE id=$1", id).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state != wantState {
			t.Fatalf("restored job %d state = %q, want %q", id, state, wantState)
		}
	}
	if current, err := riverstore.CheckSchema(ctx, restoredPool, "river", nil); err != nil || current != riverstore.PinnedSchemaVersion {
		t.Fatalf("restored River schema check = %d, %v", current, err)
	}
}

func runContainerCommand(t *testing.T, ctx context.Context, instance *containers.Instance, command ...string) {
	t.Helper()
	exitCode, output, err := instance.Container.Exec(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(io.LimitReader(output, 16*1024))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if exitCode != 0 {
		t.Fatalf("container command %s failed with %d: %s", command[0], exitCode, strings.TrimSpace(string(data)))
	}
}

func roleURI(t *testing.T, rawURI, user, password, database string) string {
	t.Helper()
	parsed, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(user, password)
	parsed.Path = "/" + database
	return parsed.String()
}

func openPool(t *testing.T, ctx context.Context, uri string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return pool
}

func closeInstance(t *testing.T, instance *containers.Instance) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := instance.Close(ctx); err != nil {
		t.Errorf("terminate PostgreSQL test dependency: %v", err)
	}
}
