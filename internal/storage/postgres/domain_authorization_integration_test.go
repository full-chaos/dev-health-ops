//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	authorizedDomainRole    = "domain_authorized"
	domainAuthorizationPass = "domain_authorized_password"
)

func TestDomainAuthorizationRequiresExactCanaryAndReconcilerPrivileges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closePostgresInstance(t, instance)

	admin := openPostgresPool(t, ctx, instance.URI)
	defer admin.Close()
	for _, statement := range []string{
		"REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE SCHEMA river",
		"CREATE TABLE river.river_job (id bigint PRIMARY KEY)",
		"CREATE SEQUENCE river.runtime_sequence",
		"CREATE FUNCTION river.runtime_probe() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
		"REVOKE ALL ON FUNCTION river.runtime_probe() FROM PUBLIC",
		"CREATE FUNCTION public.runtime_probe() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
		"REVOKE ALL ON FUNCTION public.runtime_probe() FROM PUBLIC",
		"CREATE TABLE public.integrations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.integration_sources (id bigint PRIMARY KEY)",
		"CREATE TABLE public.integration_datasets (id bigint PRIMARY KEY)",
		"CREATE TABLE public.integration_credentials (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.worker_job_routes (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_dispatch_transport_routes (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_run_units (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.sync_watermarks (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.sync_dispatch_outbox (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.worker_job_outbox (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.unrelated_semantic_table (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY)",
		"CREATE SEQUENCE public.unrelated_sequence",
		"CREATE ROLE " + authorizedDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + domainAuthorizationPass + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + authorizedDomainRole,
		"GRANT USAGE ON SCHEMA public TO " + authorizedDomainRole,
		"GRANT SELECT ON TABLE public.integrations, public.integration_sources, public.integration_datasets, public.integration_credentials, public.sync_runs, public.worker_job_routes, public.sync_dispatch_transport_routes TO " + authorizedDomainRole,
		"GRANT SELECT, UPDATE ON TABLE public.sync_run_units TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_watermarks, public.sync_dispatch_outbox TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT ON TABLE public.worker_job_outbox TO " + authorizedDomainRole,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	domain := openPostgresPool(
		t,
		ctx,
		postgresRoleURI(t, instance.URI, authorizedDomainRole, domainAuthorizationPass),
	)
	defer domain.Close()

	assertDomainAuthorized(t, ctx, domain)
	if _, err := domain.Exec(ctx, "SELECT id FROM public.integrations"); err != nil {
		t.Fatalf("domain SELECT-only inventory access failed: %v", err)
	}
	if _, err := domain.Exec(ctx, "UPDATE public.sync_run_units SET state = 'ready'"); err != nil {
		t.Fatalf("domain sync-run-unit UPDATE failed: %v", err)
	}
	if _, err := domain.Exec(ctx, "INSERT INTO public.sync_watermarks (id, state) VALUES (1, 'ready')"); err != nil {
		t.Fatalf("domain watermark INSERT failed: %v", err)
	}
	if _, err := domain.Exec(ctx, "UPDATE public.sync_watermarks SET state = 'updated' WHERE id = 1"); err != nil {
		t.Fatalf("domain watermark UPDATE failed: %v", err)
	}
	if _, err := domain.Exec(ctx, "INSERT INTO public.sync_dispatch_outbox (id, state) VALUES (1, 'ready')"); err != nil {
		t.Fatalf("domain sync-dispatch INSERT failed: %v", err)
	}
	if _, err := domain.Exec(ctx, "INSERT INTO public.worker_job_outbox (id, state) VALUES (1, 'ready')"); err != nil {
		t.Fatalf("domain worker-job INSERT failed: %v", err)
	}
	for name, statement := range map[string]string{
		"unrelated table SELECT": "SELECT id FROM public.unrelated_semantic_table",
		"Alembic SELECT":         "SELECT version_num FROM public.alembic_version",
		"route UPDATE":           "UPDATE public.sync_dispatch_transport_routes SET id = id",
		"worker outbox UPDATE":   "UPDATE public.worker_job_outbox SET state = 'forbidden'",
		"domain DELETE":          "DELETE FROM public.sync_watermarks",
		"domain TRUNCATE":        "TRUNCATE public.sync_watermarks",
		"sequence use":           "SELECT nextval('public.unrelated_sequence')",
		"public DDL":             "CREATE TABLE public.domain_ddl_forbidden (id bigint)",
	} {
		if _, err := domain.Exec(ctx, statement); err == nil {
			t.Fatalf("domain role unexpectedly permits %s", name)
		}
	}

	for _, test := range []struct {
		name   string
		grant  string
		revoke string
	}{
		{
			name:   "missing SELECT-only privilege",
			grant:  "REVOKE SELECT ON TABLE public.integrations FROM " + authorizedDomainRole,
			revoke: "GRANT SELECT ON TABLE public.integrations TO " + authorizedDomainRole,
		},
		{
			name:   "missing sync-run-unit UPDATE",
			grant:  "REVOKE UPDATE ON TABLE public.sync_run_units FROM " + authorizedDomainRole,
			revoke: "GRANT UPDATE ON TABLE public.sync_run_units TO " + authorizedDomainRole,
		},
		{
			name:   "missing watermark INSERT",
			grant:  "REVOKE INSERT ON TABLE public.sync_watermarks FROM " + authorizedDomainRole,
			revoke: "GRANT INSERT ON TABLE public.sync_watermarks TO " + authorizedDomainRole,
		},
		{
			name:   "missing worker-outbox INSERT",
			grant:  "REVOKE INSERT ON TABLE public.worker_job_outbox FROM " + authorizedDomainRole,
			revoke: "GRANT INSERT ON TABLE public.worker_job_outbox TO " + authorizedDomainRole,
		},
		{
			name:   "operator route mutation",
			grant:  "GRANT UPDATE ON TABLE public.sync_dispatch_transport_routes TO " + authorizedDomainRole,
			revoke: "REVOKE UPDATE ON TABLE public.sync_dispatch_transport_routes FROM " + authorizedDomainRole,
		},
		{
			name:   "column-level operator route mutation",
			grant:  "GRANT UPDATE (id) ON TABLE public.worker_job_routes TO " + authorizedDomainRole,
			revoke: "REVOKE UPDATE (id) ON TABLE public.worker_job_routes FROM " + authorizedDomainRole,
		},
		{
			name:   "destructive DELETE",
			grant:  "GRANT DELETE ON TABLE public.sync_dispatch_outbox TO " + authorizedDomainRole,
			revoke: "REVOKE DELETE ON TABLE public.sync_dispatch_outbox FROM " + authorizedDomainRole,
		},
		{
			name:   "unrelated semantic access",
			grant:  "GRANT SELECT ON TABLE public.unrelated_semantic_table TO " + authorizedDomainRole,
			revoke: "REVOKE SELECT ON TABLE public.unrelated_semantic_table FROM " + authorizedDomainRole,
		},
		{
			name:   "sequence privilege",
			grant:  "GRANT USAGE ON SEQUENCE public.unrelated_sequence TO " + authorizedDomainRole,
			revoke: "REVOKE USAGE ON SEQUENCE public.unrelated_sequence FROM " + authorizedDomainRole,
		},
		{
			name:   "public function execution",
			grant:  "GRANT EXECUTE ON FUNCTION public.runtime_probe() TO " + authorizedDomainRole,
			revoke: "REVOKE EXECUTE ON FUNCTION public.runtime_probe() FROM " + authorizedDomainRole,
		},
		{
			name:   "River schema usage",
			grant:  "GRANT USAGE ON SCHEMA river TO " + authorizedDomainRole,
			revoke: "REVOKE USAGE ON SCHEMA river FROM " + authorizedDomainRole,
		},
		{
			name:   "column-level River table access",
			grant:  "GRANT SELECT (id) ON TABLE river.river_job TO " + authorizedDomainRole,
			revoke: "REVOKE SELECT (id) ON TABLE river.river_job FROM " + authorizedDomainRole,
		},
		{
			name:   "River sequence access",
			grant:  "GRANT USAGE ON SEQUENCE river.runtime_sequence TO " + authorizedDomainRole,
			revoke: "REVOKE USAGE ON SEQUENCE river.runtime_sequence FROM " + authorizedDomainRole,
		},
		{
			name:   "River function execution",
			grant:  "GRANT EXECUTE ON FUNCTION river.runtime_probe() TO " + authorizedDomainRole,
			revoke: "REVOKE EXECUTE ON FUNCTION river.runtime_probe() FROM " + authorizedDomainRole,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := admin.Exec(ctx, test.grant); err != nil {
				t.Fatal(err)
			}
			assertDomainUnauthorized(t, ctx, domain)
			if _, err := admin.Exec(ctx, test.revoke); err != nil {
				t.Fatal(err)
			}
			assertDomainAuthorized(t, ctx, domain)
		})
	}
}

func assertDomainAuthorized(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if err := postgresstore.CheckDomainAuthorization(ctx, pool, authorizedDomainRole, "river"); err != nil {
		t.Fatalf("domain readiness failed: %v", err)
	}
}

func assertDomainUnauthorized(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if err := postgresstore.CheckDomainAuthorization(ctx, pool, authorizedDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("domain readiness error = %v, want ErrUnavailable", err)
	}
}

func postgresRoleURI(t *testing.T, rawURI, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(rawURI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}

func openPostgresPool(t *testing.T, ctx context.Context, uri string) *pgxpool.Pool {
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

func closePostgresInstance(t *testing.T, instance *containers.Instance) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := instance.Close(ctx); err != nil {
		t.Errorf("terminate PostgreSQL test dependency: %v", err)
	}
}
