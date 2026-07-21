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
	authorizedDomainRole     = "domain_authorized"
	connectOnlyDomainRole    = "domain_connect_only"
	domainAuthorizationPass  = "domain_authorized_password"
	domainConnectOnlyPass    = "domain_connect_only_password"
	domainAuthorizationTable = "domain_authorization_probe"
)

func TestDomainAuthorizationRequiresSemanticDML(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closePostgresInstance(t, instance)

	adminPool := openPostgresPool(t, ctx, instance.URI)
	defer adminPool.Close()
	for _, statement := range []string{
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE SCHEMA river",
		"CREATE TABLE public." + domainAuthorizationTable + " (id bigserial PRIMARY KEY, value text NOT NULL)",
		"CREATE TABLE public.worker_job_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE ROLE " + authorizedDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + domainAuthorizationPass + "'",
		"CREATE ROLE " + connectOnlyDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + domainConnectOnlyPass + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + authorizedDomainRole + ", " + connectOnlyDomainRole,
		"GRANT USAGE ON SCHEMA public TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO " + authorizedDomainRole,
		"REVOKE ALL PRIVILEGES ON TABLE public.worker_job_outbox FROM " + authorizedDomainRole,
		"GRANT SELECT, INSERT ON TABLE public.worker_job_outbox TO " + authorizedDomainRole,
		"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO " + authorizedDomainRole,
	} {
		if _, err := adminPool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	authorizedPool := openPostgresPool(
		t,
		ctx,
		postgresRoleURI(t, instance.URI, authorizedDomainRole, domainAuthorizationPass),
	)
	defer authorizedPool.Close()
	if err := postgresstore.CheckDomainAuthorization(ctx, authorizedPool, authorizedDomainRole, "river"); err != nil {
		t.Fatalf("authorized domain readiness failed: %v", err)
	}
	if _, err := adminPool.Exec(ctx, "CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, authorizedPool, authorizedDomainRole, "river"); err != nil {
		t.Fatalf("Alembic metadata without runtime grants closed readiness: %v", err)
	}
	if _, err := authorizedPool.Exec(ctx, "SELECT * FROM public.alembic_version"); err == nil {
		t.Fatal("authorized domain role unexpectedly reads Alembic metadata")
	}
	if _, err := adminPool.Exec(ctx, "CREATE TABLE public.domain_newly_migrated (id bigint PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, authorizedPool, authorizedDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("ungranted semantic table readiness error = %v, want ErrUnavailable", err)
	}
	if _, err := adminPool.Exec(
		ctx,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.domain_newly_migrated TO "+authorizedDomainRole,
	); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, authorizedPool, authorizedDomainRole, "river"); err != nil {
		t.Fatalf("refreshed semantic grants did not recover readiness: %v", err)
	}
	assertDomainDML(t, ctx, authorizedPool)
	if _, err := authorizedPool.Exec(ctx, "CREATE TABLE public.domain_ddl_forbidden (id bigint)"); err == nil {
		t.Fatal("authorized domain role unexpectedly has schema CREATE")
	}

	connectOnlyPool := openPostgresPool(
		t,
		ctx,
		postgresRoleURI(t, instance.URI, connectOnlyDomainRole, domainConnectOnlyPass),
	)
	defer connectOnlyPool.Close()
	if err := postgresstore.CheckDomainAuthorization(ctx, connectOnlyPool, connectOnlyDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("CONNECT-only domain readiness error = %v, want ErrUnavailable", err)
	}
	if _, err := connectOnlyPool.Exec(ctx, "SELECT count(*) FROM public."+domainAuthorizationTable); err == nil {
		t.Fatal("CONNECT-only domain role unexpectedly reads semantic tables")
	}
}

func assertDomainDML(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var id int64
	if err := pool.QueryRow(
		ctx,
		"INSERT INTO public."+domainAuthorizationTable+" (value) VALUES ('created') RETURNING id",
	).Scan(&id); err != nil {
		t.Fatalf("domain INSERT failed: %v", err)
	}
	if _, err := pool.Exec(
		ctx,
		"UPDATE public."+domainAuthorizationTable+" SET value = 'updated' WHERE id = $1",
		id,
	); err != nil {
		t.Fatalf("domain UPDATE failed: %v", err)
	}
	var value string
	if err := pool.QueryRow(
		ctx,
		"SELECT value FROM public."+domainAuthorizationTable+" WHERE id = $1",
		id,
	).Scan(&value); err != nil || value != "updated" {
		t.Fatalf("domain SELECT = %q, %v", value, err)
	}
	if _, err := pool.Exec(
		ctx,
		"DELETE FROM public."+domainAuthorizationTable+" WHERE id = $1",
		id,
	); err != nil {
		t.Fatalf("domain DELETE failed: %v", err)
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
