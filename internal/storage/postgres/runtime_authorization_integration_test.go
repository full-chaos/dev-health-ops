//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const (
	runtimeAuthorizationDomainRole = "runtime_authorization_domain"
	runtimeAuthorizationQueueRole  = "runtime_authorization_queue"
	runtimeAuthorizationDomainPass = "runtime_authorization_domain_password"
	runtimeAuthorizationQueuePass  = "runtime_authorization_queue_password"
)

func TestRuntimeAuthorizationBindsSeparateLeastPrivilegeRolePools(t *testing.T) {
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
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE TABLE public.runtime_semantic_probe (id bigserial PRIMARY KEY, value text NOT NULL)",
		"CREATE TABLE public.worker_job_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY)",
		"CREATE SCHEMA river",
		"CREATE TABLE river.river_job (id bigserial PRIMARY KEY, state text NOT NULL)",
		"CREATE FUNCTION river.runtime_probe() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
		"CREATE ROLE " + runtimeAuthorizationDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + runtimeAuthorizationDomainPass + "'",
		"CREATE ROLE " + runtimeAuthorizationQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + runtimeAuthorizationQueuePass + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + runtimeAuthorizationDomainRole + ", " + runtimeAuthorizationQueueRole,
		"GRANT USAGE ON SCHEMA public TO " + runtimeAuthorizationDomainRole + ", " + runtimeAuthorizationQueueRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.runtime_semantic_probe, public.worker_job_outbox TO " + runtimeAuthorizationDomainRole,
		"GRANT USAGE, SELECT, UPDATE ON SEQUENCE public.runtime_semantic_probe_id_seq TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox TO " + runtimeAuthorizationQueueRole,
		"GRANT USAGE ON SCHEMA river TO " + runtimeAuthorizationQueueRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA river TO " + runtimeAuthorizationQueueRole,
		"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA river TO " + runtimeAuthorizationQueueRole,
		"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA river TO " + runtimeAuthorizationQueueRole,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	domain := openPostgresPool(t, ctx, postgresRoleURI(t, instance.URI, runtimeAuthorizationDomainRole, runtimeAuthorizationDomainPass))
	defer domain.Close()
	queue := openPostgresPool(t, ctx, postgresRoleURI(t, instance.URI, runtimeAuthorizationQueueRole, runtimeAuthorizationQueuePass))
	defer queue.Close()

	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); err != nil {
		t.Fatalf("domain authorization failed: %v", err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); err != nil {
		t.Fatalf("queue authorization failed: %v", err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationQueueRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("mismatched domain role error = %v, want ErrUnavailable", err)
	}

	if _, err := admin.Exec(ctx, "GRANT SELECT ON TABLE public.runtime_semantic_probe TO "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("overprivileged queue authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE SELECT ON TABLE public.runtime_semantic_probe FROM "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); err != nil {
		t.Fatalf("queue authorization did not recover after revoking excess grant: %v", err)
	}

	if _, err := admin.Exec(ctx, "GRANT USAGE ON SCHEMA river TO "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("overprivileged domain authorization error = %v, want ErrUnavailable", err)
	}
}
