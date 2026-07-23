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
		"CREATE TABLE public.sync_dispatch_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_dispatch_transport_routes (kind text PRIMARY KEY, generation bigint NOT NULL)",
		"CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY)",
		"CREATE SCHEMA river",
		"CREATE TABLE river.river_job (id bigserial PRIMARY KEY, state text NOT NULL)",
		"CREATE FUNCTION river.runtime_probe() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
		"REVOKE ALL ON FUNCTION river.runtime_probe() FROM PUBLIC",
		"CREATE FUNCTION public.runtime_public_probe() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
		"REVOKE ALL ON FUNCTION public.runtime_public_probe() FROM PUBLIC",
		"CREATE ROLE " + runtimeAuthorizationDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + runtimeAuthorizationDomainPass + "'",
		"CREATE ROLE " + runtimeAuthorizationQueueRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + runtimeAuthorizationQueuePass + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + runtimeAuthorizationDomainRole + ", " + runtimeAuthorizationQueueRole,
		"GRANT USAGE ON SCHEMA public TO " + runtimeAuthorizationDomainRole + ", " + runtimeAuthorizationQueueRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.runtime_semantic_probe TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT, INSERT ON TABLE public.worker_job_outbox TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.sync_dispatch_outbox, public.sync_dispatch_transport_routes TO " + runtimeAuthorizationDomainRole,
		"GRANT USAGE, SELECT, UPDATE ON SEQUENCE public.runtime_semantic_probe_id_seq TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox TO " + runtimeAuthorizationQueueRole,
		"GRANT SELECT, UPDATE ON TABLE public.sync_dispatch_outbox TO " + runtimeAuthorizationQueueRole,
		"GRANT SELECT ON TABLE public.sync_dispatch_transport_routes TO " + runtimeAuthorizationQueueRole,
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
	if _, err := queue.Exec(ctx, "INSERT INTO public.sync_dispatch_outbox (id, state) VALUES ('00000000-0000-4000-8000-000000000001', 'forbidden')"); err == nil {
		t.Fatal("queue unexpectedly inserts sync-dispatch outbox state")
	}
	if _, err := queue.Exec(ctx, "DELETE FROM public.sync_dispatch_outbox"); err == nil {
		t.Fatal("queue unexpectedly deletes sync-dispatch outbox state")
	}
	if _, err := queue.Exec(ctx, "UPDATE public.sync_dispatch_transport_routes SET generation = generation + 1"); err == nil {
		t.Fatal("queue unexpectedly mutates sync-dispatch routes")
	}
	if _, err := admin.Exec(ctx, "GRANT DELETE ON TABLE public.sync_dispatch_outbox TO "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("queue sync-outbox DELETE authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE DELETE ON TABLE public.sync_dispatch_outbox FROM "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT UPDATE ON TABLE public.sync_dispatch_transport_routes TO "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("queue route UPDATE authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE UPDATE ON TABLE public.sync_dispatch_transport_routes FROM "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(
		ctx,
		"GRANT UPDATE (generation) ON TABLE public.sync_dispatch_transport_routes TO "+runtimeAuthorizationQueueRole,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Exec(
		ctx,
		"UPDATE public.sync_dispatch_transport_routes SET generation = generation + 1",
	); err != nil {
		t.Fatalf("column-level route UPDATE grant was not effective: %v", err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("queue column-level route UPDATE authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(
		ctx,
		"REVOKE UPDATE (generation) ON TABLE public.sync_dispatch_transport_routes FROM "+runtimeAuthorizationQueueRole,
	); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); err != nil {
		t.Fatalf("queue authorization did not recover after revoking sync-dispatch excess grants: %v", err)
	}
	if _, err := admin.Exec(ctx, "GRANT UPDATE ON TABLE public.worker_job_outbox TO "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("domain outbox-mutation authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE UPDATE ON TABLE public.worker_job_outbox FROM "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT TRUNCATE ON TABLE public.runtime_semantic_probe TO "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("domain destructive authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE TRUNCATE ON TABLE public.runtime_semantic_probe FROM "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); err != nil {
		t.Fatalf("domain authorization did not recover after revoking excess grants: %v", err)
	}
	if _, err := admin.Exec(ctx, "GRANT MAINTAIN ON TABLE public.runtime_semantic_probe TO "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("domain MAINTAIN authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE MAINTAIN ON TABLE public.runtime_semantic_probe FROM "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
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
	if _, err := admin.Exec(ctx, "GRANT MAINTAIN ON TABLE public.worker_job_outbox TO "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("queue MAINTAIN authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE MAINTAIN ON TABLE public.worker_job_outbox FROM "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT SELECT ON TABLE public.alembic_version TO "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("queue Alembic authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE SELECT ON TABLE public.alembic_version FROM "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); err != nil {
		t.Fatalf("queue authorization did not recover after revoking Alembic access: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE FUNCTION public.runtime_queue_escape() RETURNS integer LANGUAGE sql AS 'SELECT 1'"); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("queue public-function authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE EXECUTE ON FUNCTION public.runtime_queue_escape() FROM PUBLIC"); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); err != nil {
		t.Fatalf("queue authorization did not recover after revoking public function execute: %v", err)
	}
	if _, err := admin.Exec(ctx, "GRANT EXECUTE ON FUNCTION public.runtime_queue_escape() TO "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("domain public-function authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE EXECUTE ON FUNCTION public.runtime_queue_escape() FROM "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); err != nil {
		t.Fatalf("domain authorization did not recover after revoking public function execute: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE ROLE runtime_authorization_semantic_capability NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT SELECT ON TABLE public.runtime_semantic_probe TO runtime_authorization_semantic_capability"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "ALTER ROLE "+runtimeAuthorizationQueueRole+" NOINHERIT"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT runtime_authorization_semantic_capability TO "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("queue NOINHERIT capability membership error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE runtime_authorization_semantic_capability FROM "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); err != nil {
		t.Fatalf("queue authorization did not recover after revoking capability membership: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE ROLE runtime_authorization_elevated NOLOGIN CREATEROLE"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT runtime_authorization_elevated TO "+runtimeAuthorizationQueueRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("queue inherited elevated role error = %v, want ErrUnavailable", err)
	}

	if _, err := admin.Exec(ctx, "GRANT UPDATE ON TABLE public.alembic_version TO "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("domain Alembic authorization error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE UPDATE ON TABLE public.alembic_version FROM "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); err != nil {
		t.Fatalf("domain authorization did not recover after revoking Alembic access: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE ROLE runtime_authorization_river_capability NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT USAGE ON SCHEMA river TO runtime_authorization_river_capability"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT SELECT ON TABLE river.river_job TO runtime_authorization_river_capability"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "ALTER ROLE "+runtimeAuthorizationDomainRole+" NOINHERIT"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT runtime_authorization_river_capability TO "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("domain NOINHERIT capability membership error = %v, want ErrUnavailable", err)
	}
	if _, err := admin.Exec(ctx, "REVOKE runtime_authorization_river_capability FROM "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); err != nil {
		t.Fatalf("domain authorization did not recover after revoking capability membership: %v", err)
	}

	if _, err := admin.Exec(ctx, "GRANT USAGE ON SCHEMA river TO "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("overprivileged domain authorization error = %v, want ErrUnavailable", err)
	}
}
