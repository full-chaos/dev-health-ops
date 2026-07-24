//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	queueAuthorizationFenceRole = "queue_authorization_fence"
	queueAuthorizationFencePass = "queue_authorization_fence_password"
)

func TestQueueAuthorizationRequiresExactCompletionFenceGrants(t *testing.T) {
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
		"CREATE TABLE public.worker_job_outbox (id uuid PRIMARY KEY)",
		"CREATE TABLE public.worker_job_completion_fences (completion_key text PRIMARY KEY)",
		"CREATE TABLE public.sync_dispatch_outbox (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_dispatch_transport_routes (kind text PRIMARY KEY)",
		"CREATE SCHEMA river",
		"CREATE TABLE river.river_job (id bigserial PRIMARY KEY)",
		"CREATE FUNCTION river.queue_authorization_probe() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
		"REVOKE ALL ON FUNCTION river.queue_authorization_probe() FROM PUBLIC",
		"CREATE ROLE " + queueAuthorizationFenceRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + queueAuthorizationFencePass + "'",
		"GRANT CONNECT ON DATABASE worker_test TO " + queueAuthorizationFenceRole,
		"GRANT USAGE ON SCHEMA public, river TO " + queueAuthorizationFenceRole,
		"GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox, public.worker_job_completion_fences TO " + queueAuthorizationFenceRole,
		"GRANT SELECT, UPDATE ON TABLE public.sync_dispatch_outbox TO " + queueAuthorizationFenceRole,
		"GRANT SELECT ON TABLE public.sync_dispatch_transport_routes TO " + queueAuthorizationFenceRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA river TO " + queueAuthorizationFenceRole,
		"GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA river TO " + queueAuthorizationFenceRole,
		"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA river TO " + queueAuthorizationFenceRole,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	queue := openPostgresPool(t, ctx, postgresRoleURI(t, instance.URI, queueAuthorizationFenceRole, queueAuthorizationFencePass))
	defer queue.Close()
	assertQueueFenceAuthorized(t, ctx, queue)

	for _, test := range []struct {
		name    string
		grant   string
		revoke  string
		missing bool
	}{
		{name: "missing select", revoke: "REVOKE SELECT ON TABLE public.worker_job_completion_fences FROM " + queueAuthorizationFenceRole, grant: "GRANT SELECT ON TABLE public.worker_job_completion_fences TO " + queueAuthorizationFenceRole, missing: true},
		{name: "missing update", revoke: "REVOKE UPDATE ON TABLE public.worker_job_completion_fences FROM " + queueAuthorizationFenceRole, grant: "GRANT UPDATE ON TABLE public.worker_job_completion_fences TO " + queueAuthorizationFenceRole, missing: true},
		{name: "missing delete", revoke: "REVOKE DELETE ON TABLE public.worker_job_completion_fences FROM " + queueAuthorizationFenceRole, grant: "GRANT DELETE ON TABLE public.worker_job_completion_fences TO " + queueAuthorizationFenceRole, missing: true},
		{name: "insert", grant: "GRANT INSERT ON TABLE public.worker_job_completion_fences TO " + queueAuthorizationFenceRole, revoke: "REVOKE INSERT ON TABLE public.worker_job_completion_fences FROM " + queueAuthorizationFenceRole},
		{name: "column insert", grant: "GRANT INSERT (completion_key) ON TABLE public.worker_job_completion_fences TO " + queueAuthorizationFenceRole, revoke: "REVOKE INSERT (completion_key) ON TABLE public.worker_job_completion_fences FROM " + queueAuthorizationFenceRole},
		{name: "truncate", grant: "GRANT TRUNCATE ON TABLE public.worker_job_completion_fences TO " + queueAuthorizationFenceRole, revoke: "REVOKE TRUNCATE ON TABLE public.worker_job_completion_fences FROM " + queueAuthorizationFenceRole},
		{name: "references", grant: "GRANT REFERENCES ON TABLE public.worker_job_completion_fences TO " + queueAuthorizationFenceRole, revoke: "REVOKE REFERENCES ON TABLE public.worker_job_completion_fences FROM " + queueAuthorizationFenceRole},
		{name: "column references", grant: "GRANT REFERENCES (completion_key) ON TABLE public.worker_job_completion_fences TO " + queueAuthorizationFenceRole, revoke: "REVOKE REFERENCES (completion_key) ON TABLE public.worker_job_completion_fences FROM " + queueAuthorizationFenceRole},
		{name: "trigger", grant: "GRANT TRIGGER ON TABLE public.worker_job_completion_fences TO " + queueAuthorizationFenceRole, revoke: "REVOKE TRIGGER ON TABLE public.worker_job_completion_fences FROM " + queueAuthorizationFenceRole},
		{name: "maintain", grant: "GRANT MAINTAIN ON TABLE public.worker_job_completion_fences TO " + queueAuthorizationFenceRole, revoke: "REVOKE MAINTAIN ON TABLE public.worker_job_completion_fences FROM " + queueAuthorizationFenceRole},
	} {
		t.Run(test.name, func(t *testing.T) {
			statement := test.grant
			if test.missing {
				statement = test.revoke
			}
			if _, err := admin.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
			if err := postgresstore.CheckQueueAuthorization(ctx, queue, queueAuthorizationFenceRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
				t.Fatalf("queue authorization with %s fence grant error = %v, want ErrUnavailable", test.name, err)
			}
			statement = test.revoke
			if test.missing {
				statement = test.grant
			}
			if _, err := admin.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
			assertQueueFenceAuthorized(t, ctx, queue)
		})
	}
}

func assertQueueFenceAuthorized(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if err := postgresstore.CheckQueueAuthorization(ctx, pool, queueAuthorizationFenceRole, "river"); err != nil {
		t.Fatalf("queue authorization failed: %v", err)
	}
}
