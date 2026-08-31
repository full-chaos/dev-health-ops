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

const queueAuthorizationFencePass = "queue_authorization_fence_password"

func TestQueueAuthorizationRequiresExactCompletionFenceGrants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closePostgresInstance(t, instance)

	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	queueAuthorizationFenceRole, err := containers.RoleName("queue_authorization_fence", instance)
	if err != nil {
		t.Fatal(err)
	}

	admin := openPostgresPool(t, ctx, instance.URI)
	defer admin.Close()
	for _, statement := range []string{
		"REVOKE TEMPORARY ON DATABASE " + dbName + " FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE TABLE public.worker_job_outbox (id uuid PRIMARY KEY)",
		"CREATE TABLE public.worker_job_delivery_abandonments (dedupe_key text PRIMARY KEY)",
		"CREATE TABLE public.worker_job_completion_fences (completion_key text PRIMARY KEY)",
		"CREATE TABLE public.sync_dispatch_outbox (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_dispatch_transport_routes (kind text PRIMARY KEY)",
		"CREATE TABLE public.sync_runs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_run_units (id uuid PRIMARY KEY)",
		// CHAOS-3997 read-only additions. The posture query REQUIRES these to
		// exist and to carry SELECT, so a fixture without them fails closed --
		// which is how this test caught the addition in the first place.
		"CREATE TABLE public.daily_metrics_runs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_partitions (id uuid PRIMARY KEY)",
		"CREATE TABLE public.work_graph_execution_requests (id uuid PRIMARY KEY)",
		"CREATE SCHEMA river",
		"CREATE TABLE river.river_job (id bigserial PRIMARY KEY)",
		"CREATE FUNCTION river.queue_authorization_probe() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
		"REVOKE ALL ON FUNCTION river.queue_authorization_probe() FROM PUBLIC",
		"CREATE ROLE " + queueAuthorizationFenceRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + queueAuthorizationFencePass + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + queueAuthorizationFenceRole,
		"GRANT USAGE ON SCHEMA public, river TO " + queueAuthorizationFenceRole,
		"GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox, public.worker_job_completion_fences TO " + queueAuthorizationFenceRole,
		"GRANT SELECT, INSERT ON TABLE public.worker_job_delivery_abandonments TO " + queueAuthorizationFenceRole,
		"GRANT SELECT, UPDATE ON TABLE public.sync_dispatch_outbox TO " + queueAuthorizationFenceRole,
		"GRANT SELECT ON TABLE public.sync_dispatch_transport_routes TO " + queueAuthorizationFenceRole,
		"GRANT SELECT ON TABLE public.sync_runs TO " + queueAuthorizationFenceRole,
		"GRANT SELECT ON TABLE public.sync_run_units TO " + queueAuthorizationFenceRole,
		"GRANT SELECT ON TABLE public.daily_metrics_runs TO " + queueAuthorizationFenceRole,
		"GRANT SELECT ON TABLE public.daily_metrics_partitions TO " + queueAuthorizationFenceRole,
		"GRANT SELECT ON TABLE public.work_graph_execution_requests TO " + queueAuthorizationFenceRole,
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
	assertQueueFenceAuthorized(t, ctx, queue, queueAuthorizationFenceRole)

	for _, test := range []struct {
		name    string
		grant   string
		revoke  string
		missing bool
	}{
		{name: "missing abandonment select", revoke: "REVOKE SELECT ON TABLE public.worker_job_delivery_abandonments FROM " + queueAuthorizationFenceRole, grant: "GRANT SELECT ON TABLE public.worker_job_delivery_abandonments TO " + queueAuthorizationFenceRole, missing: true},
		{name: "missing abandonment insert", revoke: "REVOKE INSERT ON TABLE public.worker_job_delivery_abandonments FROM " + queueAuthorizationFenceRole, grant: "GRANT INSERT ON TABLE public.worker_job_delivery_abandonments TO " + queueAuthorizationFenceRole, missing: true},
		{name: "abandonment update", grant: "GRANT UPDATE ON TABLE public.worker_job_delivery_abandonments TO " + queueAuthorizationFenceRole, revoke: "REVOKE UPDATE ON TABLE public.worker_job_delivery_abandonments FROM " + queueAuthorizationFenceRole},
		{name: "abandonment delete", grant: "GRANT DELETE ON TABLE public.worker_job_delivery_abandonments TO " + queueAuthorizationFenceRole, revoke: "REVOKE DELETE ON TABLE public.worker_job_delivery_abandonments FROM " + queueAuthorizationFenceRole},
		{name: "abandonment truncate", grant: "GRANT TRUNCATE ON TABLE public.worker_job_delivery_abandonments TO " + queueAuthorizationFenceRole, revoke: "REVOKE TRUNCATE ON TABLE public.worker_job_delivery_abandonments FROM " + queueAuthorizationFenceRole},
		// CHAOS-3997: the strand sweep's read-only domain access. SELECT is
		// required on each, and anything beyond SELECT is refused -- the repair
		// mutates the outbox and River schema, never a domain row.
		{name: "missing daily runs select", revoke: "REVOKE SELECT ON TABLE public.daily_metrics_runs FROM " + queueAuthorizationFenceRole, grant: "GRANT SELECT ON TABLE public.daily_metrics_runs TO " + queueAuthorizationFenceRole, missing: true},
		{name: "missing daily partitions select", revoke: "REVOKE SELECT ON TABLE public.daily_metrics_partitions FROM " + queueAuthorizationFenceRole, grant: "GRANT SELECT ON TABLE public.daily_metrics_partitions TO " + queueAuthorizationFenceRole, missing: true},
		{name: "missing work graph requests select", revoke: "REVOKE SELECT ON TABLE public.work_graph_execution_requests FROM " + queueAuthorizationFenceRole, grant: "GRANT SELECT ON TABLE public.work_graph_execution_requests TO " + queueAuthorizationFenceRole, missing: true},
		{name: "daily runs update", grant: "GRANT UPDATE ON TABLE public.daily_metrics_runs TO " + queueAuthorizationFenceRole, revoke: "REVOKE UPDATE ON TABLE public.daily_metrics_runs FROM " + queueAuthorizationFenceRole},
		{name: "daily partitions update", grant: "GRANT UPDATE ON TABLE public.daily_metrics_partitions TO " + queueAuthorizationFenceRole, revoke: "REVOKE UPDATE ON TABLE public.daily_metrics_partitions FROM " + queueAuthorizationFenceRole},
		{name: "daily partitions delete", grant: "GRANT DELETE ON TABLE public.daily_metrics_partitions TO " + queueAuthorizationFenceRole, revoke: "REVOKE DELETE ON TABLE public.daily_metrics_partitions FROM " + queueAuthorizationFenceRole},
		{name: "work graph requests update", grant: "GRANT UPDATE ON TABLE public.work_graph_execution_requests TO " + queueAuthorizationFenceRole, revoke: "REVOKE UPDATE ON TABLE public.work_graph_execution_requests FROM " + queueAuthorizationFenceRole},
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
				t.Fatalf("queue authorization with %s error = %v, want ErrUnavailable", test.name, err)
			}
			statement = test.revoke
			if test.missing {
				statement = test.grant
			}
			if _, err := admin.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
			assertQueueFenceAuthorized(t, ctx, queue, queueAuthorizationFenceRole)
		})
	}

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
		{name: "missing sync runs select", revoke: "REVOKE SELECT ON TABLE public.sync_runs FROM " + queueAuthorizationFenceRole, grant: "GRANT SELECT ON TABLE public.sync_runs TO " + queueAuthorizationFenceRole, missing: true},
		{name: "sync runs update", grant: "GRANT UPDATE ON TABLE public.sync_runs TO " + queueAuthorizationFenceRole, revoke: "REVOKE UPDATE ON TABLE public.sync_runs FROM " + queueAuthorizationFenceRole},
		{name: "missing sync run units select", revoke: "REVOKE SELECT ON TABLE public.sync_run_units FROM " + queueAuthorizationFenceRole, grant: "GRANT SELECT ON TABLE public.sync_run_units TO " + queueAuthorizationFenceRole, missing: true},
		{name: "sync run units update", grant: "GRANT UPDATE ON TABLE public.sync_run_units TO " + queueAuthorizationFenceRole, revoke: "REVOKE UPDATE ON TABLE public.sync_run_units FROM " + queueAuthorizationFenceRole},
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
			assertQueueFenceAuthorized(t, ctx, queue, queueAuthorizationFenceRole)
		})
	}
}

func assertQueueFenceAuthorized(t *testing.T, ctx context.Context, pool *pgxpool.Pool, role string) {
	t.Helper()
	if err := postgresstore.CheckQueueAuthorization(ctx, pool, role, "river"); err != nil {
		t.Fatalf("queue authorization failed: %v", err)
	}
}
