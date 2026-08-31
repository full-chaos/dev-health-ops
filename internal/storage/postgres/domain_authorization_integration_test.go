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

const domainAuthorizationPass = "domain_authorized_password"

func TestDomainAuthorizationRequiresExactCanaryAndReconcilerPrivileges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closePostgresInstance(t, instance)

	// CREATE ROLE is cluster-scoped, not database-scoped -- a scratch
	// database does not isolate it (CHAOS-4661). Deriving the role name from
	// this call's own database identity is what makes two successive runs,
	// and two concurrent lanes, collision-free. Likewise "worker_test" as a
	// literal DATABASE target only resolves on the container path; dbName is
	// the database this call actually created.
	roleSuffix, err := containers.RoleSuffix(instance)
	if err != nil {
		t.Fatal(err)
	}
	dbName, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	authorizedDomainRole := "domain_authorized_" + roleSuffix

	admin := openPostgresPool(t, ctx, instance.URI)
	defer admin.Close()
	for _, statement := range []string{
		"REVOKE TEMPORARY ON DATABASE " + dbName + " FROM PUBLIC",
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
		"CREATE TABLE public.provider_oauth_credentials (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_runs (id bigint PRIMARY KEY)",
		// worker_job_routes is coordinator-exclusive under the Option B split
		// (role-partition manifest, removed in e23ede618; see git history at
		// eda2d6b91) — created here only so the "column-level operator route
		// mutation" case below has a real relation to test against; it is
		// deliberately never granted to the domain role.
		"CREATE TABLE public.worker_job_routes (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_dispatch_transport_routes (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_run_units (id bigint PRIMARY KEY, state text)",
		// CHAOS-4114: the executed-proof ledger joined domainPosture's manifest
		// (migration 0109). Every domain GRANT is wrapped in a to_regclass guard,
		// so a venue that never CREATEs it silently skips the grant and
		// CheckDomainAuthorization then fails closed with an opaque readiness
		// error naming neither the table nor this venue.
		`CREATE TABLE public.sync_executed_proof_ledger (
			provider text NOT NULL,
			dataset_key text NOT NULL,
			attempted_at timestamptz NOT NULL,
			proven_at timestamptz,
			PRIMARY KEY (provider, dataset_key),
			CONSTRAINT ck_sync_executed_proof_ledger_provider_normalized
				CHECK (provider = lower(provider) AND btrim(provider) <> ''),
			CONSTRAINT ck_sync_executed_proof_ledger_dataset_normalized
				CHECK (dataset_key = lower(dataset_key) AND btrim(dataset_key) <> '')
		)`,
		"CREATE TABLE public.sync_run_unit_effect_snapshots (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.sync_run_unit_chunk_checkpoints (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_run_unit_effect_chunks (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_watermarks (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.sync_dispatch_outbox (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.worker_job_outbox (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.sync_configurations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.scheduled_jobs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.scheduled_report_occurrences (occurrence_id text PRIMARY KEY)",
		// CHAOS-4209: the CHAOS-4175 native reference-discovery and
		// finalize_sync_run ports run on pools.Domain and write these, so
		// domainPosture() now requires them on the domain side. The coordinator
		// keeps its own, unchanged declarations for the first two.
		"CREATE TABLE public.sync_run_reference_discoveries (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_run_post_dispatches (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_compute_checkpoints (id uuid PRIMARY KEY)",
		"CREATE TABLE public.job_runs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.backfill_jobs (id uuid PRIMARY KEY)",
		"CREATE TABLE public.sync_coverage_projections (id uuid PRIMARY KEY)",
		"CREATE TABLE public.organizations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.remaining_metric_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.remaining_metric_partitions (id bigint PRIMARY KEY)",
		"CREATE TABLE public.work_graph_execution_requests (id bigint PRIMARY KEY)",
		"CREATE TABLE public.work_graph_execution_ledger (id bigint PRIMARY KEY)",
		"CREATE TABLE public.worker_job_completion_fences (completion_key text PRIMARY KEY, completed_at timestamptz NOT NULL DEFAULT now())",
		// CHAOS-3033 Option B manifest additions — domain-exclusive tables
		// (role-partition manifest, removed in e23ede618; see git history at eda2d6b91).
		"CREATE TABLE public.billing_notifications (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_partitions (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_finalize_redrive_events (id uuid PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_partition_recompute_events (id uuid PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_batch_payloads (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_batches (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_recompute_jobs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_rejections (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_sources (id bigint PRIMARY KEY)",
		"CREATE TABLE public.feature_flags (id bigint PRIMARY KEY)",
		"CREATE TABLE public.org_feature_overrides (id bigint PRIMARY KEY)",
		"CREATE TABLE public.org_licenses (id bigint PRIMARY KEY)",
		// CHAOS-4175 family 3's DispatchGuard total-cap read dual-granted this
		// table; see the doc comment above domainPosture().
		"CREATE TABLE public.tier_limits (id bigint PRIMARY KEY)",
		"CREATE TABLE public.dev_conversations (id uuid PRIMARY KEY)",
		"CREATE TABLE public.dev_conversation_tombstones (id uuid PRIMARY KEY)",
		"CREATE TABLE public.provider_rate_limit_observations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.report_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.saved_reports (id bigint PRIMARY KEY)",
		"CREATE TABLE public.webhook_deliveries (id bigint PRIMARY KEY)",
		"CREATE TABLE public.worker_job_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.worker_concurrency_leases (id bigint PRIMARY KEY)",
		"CREATE TABLE public.worker_instances (instance_id uuid PRIMARY KEY)",
		"CREATE TABLE public.unrelated_semantic_table (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.alembic_version (version_num varchar(32) PRIMARY KEY)",
		"CREATE SEQUENCE public.unrelated_sequence",
		"CREATE ROLE " + authorizedDomainRole + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + domainAuthorizationPass + "'",
		"GRANT CONNECT ON DATABASE " + dbName + " TO " + authorizedDomainRole,
		"GRANT USAGE ON SCHEMA public TO " + authorizedDomainRole,
		"GRANT SELECT ON TABLE public.integrations, public.integration_credentials, public.sync_dispatch_transport_routes, public.feature_flags, public.org_feature_overrides, public.scheduled_report_occurrences, public.organizations, public.billing_notifications, public.external_ingest_sources, public.org_licenses, public.tier_limits, public.webhook_deliveries TO " + authorizedDomainRole,
		"GRANT SELECT, UPDATE ON TABLE public.scheduled_jobs TO " + authorizedDomainRole,
		"GRANT SELECT, UPDATE ON TABLE public.provider_oauth_credentials TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT, UPDATE ON TABLE public.integration_sources, public.integration_datasets, public.sync_runs, public.sync_run_units TO " + authorizedDomainRole,
		"GRANT SELECT, UPDATE ON TABLE public.report_runs, public.saved_reports TO " + authorizedDomainRole,
		// CHAOS-4209: observeTerminalSyncRun terminalizes the backfill and
		// job_run rows a finished sync run owns. UPDATE only -- the domain
		// worker never opens either kind of row.
		"GRANT SELECT, UPDATE ON TABLE public.backfill_jobs, public.job_runs TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_coverage_projections TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_executed_proof_ledger, public.sync_watermarks, public.sync_dispatch_outbox, public.remaining_metric_runs, public.remaining_metric_partitions, public.work_graph_execution_requests, public.work_graph_execution_ledger, public.daily_metrics_partitions, public.daily_metrics_runs, public.daily_metrics_finalize_redrive_events, public.worker_job_runs TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT ON TABLE public.daily_metrics_partition_recompute_events TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.worker_concurrency_leases TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.worker_instances TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT ON TABLE public.worker_job_outbox, public.external_ingest_recompute_jobs, public.external_ingest_rejections TO " + authorizedDomainRole,
		"GRANT SELECT, DELETE ON TABLE public.external_ingest_batch_payloads TO " + authorizedDomainRole,
		// The domain role needs DELETE but explicitly NOT UPDATE here:
		// PostgreSQL treats FOR UPDATE/FOR SHARE as UPDATE-class, and the
		// snapshot read-back must never be able to take a row lock.
		"GRANT SELECT, INSERT, DELETE ON TABLE public.sync_run_unit_effect_snapshots TO " + authorizedDomainRole,
		// Chunked provider persistence (migration 0102). domainPosture() requires
		// the full SELECT/INSERT/UPDATE/DELETE set on both, and
		// internal/storage/river/migrate.go (applied by go-river-migrate) now
		// grants it. These fixtures had the CREATE TABLE but never the GRANT, so
		// CheckDomainAuthorization failed on a posture entry no deployment
		// satisfied either.
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.sync_run_unit_chunk_checkpoints TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.sync_run_unit_effect_chunks TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT ON TABLE public.dev_conversation_tombstones TO " + authorizedDomainRole,
		"GRANT SELECT, UPDATE, DELETE ON TABLE public.dev_conversations, public.external_ingest_batches, public.provider_rate_limit_observations TO " + authorizedDomainRole,
		// CHAOS-4209. sync_configurations gains UPDATE (stampCanonicalSyncConfig
		// writes last_sync_*), the discovery ledger gains INSERT+UPDATE, and the
		// post-dispatch and compute-checkpoint ledgers gain INSERT. Each set is
		// exactly what domainPosture() declares -- a venue granting more or less
		// than the manifest fails CheckDomainAuthorization in both directions.
		"GRANT SELECT, UPDATE ON TABLE public.sync_configurations TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_run_reference_discoveries TO " + authorizedDomainRole,
		"GRANT SELECT, INSERT ON TABLE public.sync_run_post_dispatches, public.sync_compute_checkpoints TO " + authorizedDomainRole,
		"GRANT SELECT (completion_key), INSERT (completion_key) ON TABLE public.worker_job_completion_fences TO " + authorizedDomainRole,
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

	assertDomainAuthorized(t, ctx, domain, authorizedDomainRole)
	if _, err := domain.Exec(ctx, "SELECT id FROM public.integrations"); err != nil {
		t.Fatalf("domain SELECT-only inventory access failed: %v", err)
	}
	for name, statement := range map[string]string{
		"integration source":  "INSERT INTO public.integration_sources (id) VALUES (1)",
		"integration dataset": "INSERT INTO public.integration_datasets (id) VALUES (1)",
		"sync run":            "INSERT INTO public.sync_runs (id) VALUES (1)",
		"sync run unit":       "INSERT INTO public.sync_run_units (id, state) VALUES (1, 'planned')",
	} {
		if _, err := domain.Exec(ctx, statement); err != nil {
			t.Fatalf("domain materializer %s INSERT failed: %v", name, err)
		}
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
			name:   "missing integration-source INSERT",
			grant:  "REVOKE INSERT ON TABLE public.integration_sources FROM " + authorizedDomainRole,
			revoke: "GRANT INSERT ON TABLE public.integration_sources TO " + authorizedDomainRole,
		},
		{
			name:   "missing integration-dataset INSERT",
			grant:  "REVOKE INSERT ON TABLE public.integration_datasets FROM " + authorizedDomainRole,
			revoke: "GRANT INSERT ON TABLE public.integration_datasets TO " + authorizedDomainRole,
		},
		{
			name:   "missing sync-run INSERT",
			grant:  "REVOKE INSERT ON TABLE public.sync_runs FROM " + authorizedDomainRole,
			revoke: "GRANT INSERT ON TABLE public.sync_runs TO " + authorizedDomainRole,
		},
		{
			name:   "missing sync-run-unit INSERT",
			grant:  "REVOKE INSERT ON TABLE public.sync_run_units FROM " + authorizedDomainRole,
			revoke: "GRANT INSERT ON TABLE public.sync_run_units TO " + authorizedDomainRole,
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
			// The mirror case: external_ingest_batches is one of the three
			// AllowDelete tables, so losing the required DELETE must fail
			// closed just as gaining an undeclared one does above.
			name:   "missing required DELETE",
			grant:  "REVOKE DELETE ON TABLE public.external_ingest_batches FROM " + authorizedDomainRole,
			revoke: "GRANT DELETE ON TABLE public.external_ingest_batches TO " + authorizedDomainRole,
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
			assertDomainUnauthorized(t, ctx, domain, authorizedDomainRole)
			if _, err := admin.Exec(ctx, test.revoke); err != nil {
				t.Fatal(err)
			}
			assertDomainAuthorized(t, ctx, domain, authorizedDomainRole)
		})
	}
}

func assertDomainAuthorized(t *testing.T, ctx context.Context, pool *pgxpool.Pool, role string) {
	t.Helper()
	if err := postgresstore.CheckDomainAuthorization(ctx, pool, role, "river"); err != nil {
		t.Fatalf("domain readiness failed: %v", err)
	}
}

func assertDomainUnauthorized(t *testing.T, ctx context.Context, pool *pgxpool.Pool, role string) {
	t.Helper()
	if err := postgresstore.CheckDomainAuthorization(ctx, pool, role, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
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
