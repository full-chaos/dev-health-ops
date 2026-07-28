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
		"REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE TABLE public.runtime_semantic_probe (id bigserial PRIMARY KEY, value text NOT NULL)",
		"CREATE TABLE public.integrations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.integration_sources (id bigint PRIMARY KEY)",
		"CREATE TABLE public.integration_datasets (id bigint PRIMARY KEY)",
		"CREATE TABLE public.integration_credentials (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_runs (id bigint PRIMARY KEY)",
		// worker_job_routes is coordinator-exclusive under the Option B split
		// (role-partition manifest, removed in e23ede618; see git history at
		// eda2d6b91) — created but never granted to the domain role here.
		"CREATE TABLE public.worker_job_routes (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_run_units (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.sync_watermarks (id bigint PRIMARY KEY, state text)",
		"CREATE TABLE public.worker_job_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.worker_job_completion_fences (completion_key text PRIMARY KEY)",
		"CREATE TABLE public.sync_configurations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.organizations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.remaining_metric_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.remaining_metric_partitions (id bigint PRIMARY KEY)",
		"CREATE TABLE public.work_graph_execution_requests (id bigint PRIMARY KEY)",
		"CREATE TABLE public.work_graph_execution_ledger (id bigint PRIMARY KEY)",
		"CREATE TABLE public.sync_dispatch_outbox (id uuid PRIMARY KEY, state text NOT NULL)",
		"CREATE TABLE public.sync_dispatch_transport_routes (kind text PRIMARY KEY, generation bigint NOT NULL)",
		// CHAOS-3033 Option B manifest additions — domain-exclusive tables
		// (role-partition manifest, removed in e23ede618; see git history at eda2d6b91).
		"CREATE TABLE public.billing_notifications (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_partitions (id bigint PRIMARY KEY)",
		"CREATE TABLE public.daily_metrics_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_batch_payloads (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_batches (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_recompute_jobs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_rejections (id bigint PRIMARY KEY)",
		"CREATE TABLE public.external_ingest_sources (id bigint PRIMARY KEY)",
		"CREATE TABLE public.feature_flags (id bigint PRIMARY KEY)",
		"CREATE TABLE public.org_feature_overrides (id bigint PRIMARY KEY)",
		"CREATE TABLE public.org_licenses (id bigint PRIMARY KEY)",
		"CREATE TABLE public.dev_conversations (id uuid PRIMARY KEY)",
		"CREATE TABLE public.dev_conversation_tombstones (id uuid PRIMARY KEY)",
		"CREATE TABLE public.provider_rate_limit_observations (id bigint PRIMARY KEY)",
		"CREATE TABLE public.report_runs (id bigint PRIMARY KEY)",
		"CREATE TABLE public.saved_reports (id bigint PRIMARY KEY)",
		"CREATE TABLE public.webhook_deliveries (id bigint PRIMARY KEY)",
		"CREATE TABLE public.worker_job_runs (id bigint PRIMARY KEY)",
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
		"GRANT SELECT ON TABLE public.integrations, public.integration_sources, public.integration_datasets, public.integration_credentials, public.sync_dispatch_transport_routes, public.sync_configurations, public.organizations, public.billing_notifications, public.external_ingest_sources, public.feature_flags, public.org_feature_overrides, public.org_licenses, public.webhook_deliveries TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT, UPDATE ON TABLE public.sync_runs, public.sync_run_units, public.report_runs, public.saved_reports TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_watermarks, public.sync_dispatch_outbox, public.remaining_metric_runs, public.remaining_metric_partitions, public.work_graph_execution_requests, public.work_graph_execution_ledger, public.daily_metrics_partitions, public.daily_metrics_runs, public.worker_job_runs TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT, INSERT ON TABLE public.worker_job_outbox, public.external_ingest_recompute_jobs, public.external_ingest_rejections TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT, DELETE ON TABLE public.external_ingest_batch_payloads TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT, INSERT ON TABLE public.dev_conversation_tombstones TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT, UPDATE, DELETE ON TABLE public.dev_conversations, public.external_ingest_batches, public.provider_rate_limit_observations TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT (completion_key), INSERT (completion_key) ON TABLE public.worker_job_completion_fences TO " + runtimeAuthorizationDomainRole,
		"GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox TO " + runtimeAuthorizationQueueRole,
		"GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_completion_fences TO " + runtimeAuthorizationQueueRole,
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
	if _, err := admin.Exec(ctx, "GRANT TEMPORARY ON DATABASE worker_test TO PUBLIC"); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		name  string
		check func() error
	}{
		{name: "domain", check: func() error {
			return postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river")
		}},
		{name: "queue", check: func() error {
			return postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river")
		}},
	} {
		if err := check.check(); !errors.Is(err, postgresstore.ErrUnavailable) {
			t.Fatalf("%s authorization with ambient PUBLIC TEMPORARY error = %v, want ErrUnavailable", check.name, err)
		}
	}
	if _, err := admin.Exec(ctx, "REVOKE TEMPORARY ON DATABASE worker_test FROM PUBLIC"); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); err != nil {
		t.Fatalf("domain authorization did not recover after revoking ambient PUBLIC TEMPORARY: %v", err)
	}
	if err := postgresstore.CheckQueueAuthorization(ctx, queue, runtimeAuthorizationQueueRole, "river"); err != nil {
		t.Fatalf("queue authorization did not recover after revoking ambient PUBLIC TEMPORARY: %v", err)
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
	// The completion-fence grant is column-scoped (SELECT/INSERT on
	// completion_key only, never completed_at). has_table_privilege cannot
	// see a column-level grant, so a naive reuse of the has_table_privilege
	// pattern here would silently accept a table-wide grant it never checked
	// for. Confirm the dedicated column-scoped check actually catches it.
	if _, err := admin.Exec(ctx, "GRANT SELECT ON TABLE public.worker_job_completion_fences TO "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); !errors.Is(err, postgresstore.ErrUnavailable) {
		t.Fatalf("domain completion-fence table-wide-leakage authorization error = %v, want ErrUnavailable", err)
	}
	// REVOKE SELECT ON TABLE (whole-table form) revokes SELECT for this
	// grantee on this relation entirely — confirmed empirically — including
	// the pre-existing column-level SELECT (completion_key) grant, not just
	// the table-wide ACL entry just added. Restore the column-scoped grant
	// before asserting recovery, or this legitimately fails closed.
	if _, err := admin.Exec(ctx, "REVOKE SELECT ON TABLE public.worker_job_completion_fences FROM "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "GRANT SELECT (completion_key) ON TABLE public.worker_job_completion_fences TO "+runtimeAuthorizationDomainRole); err != nil {
		t.Fatal(err)
	}
	if err := postgresstore.CheckDomainAuthorization(ctx, domain, runtimeAuthorizationDomainRole, "river"); err != nil {
		t.Fatalf("domain authorization did not recover after revoking completion-fence table-wide leakage: %v", err)
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
