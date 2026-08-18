\set ON_ERROR_STOP on

-- Idempotent one-time provisioning for the three unprivileged Go runtime roles
-- (domain, queue-control, coordinator) of the CHAOS-3033 Option B split.
-- Run this with the database owner's/admin connection before the pinned River
-- migration. Passwords are prompted without echo unless supplied as psql
-- variables by an external secret-aware automation.
\if :{?domain_role}
\else
  \set domain_role devhealth_domain
\endif
\if :{?queue_role}
\else
  \set queue_role devhealth_queue
\endif
\if :{?coordinator_role}
\else
  \set coordinator_role devhealth_coordinator
\endif
\if :{?domain_password}
\else
  \prompt -1 'Domain runtime role password: ' domain_password
\endif
\if :{?queue_password}
\else
  \prompt -1 'Queue-control runtime role password: ' queue_password
\endif
\if :{?coordinator_password}
\else
  \prompt -1 'Coordinator runtime role password: ' coordinator_password
\endif

SELECT (
         :'domain_role' = :'queue_role'
         OR :'domain_role' = :'coordinator_role'
         OR :'queue_role' = :'coordinator_role'
       ) AS roles_match
\gset
\if :roles_match
  \echo 'domain_role, queue_role, and coordinator_role must be distinct'
  \quit 2
\endif

SELECT current_database() AS app_database
\gset

SELECT format(
         'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
         :'domain_role',
         :'domain_password'
       )
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'domain_role')
\gexec

SELECT format(
         'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
         :'queue_role',
         :'queue_password'
       )
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'queue_role')
\gexec

SELECT format(
         'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
         :'coordinator_role',
         :'coordinator_password'
       )
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'coordinator_role')
\gexec

GRANT CONNECT ON DATABASE :"app_database" TO :"domain_role";
GRANT CONNECT ON DATABASE :"app_database" TO :"queue_role";
GRANT CONNECT ON DATABASE :"app_database" TO :"coordinator_role";
REVOKE TEMPORARY ON DATABASE :"app_database" FROM PUBLIC, :"domain_role", :"queue_role", :"coordinator_role";

-- The coordinator runtime role of the CHAOS-3033 Option B split. It gets no
-- per-table grants here on purpose: its table privileges are owned by the
-- pinned River migration (internal/storage/river/migrate.go
-- coordinatorGrantStatements), derived from postgres.CoordinatorPosture(), the
-- same declaration CheckCoordinatorAuthorization asserts at readiness. This
-- script only has to make the role exist and be connectable BEFORE that
-- migration runs -- the migration's preflight rejects a coordinator role that
-- is missing or not a least-privilege login.
GRANT USAGE ON SCHEMA public TO :"coordinator_role";
REVOKE CREATE ON SCHEMA public FROM :"coordinator_role";

-- The domain runtime receives only the semantic access exercised by the
-- executable provider-unit canary and the reconciler's observe-only paths.
-- Route mutation remains an operator concern and no current domain path uses
-- a PostgreSQL sequence.
GRANT USAGE ON SCHEMA public TO :"domain_role";
REVOKE CREATE ON SCHEMA public FROM :"domain_role";
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM :"domain_role";
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM :"domain_role";
SELECT format(
         'REVOKE ALL PRIVILEGES ON TABLE public.alembic_version FROM %I',
         :'domain_role'
       )
 WHERE to_regclass('public.alembic_version') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT ON TABLE public.%I TO %I',
         required.table_name,
         :'domain_role'
       )
  FROM (
         VALUES
           ('integrations'),
           ('integration_credentials'),
           ('sync_dispatch_transport_routes')
       ) AS required(table_name)
 WHERE to_regclass(format('public.%I', required.table_name)) IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, INSERT, UPDATE ON TABLE public.%I TO %I',
         required.table_name,
         :'domain_role'
       )
  FROM (VALUES ('integration_sources'),('integration_datasets'),('sync_runs'),('sync_run_units')) AS required(table_name)
 WHERE to_regclass(format('public.%I', required.table_name)) IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.worker_concurrency_leases TO %I',
         :'domain_role'
       )
 WHERE to_regclass('public.worker_concurrency_leases') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.worker_instances TO %I',
         :'domain_role'
       )
 WHERE to_regclass('public.worker_instances') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, INSERT, DELETE ON TABLE public.sync_run_unit_effect_snapshots TO %I',
         :'domain_role'
       )
 WHERE to_regclass('public.sync_run_unit_effect_snapshots') IS NOT NULL
\gexec
-- Chunked provider persistence (migration 0102). Both tables are written and
-- reclaimed by the domain role -- checkpoints advance per chunk, chunks are
-- inserted, superseded, and deleted on completion -- so both need the full
-- SELECT/INSERT/UPDATE/DELETE set domainPosture() declares.
--
-- This script runs BEFORE the pinned migration, so these to_regclass guards
-- skip on a first provision and runtimeGrantStatements in
-- internal/storage/river/migrate.go is what actually grants them once the
-- tables exist. They are listed here for the same reason
-- sync_run_unit_effect_snapshots is: the two lists are maintained in parallel,
-- and a re-provision of an already-migrated database must not silently narrow
-- the posture the migration established.
SELECT format(
         'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.sync_run_unit_chunk_checkpoints TO %I',
         :'domain_role'
       )
 WHERE to_regclass('public.sync_run_unit_chunk_checkpoints') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.sync_run_unit_effect_chunks TO %I',
         :'domain_role'
       )
 WHERE to_regclass('public.sync_run_unit_effect_chunks') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_watermarks TO %I',
         :'domain_role'
       )
 WHERE to_regclass('public.sync_watermarks') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, INSERT, UPDATE ON TABLE public.sync_dispatch_outbox TO %I',
         :'domain_role'
       )
 WHERE to_regclass('public.sync_dispatch_outbox') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, INSERT ON TABLE public.worker_job_outbox TO %I',
         :'domain_role'
       )
 WHERE to_regclass('public.worker_job_outbox') IS NOT NULL
\gexec

-- The queue role may atomically relay the generic outbox, append minimal
-- delivery-abandonment evidence during terminal retention, and transition the
-- sync-dispatch outbox while checking its read-only route and active-run
-- fences. It never receives INSERT or general semantic-table/sequence
-- privileges.
GRANT USAGE ON SCHEMA public TO :"queue_role";
REVOKE CREATE ON SCHEMA public FROM :"queue_role";
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM :"queue_role";
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM :"queue_role";
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC, :"domain_role", :"queue_role";
SELECT format(
         'GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox TO %I',
         :'queue_role'
       )
 WHERE to_regclass('public.worker_job_outbox') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, INSERT ON TABLE public.worker_job_delivery_abandonments TO %I',
         :'queue_role'
       )
 WHERE to_regclass('public.worker_job_delivery_abandonments') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_completion_fences TO %I',
         :'queue_role'
       )
 WHERE to_regclass('public.worker_job_completion_fences') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, UPDATE ON TABLE public.sync_dispatch_outbox TO %I',
         :'queue_role'
       )
 WHERE to_regclass('public.sync_dispatch_outbox') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT ON TABLE public.sync_dispatch_transport_routes TO %I',
         :'queue_role'
       )
 WHERE to_regclass('public.sync_dispatch_transport_routes') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT ON TABLE public.sync_runs TO %I',
         :'queue_role'
       )
 WHERE to_regclass('public.sync_runs') IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT ON TABLE public.sync_run_units TO %I',
         :'queue_role'
       )
 WHERE to_regclass('public.sync_run_units') IS NOT NULL
\gexec
