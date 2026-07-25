\set ON_ERROR_STOP on

-- Idempotent one-time provisioning for the two unprivileged Go runtime roles.
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
\if :{?domain_password}
\else
  \prompt -1 'Domain runtime role password: ' domain_password
\endif
\if :{?queue_password}
\else
  \prompt -1 'Queue-control runtime role password: ' queue_password
\endif

SELECT (:'domain_role' = :'queue_role') AS roles_match
\gset
\if :roles_match
  \echo 'domain_role and queue_role must be distinct'
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

GRANT CONNECT ON DATABASE :"app_database" TO :"domain_role";
GRANT CONNECT ON DATABASE :"app_database" TO :"queue_role";
REVOKE TEMPORARY ON DATABASE :"app_database" FROM PUBLIC, :"domain_role", :"queue_role";

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
           ('integration_sources'),
           ('integration_datasets'),
           ('integration_credentials'),
           ('sync_runs'),
           ('worker_job_routes'),
           ('sync_dispatch_transport_routes')
       ) AS required(table_name)
 WHERE to_regclass(format('public.%I', required.table_name)) IS NOT NULL
\gexec
SELECT format(
         'GRANT SELECT, UPDATE ON TABLE public.sync_run_units TO %I',
         :'domain_role'
       )
 WHERE to_regclass('public.sync_run_units') IS NOT NULL
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

-- The queue role may atomically relay the generic outbox and transition the
-- sync-dispatch outbox while checking its read-only route fence. It
-- never receives INSERT or general semantic-table/sequence privileges.
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
