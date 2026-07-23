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

-- The domain runtime owns semantic DML, never DDL or migration metadata.
-- The one-shot migration repeats these grants after every Alembic upgrade so
-- newly added application tables fail closed until their grants are current.
GRANT USAGE ON SCHEMA public TO :"domain_role";
REVOKE CREATE ON SCHEMA public FROM :"domain_role";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO :"domain_role";
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO :"domain_role";
SELECT format(
         'REVOKE ALL PRIVILEGES ON TABLE public.alembic_version FROM %I',
         :'domain_role'
       )
 WHERE to_regclass('public.alembic_version') IS NOT NULL
\gexec
SELECT format(
         'REVOKE ALL PRIVILEGES ON TABLE public.worker_job_outbox FROM %I',
         :'domain_role'
       )
 WHERE to_regclass('public.worker_job_outbox') IS NOT NULL
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
SELECT format(
         'GRANT SELECT, UPDATE, DELETE ON TABLE public.worker_job_outbox TO %I',
         :'queue_role'
       )
 WHERE to_regclass('public.worker_job_outbox') IS NOT NULL
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
