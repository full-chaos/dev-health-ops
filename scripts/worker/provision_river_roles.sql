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

-- CHAOS-4261: this script is bootstrap-only. It used to REVOKE ALL and then
-- re-GRANT a hand-maintained per-table whitelist for the domain and queue
-- roles -- a second copy of the grant manifest that silently drifted behind
-- postgres.domainPosture()/coordinatorPosture() (internal/storage/postgres/
-- domain_authorization.go) as tables were added over time. Any compose
-- service that reached this script WITHOUT then running go-river-migrate
-- (pgbouncer startup, an operator `docker compose run go-workerctl`, a
-- deploy pass that stopped after pass 1) wiped the previous, correct grants
-- down to that stale subset -- CHAOS-4261's prod incident.
--
-- The ONLY authority for per-table/sequence privileges on all three runtime
-- roles is now internal/storage/river/migrate.go's runtimeGrantStatements /
-- coordinatorGrantStatements, applied by go-river-migrate every time it runs
-- (idempotent: REVOKE ALL then re-GRANT the full declared posture in one
-- transaction). This script only has to make the three logins exist,
-- connectable, and unable to CREATEDB/CREATEROLE/self-grant TEMPORARY --
-- everything a login needs before that migration can even preflight it --
-- and it can therefore never again REVOKE a grant migrate already applied,
-- no matter how many times or in what order it is re-run.
GRANT USAGE ON SCHEMA public TO :"domain_role";
REVOKE CREATE ON SCHEMA public FROM :"domain_role";
GRANT USAGE ON SCHEMA public TO :"queue_role";
REVOKE CREATE ON SCHEMA public FROM :"queue_role";
