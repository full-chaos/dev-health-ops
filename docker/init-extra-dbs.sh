#!/bin/bash
# Creates additional databases and local-only Go worker runtime roles on the
# shared postgres instance.
# Mounted into /docker-entrypoint-initdb.d/ — runs once on first init.
set -eu

river_domain_role="${RIVER_DOMAIN_DATABASE_ROLE:-devhealth_domain}"
river_queue_role="${RIVER_QUEUE_DATABASE_ROLE:-devhealth_queue}"
river_coordinator_role="${RIVER_COORDINATOR_DATABASE_ROLE:-devhealth_coordinator}"
river_domain_password="${RIVER_DOMAIN_DATABASE_PASSWORD:-devhealth_domain}"
river_queue_password="${RIVER_QUEUE_DATABASE_PASSWORD:-devhealth_queue}"
river_coordinator_password="${RIVER_COORDINATOR_DATABASE_PASSWORD:-devhealth_coordinator}"

psql \
  -v ON_ERROR_STOP=1 \
  -v domain_role="$river_domain_role" \
  -v queue_role="$river_queue_role" \
  -v coordinator_role="$river_coordinator_role" \
  -v domain_password="$river_domain_password" \
  -v queue_password="$river_queue_password" \
  -v coordinator_password="$river_coordinator_password" \
  -v app_database="$POSTGRES_DB" \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE bugsink;

    SELECT format(
      'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
      :'domain_role', :'domain_password'
    )
      WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'domain_role')
    \gexec
    SELECT format(
      'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
      :'queue_role', :'queue_password'
    )
      WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'queue_role')
    \gexec

    SELECT format(
      'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %L',
      :'coordinator_role', :'coordinator_password'
    )
      WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'coordinator_role')
    \gexec

    GRANT CONNECT ON DATABASE :"app_database" TO :"domain_role";
    GRANT CONNECT ON DATABASE :"app_database" TO :"queue_role";
    GRANT CONNECT ON DATABASE :"app_database" TO :"coordinator_role";
    REVOKE TEMPORARY ON DATABASE :"app_database" FROM PUBLIC, :"domain_role", :"queue_role", :"coordinator_role";
    -- The coordinator role deliberately receives NO per-table grants here.
    -- Its table privileges are owned entirely by the pinned River migration
    -- (internal/storage/river/migrate.go coordinatorGrantStatements), which
    -- derives them from postgres.CoordinatorPosture() — the same declaration
    -- readiness asserts against. A second list here would be a second thing to
    -- drift. This script only has to make the role exist and be able to
    -- connect before that migration runs.
    GRANT USAGE ON SCHEMA public TO :"coordinator_role";
    REVOKE CREATE ON SCHEMA public FROM :"coordinator_role";
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
      required.table_name, :'domain_role'
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
      required.table_name, :'domain_role'
    )
      FROM (VALUES ('integration_sources'),('integration_datasets'),('sync_runs'),('sync_run_units')) AS required(table_name)
      WHERE to_regclass(format('public.%I', required.table_name)) IS NOT NULL
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
EOSQL
