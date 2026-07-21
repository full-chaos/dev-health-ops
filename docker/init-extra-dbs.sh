#!/bin/bash
# Creates additional databases and local-only Go worker runtime roles on the
# shared postgres instance.
# Mounted into /docker-entrypoint-initdb.d/ — runs once on first init.
set -eu

river_domain_role="${RIVER_DOMAIN_DATABASE_ROLE:-devhealth_domain}"
river_queue_role="${RIVER_QUEUE_DATABASE_ROLE:-devhealth_queue}"
river_domain_password="${RIVER_DOMAIN_DATABASE_PASSWORD:-devhealth_domain}"
river_queue_password="${RIVER_QUEUE_DATABASE_PASSWORD:-devhealth_queue}"

psql \
  -v ON_ERROR_STOP=1 \
  -v domain_role="$river_domain_role" \
  -v queue_role="$river_queue_role" \
  -v domain_password="$river_domain_password" \
  -v queue_password="$river_queue_password" \
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

    GRANT CONNECT ON DATABASE :"app_database" TO :"domain_role";
    GRANT CONNECT ON DATABASE :"app_database" TO :"queue_role";
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
EOSQL
